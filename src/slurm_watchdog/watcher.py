"""Slurm job monitoring core logic."""

import logging
import re
import subprocess
from datetime import datetime
from os import environ

from slurm_watchdog.config import Config
from slurm_watchdog.database import Database
from slurm_watchdog.models import Event, EventType, Job, JobState

logger = logging.getLogger(__name__)


class SlurmError(Exception):
    """Error communicating with Slurm."""

    pass


class SlurmParser:
    """Parse Slurm command outputs."""

    @staticmethod
    def _parse_delimited_line(line: str, field_names: list[str]) -> dict:
        """Parse a delimited row into field/value dict."""
        values = line.split("|")
        if len(values) < len(field_names):
            values.extend([""] * (len(field_names) - len(values)))

        return {
            field: (value if value else None)
            for field, value in zip(field_names, values, strict=False)
        }

    @staticmethod
    def _parse_key_value_line(line: str, delimiter: str = "|") -> dict:
        """Parse key=value style output line."""
        job_data = {}
        for part in line.split(delimiter):
            if "=" in part:
                key, value = part.split("=", 1)
                job_data[key] = value if value else None
        return job_data

    @staticmethod
    def parse_squeue(output: str, field_names: list[str] | None = None) -> list[dict]:
        """Parse squeue output.

        Args:
            output: Raw squeue output.
            field_names: Field order for delimited output.

        Returns:
            List of job dictionaries.
        """
        jobs = []
        for line in output.strip().split("\n"):
            line = line.strip()
            if not line:
                continue

            if "=" in line and "|" in line:
                job_data = SlurmParser._parse_key_value_line(line)
            elif field_names:
                job_data = SlurmParser._parse_delimited_line(line, field_names)
            else:
                continue

            if job_data.get("JobId"):
                jobs.append(job_data)

        return jobs

    @staticmethod
    def parse_sacct(output: str, field_names: list[str]) -> list[dict]:
        """Parse sacct output.

        Args:
            output: Raw sacct output.
            field_names: Field order for delimited output.

        Returns:
            List of job dictionaries.
        """
        return SlurmParser.parse_squeue(output, field_names)

    @staticmethod
    def parse_scontrol(output: str) -> list[dict]:
        """Parse one-line scontrol output (space-delimited key=value pairs)."""
        jobs = []
        for line in output.strip().split("\n"):
            line = line.strip()
            if not line:
                continue

            job_data = {}
            for token in line.split():
                if "=" in token:
                    key, value = token.split("=", 1)
                    job_data[key] = value if value else None

            if job_data.get("JobId"):
                jobs.append(job_data)

        return jobs

    @staticmethod
    def parse_time(time_str: str | None) -> datetime | None:
        """Parse Slurm timestamp to datetime.

        Args:
            time_str: Timestamp string from Slurm.

        Returns:
            datetime or None.
        """
        if not time_str or time_str in ("Unknown", "N/A", "", "None", "(null)"):
            return None

        # Try common formats
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(time_str, fmt)
            except ValueError:
                continue

        return None


class SlurmClient:
    """Client for querying Slurm."""

    # Fields to request from squeue
    SQUEUE_FIELDS = [
        "JobId",
        "UserId",
        "Name",
        "Partition",
        "State",
        "SubmitTime",
        "StartTime",
        "Reason",
    ]

    # squeue format placeholders corresponding to SQUEUE_FIELDS above.
    SQUEUE_FORMAT = [
        "%i",  # JobId
        "%u",  # UserId
        "%j",  # Name
        "%P",  # Partition
        "%T",  # State
        "%V",  # SubmitTime
        "%S",  # StartTime
        "%r",  # Reason
    ]

    # Fields to request from sacct
    SACCT_FIELDS = [
        "JobId",
        "User",
        "JobName",
        "Partition",
        "State",
        "ExitCode",
        "SubmitTime",
        "Start",
        "End",
        "Elapsed",
        "MaxRSS",
        "MaxVMSize",
        "CPUTime",
        "Reason",
        "StdOut",
    ]

    def __init__(self, config: Config):
        """Initialize Slurm client.

        Args:
            config: Configuration.
        """
        self.config = config

    def _run_command(self, cmd: list[str]) -> str:
        """Run a Slurm command and return output.

        Args:
            cmd: Command and arguments.

        Returns:
            Command output.

        Raises:
            SlurmError: If command fails.
        """
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            stderr = (result.stderr or "").strip()
            if result.returncode != 0:
                raise SlurmError(
                    f"Command failed ({result.returncode}): {' '.join(cmd)}\n{stderr}"
                )

            # Some Slurm commands may return code 0 but still report hard errors to stderr.
            if stderr and re.search(r"(error|failed|fatal)", stderr, re.IGNORECASE):
                raise SlurmError(
                    f"Command reported error: {' '.join(cmd)}\n{stderr}"
                )

            return result.stdout
        except subprocess.TimeoutExpired as e:
            raise SlurmError(f"Command timed out: {' '.join(cmd)}") from e
        except FileNotFoundError as e:
            raise SlurmError(f"Command not found: {cmd[0]}") from e

    def get_queue_jobs(self) -> list[dict]:
        """Get all jobs from squeue (running and pending).

        Returns:
            List of job dictionaries.
        """
        user = (
            self.config.watchdog.user
            or environ.get("USER")
            or environ.get("LOGNAME")
        )
        format_str = "|".join(self.SQUEUE_FORMAT)
        cmd = [
            "squeue",
            "--noheader",
            "--format=" + format_str,
        ]
        if user:
            cmd.extend(["--user", user])

        output = self._run_command(cmd)
        return SlurmParser.parse_squeue(output, self.SQUEUE_FIELDS)

    def get_jobs_from_sacct(self, job_ids: list[str]) -> list[dict]:
        """Get multiple jobs from sacct.

        Args:
            job_ids: List of job IDs.

        Returns:
            List of job dictionaries.
        """
        if not job_ids:
            return []

        format_str = ",".join(self.SACCT_FIELDS)
        cmd = [
            "sacct",
            "--jobs",
            ",".join(job_ids),
            "--format=" + format_str,
            "--noheader",
            "--parsable2",
            "--starttime",
            "now-7days",
        ]

        output = self._run_command(cmd)
        jobs = SlurmParser.parse_sacct(output, self.SACCT_FIELDS)

        # Filter to main jobs only (not batch steps)
        return [j for j in jobs if "." not in str(j.get("JobId", ""))]

    def get_job_from_scontrol(self, job_id: str) -> dict | None:
        """Get job details via scontrol, useful when slurmdbd/sacct is unavailable."""
        cmd = ["scontrol", "show", "job", job_id, "-o"]
        output = self._run_command(cmd)
        jobs = SlurmParser.parse_scontrol(output)
        for job in jobs:
            if str(job.get("JobId")) == str(job_id):
                return job
        return jobs[0] if jobs else None


class JobWatcher:
    """Watches Slurm jobs and tracks state changes."""

    def __init__(self, config: Config, db: Database):
        """Initialize job watcher.

        Args:
            config: Configuration.
            db: Database instance.
        """
        self.config = config
        self.db = db
        self.client = SlurmClient(config)
        self.disappeared_grace_seconds = max(
            0, self.config.watchdog.disappeared_grace_seconds
        )

    def _job_matches_filters(self, job_data: dict) -> bool:
        """Check if a job matches configured filters.

        Args:
            job_data: Job data from Slurm.

        Returns:
            True if job matches filters.
        """
        # Check name filter
        name_filter = self.config.watchdog.job_name_filter
        if name_filter:
            job_name = job_data.get("Name", job_data.get("JobName", "")) or ""
            if not re.search(name_filter, job_name):
                return False

        # Check partition filter
        partition_filter = self.config.watchdog.partition_filter
        if partition_filter:
            partition = job_data.get("Partition", "") or ""
            if not re.search(partition_filter, partition):
                return False

        return True

    def _squeue_to_job(self, job_data: dict) -> Job:
        """Convert squeue output to Job model.

        Args:
            job_data: Parsed squeue data.

        Returns:
            Job model.
        """
        # Extract user from UserId field (format: user(uid))
        user_id = job_data.get("UserId", "")
        user = user_id.split("(")[0] if user_id else self.config.watchdog.user

        return Job(
            job_id=str(job_data.get("JobId")),
            user=user,
            name=job_data.get("Name"),
            partition=job_data.get("Partition"),
            state=JobState.from_slurm_state(job_data.get("State", "UNKNOWN")),
            exit_code=job_data.get("ExitCode"),
            submit_time=SlurmParser.parse_time(job_data.get("SubmitTime")),
            start_time=SlurmParser.parse_time(job_data.get("StartTime")),
            reason=job_data.get("Reason"),
            last_seen=datetime.now(),
            updated_at=datetime.now(),
        )

    def _enrich_from_scontrol(self, job: Job) -> None:
        """Enrich a running job with WorkDir and StdOut from scontrol.

        Only called for NEW jobs to capture working directory info.

        Args:
            job: Job to enrich (modified in-place).
        """
        try:
            scontrol_data = self.client.get_job_from_scontrol(job.job_id)
            if scontrol_data:
                job.work_dir = scontrol_data.get("WorkDir")
                stdout = scontrol_data.get("StdOut")
                if stdout and stdout != "StdOut":
                    # Resolve %j placeholder in StdOut path
                    stdout = stdout.replace("%j", job.job_id)
                    stdout = stdout.replace("%x", job.name or "")
                    job.output_file = stdout
                elif job.work_dir:
                    # Default: slurm-{job_id}.out in work_dir
                    job.output_file = f"{job.work_dir}/slurm-{job.job_id}.out"
        except SlurmError:
            logger.debug("Could not enrich job %s via scontrol", job.job_id)

    def _scontrol_to_job(self, job_data: dict) -> Job:
        """Convert scontrol output to Job model."""
        user_id = job_data.get("UserId", "")
        user = user_id.split("(")[0] if user_id else self.config.watchdog.user

        return Job(
            job_id=str(job_data.get("JobId")),
            user=user,
            name=job_data.get("JobName"),
            partition=job_data.get("Partition"),
            state=JobState.from_slurm_state(job_data.get("JobState", "UNKNOWN")),
            exit_code=job_data.get("ExitCode"),
            submit_time=SlurmParser.parse_time(job_data.get("SubmitTime")),
            start_time=SlurmParser.parse_time(job_data.get("StartTime")),
            end_time=SlurmParser.parse_time(job_data.get("EndTime")),
            reason=job_data.get("Reason"),
            elapsed_time=job_data.get("RunTime"),
            output_file=job_data.get("StdOut"),
            last_seen=datetime.now(),
            updated_at=datetime.now(),
        )

    def _sacct_to_job(self, job_data: dict) -> Job:
        """Convert sacct output to Job model.

        Args:
            job_data: Parsed sacct data.

        Returns:
            Job model.
        """
        return Job(
            job_id=str(job_data.get("JobId")),
            user=job_data.get("User", self.config.watchdog.user),
            name=job_data.get("JobName"),
            partition=job_data.get("Partition"),
            state=JobState.from_slurm_state(job_data.get("State", "UNKNOWN")),
            exit_code=job_data.get("ExitCode"),
            submit_time=SlurmParser.parse_time(job_data.get("SubmitTime")),
            start_time=SlurmParser.parse_time(job_data.get("Start")),
            end_time=SlurmParser.parse_time(job_data.get("End")),
            reason=job_data.get("Reason"),
            elapsed_time=job_data.get("Elapsed"),
            max_rss=job_data.get("MaxRSS"),
            max_vmsize=job_data.get("MaxVMSize"),
            cpu_time=job_data.get("CPUTime"),
            output_file=job_data.get("StdOut"),
            last_seen=datetime.now(),
            updated_at=datetime.now(),
        )

    def scan(self) -> tuple[list[Job], list[Event]]:
        """Scan Slurm for job updates.

        Returns:
            Tuple of (updated_jobs, new_events).
        """
        now = datetime.now()
        updated_jobs = []
        new_events = []

        # 1. Get current jobs from squeue
        try:
            queue_jobs = self.client.get_queue_jobs()
        except SlurmError as e:
            # Log error but don't crash
            logger.warning("Failed to query squeue: %s", e)
            return [], []

        queue_job_ids = set()

        # 2. Process queue jobs
        for job_data in queue_jobs:
            if not self._job_matches_filters(job_data):
                continue

            job = self._squeue_to_job(job_data)
            queue_job_ids.add(job.job_id)

            # Check for state changes
            existing_job = self.db.get_job(job.job_id)
            if existing_job:
                # Update existing job
                old_state = existing_job.state
                job.created_at = existing_job.created_at
                self.db.upsert_job(job)

                # Check for state change events
                if old_state != job.state:
                    events = self._create_state_change_events(job, old_state)
                    new_events.extend(events)
            else:
                # New job
                self._enrich_from_scontrol(job)
                self.db.upsert_job(job)
                # Create start event for running jobs
                if job.state == JobState.RUNNING:
                    event = Event(
                        job_id=job.job_id,
                        event_type=EventType.JOB_STARTED,
                        event_time=now,
                    )
                    self.db.create_event(event)
                    new_events.append(event)

            updated_jobs.append(job)

        # 3. Find jobs that disappeared from queue (completed/failed)
        tracked_jobs = self.db.get_all_active_jobs()
        disappeared_jobs = [j for j in tracked_jobs if j.job_id not in queue_job_ids]

        # 4. Query details for disappeared jobs after grace period
        if disappeared_jobs:
            eligible_jobs = []
            for job in disappeared_jobs:
                age_seconds = (now - job.last_seen).total_seconds()
                if age_seconds >= self.disappeared_grace_seconds:
                    eligible_jobs.append(job)

            if eligible_jobs:
                disappeared_ids = [j.job_id for j in eligible_jobs]

                try:
                    completed_jobs_data = self.client.get_jobs_from_sacct(disappeared_ids)
                    completed_map = {str(j.get("JobId")): j for j in completed_jobs_data}
                except SlurmError as e:
                    logger.warning("Failed to query sacct, fallback to scontrol: %s", e)
                    completed_map = {}

                for old_job in eligible_jobs:
                    job_id = old_job.job_id
                    job = None

                    if job_id in completed_map:
                        # Preferred source: sacct (has richer terminal metrics).
                        job = self._sacct_to_job(completed_map[job_id])
                    else:
                        try:
                            scontrol_data = self.client.get_job_from_scontrol(job_id)
                        except SlurmError:
                            scontrol_data = None

                        if scontrol_data:
                            job = self._scontrol_to_job(scontrol_data)
                        else:
                            # Could not resolve terminal status from any source.
                            # Try to determine state from output file analysis.
                            resolved_state = self._resolve_state_from_output(old_job)
                            job = old_job.model_copy(update={
                                "state": resolved_state,
                                "last_seen": now,
                                "updated_at": now,
                            })

                    job.created_at = old_job.created_at
                    self.db.upsert_job(job)

                    # Create events for state transitions when mapped.
                    events = self._create_state_change_events(job)
                    new_events.extend(events)
                    updated_jobs.append(job)

        return updated_jobs, new_events

    def _resolve_state_from_output(self, job: Job) -> JobState:
        """Try to determine final state by analyzing the output file.

        When sacct and scontrol are both unavailable, fall back to checking
        the job's output file for error/convergence patterns.

        Args:
            job: Job that disappeared from the queue.

        Returns:
            Best-guess JobState (COMPLETED, FAILED, or UNKNOWN).
        """
        from slurm_watchdog.analyzer import OutputAnalyzer

        output_path = job.output_file
        if not output_path and job.work_dir:
            output_path = f"{job.work_dir}/slurm-{job.job_id}.out"

        if not output_path:
            return JobState.UNKNOWN

        try:
            analyzer = OutputAnalyzer(self.config)
            analysis = analyzer.analyze(output_path)
            if analysis.has_errors:
                return JobState.FAILED
            if analysis.converged:
                return JobState.COMPLETED
        except Exception as exc:
            logger.debug("Output analysis failed for job %s: %s", job.job_id, exc)

        return JobState.UNKNOWN

    def _create_state_change_events(self, job: Job) -> list[Event]:
        """Create events for state changes.

        Args:
            job: Job with new state.

        Returns:
            List of new events.
        """
        events = []
        now = datetime.now()

        # Map states to event types
        state_events = {
            JobState.RUNNING: EventType.JOB_STARTED,
            JobState.COMPLETED: EventType.JOB_COMPLETED,
            JobState.FAILED: EventType.JOB_FAILED,
            JobState.CANCELLED: EventType.JOB_CANCELLED,
            JobState.TIMEOUT: EventType.JOB_TIMEOUT,
            JobState.PREEMPTED: EventType.JOB_PREEMPTED,
            JobState.NODE_FAIL: EventType.JOB_NODE_FAIL,
            JobState.BOOT_FAIL: EventType.JOB_BOOT_FAIL,
            JobState.OUT_OF_MEMORY: EventType.JOB_OUT_OF_MEMORY,
            JobState.UNKNOWN: EventType.JOB_LOST,
        }

        event_type = state_events.get(job.state)
        if event_type and not self.db.event_exists(job.job_id, event_type):
            event = Event(
                job_id=job.job_id,
                event_type=event_type,
                event_time=now,
            )
            self.db.create_event(event)
            events.append(event)

        return events

    def get_poll_interval(self) -> int:
        """Get adaptive poll interval based on current job state.

        Returns:
            Poll interval in seconds.
        """
        active_jobs = self.db.count_jobs(state=JobState.RUNNING)
        active_jobs += self.db.count_jobs(state=JobState.PENDING)

        return self.config.get_poll_interval(active_jobs > 0)

    def cleanup_old_jobs(self, days: int = 30) -> int:
        """Remove old completed jobs from database.

        Args:
            days: Age threshold in days.

        Returns:
            Number of deleted jobs.
        """
        return self.db.delete_old_jobs(days)
