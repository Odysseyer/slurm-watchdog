"""Slurm job monitoring core logic."""

import re
import subprocess
from datetime import datetime
from typing import Optional

from slurm_watchdog.config import Config
from slurm_watchdog.database import Database
from slurm_watchdog.models import Event, EventType, Job, JobState


class SlurmError(Exception):
    """Error communicating with Slurm."""

    pass


class SlurmParser:
    """Parse Slurm command outputs."""

    @staticmethod
    def parse_squeue(output: str) -> list[dict]:
        """Parse squeue output in format=value mode.

        Args:
            output: Raw squeue output.

        Returns:
            List of job dictionaries.
        """
        jobs = []
        for line in output.strip().split("\n"):
            if not line.strip():
                continue

            # Parse format like: JobId=12345|UserId=user(1000)|...
            job_data = {}
            for part in line.split("|"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    job_data[key] = value if value else None

            if job_data.get("JobId"):
                jobs.append(job_data)

        return jobs

    @staticmethod
    def parse_sacct(output: str) -> list[dict]:
        """Parse sacct output in format=value mode.

        Args:
            output: Raw sacct output.

        Returns:
            List of job dictionaries.
        """
        return SlurmParser.parse_squeue(output)  # Same format

    @staticmethod
    def parse_time(time_str: Optional[str]) -> Optional[datetime]:
        """Parse Slurm timestamp to datetime.

        Args:
            time_str: Timestamp string from Slurm.

        Returns:
            datetime or None.
        """
        if not time_str or time_str in ("Unknown", "N/A", ""):
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
        "ExitCode",
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
                check=True,
                timeout=30,
            )
            return result.stdout
        except subprocess.TimeoutExpired as e:
            raise SlurmError(f"Command timed out: {' '.join(cmd)}") from e
        except subprocess.CalledProcessError as e:
            raise SlurmError(f"Command failed: {e.stderr}") from e
        except FileNotFoundError as e:
            raise SlurmError(f"Command not found: {cmd[0]}") from e

    def get_queue_jobs(self) -> list[dict]:
        """Get all jobs from squeue (running and pending).

        Returns:
            List of job dictionaries.
        """
        user = self.config.watchdog.user or None
        user_filter = f"--user={user}" if user else "--user=$USER"

        format_str = "|".join(self.SQUEUE_FIELDS)
        cmd = [
            "squeue",
            user_filter,
            "--noheader",
            "--format=%" + format_str,
        ]

        output = self._run_command(cmd)
        return SlurmParser.parse_squeue(output)

    def get_completed_job(self, job_id: str) -> Optional[dict]:
        """Get a completed job's details from sacct.

        Args:
            job_id: Job ID to look up.

        Returns:
            Job dictionary or None.
        """
        format_str = "|".join(self.SACCT_FIELDS)
        cmd = [
            "sacct",
            "--jobs",
            job_id,
            "--format=" + format_str,
            "--noheader",
            "--parsable2",
            "--starttime",
            "now-7days",  # Limit search range
        ]

        output = self._run_command(cmd)
        jobs = SlurmParser.parse_sacct(output)

        # Return the main job (not batch steps)
        for job in jobs:
            if "." not in str(job.get("JobId", "")):
                return job

        return jobs[0] if jobs else None

    def get_user_jobs_from_sacct(self, job_ids: list[str]) -> list[dict]:
        """Get multiple jobs from sacct.

        Args:
            job_ids: List of job IDs.

        Returns:
            List of job dictionaries.
        """
        if not job_ids:
            return []

        format_str = "|".join(self.SACCT_FIELDS)
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
        jobs = SlurmParser.parse_sacct(output)

        # Filter to main jobs only (not batch steps)
        return [j for j in jobs if "." not in str(j.get("JobId", ""))]


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
            job_name = job_data.get("Name", job_data.get("JobName", ""))
            if not re.search(name_filter, job_name):
                return False

        # Check partition filter
        partition_filter = self.config.watchdog.partition_filter
        if partition_filter:
            partition = job_data.get("Partition", "")
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
            print(f"Warning: Failed to query squeue: {e}")
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

        # 4. Query sacct for disappeared jobs
        if disappeared_jobs:
            disappeared_ids = [j.job_id for j in disappeared_jobs]

            try:
                completed_jobs_data = self.client.get_user_jobs_from_sacct(disappeared_ids)
                completed_map = {str(j.get("JobId")): j for j in completed_jobs_data}
            except SlurmError as e:
                print(f"Warning: Failed to query sacct: {e}")
                completed_map = {}

            for old_job in disappeared_jobs:
                job_id = old_job.job_id

                if job_id in completed_map:
                    # Update with sacct data
                    job = self._sacct_to_job(completed_map[job_id])
                    job.created_at = old_job.created_at
                    self.db.upsert_job(job)
                else:
                    # Mark as unknown/lost
                    job = old_job.model_copy(update={
                        "state": JobState.UNKNOWN,
                        "last_seen": now,
                        "updated_at": now,
                    })
                    self.db.upsert_job(job)

                # Create terminal state events
                events = self._create_state_change_events(job, old_job.state)
                new_events.extend(events)
                updated_jobs.append(job)

        return updated_jobs, new_events

    def _create_state_change_events(self, job: Job, old_state: JobState) -> list[Event]:
        """Create events for state changes.

        Args:
            job: Job with new state.
            old_state: Previous state.

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
