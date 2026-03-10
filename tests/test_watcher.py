"""Tests for the watcher module."""

import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from slurm_watchdog.config import Config
from slurm_watchdog.database import Database
from slurm_watchdog.models import Event, EventType, Job, JobState
from slurm_watchdog.watcher import JobWatcher, SlurmError, SlurmParser


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = Database(db_path)
    yield db

    db.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def test_config():
    """Create a test configuration."""
    return Config()


class TestSlurmParser:
    """Tests for SlurmParser."""

    def test_parse_squeue(self):
        """Test parsing squeue output."""
        output = (
            "JobId=12345|UserId=testuser(1000)|Name=my-job|Partition=cpu|State=RUNNING|"
            "SubmitTime=2024-01-01T10:00:00|StartTime=2024-01-01T10:05:00|Reason=None|"
            "ExitCode=0:0\n"
            "JobId=12346|UserId=testuser(1000)|Name=pending-job|Partition=gpu|State=PENDING|"
            "SubmitTime=2024-01-01T11:00:00|StartTime=None|Reason=Resources|ExitCode=0:0"
        )

        jobs = SlurmParser.parse_squeue(output)

        assert len(jobs) == 2
        assert jobs[0]["JobId"] == "12345"
        assert jobs[0]["Name"] == "my-job"
        assert jobs[0]["State"] == "RUNNING"
        assert jobs[1]["JobId"] == "12346"
        assert jobs[1]["State"] == "PENDING"

    def test_parse_squeue_empty(self):
        """Test parsing empty squeue output."""
        jobs = SlurmParser.parse_squeue("")
        assert jobs == []

        jobs = SlurmParser.parse_squeue("\n\n")
        assert jobs == []

    def test_parse_squeue_delimited(self):
        """Test parsing delimited squeue output with explicit field order."""
        output = "12345|testuser|my-job|cpu|RUNNING|2024-01-01T10:00:00|2024-01-01T10:05:00|None"
        fields = [
            "JobId",
            "UserId",
            "Name",
            "Partition",
            "State",
            "SubmitTime",
            "StartTime",
            "Reason",
        ]

        jobs = SlurmParser.parse_squeue(output, fields)
        assert len(jobs) == 1
        assert jobs[0]["JobId"] == "12345"
        assert jobs[0]["State"] == "RUNNING"

    def test_parse_time(self):
        """Test parsing time strings."""
        time_str = "2024-01-15T10:30:00"
        result = SlurmParser.parse_time(time_str)
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_time_invalid(self):
        """Test parsing invalid time strings."""
        assert SlurmParser.parse_time(None) is None
        assert SlurmParser.parse_time("") is None
        assert SlurmParser.parse_time("Unknown") is None

    def test_job_state_suffix_variants(self):
        """State parser should handle common Slurm suffix variants."""
        assert JobState.from_slurm_state("CANCELLED by 1000") == JobState.CANCELLED
        assert JobState.from_slurm_state("FAILED+") == JobState.FAILED
        assert (
            JobState.from_slurm_state("OUT_OF_MEMORY (ExitCode 0:9)")
            == JobState.OUT_OF_MEMORY
        )


class TestJobWatcher:
    """Tests for JobWatcher."""

    def test_watcher_initialization(self, test_config, temp_db):
        """Test watcher initialization."""
        watcher = JobWatcher(test_config, temp_db)
        assert watcher.config == test_config
        assert watcher.db == temp_db

    def test_get_poll_interval_active(self, test_config, temp_db):
        """Test poll interval with active jobs."""
        watcher = JobWatcher(test_config, temp_db)

        # Add running job
        from slurm_watchdog.models import Job

        job = Job(job_id="1", user="u", state=JobState.RUNNING)
        temp_db.upsert_job(job)

        interval = watcher.get_poll_interval()
        assert interval == test_config.watchdog.poll_interval_running

    def test_get_poll_interval_idle(self, test_config, temp_db):
        """Test poll interval with no active jobs."""
        watcher = JobWatcher(test_config, temp_db)

        interval = watcher.get_poll_interval()
        assert interval == test_config.watchdog.poll_interval_idle

    @patch("slurm_watchdog.watcher.SlurmClient.get_queue_jobs")
    def test_scan_new_job(self, mock_queue, test_config, temp_db):
        """Test scanning for new jobs."""
        mock_queue.return_value = [
            {
                "JobId": "12345",
                "UserId": "testuser(1000)",
                "Name": "my-job",
                "Partition": "cpu",
                "State": "RUNNING",
                "SubmitTime": "2024-01-01T10:00:00",
                "StartTime": "2024-01-01T10:05:00",
                "Reason": None,
                "ExitCode": "0:0",
            }
        ]

        watcher = JobWatcher(test_config, temp_db)
        updated_jobs, new_events = watcher.scan()

        assert len(updated_jobs) == 1
        assert updated_jobs[0].job_id == "12345"
        assert updated_jobs[0].state == JobState.RUNNING

    @patch("slurm_watchdog.watcher.SlurmClient.get_queue_jobs")
    def test_scan_with_filter(self, mock_queue, temp_db):
        """Test scanning with job name filter."""
        config = Config(
            watchdog={
                "job_name_filter": "^md-.*",
            }
        )

        mock_queue.return_value = [
            {"JobId": "1", "UserId": "u", "Name": "md-sim1", "State": "RUNNING"},
            {"JobId": "2", "UserId": "u", "Name": "other-job", "State": "RUNNING"},
        ]

        watcher = JobWatcher(config, temp_db)
        updated_jobs, _ = watcher.scan()

        # Only md- job should be included
        assert len(updated_jobs) == 1
        assert updated_jobs[0].name == "md-sim1"

    def test_create_state_change_events_for_oom_and_boot_fail(self, test_config, temp_db):
        """Test dedicated events for OOM and BOOT_FAIL states."""
        watcher = JobWatcher(test_config, temp_db)

        oom_job = Job(job_id="oom-job", user="u", state=JobState.OUT_OF_MEMORY)
        temp_db.upsert_job(oom_job)
        oom_events = watcher._create_state_change_events(oom_job)
        assert oom_events[0].event_type == EventType.JOB_OUT_OF_MEMORY

        boot_job = Job(job_id="boot-job", user="u", state=JobState.BOOT_FAIL)
        temp_db.upsert_job(boot_job)
        boot_events = watcher._create_state_change_events(boot_job)
        assert boot_events[0].event_type == EventType.JOB_BOOT_FAIL

    @patch("slurm_watchdog.watcher.SlurmClient.get_job_from_scontrol")
    @patch("slurm_watchdog.watcher.SlurmClient.get_jobs_from_sacct")
    @patch("slurm_watchdog.watcher.SlurmClient.get_queue_jobs")
    def test_scan_disappeared_job_respects_30s_grace(
        self,
        mock_queue,
        mock_sacct,
        mock_scontrol,
        temp_db,
    ):
        """Do not finalize a disappeared job until grace period elapses."""
        config = Config(watchdog={"disappeared_grace_seconds": 30})
        watcher = JobWatcher(config, temp_db)

        running_job = Job(
            job_id="123",
            user="u",
            state=JobState.RUNNING,
            last_seen=datetime.now(),
        )
        temp_db.upsert_job(running_job)

        mock_queue.return_value = []
        watcher.scan()

        mock_sacct.assert_not_called()
        mock_scontrol.assert_not_called()
        assert temp_db.get_job("123").state == JobState.RUNNING

    @patch("slurm_watchdog.watcher.SlurmClient.get_job_from_scontrol")
    @patch("slurm_watchdog.watcher.SlurmClient.get_jobs_from_sacct")
    @patch("slurm_watchdog.watcher.SlurmClient.get_queue_jobs")
    def test_scan_fallback_to_scontrol_when_sacct_unavailable(
        self,
        mock_queue,
        mock_sacct,
        mock_scontrol,
        temp_db,
    ):
        """Use scontrol to resolve final state when sacct is unavailable."""
        config = Config(watchdog={"disappeared_grace_seconds": 30})
        watcher = JobWatcher(config, temp_db)

        running_job = Job(
            job_id="123",
            user="u",
            state=JobState.RUNNING,
            last_seen=datetime.now() - timedelta(seconds=31),
        )
        temp_db.upsert_job(running_job)

        mock_queue.return_value = []
        mock_sacct.side_effect = SlurmError("sacct unavailable")
        mock_scontrol.return_value = {
            "JobId": "123",
            "JobName": "my-job",
            "UserId": "u(1000)",
            "Partition": "cpu",
            "JobState": "OUT_OF_MEMORY",
            "ExitCode": "0:9",
            "SubmitTime": "2024-01-01T10:00:00",
            "StartTime": "2024-01-01T10:05:00",
            "EndTime": "2024-01-01T10:06:00",
            "Reason": "OutOfMemory",
            "RunTime": "00:01:00",
            "StdOut": "/tmp/slurm-123.out",
        }

        _, new_events = watcher.scan()

        updated_job = temp_db.get_job("123")
        assert updated_job is not None
        assert updated_job.state == JobState.OUT_OF_MEMORY
        assert any(e.event_type == EventType.JOB_OUT_OF_MEMORY for e in new_events)

    def test_cleanup_old_jobs(self, test_config, temp_db):
        """Test cleaning up old jobs."""
        # Create old completed job
        old_job = Job(
            job_id="1",
            user="u",
            state=JobState.COMPLETED,
            updated_at=datetime(2020, 1, 1),
        )
        temp_db.upsert_job(old_job)

        # Create event to ensure FK rows are cleaned up together.
        event = temp_db.create_event(
            Event(
                job_id="1",
                event_type=EventType.JOB_COMPLETED,
                event_time=datetime.now(),
            )
        )
        assert event > 0

        # Create recent job
        recent_job = Job(
            job_id="2",
            user="u",
            state=JobState.RUNNING,
        )
        temp_db.upsert_job(recent_job)

        watcher = JobWatcher(test_config, temp_db)
        deleted = watcher.cleanup_old_jobs(days=30)

        assert deleted == 1
        assert temp_db.get_job("2") is not None
