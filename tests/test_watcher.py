"""Tests for the watcher module."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from slurm_watchdog.config import Config
from slurm_watchdog.database import Database
from slurm_watchdog.models import JobState
from slurm_watchdog.watcher import JobWatcher, SlurmParser


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
        output = """JobId=12345|UserId=testuser(1000)|Name=my-job|Partition=cpu|State=RUNNING|SubmitTime=2024-01-01T10:00:00|StartTime=2024-01-01T10:05:00|Reason=None|ExitCode=0:0
JobId=12346|UserId=testuser(1000)|Name=pending-job|Partition=gpu|State=PENDING|SubmitTime=2024-01-01T11:00:00|StartTime=None|Reason=Resources|ExitCode=0:0"""

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

    def test_cleanup_old_jobs(self, test_config, temp_db):
        """Test cleaning up old jobs."""
        from slurm_watchdog.models import Job

        # Create old completed job
        old_job = Job(
            job_id="1",
            user="u",
            state=JobState.COMPLETED,
            updated_at=datetime(2020, 1, 1),
        )
        temp_db.upsert_job(old_job)

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
