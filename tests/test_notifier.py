"""Tests for the notifier module."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from slurm_watchdog.config import Config
from slurm_watchdog.database import Database
from slurm_watchdog.models import Event, EventType, Job, JobState
from slurm_watchdog.notifier import Notifier


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
    return Config(
        notify={
            "urls": ["json://https://example.com/webhook"],
            "on_job_started": True,
            "on_job_completed": True,
            "on_job_failed": True,
        }
    )


def test_notifier_initialization(test_config, temp_db):
    """Test notifier initialization."""
    notifier = Notifier(test_config, temp_db)
    assert notifier.config == test_config
    assert notifier.db == temp_db


def test_format_title(test_config, temp_db):
    """Test notification title formatting."""
    notifier = Notifier(test_config, temp_db)

    job = Job(
        job_id="12345",
        user="testuser",
        name="my-job",
        state=JobState.COMPLETED,
    )

    title = notifier._format_title(EventType.JOB_COMPLETED, job)
    assert "Slurm Job Completed" in title
    assert "my-job" in title
    assert "✅" in title


def test_format_body(test_config, temp_db):
    """Test notification body formatting."""
    notifier = Notifier(test_config, temp_db)

    job = Job(
        job_id="12345",
        user="testuser",
        name="my-job",
        state=JobState.COMPLETED,
        partition="cpu",
        elapsed_time="01:30:00",
        max_rss="1.5G",
    )

    body = notifier._format_body(job)

    assert "12345" in body
    assert "testuser" in body
    assert "my-job" in body
    assert "COMPLETED" in body
    assert "01:30:00" in body
    assert "1.5G" in body


def test_format_body_with_errors(test_config, temp_db):
    """Test notification body with error information."""
    notifier = Notifier(test_config, temp_db)

    job = Job(
        job_id="12345",
        user="testuser",
        name="failed-job",
        state=JobState.FAILED,
        exit_code="1:0",
        reason="OutOfMemory",
    )

    body = notifier._format_body(job)

    assert "FAILED" in body
    assert "1:0" in body
    assert "OutOfMemory" in body


def test_event_enabled(test_config, temp_db):
    """Test event type enable/disable checking."""
    notifier = Notifier(test_config, temp_db)

    assert notifier._event_enabled(EventType.JOB_COMPLETED)
    assert notifier._event_enabled(EventType.JOB_FAILED)
    assert notifier._event_enabled(EventType.JOB_STARTED)
    assert notifier._event_enabled(EventType.JOB_OUT_OF_MEMORY)
    assert notifier._event_enabled(EventType.JOB_BOOT_FAIL)

    # Not configured but defaults to True
    assert notifier._event_enabled(EventType.JOB_CANCELLED)


def test_has_urls_configured(test_config, temp_db):
    """Test URL configuration check."""
    notifier = Notifier(test_config, temp_db)
    assert notifier.has_urls_configured()

    # Test with no URLs
    config_no_urls = Config(notify={"urls": []})
    notifier_no_urls = Notifier(config_no_urls, temp_db)
    assert not notifier_no_urls.has_urls_configured()


@patch("slurm_watchdog.notifier.apprise.Apprise")
def test_test_notify(mock_apprise, test_config, temp_db):
    """Test sending a test notification."""
    mock_apprise_instance = MagicMock()
    mock_apprise_instance.notify.return_value = True
    mock_apprise.return_value = mock_apprise_instance

    notifier = Notifier(test_config, temp_db)
    result = notifier.test_notify("Test message")

    assert result
    mock_apprise_instance.notify.assert_called_once()


def test_notify_event_idempotency(test_config, temp_db):
    """Test that already-sent events are not re-notified."""
    notifier = Notifier(test_config, temp_db)

    # Create job and event
    job = Job(job_id="1", user="u", state=JobState.COMPLETED)
    temp_db.upsert_job(job)

    event = Event(
        id=1,
        job_id="1",
        event_type=EventType.JOB_COMPLETED,
        event_time=datetime.now(),
        notified=1,  # Already sent
    )
    temp_db.create_event(event)

    # Should return True without sending
    result = notifier.notify_event(event, job)
    assert result
