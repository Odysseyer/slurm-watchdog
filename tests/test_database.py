"""Tests for the database module."""

import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from slurm_watchdog.database import Database
from slurm_watchdog.models import Event, EventType, Job, JobState


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = Database(db_path)
    yield db

    db.close()
    Path(db_path).unlink(missing_ok=True)


def test_database_creation(temp_db):
    """Test that database and tables are created."""
    # Database should be initialized on first connection
    conn = temp_db.conn
    assert conn is not None

    # Check tables exist
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}

    assert "jobs" in tables
    assert "events" in tables


def test_upsert_job(temp_db):
    """Test inserting and updating jobs."""
    job = Job(
        job_id="12345",
        user="testuser",
        name="test-job",
        partition="cpu",
        state=JobState.RUNNING,
    )

    # Insert
    temp_db.upsert_job(job)

    # Retrieve
    retrieved = temp_db.get_job("12345")
    assert retrieved is not None
    assert retrieved.job_id == "12345"
    assert retrieved.user == "testuser"
    assert retrieved.state == JobState.RUNNING

    # Update
    job_updated = job.model_copy(update={"state": JobState.COMPLETED})
    temp_db.upsert_job(job_updated)

    retrieved = temp_db.get_job("12345")
    assert retrieved.state == JobState.COMPLETED


def test_get_jobs_by_state(temp_db):
    """Test querying jobs by state."""
    # Insert jobs with different states
    jobs = [
        Job(job_id="1", user="u", state=JobState.RUNNING),
        Job(job_id="2", user="u", state=JobState.RUNNING),
        Job(job_id="3", user="u", state=JobState.PENDING),
        Job(job_id="4", user="u", state=JobState.COMPLETED),
    ]

    for job in jobs:
        temp_db.upsert_job(job)

    # Query
    running = temp_db.get_jobs_by_state(JobState.RUNNING)
    assert len(running) == 2

    pending = temp_db.get_jobs_by_state(JobState.PENDING)
    assert len(pending) == 1

    completed = temp_db.get_jobs_by_state(JobState.COMPLETED)
    assert len(completed) == 1


def test_count_jobs(temp_db):
    """Test counting jobs."""
    jobs = [
        Job(job_id="1", user="alice", state=JobState.RUNNING),
        Job(job_id="2", user="alice", state=JobState.PENDING),
        Job(job_id="3", user="bob", state=JobState.RUNNING),
    ]

    for job in jobs:
        temp_db.upsert_job(job)

    assert temp_db.count_jobs(state=JobState.RUNNING) == 2
    assert temp_db.count_jobs(state=JobState.PENDING) == 1
    assert temp_db.count_jobs(user="alice") == 2


def test_create_event(temp_db):
    """Test creating events."""
    # First create a job (due to foreign key)
    job = Job(job_id="1", user="u", state=JobState.COMPLETED)
    temp_db.upsert_job(job)

    event = Event(
        job_id="1",
        event_type=EventType.JOB_COMPLETED,
        event_time=datetime.now(),
    )

    event_id = temp_db.create_event(event)
    assert event_id > 0

    # Check event exists
    assert temp_db.event_exists("1", EventType.JOB_COMPLETED)
    assert not temp_db.event_exists("1", EventType.JOB_FAILED)


def test_event_uniqueness(temp_db):
    """Test that events are unique per job+event_type."""
    job = Job(job_id="1", user="u", state=JobState.COMPLETED)
    temp_db.upsert_job(job)

    event1 = Event(
        job_id="1",
        event_type=EventType.JOB_COMPLETED,
        event_time=datetime.now(),
    )

    event2 = Event(
        job_id="1",
        event_type=EventType.JOB_COMPLETED,
        event_time=datetime.now(),
    )

    id1 = temp_db.create_event(event1)
    id2 = temp_db.create_event(event2)

    # Should return the same ID (existing event)
    assert id1 == id2


def test_get_pending_events(temp_db):
    """Test getting pending events."""
    job = Job(job_id="1", user="u", state=JobState.COMPLETED)
    temp_db.upsert_job(job)

    event = Event(
        job_id="1",
        event_type=EventType.JOB_COMPLETED,
        event_time=datetime.now(),
        notified=0,
    )
    temp_db.create_event(event)

    pending = temp_db.get_pending_events()
    assert len(pending) == 1
    assert pending[0].job_id == "1"


def test_mark_event_sent(temp_db):
    """Test marking events as sent."""
    job = Job(job_id="1", user="u", state=JobState.COMPLETED)
    temp_db.upsert_job(job)

    event = Event(
        job_id="1",
        event_type=EventType.JOB_COMPLETED,
        event_time=datetime.now(),
    )
    event_id = temp_db.create_event(event)

    temp_db.mark_event_sent(event_id)

    pending = temp_db.get_pending_events()
    assert len(pending) == 0


def test_mark_event_failed(temp_db):
    """Test marking events as failed."""
    job = Job(job_id="1", user="u", state=JobState.COMPLETED)
    temp_db.upsert_job(job)

    event = Event(
        job_id="1",
        event_type=EventType.JOB_COMPLETED,
        event_time=datetime.now(),
    )
    event_id = temp_db.create_event(event)

    temp_db.mark_event_failed(event_id, "Connection timeout")

    events = temp_db.get_events_for_retry(max_retries=5)
    assert len(events) == 1
    assert events[0].retry_count == 1
    assert "timeout" in events[0].last_error.lower()


def test_delete_old_jobs(temp_db):
    """Test deleting old jobs."""
    # Create completed job
    job = Job(
        job_id="1",
        user="u",
        state=JobState.COMPLETED,
        updated_at=datetime(2020, 1, 1),
    )
    temp_db.upsert_job(job)

    # Should delete job older than 30 days
    deleted = temp_db.delete_old_jobs(days=30)
    assert deleted == 1

    assert temp_db.get_job("1") is None
