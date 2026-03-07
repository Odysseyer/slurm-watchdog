"""SQLite database operations for Slurm Watchdog."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from slurm_watchdog.models import Event, EventType, Job, JobState


class Database:
    """SQLite database manager for job tracking."""

    SCHEMA_VERSION = 1

    def __init__(self, db_path: str):
        """Initialize database connection.

        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    @property
    def conn(self) -> sqlite3.Connection:
        """Get database connection, creating if necessary."""
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self.db_path),
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            # Enable WAL mode for better concurrency
            self._conn.execute("PRAGMA journal_mode=WAL")
            # Enable foreign keys
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._create_tables()
        return self._conn

    def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()

        # Jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                user TEXT NOT NULL,
                name TEXT,
                partition TEXT,
                state TEXT NOT NULL,
                exit_code TEXT,
                submit_time TIMESTAMP,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                reason TEXT,
                elapsed_time TEXT,
                max_rss TEXT,
                max_vmsize TEXT,
                cpu_time TEXT,
                output_file TEXT,
                last_seen TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Events table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_time TIMESTAMP NOT NULL,
                notified INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(job_id, event_type),
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            )
        """)

        # Schema version table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            )
        """)

        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_notified ON events(notified)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_job_id ON events(job_id)")

        # Record schema version
        cursor.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
            (self.SCHEMA_VERSION,),
        )

        self.conn.commit()

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def upsert_job(self, job: Job) -> None:
        """Insert or update a job record.

        Args:
            job: Job to insert or update.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO jobs (
                job_id, user, name, partition, state, exit_code,
                submit_time, start_time, end_time, reason,
                elapsed_time, max_rss, max_vmsize, cpu_time, output_file,
                last_seen, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                user = excluded.user,
                name = excluded.name,
                partition = excluded.partition,
                state = excluded.state,
                exit_code = excluded.exit_code,
                submit_time = excluded.submit_time,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                reason = excluded.reason,
                elapsed_time = excluded.elapsed_time,
                max_rss = excluded.max_rss,
                max_vmsize = excluded.max_vmsize,
                cpu_time = excluded.cpu_time,
                output_file = excluded.output_file,
                last_seen = excluded.last_seen,
                updated_at = excluded.updated_at
            """,
            (
                job.job_id,
                job.user,
                job.name,
                job.partition,
                job.state.value,
                job.exit_code,
                job.submit_time,
                job.start_time,
                job.end_time,
                job.reason,
                job.elapsed_time,
                job.max_rss,
                job.max_vmsize,
                job.cpu_time,
                job.output_file,
                job.last_seen,
                job.updated_at,
            ),
        )
        self.conn.commit()

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID.

        Args:
            job_id: Job ID to look up.

        Returns:
            Job if found, None otherwise.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT job_id, user, name, partition, state, exit_code,
                   submit_time, start_time, end_time, reason,
                   elapsed_time, max_rss, max_vmsize, cpu_time, output_file,
                   last_seen, created_at, updated_at
            FROM jobs WHERE job_id = ?
            """,
            (job_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def get_jobs_by_state(self, *states: JobState) -> list[Job]:
        """Get all jobs in specified states.

        Args:
            *states: Job states to filter by.

        Returns:
            List of matching jobs.
        """
        if not states:
            return []

        placeholders = ",".join("?" * len(states))
        cursor = self.conn.cursor()
        cursor.execute(
            f"""
            SELECT job_id, user, name, partition, state, exit_code,
                   submit_time, start_time, end_time, reason,
                   elapsed_time, max_rss, max_vmsize, cpu_time, output_file,
                   last_seen, created_at, updated_at
            FROM jobs WHERE state IN ({placeholders})
            """,
            [s.value for s in states],
        )
        return [self._row_to_job(row) for row in cursor.fetchall()]

    def get_all_active_jobs(self) -> list[Job]:
        """Get all non-terminal jobs (PENDING, RUNNING, SUSPENDED).

        Returns:
            List of active jobs.
        """
        return self.get_jobs_by_state(
            JobState.PENDING,
            JobState.RUNNING,
            JobState.SUSPENDED,
        )

    def count_jobs(self, state: Optional[JobState] = None, user: Optional[str] = None) -> int:
        """Count jobs matching criteria.

        Args:
            state: Optional state filter.
            user: Optional user filter.

        Returns:
            Count of matching jobs.
        """
        cursor = self.conn.cursor()
        conditions = []
        params = []

        if state:
            conditions.append("state = ?")
            params.append(state.value)
        if user:
            conditions.append("user = ?")
            params.append(user)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        cursor.execute(f"SELECT COUNT(*) FROM jobs WHERE {where_clause}", params)
        return cursor.fetchone()[0]

    def delete_old_jobs(self, days: int = 30) -> int:
        """Delete completed/failed jobs older than specified days.

        Args:
            days: Age threshold in days.

        Returns:
            Number of deleted jobs.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            DELETE FROM jobs
            WHERE state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'NODE_FAIL', 'PREEMPTED', 'BOOT_FAIL', 'OUT_OF_MEMORY')
            AND updated_at < datetime('now', ? || ' days')
            """,
            (f"-{days}",),
        )
        deleted = cursor.rowcount
        self.conn.commit()
        return deleted

    def _row_to_job(self, row: tuple) -> Job:
        """Convert database row to Job model."""
        return Job(
            job_id=row[0],
            user=row[1],
            name=row[2],
            partition=row[3],
            state=JobState(row[4]),
            exit_code=row[5],
            submit_time=row[6],
            start_time=row[7],
            end_time=row[8],
            reason=row[9],
            elapsed_time=row[10],
            max_rss=row[11],
            max_vmsize=row[12],
            cpu_time=row[13],
            output_file=row[14],
            last_seen=row[15],
            created_at=row[16],
            updated_at=row[17],
        )

    # Event operations

    def create_event(self, event: Event) -> int:
        """Create an event record.

        Args:
            event: Event to create.

        Returns:
            Event ID.

        Raises:
            sqlite3.IntegrityError: If event already exists (job_id + event_type unique).
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO events (job_id, event_type, event_time, notified, retry_count, last_error)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.job_id,
                    event.event_type.value,
                    event.event_time,
                    event.notified,
                    event.retry_count,
                    event.last_error,
                ),
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Event already exists
            cursor.execute(
                """
                SELECT id FROM events WHERE job_id = ? AND event_type = ?
                """,
                (event.job_id, event.event_type.value),
            )
            return cursor.fetchone()[0]

    def event_exists(self, job_id: str, event_type: EventType) -> bool:
        """Check if an event has been recorded.

        Args:
            job_id: Job ID.
            event_type: Event type.

        Returns:
            True if event exists.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM events WHERE job_id = ? AND event_type = ?",
            (job_id, event_type.value),
        )
        return cursor.fetchone() is not None

    def get_pending_events(self) -> list[Event]:
        """Get all pending events (notified=0).

        Returns:
            List of pending events.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id, job_id, event_type, event_time, notified, retry_count, last_error, created_at
            FROM events WHERE notified = 0
            ORDER BY event_time
            """
        )
        return [self._row_to_event(row) for row in cursor.fetchall()]

    def get_events_for_retry(self, max_retries: int) -> list[Event]:
        """Get failed events eligible for retry.

        Args:
            max_retries: Maximum retry count.

        Returns:
            List of events to retry.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id, job_id, event_type, event_time, notified, retry_count, last_error, created_at
            FROM events
            WHERE notified = -1 AND retry_count < ?
            ORDER BY event_time
            """,
            (max_retries,),
        )
        return [self._row_to_event(row) for row in cursor.fetchall()]

    def mark_event_sent(self, event_id: int) -> None:
        """Mark an event as successfully sent.

        Args:
            event_id: Event ID.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE events SET notified = 1 WHERE id = ?",
            (event_id,),
        )
        self.conn.commit()

    def mark_event_failed(self, event_id: int, error: str) -> None:
        """Mark an event as failed.

        Args:
            event_id: Event ID.
            error: Error message.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE events
            SET notified = -1, retry_count = retry_count + 1, last_error = ?
            WHERE id = ?
            """,
            (error, event_id),
        )
        self.conn.commit()

    def _row_to_event(self, row: tuple) -> Event:
        """Convert database row to Event model."""
        return Event(
            id=row[0],
            job_id=row[1],
            event_type=EventType(row[2]),
            event_time=row[3],
            notified=row[4],
            retry_count=row[5],
            last_error=row[6],
            created_at=row[7],
        )

    def get_job_events(self, job_id: str) -> list[Event]:
        """Get all events for a job.

        Args:
            job_id: Job ID.

        Returns:
            List of events.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id, job_id, event_type, event_time, notified, retry_count, last_error, created_at
            FROM events WHERE job_id = ?
            ORDER BY event_time
            """,
            (job_id,),
        )
        return [self._row_to_event(row) for row in cursor.fetchall()]

    def __enter__(self) -> "Database":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
