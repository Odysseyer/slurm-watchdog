"""Pydantic data models for Slurm Watchdog."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class JobState(str, Enum):
    """Slurm job states."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    NODE_FAIL = "NODE_FAIL"
    PREEMPTED = "PREEMPTED"
    BOOT_FAIL = "BOOT_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_slurm_state(cls, state: str) -> "JobState":
        """Convert Slurm state string to JobState enum."""
        state_map = {
            "PENDING": cls.PENDING,
            "PD": cls.PENDING,
            "RUNNING": cls.RUNNING,
            "R": cls.RUNNING,
            "SUSPENDED": cls.SUSPENDED,
            "S": cls.SUSPENDED,
            "COMPLETED": cls.COMPLETED,
            "CD": cls.COMPLETED,
            "CANCELLED": cls.CANCELLED,
            "CA": cls.CANCELLED,
            "FAILED": cls.FAILED,
            "F": cls.FAILED,
            "TIMEOUT": cls.TIMEOUT,
            "TO": cls.TIMEOUT,
            "NODE_FAIL": cls.NODE_FAIL,
            "NF": cls.NODE_FAIL,
            "PREEMPTED": cls.PREEMPTED,
            "PR": cls.PREEMPTED,
            "BOOT_FAIL": cls.BOOT_FAIL,
            "BF": cls.BOOT_FAIL,
            "OUT_OF_MEMORY": cls.OUT_OF_MEMORY,
            "OOM": cls.OUT_OF_MEMORY,
        }
        return state_map.get(state.upper(), cls.UNKNOWN)

    def is_terminal(self) -> bool:
        """Check if this is a terminal state (job won't change anymore)."""
        terminal_states = {
            self.COMPLETED,
            self.CANCELLED,
            self.FAILED,
            self.TIMEOUT,
            self.NODE_FAIL,
            self.PREEMPTED,
            self.BOOT_FAIL,
            self.OUT_OF_MEMORY,
        }
        return self in terminal_states

    def is_success(self) -> bool:
        """Check if the job completed successfully."""
        return self == self.COMPLETED


class EventType(str, Enum):
    """Event types for notifications."""

    JOB_STARTED = "JOB_STARTED"
    JOB_COMPLETED = "JOB_COMPLETED"
    JOB_FAILED = "JOB_FAILED"
    JOB_CANCELLED = "JOB_CANCELLED"
    JOB_TIMEOUT = "JOB_TIMEOUT"
    JOB_PREEMPTED = "JOB_PREEMPTED"
    JOB_NODE_FAIL = "JOB_NODE_FAIL"


class Job(BaseModel):
    """Represents a Slurm job."""

    job_id: str
    user: str
    name: Optional[str] = None
    partition: Optional[str] = None
    state: JobState
    exit_code: Optional[str] = None  # Format: "exit_code:signal"
    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    reason: Optional[str] = None
    last_seen: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # Resource usage (from sacct)
    elapsed_time: Optional[str] = None
    max_rss: Optional[str] = None
    max_vmsize: Optional[str] = None
    cpu_time: Optional[str] = None

    # Output file path
    output_file: Optional[str] = None

    def get_elapsed_seconds(self) -> Optional[int]:
        """Parse elapsed time string to seconds."""
        if not self.elapsed_time:
            return None

        parts = self.elapsed_time.split(":")
        try:
            if len(parts) == 3:
                # HH:MM:SS
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                # MM:SS
                return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, IndexError):
            pass
        return None


class Event(BaseModel):
    """Represents a notification event."""

    id: Optional[int] = None
    job_id: str
    event_type: EventType
    event_time: datetime = Field(default_factory=datetime.now)
    notified: int = 0  # 0=pending, 1=sent, -1=failed
    retry_count: int = 0
    last_error: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)


class OutputAnalysis(BaseModel):
    """Analysis result from job output file."""

    converged: bool = False
    has_errors: bool = False
    convergence_lines: list[str] = []
    error_lines: list[str] = []
    tail_lines: list[str] = []

    def get_summary(self) -> str:
        """Get a summary of the analysis."""
        parts = []
        if self.converged:
            parts.append("Converged: Yes")
        if self.has_errors:
            parts.append("Errors detected")
        if not parts:
            parts.append("No issues detected")
        return " | ".join(parts)


class RetryConfig(BaseModel):
    """Retry configuration for notifications."""

    max_retries: int = 3
    backoff_factor: float = 2.0


class NotifyConfig(BaseModel):
    """Notification configuration."""

    urls: list[str] = []
    on_job_started: bool = False
    on_job_completed: bool = True
    on_job_failed: bool = True
    on_job_cancelled: bool = True
    on_job_timeout: bool = True
    retry: RetryConfig = Field(default_factory=RetryConfig)


class OutputAnalysisConfig(BaseModel):
    """Output file analysis configuration."""

    enabled: bool = True
    tail_lines: int = 50
    convergence_patterns: list[str] = [
        "Convergence criteria met",
        "Normal termination",
        "SCF converged",
        "completed successfully",
        "CONVERGED",
        "Finished",
    ]
    error_patterns: list[str] = [
        "ERROR:",
        "FATAL:",
        "Segmentation fault",
        "MPI_ERR",
        "Killed",
        "Abort",
        "Exception:",
    ]


class WatchdogConfig(BaseModel):
    """Watchdog core configuration."""

    poll_interval_running: int = 60
    poll_interval_idle: int = 300
    user: str = ""  # Empty means current user
    job_name_filter: Optional[str] = None
    partition_filter: Optional[str] = None


class DatabaseConfig(BaseModel):
    """Database configuration."""

    path: str = "~/.local/share/slurm-watchdog/watchdog.db"


class Config(BaseModel):
    """Root configuration model."""

    watchdog: WatchdogConfig = Field(default_factory=WatchdogConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    notify: NotifyConfig = Field(default_factory=NotifyConfig)
    output_analysis: OutputAnalysisConfig = Field(default_factory=OutputAnalysisConfig)

    def get_poll_interval(self, has_active_jobs: bool) -> int:
        """Get the appropriate poll interval based on job state."""
        if has_active_jobs:
            return self.watchdog.poll_interval_running
        return self.watchdog.poll_interval_idle

    def get_event_types_for_state(self, state: JobState) -> list[EventType]:
        """Get the event types that should trigger notification for a state."""
        event_map = {
            JobState.RUNNING: [EventType.JOB_STARTED],
            JobState.COMPLETED: [EventType.JOB_COMPLETED],
            JobState.FAILED: [EventType.JOB_FAILED],
            JobState.CANCELLED: [EventType.JOB_CANCELLED],
            JobState.TIMEOUT: [EventType.JOB_TIMEOUT],
            JobState.PREEMPTED: [EventType.JOB_PREEMPTED],
            JobState.NODE_FAIL: [EventType.JOB_NODE_FAIL],
        }
        return event_map.get(state, [])
