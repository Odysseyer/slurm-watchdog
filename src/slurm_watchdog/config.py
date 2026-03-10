"""Configuration loading and validation for Slurm Watchdog."""

import os
import sys
from pathlib import Path

from pydantic import ValidationError

from slurm_watchdog.models import Config

# Python 3.11+ has built-in tomllib, otherwise use tomli
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

DEFAULT_CONFIG_DIR = Path("~/.config/slurm-watchdog").expanduser()
DEFAULT_CONFIG_PATH = DEFAULT_CONFIG_DIR / "config.toml"
PROJECT_URL = "https://github.com/Odysseyer/slurm-watchdog"

DEFAULT_CONFIG_TEMPLATE = """# Slurm Watchdog Configuration
# See: https://github.com/Odysseyer/slurm-watchdog

[watchdog]
# Polling interval in seconds
poll_interval_running = 60   # When jobs are running/pending
poll_interval_idle = 300     # When no active jobs
disappeared_grace_seconds = 30  # Wait before treating disappeared jobs as terminal/lost

# Monitor only current user's jobs (leave empty for auto-detect)
user = ""

# Optional: Filter jobs by name (regex)
# job_name_filter = "^md-.*"

# Optional: Filter jobs by partition (regex)
# partition_filter = "gpu"

[database]
# SQLite database path
path = "~/.local/share/slurm-watchdog/watchdog.db"

[notify]
# Apprise notification URLs
# See: https://github.com/caronc/apprise#popular-notification-services
urls = [
    # Examples (uncomment and configure as needed):
    # "json://https://your-server.com/webhook",
    # "wecombot://corpid/secret/agentid",
    # "dingtalk://token/secret",
    # "feishu://token/secret",
    # "telegram://bottoken/chatid",
    # "slack://tokena/tokenb/tokenc",
    # "mailtos://user:pass@host:port?to=email@example.com",
]

# Which events to notify
on_job_started = false
on_job_completed = true
on_job_failed = true
on_job_cancelled = true
on_job_timeout = true
on_job_boot_fail = true
on_job_out_of_memory = true
on_job_lost = true

[notify.retry]
max_retries = 3
backoff_factor = 2.0

[output_analysis]
# Analyze job output files on completion
enabled = true
tail_lines = 50

# Patterns indicating successful convergence
convergence_patterns = [
    "Convergence criteria met",
    "Normal termination",
    "SCF converged",
    "completed successfully",
    "CONVERGED",
    "Finished",
]

# Patterns indicating errors
error_patterns = [
    "ERROR:",
    "FATAL:",
    "Segmentation fault",
    "MPI_ERR",
    "Killed",
    "Abort",
    "Exception:",
]
"""


def resolve_config_path(config_path: str | Path | None = None) -> Path:
    """Resolve a config path, falling back to the default location."""
    if config_path is None:
        return DEFAULT_CONFIG_PATH
    return Path(config_path).expanduser()


def get_default_config_path() -> Path:
    """Get the default configuration file path."""
    return DEFAULT_CONFIG_PATH


def ensure_config_dir() -> Path:
    """Ensure the configuration directory exists."""
    config_dir = DEFAULT_CONFIG_DIR
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def create_default_config(config_path: str | Path | None = None) -> Path:
    """Create a configuration file with defaults if it doesn't exist."""
    config_path = resolve_config_path(config_path)

    if config_path.exists():
        return config_path

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(DEFAULT_CONFIG_TEMPLATE, encoding="utf-8")
    return config_path


def load_config(
    config_path: str | Path | None = None,
    create_if_missing: bool = True,
) -> Config:
    """Load configuration from a TOML file.

    Args:
        config_path: Path to config file. If None, uses default path.
        create_if_missing: If True, creates default config if file doesn't exist.

    Returns:
        Config object.

    Raises:
        FileNotFoundError: If config file doesn't exist and create_if_missing is False.
        ValueError: If config file is invalid.
    """
    config_path = resolve_config_path(config_path)

    if not config_path.exists():
        if create_if_missing:
            create_default_config(config_path)
        else:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with config_path.open("rb") as f:
            data = tomllib.load(f)
    except Exception as e:
        raise ValueError(f"Failed to parse config file {config_path}: {e}") from e

    try:
        config = Config(**data)
    except ValidationError as e:
        raise ValueError(f"Invalid configuration in {config_path}:\n{e}") from e

    # Auto-detect user if not specified
    if not config.watchdog.user:
        config.watchdog.user = os.environ.get("USER", os.environ.get("LOGNAME", ""))

    return config


def get_config_template() -> str:
    """Get the default configuration template."""
    return DEFAULT_CONFIG_TEMPLATE


def validate_config(config: Config) -> list[str]:
    """Validate configuration and return list of warnings.

    Args:
        config: Configuration to validate.

    Returns:
        List of warning messages (empty if all valid).
    """
    warnings = []

    # Check if notification URLs are configured
    if not config.notify.urls:
        warnings.append(
            "No notification URLs configured. "
            "Edit config.toml to add at least one notification service."
        )

    # Validate poll intervals
    if config.watchdog.poll_interval_running < 10:
        warnings.append(
            f"poll_interval_running ({config.watchdog.poll_interval_running}s) is very low. "
            "This may cause excessive load on Slurm. Consider increasing to at least 30s."
        )

    if config.watchdog.poll_interval_idle < 60:
        warnings.append(
            f"poll_interval_idle ({config.watchdog.poll_interval_idle}s) is very low. "
            "Consider increasing to reduce system load when idle."
        )

    if config.watchdog.disappeared_grace_seconds < 0:
        warnings.append(
            "disappeared_grace_seconds is negative. "
            "Use 0 or a positive number."
        )

    # Validate database path
    db_path = Path(config.database.path).expanduser()
    db_dir = db_path.parent
    if not db_dir.exists():
        warnings.append(
            f"Database directory ({db_dir}) does not exist. "
            "It will be created automatically."
        )

    return warnings
