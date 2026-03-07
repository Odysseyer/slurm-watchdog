"""Configuration loading and validation for Slurm Watchdog."""

import os
import sys
from pathlib import Path
from typing import Optional

from pydantic import ValidationError

from slurm_watchdog.models import Config

# Python 3.11+ has built-in tomllib, otherwise use tomli
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

DEFAULT_CONFIG_DIR = Path("~/.config/slurm-watchdog").expanduser()
DEFAULT_CONFIG_PATH = DEFAULT_CONFIG_DIR / "config.toml"

DEFAULT_CONFIG_TEMPLATE = """# Slurm Watchdog Configuration
# See: https://github.com/yourname/slurm-watchdog

[watchdog]
# Polling interval in seconds
poll_interval_running = 60   # When jobs are running/pending
poll_interval_idle = 300     # When no active jobs

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


def get_default_config_path() -> Path:
    """Get the default configuration file path."""
    return DEFAULT_CONFIG_PATH


def ensure_config_dir() -> Path:
    """Ensure the configuration directory exists."""
    config_dir = DEFAULT_CONFIG_DIR
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def create_default_config() -> Path:
    """Create the default configuration file if it doesn't exist."""
    config_path = DEFAULT_CONFIG_PATH

    if config_path.exists():
        return config_path

    ensure_config_dir()
    config_path.write_text(DEFAULT_CONFIG_TEMPLATE)
    return config_path


def load_config(config_path: Optional[Path] = None, create_if_missing: bool = True) -> Config:
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
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    else:
        config_path = Path(config_path).expanduser()

    config_path = config_path.expanduser()

    if not config_path.exists():
        if create_if_missing:
            create_default_config()
            # If still doesn't exist, return default config
            if not config_path.exists():
                return Config()
        else:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, "rb") as f:
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

    # Validate database path
    db_path = Path(config.database.path).expanduser()
    db_dir = db_path.parent
    if not db_dir.exists():
        warnings.append(
            f"Database directory ({db_dir}) does not exist. "
            "It will be created automatically."
        )

    return warnings
