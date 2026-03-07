"""Slurm Watchdog - A lightweight Slurm job monitor with multi-platform notifications."""

__version__ = "0.1.0"

from slurm_watchdog.config import Config, load_config
from slurm_watchdog.models import JobState

__all__ = ["__version__", "Config", "load_config", "JobState"]
