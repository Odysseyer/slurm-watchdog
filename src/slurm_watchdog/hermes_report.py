"""Hermes notification integration for Slurm Watchdog.

Outputs formatted notifications to stdout for delivery via Hermes cronjob
to WeChat (or other messaging platforms).

Usage: slurm-watchdog hermes-scan
       (called by Hermes cronjob with no_agent=True)

Empty stdout means no notifications — the cronjob stays silent.
"""

import logging
from datetime import datetime

from slurm_watchdog.analyzer import OutputAnalyzer
from slurm_watchdog.config import load_config
from slurm_watchdog.database import Database
from slurm_watchdog.models import (
    Event,
    EventType,
    Job,
    JobState,
    OutputAnalysis,
)
from slurm_watchdog.watcher import JobWatcher

logger = logging.getLogger(__name__)

# LAMMPS-specific patterns for better analysis
LAMMPS_PATTERNS = {
    "convergence": [
        "Total wall time",
        "Loop time of",
        "Memory usage per processor",
        "LAMMPS",
    ],
    "error": [
        "ERROR",
        "Lost atoms",
        "Bond atom missing",
        "Angle atom missing",
        "Dihedral atom missing",
        "Non-numeric atom coords",
        "Out of range atoms",
        "Particle on or inside fix",
        "shake/determine constraint",
        "Invalid bond length",
        "FENE bond is longer than",
        "Domain too small",
        "Cannot compute stress/pressure",
        "Lost bond",
    ],
}


def _compute_runtime_seconds(job: Job) -> int | None:
    """Compute approximate runtime in seconds from available data."""
    # Try elapsed_time from sacct
    secs = job.get_elapsed_seconds()
    if secs is not None:
        return secs

    # Compute from start/end times
    if job.start_time and job.end_time:
        return int((job.end_time - job.start_time).total_seconds())

    # Approximate from start_time to last_seen
    if job.start_time and job.last_seen:
        return int((job.last_seen - job.start_time).total_seconds())

    return None


def _format_runtime(seconds: int | None) -> str:
    """Format seconds into human-readable duration."""
    if seconds is None:
        return "未知"
    if seconds < 60:
        return f"{seconds}秒"
    if seconds < 3600:
        return f"{seconds // 60}分{seconds % 60}秒"
    hours = seconds // 3600
    mins = (seconds % 3600) // 60
    if hours < 24:
        return f"{hours}时{mins}分"
    days = hours // 24
    remain_hours = hours % 24
    return f"{days}天{remain_hours}时{mins}分"


def _format_notification(
    job: Job,
    event_type: EventType,
    analysis: OutputAnalysis | None = None,
    runtime_seconds: int | None = None,
) -> str:
    """Format a notification message in Chinese for WeChat delivery."""
    is_success = job.state == JobState.COMPLETED
    state_text = "正常结束" if is_success else "异常结束"
    emoji = "✅" if is_success else "❌"

    lines = []
    lines.append(f"{emoji} Slurm任务{state_text}")
    lines.append(f"任务名称: {job.name or 'N/A'}")
    lines.append(f"任务ID: {job.job_id}")

    if is_success:
        lines.append(f"状态: COMPLETED")
    else:
        lines.append(f"状态: {job.state.value}")
        if job.exit_code:
            lines.append(f"退出码: {job.exit_code}")

    lines.append(f"运行时间: {_format_runtime(runtime_seconds)}")

    if job.work_dir:
        lines.append(f"工作目录: {job.work_dir}")

    # Analysis for failed/unknown jobs
    if analysis and not is_success:
        if analysis.has_errors and analysis.error_lines:
            lines.append("")
            lines.append("--- 错误分析 ---")
            for line in analysis.error_lines[:8]:
                lines.append(f"  ! {line[:120]}")
        if analysis.tail_lines:
            lines.append("")
            lines.append("--- 最后输出 ---")
            for line in analysis.tail_lines[-8:]:
                stripped = line.strip()
                if stripped:
                    lines.append(f"  {stripped[:120]}")

    return "\n".join(lines)


def _analyze_output(job: Job, config) -> OutputAnalysis | None:
    """Analyze job output file, using extended LAMMPS patterns."""
    output_path = job.output_file
    if not output_path and job.work_dir:
        output_path = f"{job.work_dir}/slurm-{job.job_id}.out"

    if not output_path:
        return None

    # Extend config patterns with LAMMPS-specific ones
    extended_config = config.model_copy(deep=True)
    existing_errors = list(extended_config.output_analysis.error_patterns)
    existing_conv = list(extended_config.output_analysis.convergence_patterns)

    for p in LAMMPS_PATTERNS["error"]:
        if p not in existing_errors:
            existing_errors.append(p)
    for p in LAMMPS_PATTERNS["convergence"]:
        if p not in existing_conv:
            existing_conv.append(p)

    extended_config.output_analysis.error_patterns = existing_errors
    extended_config.output_analysis.convergence_patterns = existing_conv

    try:
        analyzer = OutputAnalyzer(extended_config)
        return analyzer.analyze(output_path)
    except Exception as exc:
        logger.warning("Failed to analyze output for job %s: %s", job.job_id, exc)
        return None


def run_hermes_scan(config=None) -> str:
    """Run one scan cycle and return formatted notifications.

    Returns:
        Notification text (empty string if nothing to report).
        Empty output means the Hermes cronjob stays silent.

    Args:
        config: Optional Config object. If None, loads from default path.
    """
    if config is None:
        config = load_config()

    notifications = []
    min_runtime = config.watchdog.min_runtime_seconds

    with Database(config.database.path) as db:
        watcher = JobWatcher(config, db)

        # Run one scan cycle
        updated_jobs, new_events = watcher.scan()

        if not new_events:
            return ""

        for event in new_events:
            job = db.get_job(event.job_id)
            if not job:
                continue

            # Only notify on terminal states
            if not job.state.is_terminal():
                continue

            # Compute runtime and apply minimum filter
            runtime_seconds = _compute_runtime_seconds(job)
            if runtime_seconds is not None and runtime_seconds < min_runtime:
                logger.info(
                    "Skipping notification for job %s: runtime %ds < %ds minimum",
                    job.job_id,
                    runtime_seconds,
                    min_runtime,
                )
                continue

            # Analyze output for non-success jobs
            analysis = None
            if not job.state.is_success():
                analysis = _analyze_output(job, config)

            notification = _format_notification(
                job, event.event_type, analysis, runtime_seconds
            )
            notifications.append(notification)

    if notifications:
        return "\n\n━━━━━━━━━━━━━━\n\n".join(notifications)

    return ""
