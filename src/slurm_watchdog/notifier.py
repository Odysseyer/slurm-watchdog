"""Notification system using Apprise for multi-platform support."""

import time
from datetime import datetime
from typing import Optional

import apprise

from slurm_watchdog.config import Config
from slurm_watchdog.database import Database
from slurm_watchdog.models import Event, EventType, Job, OutputAnalysis


class Notifier:
    """Handles multi-platform notifications via Apprise."""

    def __init__(self, config: Config, db: Database):
        """Initialize notifier.

        Args:
            config: Configuration.
            db: Database instance.
        """
        self.config = config
        self.db = db
        self._apprise: Optional[apprise.Apprise] = None

    @property
    def apprise(self) -> apprise.Apprise:
        """Get configured Apprise instance."""
        if self._apprise is None:
            self._apprise = apprise.Apprise()
            for url in self.config.notify.urls:
                self._apprise.add(url)
        return self._apprise

    def _event_enabled(self, event_type: EventType) -> bool:
        """Check if notification is enabled for an event type.

        Args:
            event_type: Event type to check.

        Returns:
            True if notifications are enabled.
        """
        enabled_map = {
            EventType.JOB_STARTED: self.config.notify.on_job_started,
            EventType.JOB_COMPLETED: self.config.notify.on_job_completed,
            EventType.JOB_FAILED: self.config.notify.on_job_failed,
            EventType.JOB_CANCELLED: self.config.notify.on_job_cancelled,
            EventType.JOB_TIMEOUT: self.config.notify.on_job_timeout,
            EventType.JOB_PREEMPTED: True,  # Always notify preemption
            EventType.JOB_NODE_FAIL: True,  # Always notify node failures
        }
        return enabled_map.get(event_type, True)

    def _format_title(self, event_type: EventType, job: Job) -> str:
        """Format notification title.

        Args:
            event_type: Event type.
            job: Job information.

        Returns:
            Formatted title.
        """
        job_name = job.name or job.job_id
        event_name = event_type.value.replace("JOB_", "").replace("_", " ").title()

        # Add emoji for different states
        emoji_map = {
            EventType.JOB_STARTED: "🚀",
            EventType.JOB_COMPLETED: "✅",
            EventType.JOB_FAILED: "❌",
            EventType.JOB_CANCELLED: "🚫",
            EventType.JOB_TIMEOUT: "⏱️",
            EventType.JOB_PREEMPTED: "⚠️",
            EventType.JOB_NODE_FAIL: "💥",
        }
        emoji = emoji_map.get(event_type, "📢")

        return f"{emoji} Slurm Job {event_name}: {job_name}"

    def _format_body(
        self,
        job: Job,
        event_type: EventType,
        analysis: Optional[OutputAnalysis] = None,
    ) -> str:
        """Format notification body.

        Args:
            job: Job information.
            event_type: Event type.
            analysis: Optional output analysis.

        Returns:
            Formatted body text.
        """
        lines = []
        job_name = job.name or "N/A"

        # Header
        lines.append(f"Job: {job_name} ({job.job_id})")
        lines.append(f"State: {job.state.value}")
        if job.exit_code:
            lines.append(f"Exit Code: {job.exit_code}")
        lines.append(f"User: {job.user}")
        if job.partition:
            lines.append(f"Partition: {job.partition}")

        # Timing
        lines.append("")
        lines.append("Timing:")
        if job.submit_time:
            lines.append(f"  Submitted: {job.submit_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.start_time:
            lines.append(f"  Started: {job.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.end_time:
            lines.append(f"  Ended: {job.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.elapsed_time:
            lines.append(f"  Elapsed: {job.elapsed_time}")

        # Resource usage (for completed jobs)
        if job.state.is_terminal() and (job.max_rss or job.max_vmsize):
            lines.append("")
            lines.append("Resource Usage:")
            if job.max_rss:
                lines.append(f"  Max RSS: {job.max_rss}")
            if job.max_vmsize:
                lines.append(f"  Max VMSize: {job.max_vmsize}")
            if job.cpu_time:
                lines.append(f"  CPU Time: {job.cpu_time}")

        # Failure reason
        if job.reason and not job.state.is_success():
            lines.append("")
            lines.append(f"Reason: {job.reason}")

        # Output analysis
        if analysis and self.config.output_analysis.enabled:
            lines.append("")
            lines.append("Output Analysis:")
            lines.append(f"  {analysis.get_summary()}")

            if analysis.convergence_lines:
                lines.append("")
                lines.append("  Convergence indicators:")
                for line in analysis.convergence_lines[:3]:
                    lines.append(f"    • {line[:80]}")

            if analysis.has_errors and analysis.error_lines:
                lines.append("")
                lines.append("  Errors detected:")
                for line in analysis.error_lines[:5]:
                    lines.append(f"    • {line[:80]}")

        # Output file location
        if job.output_file and job.state.is_terminal():
            lines.append("")
            lines.append(f"Output file: {job.output_file}")

        return "\n".join(lines)

    def notify_event(
        self,
        event: Event,
        job: Job,
        analysis: Optional[OutputAnalysis] = None,
    ) -> bool:
        """Send notification for an event.

        Args:
            event: Event to notify.
            job: Job information.
            analysis: Optional output analysis.

        Returns:
            True if notification was sent successfully.
        """
        # Check if this event type is enabled
        if not self._event_enabled(event.event_type):
            self.db.mark_event_sent(event.id)
            return True

        # Check if already notified (idempotency)
        if event.notified == 1:
            return True

        title = self._format_title(event.event_type, job)
        body = self._format_body(job, event.event_type, analysis)

        try:
            success = self.apprise.notify(
                title=title,
                body=body,
            )

            if success:
                self.db.mark_event_sent(event.id)
                return True
            else:
                self.db.mark_event_failed(event.id, "Apprise returned False")
                return False

        except Exception as e:
            error_msg = str(e)
            self.db.mark_event_failed(event.id, error_msg)
            print(f"Notification failed: {error_msg}")
            return False

    def test_notify(self, message: str = "Test notification from Slurm Watchdog") -> bool:
        """Send a test notification.

        Args:
            message: Test message to send.

        Returns:
            True if successful.
        """
        if not self.config.notify.urls:
            print("No notification URLs configured!")
            return False

        try:
            return self.apprise.notify(
                title="🧪 Slurm Watchdog Test",
                body=message,
            )
        except Exception as e:
            print(f"Test notification failed: {e}")
            return False

    def process_pending_events(self) -> tuple[int, int]:
        """Process all pending notification events.

        Returns:
            Tuple of (success_count, failure_count).
        """
        from slurm_watchdog.analyzer import OutputAnalyzer

        success = 0
        failed = 0

        pending_events = self.db.get_pending_events()

        for event in pending_events:
            job = self.db.get_job(event.job_id)
            if not job:
                # Job no longer exists, mark as sent
                self.db.mark_event_sent(event.id)
                continue

            # Analyze output file if available and job is terminal
            analysis = None
            if job.output_file and job.state.is_terminal() and self.config.output_analysis.enabled:
                analyzer = OutputAnalyzer(self.config)
                try:
                    analysis = analyzer.analyze(job.output_file)
                except Exception as e:
                    print(f"Warning: Failed to analyze output file: {e}")

            if self.notify_event(event, job, analysis):
                success += 1
            else:
                failed += 1

        return success, failed

    def retry_failed_events(self) -> tuple[int, int]:
        """Retry failed notification events with exponential backoff.

        Returns:
            Tuple of (success_count, failure_count).
        """
        from slurm_watchdog.analyzer import OutputAnalyzer

        success = 0
        failed = 0
        max_retries = self.config.notify.retry.max_retries
        backoff_factor = self.config.notify.retry.backoff_factor

        retry_events = self.db.get_events_for_retry(max_retries)

        for event in retry_events:
            # Apply backoff based on retry count
            backoff_seconds = backoff_factor**event.retry_count
            time.sleep(min(backoff_seconds, 60))  # Cap at 60 seconds

            job = self.db.get_job(event.job_id)
            if not job:
                self.db.mark_event_sent(event.id)
                continue

            # Analyze output if needed
            analysis = None
            if job.output_file and job.state.is_terminal() and self.config.output_analysis.enabled:
                analyzer = OutputAnalyzer(self.config)
                try:
                    analysis = analyzer.analyze(job.output_file)
                except Exception:
                    pass

            # Reset notified status for retry
            event.notified = 0
            if self.notify_event(event, job, analysis):
                success += 1
            else:
                failed += 1

        return success, failed

    def has_urls_configured(self) -> bool:
        """Check if any notification URLs are configured.

        Returns:
            True if at least one URL is configured.
        """
        return len(self.config.notify.urls) > 0
