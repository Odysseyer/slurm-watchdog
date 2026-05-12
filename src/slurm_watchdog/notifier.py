"""Notification system using Apprise for multi-platform support."""

import asyncio
import logging
import time
from typing import Any

import apprise

from slurm_watchdog.analyzer import OutputAnalyzer
from slurm_watchdog.config import Config
from slurm_watchdog.database import Database
from slurm_watchdog.models import Event, EventType, Job, JobState, OutputAnalysis

logger = logging.getLogger(__name__)


class Notifier:
    """Handles multi-platform notifications via Apprise and QQ Bot."""

    def __init__(self, config: Config, db: Database):
        """Initialize notifier.

        Args:
            config: Configuration.
            db: Database instance.
        """
        self.config = config
        self.db = db
        self._apprise: apprise.Apprise | None = None
        self._qqbot_client: Any = None  # QQBotClient, lazy loaded

    @property
    def apprise(self) -> apprise.Apprise:
        """Get configured Apprise instance."""
        if self._apprise is None:
            self._apprise = apprise.Apprise()
            for url in self.config.notify.urls:
                self._apprise.add(url)
        return self._apprise

    @property
    def qqbot_client(self) -> Any:
        """Get QQ Bot client instance (lazy loaded)."""
        if self._qqbot_client is None and self.config.qqbot.enabled:
            from slurm_watchdog.qqbot import QQBotClient

            if self.config.qqbot.app_id and self.config.qqbot.client_secret:
                self._qqbot_client = QQBotClient(
                    app_id=self.config.qqbot.app_id,
                    client_secret=self.config.qqbot.client_secret,
                )
        return self._qqbot_client

    def has_qqbot_configured(self) -> bool:
        """Check if QQ Bot is configured and enabled.

        Returns:
            True if QQ Bot is enabled with valid credentials.
        """
        qqbot = self.config.qqbot
        return (
            qqbot.enabled
            and bool(qqbot.app_id)
            and bool(qqbot.client_secret)
            and (bool(qqbot.notify_groups) or bool(qqbot.notify_users))
        )

    async def notify_event_qqbot(
        self,
        job: Job,
        event_type: EventType,
        analysis: OutputAnalysis | None = None,
    ) -> bool:
        """Send notification via QQ Bot.

        Args:
            job: Job information.
            event_type: Event type.
            analysis: Optional output analysis.

        Returns:
            True if notification was sent successfully.
        """
        if not self.has_qqbot_configured():
            return True  # Not configured, skip silently

        client = self.qqbot_client
        if client is None:
            return True

        from slurm_watchdog.qqbot import format_job_notification, QQBotError

        message = format_job_notification(job, event_type, analysis)
        success = True

        # Send to groups
        for group_openid in self.config.qqbot.notify_groups:
            try:
                await client.send_group_message(group_openid, message)
                logger.debug("QQ Bot notification sent to group %s", group_openid)
            except QQBotError as e:
                logger.warning("Failed to send QQ Bot message to group %s: %s", group_openid, e)
                success = False

        # Send to users
        for user_openid in self.config.qqbot.notify_users:
            try:
                await client.send_private_message(user_openid, message)
                logger.debug("QQ Bot notification sent to user %s", user_openid)
            except QQBotError as e:
                logger.warning("Failed to send QQ Bot message to user %s: %s", user_openid, e)
                success = False

        return success

    def notify_event_qqbot_sync(
        self,
        job: Job,
        event_type: EventType,
        analysis: OutputAnalysis | None = None,
    ) -> bool:
        """Synchronous wrapper for QQ Bot notification.

        Args:
            job: Job information.
            event_type: Event type.
            analysis: Optional output analysis.

        Returns:
            True if notification was sent successfully.
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Create a new event loop if current one is running
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self.notify_event_qqbot(job, event_type, analysis),
                    )
                    return future.result(timeout=30)
            else:
                return loop.run_until_complete(
                    self.notify_event_qqbot(job, event_type, analysis)
                )
        except Exception as e:
            logger.warning("Failed to send QQ Bot notification: %s", e)
            return False

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
            EventType.JOB_BOOT_FAIL: self.config.notify.on_job_boot_fail,
            EventType.JOB_OUT_OF_MEMORY: self.config.notify.on_job_out_of_memory,
            EventType.JOB_LOST: self.config.notify.on_job_lost,
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
            EventType.JOB_BOOT_FAIL: "🧨",
            EventType.JOB_OUT_OF_MEMORY: "🧠",
            EventType.JOB_LOST: "❓",
        }
        emoji = emoji_map.get(event_type, "📢")

        return f"{emoji} Slurm Job {event_name}: {job_name}"

    def _format_body(
        self,
        job: Job,
        analysis: OutputAnalysis | None = None,
    ) -> str:
        """Format notification body.

        Args:
            job: Job information.
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
        analysis: OutputAnalysis | None = None,
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
        body = self._format_body(job, analysis)

        # Track success across all notification channels
        all_success = True
        errors = []

        # Send via Apprise (if configured)
        if self.config.notify.urls:
            try:
                success = self.apprise.notify(
                    title=title,
                    body=body,
                )
                if not success:
                    all_success = False
                    errors.append("Apprise returned False")
            except Exception as e:
                all_success = False
                errors.append(f"Apprise: {e}")
                logger.warning("Apprise notification failed: %s", e)

        # Send via QQ Bot (if configured)
        if self.has_qqbot_configured():
            qqbot_success = self.notify_event_qqbot_sync(job, event.event_type, analysis)
            if not qqbot_success:
                all_success = False
                errors.append("QQ Bot notification failed")

        # Update event status
        if all_success or not self.config.notify.urls and not self.has_qqbot_configured():
            self.db.mark_event_sent(event.id)
            return True
        else:
            error_msg = "; ".join(errors) if errors else "Unknown error"
            self.db.mark_event_failed(event.id, error_msg)
            return False

    def _analyze_job_output(self, job: Job) -> OutputAnalysis | None:
        """Analyze terminal job output when configured and available."""
        if not (
            job.output_file
            and job.state.is_terminal()
            and self.config.output_analysis.enabled
        ):
            return None

        analyzer = OutputAnalyzer(self.config)
        try:
            return analyzer.analyze(job.output_file)
        except Exception as exc:
            logger.warning("Failed to analyze output file for job %s: %s", job.job_id, exc)
            return None

    def test_notify(self, message: str = "Test notification from Slurm Watchdog") -> bool:
        """Send a test notification.

        Args:
            message: Test message to send.

        Returns:
            True if successful.
        """
        success = True

        # Test Apprise notifications
        if self.config.notify.urls:
            try:
                apprise_success = self.apprise.notify(
                    title="🧪 Slurm Watchdog Test",
                    body=message,
                )
                if not apprise_success:
                    success = False
            except Exception as e:
                logger.warning("Apprise test notification failed: %s", e)
                success = False
        else:
            logger.warning("No Apprise notification URLs configured")

        # Test QQ Bot notifications
        if self.has_qqbot_configured():
            try:
                qqbot_success = self.notify_event_qqbot_sync(
                    job=Job(
                        job_id="test",
                        user="test",
                        state=JobState.COMPLETED,
                    ),
                    event_type=EventType.JOB_COMPLETED,
                )
                if not qqbot_success:
                    success = False
            except Exception as e:
                logger.warning("QQ Bot test notification failed: %s", e)
                success = False

        return success

    def process_pending_events(self) -> tuple[int, int]:
        """Process all pending notification events.

        Returns:
            Tuple of (success_count, failure_count).
        """
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
            analysis = self._analyze_job_output(job)

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
            analysis = self._analyze_job_output(job)

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
