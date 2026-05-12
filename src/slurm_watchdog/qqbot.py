"""QQ Bot integration for Slurm Watchdog."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# QQ Bot API endpoints
TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
API_BASE_URL = "https://api.sgroup.qq.com"

# Token refresh buffer (5 minutes before expiry)
TOKEN_REFRESH_BUFFER = 300


class QQBotError(Exception):
    """Base exception for QQ Bot errors."""

    pass


class TokenRefreshError(QQBotError):
    """Error refreshing access token."""

    pass


class MessageSendError(QQBotError):
    """Error sending message."""

    pass


class RateLimitError(QQBotError):
    """Rate limit exceeded."""

    pass


@dataclass
class TokenInfo:
    """Access token information."""

    access_token: str
    expires_at: float  # Unix timestamp

    def is_expired(self, buffer: float = TOKEN_REFRESH_BUFFER) -> bool:
        """Check if token is expired or about to expire."""
        return time.time() >= (self.expires_at - buffer)


@dataclass
class RateLimiter:
    """Token bucket rate limiter."""

    capacity: int = 5
    refill_rate: float = 1.0  # Tokens per second
    _tokens: float = field(default=0.0, init=False)
    _last_refill: float = field(default_factory=time.time, init=False)

    def __post_init__(self) -> None:
        self._tokens = float(self.capacity)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if successful."""
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def wait_time(self, tokens: int = 1) -> float:
        """Get time to wait before tokens are available."""
        self._refill()
        if self._tokens >= tokens:
            return 0.0
        needed = tokens - self._tokens
        return needed / self.refill_rate


class QQBotClient:
    """Client for QQ Bot API interactions."""

    def __init__(
        self,
        app_id: str,
        client_secret: str,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        """Initialize QQ Bot client.

        Args:
            app_id: QQ Bot application ID.
            client_secret: QQ Bot client secret.
            session: Optional aiohttp session to use.
        """
        self.app_id = app_id
        self.client_secret = client_secret
        self._session = session
        self._owned_session = session is None
        self._token: TokenInfo | None = None
        self._token_lock = asyncio.Lock()
        self._rate_limiter = RateLimiter()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._owned_session = True
        return self._session

    async def close(self) -> None:
        """Close the HTTP session if we own it."""
        if self._owned_session and self._session and not self._session.closed:
            await self._session.close()

    async def _refresh_token(self) -> str:
        """Refresh access token.

        Returns:
            New access token.

        Raises:
            TokenRefreshError: If token refresh fails.
        """
        session = await self._get_session()
        payload = {
            "appId": self.app_id,
            "clientSecret": self.client_secret,
        }

        try:
            async with session.post(TOKEN_URL, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise TokenRefreshError(f"Token refresh failed: {resp.status} - {text}")

                data = await resp.json()
                access_token = data.get("access_token")
                expires_in = data.get("expires_in", 7200)

                if not access_token:
                    raise TokenRefreshError("No access_token in response")

                self._token = TokenInfo(
                    access_token=access_token,
                    expires_at=time.time() + expires_in,
                )
                logger.debug("QQ Bot token refreshed, expires in %ds", expires_in)
                return access_token

        except aiohttp.ClientError as e:
            raise TokenRefreshError(f"Network error during token refresh: {e}") from e

    async def get_access_token(self) -> str:
        """Get valid access token, refreshing if necessary.

        Returns:
            Valid access token.
        """
        async with self._token_lock:
            if self._token is None or self._token.is_expired():
                return await self._refresh_token()
            return self._token.access_token

    def _get_headers(self, token: str) -> dict[str, str]:
        """Get API request headers."""
        return {
            "Authorization": f"QQBot {token}",
            "Content-Type": "application/json",
        }

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make API request with automatic token refresh on 401.

        Args:
            method: HTTP method.
            url: Request URL.
            **kwargs: Additional request arguments.

        Returns:
            Response JSON data.
        """
        session = await self._get_session()
        token = await self.get_access_token()
        headers = self._get_headers(token)
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        async with session.request(method, url, headers=headers, **kwargs) as resp:
            if resp.status == 401:
                # Token expired, refresh and retry
                logger.debug("Token expired, refreshing...")
                token = await self._refresh_token()
                headers = self._get_headers(token)
                async with session.request(method, url, headers=headers, **kwargs) as retry_resp:
                    return await self._handle_response(retry_resp)

            return await self._handle_response(resp)

    async def _handle_response(self, resp: aiohttp.ClientResponse) -> dict[str, Any]:
        """Handle API response.

        Args:
            resp: HTTP response.

        Returns:
            Response JSON data.

        Raises:
            MessageSendError: If request fails.
            RateLimitError: If rate limited.
        """
        if resp.status == 429:
            raise RateLimitError("Rate limit exceeded")

        if resp.status not in (200, 201, 204):
            text = await resp.text()
            raise MessageSendError(f"API error: {resp.status} - {text}")

        if resp.status == 204:
            return {}

        return await resp.json()

    async def send_group_message(
        self,
        group_openid: str,
        content: str,
        msg_type: int = 0,
        msg_id: str | None = None,
    ) -> dict[str, Any]:
        """Send message to group.

        Args:
            group_openid: Group Open ID.
            content: Message content.
            msg_type: Message type (0=text, 1=markdown, 2=ark).
            msg_id: Message ID for reply (from callback).

        Returns:
            API response data.
        """
        # Rate limiting
        wait_time = self._rate_limiter.wait_time()
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        if not self._rate_limiter.consume():
            raise RateLimitError("Rate limit exceeded")

        url = f"{API_BASE_URL}/v2/groups/{group_openid}/messages"
        payload: dict[str, Any] = {
            "content": content,
            "msg_type": msg_type,
        }
        if msg_id:
            payload["msg_id"] = msg_id

        return await self._request_with_retry("POST", url, json=payload)

    async def send_private_message(
        self,
        openid: str,
        content: str,
        msg_type: int = 0,
        msg_id: str | None = None,
    ) -> dict[str, Any]:
        """Send private message to user.

        Args:
            openid: User Open ID.
            content: Message content.
            msg_type: Message type (0=text, 1=markdown, 2=ark).
            msg_id: Message ID for reply (from callback).

        Returns:
            API response data.
        """
        # Rate limiting
        wait_time = self._rate_limiter.wait_time()
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        if not self._rate_limiter.consume():
            raise RateLimitError("Rate limit exceeded")

        url = f"{API_BASE_URL}/v2/users/{openid}/messages"
        payload: dict[str, Any] = {
            "content": content,
            "msg_type": msg_type,
        }
        if msg_id:
            payload["msg_id"] = msg_id

        return await self._request_with_retry("POST", url, json=payload)


class CommandHandler(ABC):
    """Abstract base class for command handlers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Command name."""
        pass

    @property
    @abstractmethod
    def aliases(self) -> list[str]:
        """Command aliases."""
        pass

    @property
    def description(self) -> str:
        """Command description."""
        return ""

    @abstractmethod
    async def execute(
        self,
        args: str,
        context: CommandContext,
    ) -> str:
        """Execute command.

        Args:
            args: Command arguments.
            context: Execution context.

        Returns:
            Response message.
        """
        pass


@dataclass
class CommandContext:
    """Context for command execution."""

    user_openid: str
    group_openid: str | None = None
    is_group: bool = False
    db: Any = None  # Database instance
    config: Any = None  # Config instance
    client: QQBotClient | None = None


class HelpCommand(CommandHandler):
    """Help command showing available commands."""

    def __init__(self, commands: list[CommandHandler]) -> None:
        self._commands = commands

    @property
    def name(self) -> str:
        return "help"

    @property
    def aliases(self) -> list[str]:
        return ["帮助", "?"]

    @property
    def description(self) -> str:
        return "Show available commands"

    async def execute(self, args: str, context: CommandContext) -> str:
        lines = ["Available commands:"]
        for cmd in self._commands:
            aliases = f" ({', '.join(cmd.aliases)})" if cmd.aliases else ""
            lines.append(f"  {cmd.name}{aliases}: {cmd.description}")
        return "\n".join(lines)


class StatusCommand(CommandHandler):
    """Query running/pending jobs."""

    @property
    def name(self) -> str:
        return "status"

    @property
    def aliases(self) -> list[str]:
        return ["状态"]

    @property
    def description(self) -> str:
        return "Query running/pending jobs"

    async def execute(self, args: str, context: CommandContext) -> str:
        from slurm_watchdog.models import JobState

        if context.db is None:
            return "Database not available"

        running_jobs = context.db.get_jobs_by_state(JobState.RUNNING)
        pending_jobs = context.db.get_jobs_by_state(JobState.PENDING)

        lines = [
            f"Running: {len(running_jobs)}",
            f"Pending: {len(pending_jobs)}",
        ]

        if running_jobs:
            lines.append("\nRunning jobs:")
            for job in running_jobs[:10]:
                name = job.name or job.job_id
                elapsed = job.elapsed_time or "N/A"
                lines.append(f"  {name} ({job.job_id}) - {elapsed}")

        if pending_jobs:
            lines.append("\nPending jobs:")
            for job in pending_jobs[:10]:
                name = job.name or job.job_id
                reason = f" ({job.reason})" if job.reason else ""
                lines.append(f"  {name} ({job.job_id}){reason}")

        return "\n".join(lines)


class HistoryCommand(CommandHandler):
    """Query recent completed jobs."""

    @property
    def name(self) -> str:
        return "history"

    @property
    def aliases(self) -> list[str]:
        return ["历史"]

    @property
    def description(self) -> str:
        return "Query recent completed jobs (default: 5)"

    async def execute(self, args: str, context: CommandContext) -> str:
        from slurm_watchdog.models import JobState

        if context.db is None:
            return "Database not available"

        try:
            limit = int(args.strip()) if args.strip() else 5
            limit = min(limit, 20)  # Cap at 20
        except ValueError:
            limit = 5

        # Get terminal state jobs
        completed = context.db.get_jobs_by_state(JobState.COMPLETED)
        failed = context.db.get_jobs_by_state(JobState.FAILED)
        cancelled = context.db.get_jobs_by_state(JobState.CANCELLED)
        timeout = context.db.get_jobs_by_state(JobState.TIMEOUT)

        all_terminal = completed + failed + cancelled + timeout
        # Sort by end time descending
        all_terminal.sort(key=lambda j: j.end_time or datetime.min, reverse=True)
        recent = all_terminal[:limit]

        if not recent:
            return f"No completed jobs found (last {limit})"

        lines = [f"Recent {len(recent)} completed jobs:"]
        for job in recent:
            name = job.name or job.job_id
            state = job.state.value
            elapsed = job.elapsed_time or "N/A"
            lines.append(f"  {name} ({job.job_id}) - {state} - {elapsed}")

        return "\n".join(lines)


class JobCommand(CommandHandler):
    """Query specific job details."""

    @property
    def name(self) -> str:
        return "job"

    @property
    def aliases(self) -> list[str]:
        return ["作业"]

    @property
    def description(self) -> str:
        return "Query specific job details: job <id>"

    async def execute(self, args: str, context: CommandContext) -> str:
        if context.db is None:
            return "Database not available"

        job_id = args.strip()
        if not job_id:
            return "Usage: job <job_id>"

        job = context.db.get_job(job_id)
        if not job:
            return f"Job {job_id} not found"

        lines = [
            f"Job: {job.name or 'N/A'} ({job.job_id})",
            f"State: {job.state.value}",
            f"User: {job.user}",
        ]

        if job.partition:
            lines.append(f"Partition: {job.partition}")
        if job.exit_code:
            lines.append(f"Exit Code: {job.exit_code}")
        if job.submit_time:
            lines.append(f"Submitted: {job.submit_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.start_time:
            lines.append(f"Started: {job.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.end_time:
            lines.append(f"Ended: {job.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if job.elapsed_time:
            lines.append(f"Elapsed: {job.elapsed_time}")
        if job.max_rss:
            lines.append(f"Max RSS: {job.max_rss}")
        if job.reason and not job.state.is_success():
            lines.append(f"Reason: {job.reason}")
        if job.output_file:
            lines.append(f"Output: {job.output_file}")

        return "\n".join(lines)


class TestCommand(CommandHandler):
    """Test bot connection."""

    @property
    def name(self) -> str:
        return "test"

    @property
    def aliases(self) -> list[str]:
        return ["测试"]

    @property
    def description(self) -> str:
        return "Test bot connection"

    async def execute(self, args: str, context: CommandContext) -> str:
        return f"Pong! Bot is working. Your openid: {context.user_openid}"


class QQBotCommandProcessor:
    """Process and route bot commands."""

    def __init__(
        self,
        db: Any = None,
        config: Any = None,
        client: QQBotClient | None = None,
    ) -> None:
        """Initialize command processor.

        Args:
            db: Database instance.
            config: Configuration instance.
            client: QQ Bot client.
        """
        self.db = db
        self.config = config
        self.client = client
        self._commands: dict[str, CommandHandler] = {}
        self._register_default_commands()

    def _register_default_commands(self) -> None:
        """Register built-in commands."""
        default_commands = [
            StatusCommand(),
            HistoryCommand(),
            JobCommand(),
            TestCommand(),
        ]
        # Register help after other commands are known
        help_cmd = HelpCommand(default_commands)
        all_commands = default_commands + [help_cmd]

        for cmd in all_commands:
            self.register_command(cmd)

    def register_command(self, command: CommandHandler) -> None:
        """Register a command handler.

        Args:
            command: Command handler to register.
        """
        self._commands[command.name.lower()] = command
        for alias in command.aliases:
            self._commands[alias.lower()] = command

    async def process_command(
        self,
        text: str,
        context: CommandContext,
    ) -> str | None:
        """Process command text and return response.

        Args:
            text: Command text (without prefix/at-mention).
            context: Execution context.

        Returns:
            Response message or None if not a command.
        """
        text = text.strip()
        if not text:
            return None

        # Parse command and args
        parts = text.split(maxsplit=1)
        cmd_name = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        # Look up command
        handler = self._commands.get(cmd_name)
        if handler is None:
            return f"Unknown command: {cmd_name}. Type 'help' for available commands."

        try:
            return await handler.execute(args, context)
        except Exception as e:
            logger.error("Command execution failed: %s", e, exc_info=True)
            return f"Command failed: {e}"

    def is_authorized(
        self,
        user_openid: str,
        group_openid: str | None,
    ) -> bool:
        """Check if user/group is authorized.

        Args:
            user_openid: User Open ID.
            group_openid: Group Open ID (if in group).

        Returns:
            True if authorized.
        """
        if self.config is None:
            return True

        qqbot_config = self.config.qqbot
        authorized_users = qqbot_config.authorized_users
        authorized_groups = qqbot_config.authorized_groups

        # Empty lists mean all authorized
        if not authorized_users and not authorized_groups:
            return True

        # Check user authorization
        if user_openid in authorized_users:
            return True

        # Check group authorization (if in group)
        if group_openid and group_openid in authorized_groups:
            return True

        return False


class QQBotWebhookHandler:
    """Handle QQ Bot webhook callbacks."""

    def __init__(
        self,
        client: QQBotClient,
        processor: QQBotCommandProcessor,
        config: Any,
    ) -> None:
        """Initialize webhook handler.

        Args:
            client: QQ Bot client.
            processor: Command processor.
            config: Configuration.
        """
        self.client = client
        self.processor = processor
        self.config = config

    async def handle_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """Handle incoming webhook event.

        Args:
            event: Event data from QQ Bot.

        Returns:
            Response dict or None.
        """
        op = event.get("op")

        # Heartbeat/Hello (op=13)
        if op == 13:
            return {"op": 0, "d": {}}

        # Dispatch event
        if op == 0:
            return await self._handle_dispatch(event)

        return None

    async def _handle_dispatch(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """Handle dispatch event.

        Args:
            event: Dispatch event data.

        Returns:
            Response or None.
        """
        data = event.get("d", {})
        event_type = data.get("t")

        if event_type == "GROUP_AT_MESSAGE_CREATE":
            return await self._handle_group_at_message(data)
        elif event_type == "C2C_MESSAGE_CREATE":
            return await self._handle_c2c_message(data)

        return None

    async def _handle_group_at_message(self, data: dict[str, Any]) -> None:
        """Handle group @ message.

        Args:
            data: Message data.
        """
        group_openid = data.get("group_openid")
        author = data.get("author", {})
        user_openid = author.get("member_openid", "")
        content = data.get("content", "")
        msg_id = data.get("id")

        # Remove @ mention prefix
        content = re.sub(r"<@!\d+>", "", content).strip()

        if not content:
            return

        # Check authorization
        if not self.processor.is_authorized(user_openid, group_openid):
            logger.warning(
                "Unauthorized access by user %s in group %s",
                user_openid,
                group_openid,
            )
            return

        context = CommandContext(
            user_openid=user_openid,
            group_openid=group_openid,
            is_group=True,
            db=self.processor.db,
            config=self.processor.config,
            client=self.client,
        )

        response = await self.processor.process_command(content, context)
        if response:
            try:
                await self.client.send_group_message(
                    group_openid=group_openid,
                    content=response,
                    msg_id=msg_id,
                )
            except QQBotError as e:
                logger.error("Failed to send group message: %s", e)

    async def _handle_c2c_message(self, data: dict[str, Any]) -> None:
        """Handle private message.

        Args:
            data: Message data.
        """
        author = data.get("author", {})
        user_openid = author.get("user_openid", "")
        content = data.get("content", "")
        msg_id = data.get("id")

        content = content.strip()
        if not content:
            return

        # Check authorization
        if not self.processor.is_authorized(user_openid, None):
            logger.warning("Unauthorized access by user %s", user_openid)
            return

        context = CommandContext(
            user_openid=user_openid,
            is_group=False,
            db=self.processor.db,
            config=self.processor.config,
            client=self.client,
        )

        response = await self.processor.process_command(content, context)
        if response:
            try:
                await self.client.send_private_message(
                    openid=user_openid,
                    content=response,
                    msg_id=msg_id,
                )
            except QQBotError as e:
                logger.error("Failed to send private message: %s", e)


def format_job_notification(
    job: Any,
    event_type: Any,
    analysis: Any = None,
) -> str:
    """Format job notification message for QQ.

    Args:
        job: Job information.
        event_type: Event type.
        analysis: Optional output analysis.

    Returns:
        Formatted message.
    """
    job_name = job.name or job.job_id
    event_name = event_type.value.replace("JOB_", "").replace("_", " ").title()

    # Emoji mapping
    emoji_map = {
        "JOB_STARTED": "🚀",
        "JOB_COMPLETED": "✅",
        "JOB_FAILED": "❌",
        "JOB_CANCELLED": "🚫",
        "JOB_TIMEOUT": "⏱️",
        "JOB_PREEMPTED": "⚠️",
        "JOB_NODE_FAIL": "💥",
        "JOB_BOOT_FAIL": "🧨",
        "JOB_OUT_OF_MEMORY": "🧠",
        "JOB_LOST": "❓",
    }
    emoji = emoji_map.get(event_type.value, "📢")

    lines = [
        f"{emoji} {event_name}",
        f"Job: {job_name} ({job.job_id})",
        f"State: {job.state.value}",
    ]

    if job.partition:
        lines.append(f"Partition: {job.partition}")

    if job.elapsed_time:
        lines.append(f"Elapsed: {job.elapsed_time}")

    if job.exit_code:
        lines.append(f"Exit Code: {job.exit_code}")

    if job.reason and not job.state.is_success():
        lines.append(f"Reason: {job.reason}")

    if analysis:
        lines.append(f"Analysis: {analysis.get_summary()}")

    return "\n".join(lines)
