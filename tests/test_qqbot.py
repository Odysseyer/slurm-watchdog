"""Tests for QQ Bot integration."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from slurm_watchdog.models import (
    Config,
    Event,
    EventType,
    Job,
    JobState,
    OutputAnalysis,
    QQBotConfig,
)
from slurm_watchdog.qqbot import (
    CommandContext,
    QQBotClient,
    QQBotCommandProcessor,
    QQBotError,
    RateLimiter,
    TokenInfo,
    format_job_notification,
)


class TestRateLimiter:
    """Tests for RateLimiter."""

    def test_initial_state(self) -> None:
        """Test initial state has full capacity."""
        limiter = RateLimiter(capacity=5, refill_rate=1.0)
        assert limiter.consume(5) is True

    def test_consume_reduces_tokens(self) -> None:
        """Test consuming reduces available tokens."""
        limiter = RateLimiter(capacity=5, refill_rate=1.0)
        assert limiter.consume(3) is True
        assert limiter.consume(3) is False  # Only 2 left

    def test_refill_over_time(self) -> None:
        """Test tokens refill over time."""
        limiter = RateLimiter(capacity=5, refill_rate=10.0)  # 10 tokens/sec
        limiter.consume(5)  # Empty
        assert limiter.consume(1) is False
        # After some time, should have tokens again
        import time

        time.sleep(0.2)  # Should get ~2 tokens
        assert limiter.consume(1) is True

    def test_wait_time(self) -> None:
        """Test wait time calculation."""
        limiter = RateLimiter(capacity=5, refill_rate=1.0)
        limiter.consume(5)  # Empty
        wait = limiter.wait_time(2)
        assert wait > 0
        assert wait < 3  # Should be ~2 seconds


class TestTokenInfo:
    """Tests for TokenInfo."""

    def test_not_expired_initially(self) -> None:
        """Test new token is not expired."""
        import time

        token = TokenInfo(access_token="test", expires_at=time.time() + 7200)
        assert not token.is_expired()

    def test_expired_after_time(self) -> None:
        """Test token is expired after expiry time."""
        import time

        token = TokenInfo(access_token="test", expires_at=time.time() - 1)
        assert token.is_expired()

    def test_expired_with_buffer(self) -> None:
        """Test token is expired when within buffer."""
        import time

        # Token expires in 60 seconds, buffer is 300
        token = TokenInfo(access_token="test", expires_at=time.time() + 60)
        assert token.is_expired(buffer=300)


class TestQQBotClient:
    """Tests for QQBotClient."""

    @pytest.fixture
    def mock_session(self) -> MagicMock:
        """Create mock aiohttp session."""
        session = MagicMock(spec=asyncio.AbstractEventLoop)
        return session

    @pytest.fixture
    def client(self) -> QQBotClient:
        """Create QQ Bot client."""
        return QQBotClient(app_id="test_app", client_secret="test_secret")

    def test_init(self, client: QQBotClient) -> None:
        """Test client initialization."""
        assert client.app_id == "test_app"
        assert client.client_secret == "test_secret"
        assert client._token is None

    @pytest.mark.asyncio
    async def test_refresh_token_success(self, client: QQBotClient) -> None:
        """Test successful token refresh."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"access_token": "new_token", "expires_in": 7200})

        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_post.return_value.__aexit__ = AsyncMock(return_value=None)

            token = await client._refresh_token()
            assert token == "new_token"
            assert client._token is not None
            assert client._token.access_token == "new_token"

    @pytest.mark.asyncio
    async def test_send_group_message(self, client: QQBotClient) -> None:
        """Test sending group message."""
        # Mock token
        client._token = TokenInfo(access_token="test_token", expires_at=9999999999)

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"id": "msg123"})

        with patch("aiohttp.ClientSession.request") as mock_request:
            mock_request.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_request.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await client.send_group_message("group123", "Hello")
            assert result["id"] == "msg123"


class TestQQBotCommandProcessor:
    """Tests for QQBotCommandProcessor."""

    @pytest.fixture
    def mock_db(self) -> MagicMock:
        """Create mock database."""
        db = MagicMock()
        db.get_jobs_by_state.return_value = []
        return db

    @pytest.fixture
    def config(self) -> Config:
        """Create test config."""
        return Config(qqbot=QQBotConfig(enabled=True))

    @pytest.fixture
    def processor(self, mock_db: MagicMock, config: Config) -> QQBotCommandProcessor:
        """Create command processor."""
        return QQBotCommandProcessor(db=mock_db, config=config)

    @pytest.fixture
    def context(self, mock_db: MagicMock, config: Config) -> CommandContext:
        """Create command context."""
        return CommandContext(
            user_openid="user123",
            group_openid="group123",
            is_group=True,
            db=mock_db,
            config=config,
        )

    @pytest.mark.asyncio
    async def test_status_command(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test status command."""
        response = await processor.process_command("status", context)
        assert "Running:" in response
        assert "Pending:" in response

    @pytest.mark.asyncio
    async def test_status_command_alias(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test status command with Chinese alias."""
        response = await processor.process_command("状态", context)
        assert "Running:" in response

    @pytest.mark.asyncio
    async def test_history_command(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test history command."""
        response = await processor.process_command("history", context)
        assert "completed jobs" in response

    @pytest.mark.asyncio
    async def test_job_command_not_found(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test job command with non-existent job."""
        context.db.get_job.return_value = None  # Job not found
        response = await processor.process_command("job 12345", context)
        assert "not found" in response

    @pytest.mark.asyncio
    async def test_job_command_no_args(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test job command without arguments."""
        response = await processor.process_command("job", context)
        assert "Usage:" in response

    @pytest.mark.asyncio
    async def test_test_command(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test test command."""
        response = await processor.process_command("test", context)
        assert "Pong" in response

    @pytest.mark.asyncio
    async def test_help_command(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test help command."""
        response = await processor.process_command("help", context)
        assert "Available commands:" in response

    @pytest.mark.asyncio
    async def test_unknown_command(
        self,
        processor: QQBotCommandProcessor,
        context: CommandContext,
    ) -> None:
        """Test unknown command."""
        response = await processor.process_command("unknowncommand", context)
        assert "Unknown command" in response

    def test_is_authorized_empty_lists(
        self,
        processor: QQBotCommandProcessor,
    ) -> None:
        """Test authorization with empty lists allows all."""
        assert processor.is_authorized("any_user", "any_group") is True

    def test_is_authorized_user_in_list(
        self,
        config: Config,
        mock_db: MagicMock,
    ) -> None:
        """Test authorization with user in list."""
        config.qqbot.authorized_users = ["user123"]
        processor = QQBotCommandProcessor(db=mock_db, config=config)
        assert processor.is_authorized("user123", None) is True
        assert processor.is_authorized("other_user", None) is False

    def test_is_authorized_group_in_list(
        self,
        config: Config,
        mock_db: MagicMock,
    ) -> None:
        """Test authorization with group in list."""
        config.qqbot.authorized_groups = ["group123"]
        processor = QQBotCommandProcessor(db=mock_db, config=config)
        assert processor.is_authorized("any_user", "group123") is True
        assert processor.is_authorized("any_user", "other_group") is False


class TestFormatJobNotification:
    """Tests for format_job_notification."""

    @pytest.fixture
    def job(self) -> Job:
        """Create test job."""
        return Job(
            job_id="12345",
            user="testuser",
            name="test-job",
            partition="gpu",
            state=JobState.COMPLETED,
            elapsed_time="01:23:45",
            exit_code="0:0",
        )

    def test_format_completed_job(self, job: Job) -> None:
        """Test formatting completed job notification."""
        msg = format_job_notification(job, EventType.JOB_COMPLETED)
        assert "✅" in msg
        assert "Completed" in msg
        assert "test-job" in msg
        assert "12345" in msg
        assert "COMPLETED" in msg

    def test_format_failed_job(self, job: Job) -> None:
        """Test formatting failed job notification."""
        job.state = JobState.FAILED
        job.exit_code = "1:0"
        job.reason = "Some error"
        msg = format_job_notification(job, EventType.JOB_FAILED)
        assert "❌" in msg
        assert "Failed" in msg
        assert "Some error" in msg

    def test_format_with_analysis(self, job: Job) -> None:
        """Test formatting with output analysis."""
        analysis = OutputAnalysis(
            converged=True,
            has_errors=False,
            convergence_lines=["Converged successfully"],
        )
        msg = format_job_notification(job, EventType.JOB_COMPLETED, analysis)
        assert "Analysis:" in msg
        assert "Converged: Yes" in msg


class TestQQBotNotifierIntegration:
    """Tests for QQ Bot integration with Notifier."""

    @pytest.fixture
    def config_with_qqbot(self) -> Config:
        """Create config with QQ Bot enabled."""
        return Config(
            qqbot=QQBotConfig(
                enabled=True,
                app_id="test_app",
                client_secret="test_secret",
                notify_groups=["group1", "group2"],
                notify_users=["user1"],
            )
        )

    @pytest.fixture
    def mock_db(self) -> MagicMock:
        """Create mock database."""
        return MagicMock()

    def test_has_qqbot_configured(self, config_with_qqbot: Config, mock_db: MagicMock) -> None:
        """Test checking if QQ Bot is configured."""
        from slurm_watchdog.notifier import Notifier

        notifier = Notifier(config_with_qqbot, mock_db)
        assert notifier.has_qqbot_configured() is True

    def test_has_qqbot_not_configured(self, mock_db: MagicMock) -> None:
        """Test checking if QQ Bot is not configured."""
        from slurm_watchdog.notifier import Notifier

        config = Config(qqbot=QQBotConfig(enabled=False))
        notifier = Notifier(config, mock_db)
        assert notifier.has_qqbot_configured() is False

    @pytest.mark.asyncio
    async def test_notify_event_qqbot(self, config_with_qqbot: Config, mock_db: MagicMock) -> None:
        """Test sending notification via QQ Bot."""
        from slurm_watchdog.notifier import Notifier

        job = Job(
            job_id="12345",
            user="testuser",
            name="test-job",
            state=JobState.COMPLETED,
        )

        notifier = Notifier(config_with_qqbot, mock_db)

        # Mock the QQ Bot client
        mock_client = AsyncMock()
        mock_client.send_group_message = AsyncMock(return_value={"id": "msg1"})
        mock_client.send_private_message = AsyncMock(return_value={"id": "msg2"})
        notifier._qqbot_client = mock_client

        result = await notifier.notify_event_qqbot(job, EventType.JOB_COMPLETED)
        assert result is True

        # Check that messages were sent to all targets
        assert mock_client.send_group_message.call_count == 2
        assert mock_client.send_private_message.call_count == 1
