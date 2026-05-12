"""QQ Bot webhook server with ed25519 signature verification."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from typing import Any

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

# Try to import nacl for ed25519 signature verification
try:
    from nacl.signing import VerifyKey
    from nacl.exceptions import BadSignatureError

    NACL_AVAILABLE = True
except ImportError:
    NACL_AVAILABLE = False
    VerifyKey = None  # type: ignore
    BadSignatureError = Exception  # type: ignore


class SignatureVerificationError(Exception):
    """Signature verification failed."""

    pass


class WebhookServer:
    """QQ Bot webhook server."""

    def __init__(
        self,
        handler: Any,
        host: str = "0.0.0.0",
        port: int = 8080,
        path: str = "/qqbot/callback",
    ) -> None:
        """Initialize webhook server.

        Args:
            handler: Event handler with handle_event method.
            host: Listen host.
            port: Listen port.
            path: Callback path.
        """
        self.handler = handler
        self.host = host
        self.port = port
        self.path = path
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    async def handle_callback(self, request: web.Request) -> web.Response:
        """Handle incoming webhook callback.

        Args:
            request: HTTP request.

        Returns:
            HTTP response.
        """
        try:
            # Read raw body for signature verification
            body = await request.read()

            # Parse JSON
            try:
                event = json.loads(body)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON in callback: %s", e)
                return web.Response(status=400, text="Invalid JSON")

            # Log event (without sensitive data)
            op = event.get("op")
            logger.debug("Received webhook event: op=%s", op)

            # Handle event
            response = await self.handler.handle_event(event)

            if response is not None:
                return web.json_response(response)
            return web.Response(status=204)

        except Exception as e:
            logger.error("Error handling callback: %s", e, exc_info=True)
            return web.Response(status=500, text="Internal error")

    async def start(self) -> None:
        """Start the webhook server."""
        self._app = web.Application()
        self._app.router.add_post(self.path, self.handle_callback)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()

        logger.info("QQ Bot webhook server started on %s:%s%s", self.host, self.port, self.path)

    async def stop(self) -> None:
        """Stop the webhook server."""
        if self._site:
            await self._site.stop()
            self._site = None

        if self._runner:
            await self._runner.cleanup()
            self._runner = None

        self._app = None
        logger.info("QQ Bot webhook server stopped")


def verify_ed25519_signature(
    public_key: str,
    body: bytes,
    signature: str,
    timestamp: str,
) -> bool:
    """Verify ed25519 signature.

    Args:
        public_key: Base64-encoded public key.
        body: Raw request body.
        signature: Base64-encoded signature.
        timestamp: Request timestamp.

    Returns:
        True if signature is valid.

    Raises:
        SignatureVerificationError: If verification fails or nacl not available.
    """
    if not NACL_AVAILABLE:
        raise SignatureVerificationError(
            "ed25519 signature verification requires the 'pynacl' package. "
            "Install it with: pip install pynacl"
        )

    try:
        # Decode public key
        pk_bytes = base64.b64decode(public_key)
        verify_key = VerifyKey(pk_bytes)

        # Decode signature
        sig_bytes = base64.b64decode(signature)

        # Construct message: timestamp + body
        message = timestamp.encode() + body

        # Verify signature
        verify_key.verify(message, sig_bytes)
        return True

    except Exception as e:
        raise SignatureVerificationError(f"Signature verification failed: {e}") from e


class QQBotWebhookServer(WebhookServer):
    """QQ Bot webhook server with ed25519 signature verification."""

    def __init__(
        self,
        handler: Any,
        host: str = "0.0.0.0",
        port: int = 8080,
        path: str = "/qqbot/callback",
        public_key: str | None = None,
        verify_signature: bool = True,
    ) -> None:
        """Initialize webhook server.

        Args:
            handler: Event handler.
            host: Listen host.
            port: Listen port.
            path: Callback path.
            public_key: ed25519 public key for signature verification.
            verify_signature: Whether to verify signatures.
        """
        super().__init__(handler, host, port, path)
        self.public_key = public_key
        self.verify_signature = verify_signature

    async def handle_callback(self, request: web.Request) -> web.Response:
        """Handle incoming webhook callback with signature verification.

        Args:
            request: HTTP request.

        Returns:
            HTTP response.
        """
        try:
            # Read raw body
            body = await request.read()

            # Signature verification (if enabled)
            if self.verify_signature and self.public_key:
                signature = request.headers.get("X-Signature-Ed25519", "")
                timestamp = request.headers.get("X-Signature-Timestamp", "")

                if not signature or not timestamp:
                    logger.warning("Missing signature headers")
                    return web.Response(status=401, text="Missing signature")

                try:
                    verify_ed25519_signature(
                        self.public_key,
                        body,
                        signature,
                        timestamp,
                    )
                except SignatureVerificationError as e:
                    logger.warning("Signature verification failed: %s", e)
                    return web.Response(status=401, text="Invalid signature")

            # Parse JSON
            try:
                event = json.loads(body)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON in callback: %s", e)
                return web.Response(status=400, text="Invalid JSON")

            # Handle callback verification (op=13)
            op = event.get("op")
            if op == 13:
                # Ping/heartbeat - just respond
                logger.debug("Received ping (op=13)")
                return web.json_response({"op": 0, "d": {}})

            # Log event
            data = event.get("d", {})
            event_type = data.get("t", "unknown")
            logger.info("Received webhook event: op=%s, type=%s", op, event_type)

            # Handle event
            response = await self.handler.handle_event(event)

            if response is not None:
                return web.json_response(response)
            return web.Response(status=204)

        except Exception as e:
            logger.error("Error handling callback: %s", e, exc_info=True)
            return web.Response(status=500, text="Internal error")


async def run_qqbot_server(
    handler: Any,
    config: Any,
) -> QQBotWebhookServer:
    """Run QQ Bot webhook server.

    Args:
        handler: Event handler.
        config: Configuration with qqbot settings.

    Returns:
        Running server instance.
    """
    qqbot_config = config.qqbot

    server = QQBotWebhookServer(
        handler=handler,
        host=qqbot_config.callback_host,
        port=qqbot_config.callback_port,
        path=qqbot_config.callback_path,
        verify_signature=False,  # Set to True when public key is configured
    )

    await server.start()
    return server
