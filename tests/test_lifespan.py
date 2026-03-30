"""
tests/test_lifespan.py

Unit tests for daphne.lifespan.LifespanHandler.

These tests are pure asyncio — no Twisted reactor required. They exercise
the handler directly by passing in small async ASGI app callables and
asserting on the messages exchanged and the handler's resulting state.
"""

import asyncio
import logging

import pytest

from daphne.lifespan import LifespanHandler

# ---------------------------------------------------------------------------
# Reusable ASGI lifespan app callables
# ---------------------------------------------------------------------------


async def lifecycle_app(scope, receive, send):
    """Fully-compliant app: handles startup and shutdown cleanly."""
    event = await receive()
    assert event["type"] == "lifespan.startup"
    await send({"type": "lifespan.startup.complete"})
    event = await receive()
    assert event["type"] == "lifespan.shutdown"
    await send({"type": "lifespan.shutdown.complete"})


async def startup_failed_app(scope, receive, send):
    """App that deliberately signals a startup failure."""
    await receive()
    await send({"type": "lifespan.startup.failed", "message": "startup boom"})


async def raises_before_receive_app(scope, receive, send):
    """App that raises before consuming any events — lifespan not supported."""
    raise NotImplementedError("this app does not support lifespan")


async def exits_without_response_app(scope, receive, send):
    """App that consumes startup but returns without sending any response."""
    await receive()  # consumes lifespan.startup, then exits silently


async def shutdown_failed_app(scope, receive, send):
    """App that completes startup but signals a shutdown failure."""
    await receive()
    await send({"type": "lifespan.startup.complete"})
    await receive()
    await send({"type": "lifespan.shutdown.failed", "message": "shutdown boom"})


async def no_shutdown_response_app(scope, receive, send):
    """App that completes startup but never responds to shutdown."""
    await receive()
    await send({"type": "lifespan.startup.complete"})
    await receive()
    await asyncio.sleep(9999)  # hangs — simulates a frozen shutdown handler


async def unexpected_startup_message_app(scope, receive, send):
    """App that sends an unrecognised message type during startup."""
    await receive()
    await send({"type": "lifespan.unexpected"})
    # Keep running so shutdown can proceed
    await receive()
    await send({"type": "lifespan.shutdown.complete"})


# ---------------------------------------------------------------------------
# Startup tests
# ---------------------------------------------------------------------------


class TestLifespanHandlerStartup:
    @pytest.mark.asyncio
    async def test_startup_passes_correct_scope(self):
        """startup() calls the application with the ASGI lifespan scope."""
        received_scope = None

        async def capturing_app(scope, receive, send):
            nonlocal received_scope
            received_scope = scope
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

        handler = LifespanHandler(capturing_app)
        await handler.startup()
        await handler.shutdown()

        assert received_scope == {"type": "lifespan", "asgi": {"version": "3.0"}}

    @pytest.mark.asyncio
    async def test_startup_sends_startup_event(self):
        """startup() feeds a lifespan.startup event into the application."""
        received_events = []

        async def capturing_app(scope, receive, send):
            received_events.append(await receive())
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

        handler = LifespanHandler(capturing_app)
        await handler.startup()
        await handler.shutdown()

        assert received_events[0] == {"type": "lifespan.startup"}

    @pytest.mark.asyncio
    async def test_startup_complete_returns_normally(self):
        """startup() returns without raising when the app sends startup.complete."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()  # must not raise
        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_startup_complete_marks_handler_supported(self):
        """After startup.complete the handler is marked as supported."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()

        assert handler._supported is True

        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_startup_complete_task_stays_alive(self):
        """After startup.complete the lifespan task is still running for shutdown."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()

        assert handler._task is not None
        assert not handler._task.done()

        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_startup_failed_raises_runtime_error(self):
        """startup() raises RuntimeError when the app sends startup.failed."""
        handler = LifespanHandler(startup_failed_app)

        with pytest.raises(RuntimeError, match="startup boom"):
            await handler.startup()

    @pytest.mark.asyncio
    async def test_startup_failed_message_included_in_error(self):
        """The startup.failed message is propagated in the RuntimeError."""
        handler = LifespanHandler(startup_failed_app)

        with pytest.raises(RuntimeError) as exc_info:
            await handler.startup()

        assert "startup boom" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_startup_fallback_when_app_raises(self):
        """startup() falls back gracefully if the app raises before receiving."""
        handler = LifespanHandler(raises_before_receive_app)
        await handler.startup()  # must not raise

        assert handler._supported is False
        assert handler._task is None

    @pytest.mark.asyncio
    async def test_startup_fallback_when_app_exits_silently(self):
        """startup() falls back gracefully if the app exits without responding."""
        handler = LifespanHandler(exits_without_response_app)
        await handler.startup()  # must not raise

        assert handler._supported is False
        assert handler._task is None

    @pytest.mark.asyncio
    async def test_startup_unexpected_message_logs_warning(self, caplog):
        """startup() logs a warning when the app sends an unrecognised message type."""
        handler = LifespanHandler(unexpected_startup_message_app)

        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            await handler.startup()

        assert "Unexpected lifespan startup message type" in caplog.text
        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_startup_fallback_when_task_cancelled(self):
        """startup() falls back gracefully when the app task is cancelled mid-startup."""

        async def hangs_before_responding(scope, receive, send):
            await receive()  # consumes lifespan.startup
            await asyncio.sleep(9999)  # never sends a response

        handler = LifespanHandler(hangs_before_responding)
        startup_coro = asyncio.ensure_future(handler.startup())

        # Let startup() run until it is suspended on startup_future
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        # Cancel the app task from outside while startup() is still waiting
        assert handler._task is not None
        handler._task.cancel()

        # startup() should now complete cleanly with supported=False
        await startup_coro

        assert handler._supported is False
        assert handler._task is None

    @pytest.mark.asyncio
    async def test_startup_failed_cancels_still_running_task(self):
        """When startup.failed is sent by a task that keeps running, the task is cancelled."""

        async def startup_failed_then_hangs(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.failed", "message": "startup boom"})
            await asyncio.sleep(9999)  # task stays alive after sending failed

        handler = LifespanHandler(startup_failed_then_hangs)

        with pytest.raises(RuntimeError, match="startup boom"):
            await handler.startup()

        # The still-running task should have been cancelled by startup()
        assert handler._task is None or handler._task.done()


# ---------------------------------------------------------------------------
# Shutdown tests
# ---------------------------------------------------------------------------


class TestLifespanHandlerShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_sends_shutdown_event(self):
        """shutdown() feeds a lifespan.shutdown event into the application."""
        received_events = []

        async def capturing_app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            received_events.append(await receive())
            await send({"type": "lifespan.shutdown.complete"})

        handler = LifespanHandler(capturing_app)
        await handler.startup()
        await handler.shutdown()

        assert received_events[0] == {"type": "lifespan.shutdown"}

    @pytest.mark.asyncio
    async def test_shutdown_complete_returns_normally(self):
        """shutdown() returns without raising when the app sends shutdown.complete."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()
        await handler.shutdown()  # must not raise

    @pytest.mark.asyncio
    async def test_shutdown_failed_does_not_raise(self, caplog):
        """shutdown() does not raise when the app sends shutdown.failed."""
        handler = LifespanHandler(shutdown_failed_app)
        await handler.startup()

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await handler.shutdown()  # must not raise

    @pytest.mark.asyncio
    async def test_shutdown_failed_logs_error(self, caplog):
        """shutdown() logs the failure message when app sends shutdown.failed."""
        handler = LifespanHandler(shutdown_failed_app)
        await handler.startup()

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await handler.shutdown()

        assert "shutdown boom" in caplog.text

    @pytest.mark.asyncio
    async def test_shutdown_timeout_does_not_raise(self):
        """shutdown() does not raise when the app fails to respond in time."""
        handler = LifespanHandler(no_shutdown_response_app, shutdown_timeout=0.05)
        await handler.startup()
        await handler.shutdown()  # must not raise despite timeout

    @pytest.mark.asyncio
    async def test_shutdown_timeout_logs_error(self, caplog):
        """shutdown() logs a timeout error when the app fails to respond in time."""
        handler = LifespanHandler(no_shutdown_response_app, shutdown_timeout=0.05)
        await handler.startup()

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await handler.shutdown()

        assert "timed out" in caplog.text

    @pytest.mark.asyncio
    async def test_shutdown_skipped_when_not_supported(self):
        """shutdown() returns immediately when lifespan is not supported."""
        handler = LifespanHandler(raises_before_receive_app)
        await handler.startup()

        assert handler._supported is False
        await handler.shutdown()  # must return immediately without error

    @pytest.mark.asyncio
    async def test_shutdown_skipped_when_never_started(self):
        """shutdown() returns immediately when startup was never called."""
        handler = LifespanHandler(lifecycle_app)
        await handler.shutdown()  # must not hang or raise

    @pytest.mark.asyncio
    async def test_shutdown_skipped_when_task_already_done(self):
        """shutdown() returns immediately when the lifespan task is already done."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()

        handler._task.cancel()
        try:
            await handler._task
        except asyncio.CancelledError:
            pass

        await handler.shutdown()  # must not hang or raise

    @pytest.mark.asyncio
    async def test_shutdown_unexpected_message_logs_warning(self, caplog):
        """shutdown() logs a warning for an unrecognised shutdown message type."""

        async def unexpected_shutdown_app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"type": "lifespan.unexpected"})

        handler = LifespanHandler(unexpected_shutdown_app)
        await handler.startup()

        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            await handler.shutdown()

        assert "Unexpected lifespan shutdown message type" in caplog.text

    @pytest.mark.asyncio
    async def test_shutdown_exception_logged(self, caplog):
        """shutdown() logs unexpected exceptions without raising."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()

        # Force an unexpected error by breaking the send queue
        handler._send_queue = None  # will raise AttributeError on .get()

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await handler.shutdown()  # must not raise

        assert "Lifespan shutdown error" in caplog.text

    @pytest.mark.asyncio
    async def test_on_task_done_logs_unexpected_mid_run_exception(self, caplog):
        """_on_task_done logs an error when the task raises unexpectedly after startup."""

        async def crashes_after_startup(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            raise RuntimeError("mid-run crash")

        handler = LifespanHandler(crashes_after_startup)
        await handler.startup()

        # Give the event loop a tick so the task exception fires _on_task_done
        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await asyncio.sleep(0)

        assert "unexpected exception" in caplog.text
