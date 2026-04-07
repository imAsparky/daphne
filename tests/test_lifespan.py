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

from daphne.lifespan import LifespanHandler, _sanitise_message

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
# _sanitise_message unit tests
# ---------------------------------------------------------------------------


class TestSanitiseMessage:
    def test_non_string_is_coerced_to_str(self):
        """Line 47: non-str values are coerced via str() before any other processing."""
        assert _sanitise_message(42) == "42"
        assert _sanitise_message(3.14) == "3.14"
        assert _sanitise_message(None) == "None"
        assert _sanitise_message(["a", "b"]) == "['a', 'b']"

    def test_exception_object_is_coerced_to_str(self):
        """Line 47: Exception instances (the common real-world non-str case) are coerced."""
        exc = RuntimeError("db://user:password@host/db")
        result = _sanitise_message(exc)
        assert result == "db://user:password@host/db"

    def test_newlines_are_escaped(self):
        """CR and LF are replaced with their escape representations."""
        assert "\\n" in _sanitise_message("line one\nline two")
        assert "\\r" in _sanitise_message("line one\rline two")
        assert "\n" not in _sanitise_message("line one\nline two")
        assert "\r" not in _sanitise_message("line one\rline two")

    def test_ansi_sequences_are_stripped(self):
        """ANSI CSI escape sequences are removed."""
        assert _sanitise_message("\x1b[31mred\x1b[0m") == "red"

    def test_long_message_is_truncated(self):
        """Messages longer than 500 characters are truncated."""
        long = "x" * 1000
        result = _sanitise_message(long)
        assert len(result) == 500

    def test_short_message_is_unchanged(self):
        """Messages within the length limit pass through intact."""
        msg = "normal startup error"
        assert _sanitise_message(msg) == msg


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

        assert received_scope == {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            # state is always provided; it is empty at construction time and
            # populated by the application during its startup hook.
            "state": {},
        }

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
    async def test_startup_timeout_raises_runtime_error(self):
        """startup() raises RuntimeError when the app does not respond within startup_timeout."""

        async def hangs_forever(scope, receive, send):
            await receive()
            await asyncio.sleep(9999)  # never sends startup.complete

        handler = LifespanHandler(hangs_forever, startup_timeout=0.05)

        with pytest.raises(RuntimeError, match="timed out"):
            await handler.startup()

    @pytest.mark.asyncio
    async def test_startup_timeout_cancels_task(self):
        """When startup times out the lifespan task is cancelled and cleaned up."""

        async def hangs_forever(scope, receive, send):
            await receive()
            await asyncio.sleep(9999)

        handler = LifespanHandler(hangs_forever, startup_timeout=0.05)

        with pytest.raises(RuntimeError):
            await handler.startup()

        assert handler._task is None or handler._task.done()

    @pytest.mark.asyncio
    async def test_startup_timeout_logs_error(self, caplog):
        """startup() logs an ERROR when startup times out."""

        async def hangs_forever(scope, receive, send):
            await receive()
            await asyncio.sleep(9999)

        handler = LifespanHandler(hangs_forever, startup_timeout=0.05)

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            with pytest.raises(RuntimeError):
                await handler.startup()

        assert "timed out" in caplog.text

    @pytest.mark.asyncio
    async def test_startup_failed_message_newlines_sanitised(self, caplog):
        """Newlines in startup.failed messages are escaped before logging."""

        async def newline_injection_app(scope, receive, send):
            await receive()
            await send(
                {
                    "type": "lifespan.startup.failed",
                    "message": "line one\nINFO fake injected log line\r\nline two",
                }
            )

        handler = LifespanHandler(newline_injection_app)

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            with pytest.raises(RuntimeError):
                await handler.startup()

        # The raw newline must not appear in the log record
        for record in caplog.records:
            assert "\n" not in record.getMessage()
            assert "\r" not in record.getMessage()
        # The escaped representation should be present instead
        assert "\\n" in caplog.text

    @pytest.mark.asyncio
    async def test_startup_failed_message_truncated(self, caplog):
        """Messages longer than 500 chars in startup.failed are truncated before logging."""

        long_message = "x" * 1000

        async def long_message_app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.failed", "message": long_message})

        handler = LifespanHandler(long_message_app)

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            with pytest.raises(RuntimeError):
                await handler.startup()

        logged = next(
            r.getMessage()
            for r in caplog.records
            if "startup failed" in r.getMessage().lower()
        )
        assert len(logged) < len(long_message)

    @pytest.mark.asyncio
    async def test_shutdown_failed_message_sanitised(self, caplog):
        """Newlines in shutdown.failed messages are escaped before logging."""

        async def newline_shutdown_app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send(
                {
                    "type": "lifespan.shutdown.failed",
                    "message": "line one\nINFO fake injected log line",
                }
            )

        handler = LifespanHandler(newline_shutdown_app)
        await handler.startup()

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await handler.shutdown()

        for record in caplog.records:
            assert "\n" not in record.getMessage()

    @pytest.mark.asyncio
    async def test_unexpected_startup_message_type_sanitised(self, caplog):
        """M-1: Newlines/ANSI in an unexpected startup message type are escaped before logging."""

        async def injecting_type_app(scope, receive, send):
            await receive()
            # Embed a newline in the type string to attempt log injection
            await send({"type": "lifespan.unexpected\nINFO fake injected log line"})
            # Keep running so shutdown can proceed
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

        handler = LifespanHandler(injecting_type_app)

        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            await handler.startup()

        for record in caplog.records:
            assert "\n" not in record.getMessage()
            assert "\r" not in record.getMessage()

        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_startup_message_missing_type_key_logs_warning(self, caplog):
        """M-2 lines 271-276: a dict with no 'type' key during startup logs a warning and continues."""

        async def missing_type_key_app(scope, receive, send):
            await receive()
            # Send a dict with no 'type' key at all
            await send({"message": "oops no type key"})
            # Keep running so shutdown can proceed
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

        handler = LifespanHandler(missing_type_key_app)

        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            await handler.startup()  # must not raise

        warning_messages = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("malformed" in m and "type" in m for m in warning_messages)
        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_message_missing_type_key_logs_warning(self, caplog):
        """M-2: a dict with no 'type' key during shutdown logs a warning and does not raise."""

        async def missing_type_shutdown_app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"message": "oops no type key"})

        handler = LifespanHandler(missing_type_shutdown_app)
        await handler.startup()

        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            await handler.shutdown()  # must not raise

        warning_messages = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("malformed" in m and "type" in m for m in warning_messages)

    @pytest.mark.asyncio
    async def test_unexpected_shutdown_message_type_sanitised(self, caplog):
        """M-1: Newlines/ANSI in an unexpected shutdown message type are escaped before logging."""

        async def injecting_shutdown_type_app(scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"type": "lifespan.unexpected\nINFO fake injected shutdown log"})

        handler = LifespanHandler(injecting_shutdown_type_app)
        await handler.startup()

        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            await handler.shutdown()

        for record in caplog.records:
            assert "\n" not in record.getMessage()
            assert "\r" not in record.getMessage()

    @pytest.mark.asyncio
    async def test_send_non_dict_falls_back_gracefully(self):
        """L-2: send() receiving a non-dict raises TypeError, causing a clean fallback."""

        async def sends_string(scope, receive, send):
            await receive()
            await send("lifespan.startup.complete")  # plain string, not a dict

        handler = LifespanHandler(sends_string)
        # The TypeError propagates back to the app task, which the fallback
        # mechanism catches — lifespan is disabled rather than crashing.
        await handler.startup()  # must not raise
        assert handler._supported is False

    def test_none_startup_timeout_logs_warning(self, caplog):
        """L-3: startup_timeout=None emits a WARNING so misconfiguration is always visible."""
        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            LifespanHandler(lifecycle_app, startup_timeout=None)

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("indefinitely" in r.getMessage() for r in warning_records)

    def test_negative_startup_timeout_raises(self):
        """Passing a negative startup_timeout raises ValueError immediately."""
        with pytest.raises(ValueError, match="startup_timeout"):
            LifespanHandler(lifecycle_app, startup_timeout=-1)

    def test_negative_shutdown_timeout_raises(self):
        """Passing a negative shutdown_timeout raises ValueError immediately."""
        with pytest.raises(ValueError, match="shutdown_timeout"):
            LifespanHandler(lifecycle_app, shutdown_timeout=-1)

    def test_zero_startup_timeout_is_clamped(self, caplog):
        """A startup_timeout of 0 is clamped to the minimum and a WARNING is emitted."""
        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            handler = LifespanHandler(lifecycle_app, startup_timeout=0)
        assert handler._startup_timeout > 0
        assert "startup_timeout" in caplog.text

    def test_zero_shutdown_timeout_is_clamped(self, caplog):
        """A shutdown_timeout of 0 is clamped to the minimum and a WARNING is emitted."""
        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            handler = LifespanHandler(lifecycle_app, shutdown_timeout=0)
        assert handler._shutdown_timeout > 0
        assert "shutdown_timeout" in caplog.text

    @pytest.mark.asyncio
    async def test_startup_none_timeout_means_no_limit(self):
        """startup_timeout=None disables the startup timeout entirely."""
        # This just verifies the handler constructs and runs without error
        handler = LifespanHandler(lifecycle_app, startup_timeout=None)
        await handler.startup()
        await handler.shutdown()

    def test_none_shutdown_timeout_logs_warning(self, caplog):
        """shutdown_timeout=None emits a WARNING so misconfiguration is always visible."""
        with caplog.at_level(logging.WARNING, logger="daphne.lifespan"):
            LifespanHandler(lifecycle_app, shutdown_timeout=None)

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("indefinitely" in r.getMessage() for r in warning_records)

    @pytest.mark.asyncio
    async def test_shutdown_none_timeout_means_no_limit(self):
        """shutdown_timeout=None disables the shutdown timeout entirely."""
        handler = LifespanHandler(lifecycle_app, shutdown_timeout=None)
        await handler.startup()
        await handler.shutdown()  # must not raise

    @pytest.mark.asyncio
    async def test_fallback_value_error_logs_at_debug(self, caplog):
        """ValueError during startup (e.g. ProtocolTypeRouter) falls back at DEBUG level only."""

        async def raises_value_error(scope, receive, send):
            raise ValueError("No route for lifespan")

        handler = LifespanHandler(raises_value_error)

        with caplog.at_level(logging.DEBUG, logger="daphne.lifespan"):
            await handler.startup()

        assert handler._supported is False
        debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert any("does not support lifespan" in r.getMessage() for r in debug_records)
        # Must not have been escalated to WARNING
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert not any(
            "does not support lifespan" in r.getMessage() for r in warning_records
        )

    @pytest.mark.asyncio
    async def test_fallback_unexpected_exception_logs_at_warning(self, caplog):
        """Unexpected exception types during startup fallback are escalated to WARNING."""

        async def raises_import_error(scope, receive, send):
            raise ImportError("missing_security_dependency")

        handler = LifespanHandler(raises_import_error)

        with caplog.at_level(logging.DEBUG, logger="daphne.lifespan"):
            await handler.startup()

        assert handler._supported is False
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "does not support lifespan" in r.getMessage() for r in warning_records
        )

    @pytest.mark.asyncio
    async def test_fallback_cancelled_error_logs_at_warning(self, caplog):
        """CancelledError during startup fallback is escalated to WARNING."""

        async def hangs_before_responding(scope, receive, send):
            await receive()
            await asyncio.sleep(9999)

        handler = LifespanHandler(hangs_before_responding)
        startup_coro = asyncio.ensure_future(handler.startup())

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert handler._task is not None
        handler._task.cancel()

        with caplog.at_level(logging.DEBUG, logger="daphne.lifespan"):
            await startup_coro

        assert handler._supported is False
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("cancelled" in r.getMessage().lower() for r in warning_records)

    @pytest.mark.asyncio
    async def test_startup_failed_task_is_cancelled(self):
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

    @pytest.mark.asyncio
    async def test_shutdown_exception_message_sanitised(self, caplog):
        """Newlines/ANSI in an unexpected shutdown exception are escaped before logging."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()

        # Replace the send queue with one whose .get() raises an exception
        # carrying injected newlines, simulating a malicious or buggy app.
        class _InjectedErrorQueue:
            def get(self):
                raise RuntimeError(
                    "connection error\nINFO fake injected log line\r\nanother line"
                )
            async def put(self, item):
                pass  # allow dispatching_send to put without error

        handler._send_queue = _InjectedErrorQueue()

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await handler.shutdown()  # must not raise

        for record in caplog.records:
            assert "\n" not in record.getMessage()
            assert "\r" not in record.getMessage()
        assert "\\n" in caplog.text

    @pytest.mark.asyncio
    async def test_shutdown_exception_includes_exc_info(self, caplog):
        """Unexpected shutdown exceptions attach exc_info for structured log aggregators."""
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()

        handler._send_queue = None  # raises AttributeError on .get()

        with caplog.at_level(logging.ERROR, logger="daphne.lifespan"):
            await handler.shutdown()

        error_records = [
            r for r in caplog.records
            if r.levelno == logging.ERROR and "Lifespan shutdown error" in r.getMessage()
        ]
        assert error_records, "Expected an ERROR log record for the shutdown exception"
        assert any(r.exc_info is not None for r in error_records)


# ---------------------------------------------------------------------------
# scope["state"] propagation
# ---------------------------------------------------------------------------


class TestLifespanState:
    @pytest.mark.asyncio
    async def test_state_written_during_startup_is_readable_on_handler(self):
        """
        Values written to scope["state"] during the startup hook are
        accessible on handler.state after startup() returns. This is the
        mechanism that feeds shared objects (pools, clients) to request
        handlers via Server.create_application().
        """

        async def state_writing_app(scope, receive, send):
            scope["state"]["db"] = "pool"
            scope["state"]["cache"] = [1, 2, 3]
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

        handler = LifespanHandler(state_writing_app)
        await handler.startup()

        assert handler.state == {"db": "pool", "cache": [1, 2, 3]}

        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_scope_state_is_same_object_as_handler_state(self):
        """
        scope["state"] given to the application is the exact same dict object
        as handler.state, not a copy. If it were a copy, every write made
        during the startup hook would be silently lost and handler.state
        would always be empty regardless of what the app wrote.
        """
        captured_state_id = None

        async def capturing_app(scope, receive, send):
            nonlocal captured_state_id
            captured_state_id = id(scope["state"])
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

        handler = LifespanHandler(capturing_app)
        await handler.startup()

        assert captured_state_id == id(handler.state)

        await handler.shutdown()

    @pytest.mark.asyncio
    async def test_state_is_empty_dict_when_app_writes_nothing(self):
        """
        handler.state remains an empty dict when the app does not write to
        scope["state"]. Server.create_application() copies it unconditionally;
        an empty dict is the correct no-op and must never be None.
        """
        handler = LifespanHandler(lifecycle_app)
        await handler.startup()

        assert handler.state == {}
        assert isinstance(handler.state, dict)

        await handler.shutdown()
