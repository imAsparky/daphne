"""
tests/test_server_lifespan.py

Unit tests for the lifespan-related additions to daphne.server.Server.

These tests avoid starting the Twisted reactor entirely.  The async methods
(_lifespan_startup_then_listen) are awaited directly inside a plain asyncio
test loop via pytest-asyncio.  The parts of Server that touch the reactor
(stop, serverFromString, defer.Deferred.fromFuture) are patched out so the
tests remain fast and hermetic.

Coverage targets
----------------
Server.__init__             lifespan_startup_timeout / lifespan_shutdown_timeout
                            stored; _lifespan_handler initialised to None.
_lifespan_startup_then_listen
                            LifespanHandler constructed with correct timeouts.
                            Successful startup -> endpoints bound.
                            RuntimeError (startup.failed) -> stop(), no bind.
                            Unexpected Exception -> stop(), no bind, sanitised log.
                            _lifespan_handler assigned before startup() is called.
_on_lifespan_startup_done   Success -> no stop().
                            Cancelled task -> ERROR log + stop().
                            Exception task -> ERROR log + stop().
_lifespan_shutdown          No handler -> returns None.
                            Handler present -> handler.shutdown() called + Deferred returned.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from daphne.server import Server

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def make_server(**overrides):
    """
    Return a Server with the minimum configuration needed to avoid sys.exit.

    server.stop() is replaced with a MagicMock so tests can assert on it
    without touching the Twisted reactor.
    server.http_factory is set to a MagicMock so _lifespan_startup_then_listen
    can pass it to ep.listen() without needing the real factory.
    """
    kwargs = dict(
        application=AsyncMock(),
        endpoints=["tcp:8000"],
        lifespan_startup_timeout=60,
        lifespan_shutdown_timeout=30,
    )
    kwargs.update(overrides)
    server = Server(**kwargs)
    server.stop = MagicMock()
    server.http_factory = MagicMock()
    return server


def make_done_task(*, cancelled=False, exception=None):
    """
    Return a mock asyncio.Task whose outcome is fully determined.

    - cancelled=True  →  task.cancelled() is True; exception() is never called.
    - exception=<exc> →  task.cancelled() is False; task.exception() returns exc.
    - (default)       →  success: cancelled() False, exception() returns None.
    """
    task = MagicMock()
    task.cancelled.return_value = cancelled
    if not cancelled:
        task.exception.return_value = exception
    return task


def patched_serverFromString():
    """
    Context-manager that patches serverFromString in daphne.server and returns
    a mock endpoint whose listen() completes synchronously via callbacks.
    """
    mock_port = MagicMock()
    mock_listener = MagicMock()
    mock_listener.addCallback = lambda f: f(mock_port)
    mock_listener.addErrback = lambda f: None
    mock_ep = MagicMock()
    mock_ep.listen.return_value = mock_listener
    return patch("daphne.server.serverFromString", return_value=mock_ep), mock_ep


# ---------------------------------------------------------------------------
# Construction / configuration
# ---------------------------------------------------------------------------


class TestServerLifespanConfig:
    def test_custom_timeouts_are_stored(self):
        """lifespan_startup_timeout and lifespan_shutdown_timeout are stored as-is."""
        server = make_server(lifespan_startup_timeout=120, lifespan_shutdown_timeout=45)
        assert server.lifespan_startup_timeout == 120
        assert server.lifespan_shutdown_timeout == 45

    def test_default_lifespan_startup_timeout(self):
        """Default lifespan_startup_timeout is 60 seconds."""
        server = make_server()
        assert server.lifespan_startup_timeout == 60

    def test_default_lifespan_shutdown_timeout(self):
        """Default lifespan_shutdown_timeout is 30 seconds."""
        server = make_server()
        assert server.lifespan_shutdown_timeout == 30

    def test_lifespan_handler_initially_none(self):
        """_lifespan_handler starts as None before any startup has run."""
        server = make_server()
        assert server._lifespan_handler is None


# ---------------------------------------------------------------------------
# _lifespan_startup_then_listen
# ---------------------------------------------------------------------------


class TestLifespanStartupThenListen:
    @pytest.mark.asyncio
    async def test_constructs_handler_with_correct_timeouts(self):
        """LifespanHandler is constructed with the server's configured timeouts."""
        server = make_server(lifespan_startup_timeout=90, lifespan_shutdown_timeout=15)

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup = AsyncMock()
            MockHandler.return_value = mock_handler

            sfr_patch, mock_ep = patched_serverFromString()
            with sfr_patch:
                await server._lifespan_startup_then_listen()

        MockHandler.assert_called_once_with(
            server.application,
            startup_timeout=90,
            shutdown_timeout=15,
        )

    @pytest.mark.asyncio
    async def test_handler_assigned_to_server_before_startup_called(self):
        """_lifespan_handler is set on self before startup() is awaited."""
        server = make_server()
        handler_at_startup_time = []

        async def capture(*args, **kwargs):
            # At the point startup() is called, self._lifespan_handler must
            # already be the handler instance.
            handler_at_startup_time.append(server._lifespan_handler)

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup = capture
            MockHandler.return_value = mock_handler

            sfr_patch, _ = patched_serverFromString()
            with sfr_patch:
                await server._lifespan_startup_then_listen()

        assert len(handler_at_startup_time) == 1
        assert handler_at_startup_time[0] is mock_handler

    @pytest.mark.asyncio
    async def test_successful_startup_binds_all_endpoints(self):
        """After a successful startup, ep.listen() is called for every endpoint."""
        server = make_server(endpoints=["tcp:8000", "tcp:8001"])

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup = AsyncMock()
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString") as mock_sfr:
                mock_ep = MagicMock()
                mock_listener = MagicMock()
                mock_listener.addCallback = lambda f: f(MagicMock())
                mock_listener.addErrback = lambda f: None
                mock_ep.listen.return_value = mock_listener
                mock_sfr.return_value = mock_ep

                await server._lifespan_startup_then_listen()

        assert mock_ep.listen.call_count == 2

    @pytest.mark.asyncio
    async def test_successful_startup_does_not_stop_server(self):
        """stop() is never called after a clean startup."""
        server = make_server()

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup = AsyncMock()
            MockHandler.return_value = mock_handler

            sfr_patch, _ = patched_serverFromString()
            with sfr_patch:
                await server._lifespan_startup_then_listen()

        server.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_successful_startup_populates_listeners(self):
        """server.listeners is populated after a clean startup."""
        server = make_server()

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup = AsyncMock()
            MockHandler.return_value = mock_handler

            sfr_patch, _ = patched_serverFromString()
            with sfr_patch:
                await server._lifespan_startup_then_listen()

        assert len(server.listeners) == 1

    @pytest.mark.asyncio
    async def test_runtime_error_stops_server(self):
        """RuntimeError from startup (startup.failed path) calls stop()."""
        server = make_server()

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup.side_effect = RuntimeError("lifespan startup failed")
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString"):
                await server._lifespan_startup_then_listen()

        server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_runtime_error_does_not_bind_endpoints(self):
        """No endpoint is bound when startup raises RuntimeError."""
        server = make_server()

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup.side_effect = RuntimeError("boom")
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString") as mock_sfr:
                await server._lifespan_startup_then_listen()

        mock_sfr.assert_not_called()

    @pytest.mark.asyncio
    async def test_unexpected_exception_stops_server(self):
        """An unexpected non-RuntimeError exception also calls stop()."""
        server = make_server()

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup.side_effect = OSError("disk full")
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString"):
                await server._lifespan_startup_then_listen()

        server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_unexpected_exception_does_not_bind_endpoints(self):
        """No endpoint is bound when an unexpected exception occurs during startup."""
        server = make_server()

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup.side_effect = OSError("disk full")
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString") as mock_sfr:
                await server._lifespan_startup_then_listen()

        mock_sfr.assert_not_called()

    @pytest.mark.asyncio
    async def test_unexpected_exception_message_is_sanitised_in_log(self, caplog):
        """M-4: newlines in an unexpected exception message are escaped before logging."""
        server = make_server()
        injected = "connection failed\nINFO fake log line injected by app"

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup.side_effect = OSError(injected)
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString"):
                with caplog.at_level(logging.ERROR, logger="daphne.server"):
                    await server._lifespan_startup_then_listen()

        for record in caplog.records:
            assert "\n" not in record.getMessage()
            assert "\r" not in record.getMessage()

    @pytest.mark.asyncio
    async def test_unexpected_exception_logs_exception_type(self, caplog):
        """The exception type name is present in the error log."""
        server = make_server()

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup.side_effect = PermissionError("denied")
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString"):
                with caplog.at_level(logging.ERROR, logger="daphne.server"):
                    await server._lifespan_startup_then_listen()

        assert any("PermissionError" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_listen_failure_stops_server(self):
        """A listen failure fires the errback which calls stop()."""
        server = make_server(endpoints=["tcp:8000"])

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup = AsyncMock()
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString") as mock_sfr:
                mock_ep = MagicMock()
                mock_listener = MagicMock()
                mock_listener.addCallback = lambda f: None
                mock_listener.addErrback = lambda f: f(
                    MagicMock(getErrorMessage=lambda: "address already in use")
                )
                mock_ep.listen.return_value = mock_listener
                mock_sfr.return_value = mock_ep

                await server._lifespan_startup_then_listen()

        server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_listen_failure_wires_errback(self):
        """listen() has an errback registered so failures reach listen_error."""
        server = make_server(endpoints=["tcp:8000"])

        with patch("daphne.server.LifespanHandler") as MockHandler:
            mock_handler = AsyncMock()
            mock_handler.startup = AsyncMock()
            MockHandler.return_value = mock_handler

            with patch("daphne.server.serverFromString") as mock_sfr:
                mock_ep = MagicMock()
                mock_listener = MagicMock()
                # addCallback is a no-op; addErrback captures the registered handler
                mock_listener.addCallback = lambda f: None
                errback_registered = []
                mock_listener.addErrback = lambda f: errback_registered.append(f)
                mock_ep.listen.return_value = mock_listener
                mock_sfr.return_value = mock_ep

                await server._lifespan_startup_then_listen()

        assert len(errback_registered) == 1
        assert callable(errback_registered[0])


# ---------------------------------------------------------------------------
# _on_lifespan_startup_done
# ---------------------------------------------------------------------------


class TestOnLifespanStartupDone:
    def test_successful_task_does_not_stop_server(self):
        """A cleanly completed startup task leaves the server running."""
        server = make_server()
        task = make_done_task()  # success: not cancelled, no exception
        server._on_lifespan_startup_done(task)
        server.stop.assert_not_called()

    def test_cancelled_task_stops_server(self):
        """A cancelled startup task calls stop()."""
        server = make_server()
        task = make_done_task(cancelled=True)
        server._on_lifespan_startup_done(task)
        server.stop.assert_called_once()

    def test_cancelled_task_logs_error(self, caplog):
        """A cancelled startup task emits an ERROR-level log message."""
        server = make_server()
        task = make_done_task(cancelled=True)
        with caplog.at_level(logging.ERROR, logger="daphne.server"):
            server._on_lifespan_startup_done(task)
        assert any(
            "cancelled" in r.getMessage().lower() and r.levelno == logging.ERROR
            for r in caplog.records
        )

    def test_exception_task_stops_server(self):
        """A startup task that raised an exception calls stop()."""
        server = make_server()
        task = make_done_task(exception=RuntimeError("startup exploded"))
        server._on_lifespan_startup_done(task)
        server.stop.assert_called_once()

    def test_exception_task_logs_error(self, caplog):
        """A startup task that raised emits an ERROR-level log message."""
        server = make_server()
        task = make_done_task(exception=RuntimeError("startup exploded"))
        with caplog.at_level(logging.ERROR, logger="daphne.server"):
            server._on_lifespan_startup_done(task)
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    def test_exception_task_log_includes_exc_info(self, caplog):
        """The ERROR log record includes exc_info for structured log aggregators."""
        server = make_server()
        exc = RuntimeError("startup exploded")
        task = make_done_task(exception=exc)
        with caplog.at_level(logging.ERROR, logger="daphne.server"):
            server._on_lifespan_startup_done(task)
        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any(r.exc_info is not None for r in error_records)

    def test_successful_task_does_not_log_error(self, caplog):
        """A clean startup task emits no ERROR or WARNING logs."""
        server = make_server()
        task = make_done_task()
        with caplog.at_level(logging.WARNING, logger="daphne.server"):
            server._on_lifespan_startup_done(task)
        assert not any(r.levelno >= logging.WARNING for r in caplog.records)


# ---------------------------------------------------------------------------
# _lifespan_shutdown
# ---------------------------------------------------------------------------


class TestLifespanShutdown:
    def test_returns_none_when_no_handler(self):
        """_lifespan_shutdown returns None when _lifespan_handler has not been set."""
        server = make_server()
        assert server._lifespan_handler is None
        result = server._lifespan_shutdown()
        assert result is None

    def test_does_not_schedule_task_when_no_handler(self):
        """get_event_loop().create_task is never called when there is no handler."""
        server = make_server()
        with patch("daphne.server.asyncio.get_event_loop") as mock_get_loop:
            server._lifespan_shutdown()
        mock_get_loop.return_value.create_task.assert_not_called()

    def test_returns_deferred_when_handler_is_set(self):
        """_lifespan_shutdown returns a Twisted Deferred when a handler is set."""
        from twisted.internet import defer

        server = make_server()
        # MagicMock, not AsyncMock: create_task is patched out so the return
        # value of shutdown() is never awaited.  Using AsyncMock here would
        # create an unawaited coroutine that leaks across tests in Python 3.13.
        mock_handler = MagicMock()
        server._lifespan_handler = mock_handler

        mock_future = MagicMock()
        mock_deferred = MagicMock(spec=defer.Deferred)

        with patch("daphne.server.asyncio.get_event_loop") as mock_get_loop:
            mock_get_loop.return_value.create_task.return_value = mock_future
            with patch.object(defer.Deferred, "fromFuture", return_value=mock_deferred):
                result = server._lifespan_shutdown()

        assert result is mock_deferred

    def test_calls_handler_shutdown_when_handler_is_set(self):
        """handler.shutdown() is called when _lifespan_handler is present."""
        from twisted.internet import defer

        server = make_server()
        mock_handler = MagicMock()
        server._lifespan_handler = mock_handler

        with patch("daphne.server.asyncio.get_event_loop") as mock_get_loop:
            with patch.object(defer.Deferred, "fromFuture"):
                server._lifespan_shutdown()

        mock_get_loop.return_value.create_task.assert_called_once()
        mock_handler.shutdown.assert_called_once()

    def test_deferred_errback_suppresses_errors(self):
        """
        The Deferred returned by _lifespan_shutdown has an errback so that
        errors from the shutdown coroutine never propagate to Twisted's
        unhandled-error machinery.
        """
        from twisted.internet import defer

        server = make_server()
        mock_handler = MagicMock()
        server._lifespan_handler = mock_handler

        with patch("daphne.server.asyncio.get_event_loop") as mock_get_loop:
            mock_get_loop.return_value.create_task.return_value = MagicMock()
            with patch.object(defer.Deferred, "fromFuture") as mock_from_future:
                mock_d = MagicMock(spec=defer.Deferred)
                mock_from_future.return_value = mock_d
                server._lifespan_shutdown()

        # addErrback must have been called on the returned Deferred
        mock_d.addErrback.assert_called_once()
        # The errback should be a callable that swallows failures
        errback = mock_d.addErrback.call_args[0][0]
        assert callable(errback)
        sentinel = object()
        assert errback(sentinel) is None  # returns None, suppressing the failure


# ---------------------------------------------------------------------------
# create_application — scope["state"] propagation
# ---------------------------------------------------------------------------


class TestCreateApplication:
    """
    Tests for Server.create_application()'s scope["state"] injection.

    create_application() uses asyncio.create_task(), which requires a running
    loop, so these tests are async. server.connections is normally initialised
    by Server.run(); it is set manually here to avoid starting the reactor.
    """

    @pytest.mark.asyncio
    async def test_handler_state_is_shallow_copied_into_scope(self):
        """
        create_application() shallow-copies handler.state into scope["state"].
        The result is an independent dict per request, but shared object
        references are preserved — the pool/client is the same live object.
        """
        server = make_server()
        server.connections = {}

        sentinel = object()
        mock_handler = MagicMock()
        mock_handler.state = {"pool": sentinel, "extra": "value"}
        server._lifespan_handler = mock_handler

        protocol = MagicMock()
        server.connections[protocol] = {"connected": 0}

        captured_scope = {}

        async def capturing_app(scope, receive, send):
            captured_scope.update(scope)
            await asyncio.sleep(9999)

        server.application = capturing_app
        server.create_application(protocol, {"type": "http"})
        await asyncio.sleep(0)

        # The value is present and is the same reference (shallow copy)
        assert captured_scope["state"]["pool"] is sentinel
        # It is a new dict, not the handler's own dict
        assert captured_scope["state"] is not mock_handler.state

        task = server.connections[protocol]["application_instance"]
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_each_request_gets_independent_state_dict(self):
        """
        Two concurrent calls to create_application() produce separate
        scope["state"] dicts. Mutating one request's state does not affect
        the other — the copy is per-request, not shared.
        """
        server = make_server()
        server.connections = {}

        mock_handler = MagicMock()
        mock_handler.state = {"counter": 0}
        server._lifespan_handler = mock_handler

        captured_scopes = []

        async def capturing_app(scope, receive, send):
            captured_scopes.append(scope)
            await asyncio.sleep(9999)

        server.application = capturing_app

        protocols = [MagicMock(), MagicMock()]
        for protocol in protocols:
            server.connections[protocol] = {"connected": 0}
            server.create_application(protocol, {"type": "http"})

        await asyncio.sleep(0)

        # Mutating one request's state must not affect the other
        captured_scopes[0]["state"]["counter"] = 99
        assert captured_scopes[1]["state"]["counter"] == 0

        for protocol in protocols:
            task = server.connections[protocol].get("application_instance")
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    @pytest.mark.asyncio
    async def test_empty_state_when_no_lifespan_handler(self):
        """
        When _lifespan_handler is None (lifespan not supported or not yet
        started), scope["state"] is an empty dict. The spec requires state
        to always be a dict, never absent or None.
        """
        server = make_server()
        server.connections = {}
        assert server._lifespan_handler is None

        protocol = MagicMock()
        server.connections[protocol] = {"connected": 0}

        captured_scope = {}

        async def capturing_app(scope, receive, send):
            captured_scope.update(scope)
            await asyncio.sleep(9999)

        server.application = capturing_app
        server.create_application(protocol, {"type": "http"})
        await asyncio.sleep(0)

        assert captured_scope["state"] == {}
        assert isinstance(captured_scope["state"], dict)

        task = server.connections[protocol]["application_instance"]
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# _daphne_loop identity invariant
# ---------------------------------------------------------------------------


class TestLoopIdentityInvariant:
    def test_daphne_loop_is_current_after_set_event_loop(self):
        """
        The core invariant: after Server.run() calls
        asyncio.set_event_loop(_daphne_loop), asyncio.get_event_loop() returns
        the same object. This is what makes asyncio.get_event_loop().create_task()
        in the synchronous Twisted callbacks schedule tasks on the correct loop.

        If _daphne_loop were an uninstalled orphan loop — the failure mode in
        the subprocess/test reimport scenario — tasks would be placed on a loop
        that is never driven and would silently never run.
        """
        import daphne.server as server_module

        asyncio.set_event_loop(server_module._daphne_loop)
        assert asyncio.get_event_loop() is server_module._daphne_loop

    def test_daphne_loop_is_not_closed(self):
        """
        _daphne_loop must be a live, open loop. A closed or discarded loop
        would accept create_task() calls without error but never execute them,
        reproducing the same silent failure as the wrong-loop bug.
        """
        import daphne.server as server_module

        assert not server_module._daphne_loop.is_closed()

    def test_schedule_lifespan_startup_uses_get_event_loop_create_task(self):
        """
        _schedule_lifespan_startup() must call asyncio.get_event_loop().create_task()
        rather than asyncio.create_task(). asyncio.create_task() calls
        get_running_loop() internally, which raises RuntimeError when invoked
        from a synchronous Twisted callWhenRunning callback where no asyncio
        loop is currently running.
        """
        server = make_server()

        mock_loop = MagicMock()
        mock_task = MagicMock()
        mock_loop.create_task.return_value = mock_task

        with patch("daphne.server.asyncio.get_event_loop", return_value=mock_loop):
            server._schedule_lifespan_startup()

        mock_loop.create_task.assert_called_once()

        # Close the unawaited coroutine to suppress ResourceWarning
        coro = mock_loop.create_task.call_args[0][0]
        coro.close()

        # Must be named for observability in asyncio task introspection
        assert mock_loop.create_task.call_args[1].get("name") == "daphne.lifespan.startup"
