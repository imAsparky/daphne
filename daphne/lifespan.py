"""
daphne/lifespan.py

ASGI Lifespan protocol handler for Daphne.

Runs the application's lifespan scope before connections are accepted and
after they are all closed, per the ASGI Lifespan sub-specification:
https://asgi.readthedocs.io/en/latest/specs/lifespan.html

Lifespan support is optional: if the application raises an exception before
consuming the first receive() event, we treat that as "lifespan not
supported" and continue without it, as the spec requires.

If the application explicitly sends lifespan.startup.failed the process
exits before accepting any connections.
"""

import asyncio
import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Log-message sanitisation
# ---------------------------------------------------------------------------

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_MAX_MESSAGE_LENGTH = 500


def _sanitise_message(message):
    """
    Sanitise an application-supplied message string before it is written to
    the log.

    Specifically:
    - Coerces non-str values to str.
    - Escapes CR and LF to prevent log-injection attacks (an attacker with
      control over the ASGI app could otherwise inject fake log lines).
    - Strips ANSI CSI escape sequences so terminal emulators in developer
      environments cannot be targeted with escape-code exploits.
    - Truncates to _MAX_MESSAGE_LENGTH characters so a malicious or buggy
      application cannot produce unbounded log output.
    """
    if not isinstance(message, str):
        message = str(message)
    message = message.replace("\r", "\\r").replace("\n", "\\n")
    message = _ANSI_ESCAPE_RE.sub("", message)
    return message[:_MAX_MESSAGE_LENGTH]


class LifespanHandler:
    """
    Manages the ASGI lifespan connection for a single Daphne process.

    Usage in Server._lifespan_startup_then_listen():
        self._lifespan_handler = LifespanHandler(self.application)
        await self._lifespan_handler.startup()
        # ... then start listening

    Usage in Server._lifespan_shutdown() (registered as "before shutdown"):
        await self._lifespan_handler.shutdown()
    """

    def __init__(self, application, startup_timeout=60, shutdown_timeout=30):
        self.application = application
        # Events sent from Daphne to the app (the app calls receive())
        self._receive_queue = asyncio.Queue()
        # Messages sent from the app to Daphne during shutdown (the app calls send())
        self._send_queue = asyncio.Queue()
        self._task = None
        self._supported = True  # Flipped to False on graceful fallback

        # Populated by the application during its startup hook via scope["state"].
        # After startup completes, Server.create_application() shallow-copies this
        # dict into every HTTP and WebSocket scope so request handlers can access
        # objects (pools, clients, etc.) that were initialised at startup — without
        # globals and without any dependency on import order.
        # Initialised here so it is always a dict regardless of whether lifespan
        # is supported; Server.create_application() can copy it unconditionally.
        self.state = {}

        # Negative timeout values are rejected outright; very small positive values
        # are clamped to a minimum so that setting a timeout to 0 does not silently
        # skip cleanup by timing out before the app has any chance to respond.
        _MIN_TIMEOUT = 0.1
        if startup_timeout is not None:
            if startup_timeout < 0:
                raise ValueError(
                    f"lifespan startup_timeout must be non-negative, got {startup_timeout!r}"
                )
            if startup_timeout < _MIN_TIMEOUT:
                logger.warning(
                    "lifespan startup_timeout %.2f is very low; "
                    "clamping to %.2f seconds to prevent immediate timeout.",
                    startup_timeout,
                    _MIN_TIMEOUT,
                )
                startup_timeout = _MIN_TIMEOUT
        else:
            # None is a deliberate operator opt-out of the startup timeout, but it
            # means the server will wait indefinitely for startup.complete and never
            # accept connections if the application hangs.  Emit a WARNING so that
            # accidental misconfiguration is always visible in logs.
            logger.warning(
                "lifespan startup_timeout is None; the server will wait indefinitely "
                "for the application's startup.complete. "
                "Set an explicit timeout for production deployments."
            )
        if shutdown_timeout is not None:
            if shutdown_timeout < 0:
                raise ValueError(
                    f"lifespan shutdown_timeout must be non-negative, got {shutdown_timeout!r}"
                )
            if shutdown_timeout < _MIN_TIMEOUT:
                logger.warning(
                    "lifespan shutdown_timeout %.2f is very low; "
                    "clamping to %.2f seconds. Cleanup may be skipped.",
                    shutdown_timeout,
                    _MIN_TIMEOUT,
                )
                shutdown_timeout = _MIN_TIMEOUT
        else:
            # None is a deliberate operator opt-out of the shutdown timeout, but
            # it means the server will wait indefinitely for shutdown.complete and
            # may hang during shutdown if the application does not respond.
            # Emit a WARNING so accidental misconfiguration is visible in logs.
            logger.warning(
                "lifespan shutdown_timeout is None; the server will wait indefinitely "
                "for the application's shutdown.complete. "
                "Set an explicit timeout for production deployments."
            )

        self._startup_timeout = startup_timeout  # None means no timeout
        self._shutdown_timeout = shutdown_timeout

    async def startup(self):
        """
        Run the lifespan startup sequence.

        Starts the application's lifespan coroutine, sends lifespan.startup,
        and waits for the application to respond.

        - startup.complete  -> returns normally, task stays alive for shutdown
        - startup.failed    -> raises RuntimeError (caller should stop the server)
        - task exits early  -> app does not support lifespan; falls back silently
        """
        scope = {
            "type": "lifespan",
            "asgi": {
                "version": "3.0",
                # spec_version identifies the lifespan sub-spec revision (distinct
                # from the ASGI protocol version above).  The current revision is
                # "2.0" (added startup.failed / shutdown.failed in March 2019).
                "spec_version": "2.0",
            },
            # state is the ASGI-standard mechanism for sharing objects initialised
            # during startup with every subsequent request handler.  The application
            # writes into this dict during its startup hook; after startup.complete
            # is received, Server.create_application() shallow-copies it into every
            # HTTP and WebSocket scope.  We assign to self.state here so that the
            # same dict object is reachable by the server after startup ends.
            "state": self.state,
        }
        loop = asyncio.get_running_loop()

        # A Future resolved by whichever comes first:
        #   - the app sends its first message (startup.complete or startup.failed)
        #   - the app task exits without sending anything
        startup_future = loop.create_future()

        # Track whether the startup response has been received so we can
        # route subsequent send() calls (shutdown.complete) to _send_queue.
        startup_done = False

        async def dispatching_send(message):
            """
            During startup: resolves startup_future with the message.
            During shutdown: forwards to _send_queue for shutdown() to consume.
            """
            # Reject non-dict messages early so a buggy application gets a
            # clear TypeError rather than a confusing AttributeError or KeyError
            # deep inside the handler.
            if not isinstance(message, dict):
                raise TypeError(
                    f"lifespan send() expected a dict, got {type(message).__name__!r}"
                )
            nonlocal startup_done
            if not startup_done:
                startup_done = True
                if not startup_future.done():
                    startup_future.set_result(message)
            else:
                await self._send_queue.put(message)

        def on_task_done_during_startup(task):
            """
            If the task exits before sending a startup response, resolve
            startup_future so startup() does not block forever.
            """
            if startup_future.done():
                return
            if task.cancelled():
                startup_future.cancel()
            elif task.exception():
                startup_future.set_exception(task.exception())
            else:
                # Task exited cleanly without sending -- not supported
                startup_future.set_result(None)

        self._task = asyncio.create_task(
            self.application(
                scope=scope,
                receive=self._receive,
                send=dispatching_send,
            ),
            name="daphne.lifespan",
        )
        self._task.add_done_callback(on_task_done_during_startup)

        # Feed the startup event so the app can consume it
        await self._receive_queue.put({"type": "lifespan.startup"})

        # Wait for the startup outcome.
        # asyncio.shield() prevents a timeout from cancelling startup_future itself,
        # which is also watched by on_task_done_during_startup.  Without the shield,
        # a timeout and the done-callback could race to resolve the same future.
        try:
            if self._startup_timeout is not None:
                message = await asyncio.wait_for(
                    asyncio.shield(startup_future),
                    timeout=self._startup_timeout,
                )
            else:
                message = await startup_future
        except asyncio.TimeoutError:
            logger.error(
                "Lifespan startup timed out after %s seconds. "
                "The application did not send startup.complete or startup.failed. "
                "Stopping the server.",
                self._startup_timeout,
            )
            if self._task and not self._task.done():
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass
            self._task = None
            raise RuntimeError(
                f"ASGI lifespan startup timed out after {self._startup_timeout} seconds"
            )
        except asyncio.CancelledError:
            # The task was cancelled externally before it could send any startup
            # response.  This is distinct from a normal "not supported" fallback
            # (where the task exits cleanly or raises before receive()) and warrants
            # a WARNING rather than a DEBUG message.
            logger.warning(
                "Application does not support lifespan protocol "
                "(task was cancelled before responding), continuing without lifespan."
            )
            self._supported = False
            self._task = None
            return
        except Exception as exc:
            # Distinguish the expected fallback path — ValueError raised by
            # ProtocolTypeRouter when no "lifespan" key is present — from
            # genuinely unexpected exceptions (ImportError, PermissionError, etc.)
            # that may indicate a real startup problem the operator should see.
            exc_type_name = type(exc).__name__
            if isinstance(exc, ValueError):
                logger.debug(
                    "Application does not support lifespan protocol "
                    "(task raised %s), continuing without lifespan.",
                    exc_type_name,
                )
            else:
                logger.warning(
                    "Application does not support lifespan protocol "
                    "(task raised %s: %s), continuing without lifespan. "
                    "If this is unexpected, check your application for startup errors.",
                    exc_type_name,
                    _sanitise_message(str(exc)),
                )
            self._supported = False
            self._task = None
            return

        # Swap callbacks: remove the startup-only one, add the permanent handler
        self._task.remove_done_callback(on_task_done_during_startup)
        self._task.add_done_callback(self._on_task_done)

        if message is None:
            # Task exited cleanly before sending any response
            logger.debug(
                "Application does not support lifespan protocol "
                "(task exited without response), continuing without lifespan."
            )
            self._supported = False
            self._task = None
            return

        # Use .get() so a missing "type" key never raises KeyError, and sanitise
        # the type string before logging so a crafted value cannot inject fake log
        # lines or ANSI sequences into the process log.
        msg_type = message.get("type")
        if msg_type is None:
            logger.warning(
                "Lifespan app sent a malformed startup message with no 'type' key "
                "(message type was %r); ignoring.",
                type(message).__name__,
            )
        elif msg_type == "lifespan.startup.complete":
            logger.info("Lifespan startup complete")

        elif msg_type == "lifespan.startup.failed":
            reason = _sanitise_message(message.get("message", ""))
            logger.error("Lifespan startup failed: %s", reason)
            if self._task and not self._task.done():
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass
            raise RuntimeError(f"ASGI lifespan startup failed: {reason}")

        else:
            logger.warning(
                "Unexpected lifespan startup message type: %s",
                _sanitise_message(msg_type),
            )

    async def shutdown(self):
        """
        Run the lifespan shutdown sequence.

        Sends lifespan.shutdown and waits for the application to respond.
        Errors are logged but never re-raised -- the process is already stopping.
        """
        if not self._supported or self._task is None or self._task.done():
            return

        try:
            await self._receive_queue.put({"type": "lifespan.shutdown"})
            message = await asyncio.wait_for(
                self._send_queue.get(),
                timeout=self._shutdown_timeout,
            )
            # Same .get() + sanitise pattern as startup: guard against a missing
            # "type" key and prevent log injection via a crafted message type.
            msg_type = message.get("type") if isinstance(message, dict) else None
            if msg_type is None:
                logger.warning(
                    "Lifespan app sent a malformed shutdown message with no 'type' key "
                    "(message type was %r); ignoring.",
                    type(message).__name__,
                )
            elif msg_type == "lifespan.shutdown.complete":
                logger.info("Lifespan shutdown complete")
            elif msg_type == "lifespan.shutdown.failed":
                logger.error(
                    "Lifespan shutdown failed: %s",
                    _sanitise_message(message.get("message", "")),
                )
            else:
                logger.warning(
                    "Unexpected lifespan shutdown message type: %s",
                    _sanitise_message(msg_type),
                )
        except asyncio.TimeoutError:
            logger.error(
                "Lifespan shutdown timed out after %s seconds",
                self._shutdown_timeout,
            )
        except Exception as exc:
            logger.error(
                "Lifespan shutdown error: %s",
                _sanitise_message(str(exc)),
                exc_info=exc,
            )
        finally:
            if self._task and not self._task.done():
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass

    async def _receive(self):
        """Called by the application to receive the next lifespan event."""
        return await self._receive_queue.get()

    def _on_task_done(self, task):
        """
        Permanent done callback -- fires only for unexpected mid-run failures.
        Normal task exit after shutdown.complete is expected and silent.
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error(
                "Lifespan application task raised an unexpected exception: %s",
                # Sanitise so an exception carrying ANSI codes or newlines
                # (e.g. a connection string leaked via RuntimeError) cannot inject
                # fake log lines.  exc_info still attaches the full traceback for
                # structured log aggregators that separate message from traceback.
                _sanitise_message(str(exc)),
                exc_info=exc,
            )
