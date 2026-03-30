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

logger = logging.getLogger(__name__)


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

    def __init__(self, application, shutdown_timeout=30):
        self.application = application
        # Events sent from Daphne to the app (the app calls receive())
        self._receive_queue = asyncio.Queue()
        # Messages sent from the app to Daphne during shutdown (the app calls send())
        self._send_queue = asyncio.Queue()
        self._task = None
        self._supported = True  # Flipped to False on graceful fallback
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
        scope = {"type": "lifespan", "asgi": {"version": "3.0"}}
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

        self._task = asyncio.ensure_future(
            self.application(
                scope=scope,
                receive=self._receive,
                send=dispatching_send,
            )
        )
        self._task.add_done_callback(on_task_done_during_startup)

        # Feed the startup event so the app can consume it
        await self._receive_queue.put({"type": "lifespan.startup"})

        # Wait for the startup outcome
        try:
            message = await startup_future
        except (asyncio.CancelledError, Exception) as exc:
            logger.debug(
                "Application does not support lifespan protocol "
                "(task raised %s), continuing without lifespan.",
                type(exc).__name__,
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

        if message["type"] == "lifespan.startup.complete":
            logger.info("Lifespan startup complete")

        elif message["type"] == "lifespan.startup.failed":
            reason = message.get("message", "")
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
                "Unexpected lifespan startup message type: %s", message["type"]
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
            if message["type"] == "lifespan.shutdown.complete":
                logger.info("Lifespan shutdown complete")
            elif message["type"] == "lifespan.shutdown.failed":
                logger.error(
                    "Lifespan shutdown failed: %s",
                    message.get("message", ""),
                )
            else:
                logger.warning(
                    "Unexpected lifespan shutdown message type: %s", message["type"]
                )
        except asyncio.TimeoutError:
            logger.error(
                "Lifespan shutdown timed out after %s seconds",
                self._shutdown_timeout,
            )
        except Exception as exc:
            logger.error("Lifespan shutdown error: %s", exc)
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
                exc,
                exc_info=exc,
            )
