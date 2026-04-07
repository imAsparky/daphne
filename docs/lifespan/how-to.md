# How to use ASGI lifespan hooks with Daphne

This guide shows how to wire startup and shutdown hooks into a Django
Channels application running on Daphne. It covers the
[ASGI Lifespan sub-specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html)
features that Daphne now implements: lifecycle hooks and `scope["state"]`
propagation. For a deeper reference on the protocol mechanics and server
internals, see the [reference document](reference.md).


## Prerequisites

- Daphne — the release that introduced lifespan support
- Django Channels 4.x
- A `ProtocolTypeRouter` in your `asgi.py`


## Step 1 — Write a lifespan handler

A lifespan handler is a plain ASGI callable that accepts a `lifespan` scope.
The example below shows the complete, production-ready pattern including state
propagation, error handling on both startup and shutdown, and guards against
unexpected event types.

```python
# myapp/lifespan.py

import logging

import httpx

logger = logging.getLogger(__name__)


async def lifespan(scope, receive, send):
    if scope["type"] != "lifespan":
        raise ValueError(f"lifespan handler called with unexpected scope type: {scope['type']!r}")

    # scope["state"] is an empty dict provided by Daphne. Populate it
    # during startup and Daphne will pass a shallow copy into every
    # subsequent HTTP and WebSocket scope (see "Sharing state" below).
    state = scope["state"]

    event = await receive()
    if event["type"] == "lifespan.startup":
        try:
            await on_startup(state)
        except Exception:
            # logger.exception() captures the full traceback automatically.
            logger.exception("Startup hook raised an exception")
            await send({
                "type": "lifespan.startup.failed",
                # Do NOT use str(exc) here — exception strings can contain
                # connection strings, passwords, or internal hostnames that
                # would be written to Daphne's log and any external
                # aggregators that receive it.
                "message": "Startup failed — see server logs for details",
            })
            # Re-raise to terminate the lifespan task. This is what signals
            # Daphne that startup has failed: Daphne catches the resulting
            # RuntimeError in Server._lifespan_startup_then_listen() and
            # stops the process without ever calling ep.listen().
            raise
        await send({"type": "lifespan.startup.complete"})
    else:
        logger.warning("Unexpected first lifespan event type: %r", event["type"])
        return

    event = await receive()
    if event["type"] == "lifespan.shutdown":
        try:
            await on_shutdown(state)
        except Exception:
            # logger.exception() captures the full traceback automatically.
            logger.exception("Shutdown hook raised an exception")
            # Unlike startup.failed, shutdown.failed does not stop the
            # process — Daphne logs the message and continues shutting
            # down. Send it so operators have a clear record that cleanup
            # was incomplete (e.g. a pool that could not be drained).
            # Do NOT use str(exc) in the message for the same reason as
            # startup — it may contain sensitive connection details.
            await send({
                "type": "lifespan.shutdown.failed",
                "message": "Shutdown failed — see server logs for details",
            })
            return
        await send({"type": "lifespan.shutdown.complete"})
    else:
        logger.warning("Unexpected lifespan shutdown event type: %r", event["type"])


async def on_startup(state: dict) -> None:
    logger.info("Server starting up — initialising resources")
    # Initialise async clients, connection pools, etc., then store them
    # in `state` so request handlers can reach them (see "Sharing state").
    state["http_client"] = httpx.AsyncClient()


async def on_shutdown(state: dict) -> None:
    logger.info("Server shutting down — releasing resources")
    # Close and release everything stored in state.
    if client := state.get("http_client"):
        await client.aclose()
```


## Step 2 — Register the handler in ProtocolTypeRouter

```python
# myapp/asgi.py

from channels.routing import ProtocolTypeRouter
from django.core.asgi import get_asgi_application
from myapp.lifespan import lifespan

application = ProtocolTypeRouter({
    "http":      get_asgi_application(),
    "websocket": ...,
    "lifespan":  lifespan,   # Daphne routes the lifespan scope here
})
```

`ProtocolTypeRouter` dispatches on `scope["type"]`. Daphne sends exactly one
lifespan scope per process, so this callable runs once at startup and once at
shutdown.


## Step 3 — Start Daphne normally

```bash
daphne myapp.asgi:application
```

On startup you will see log output in this order:

```
Server starting up — initialising resources
Lifespan startup complete
Listening on TCP address 0.0.0.0:8000
```

The listen line must appear after your startup log. If it appears before,
something is wrong with the hook wiring.

On shutdown (SIGTERM or CTRL-C):

```
Server shutting down — releasing resources
Lifespan shutdown complete
Killed 0 pending application instances
```


## Signalling a fatal startup failure

If a startup hook cannot complete — for example a required external service
is unreachable — raise inside `on_startup`. The handler will send
`lifespan.startup.failed` and Daphne will log the message and stop the
process before accepting any connections:

```python
async def on_startup(state: dict) -> None:
    if not await database_is_reachable():
        raise RuntimeError("Database unreachable at startup")
```

```
Lifespan startup failed: Startup failed — see server logs for details
```

The process exits with a non-zero status, which is correct behaviour for a
container orchestrator such as Kubernetes — the pod will be restarted rather
than serving requests with a broken dependency.


## Sharing state between hooks and request handlers

`scope["state"]` is the [ASGI-standard mechanism](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#scope)
for passing objects from the lifespan hooks into request handlers. Daphne
provides it as an empty dict at startup. The application populates it; Daphne
then passes a **shallow copy** into every HTTP and WebSocket scope for the
lifetime of the process.

This keeps shared objects — pools, clients, caches — explicit and
co-located with the code that owns them, with no module-level globals, no
import-order dependencies, and no risk of leaking state across processes. For
a fuller explanation of why shallow copy is used and the event-loop binding
guarantees it provides, see the [reference document](reference.md#lifespan-state).

The handler in Step 1 already uses this pattern. On the request side, access
state directly from the ASGI scope (e.g. in a Channels consumer):

```python
class MyConsumer(AsyncHttpConsumer):
    async def handle(self, body):
        client = self.scope["state"]["http_client"]
        ...
```

Or through the Django request object if you are using middleware that surfaces
`scope["state"]` onto the request:

```python
async def my_view(request):
    client = request.state["http_client"]
    ...
```


## Verifying lifespan is active

Lifespan events are emitted via Python's standard `logging` module, not
Daphne's NCSA access log. To see them, configure your Django logging so that
Daphne's logger is at `INFO` level (or `DEBUG` to also see the fallback
message). For example, in `settings.py`:

```python
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {"class": "logging.StreamHandler"},
    },
    "loggers": {
        "daphne": {
            "handlers": ["console"],
            "level": "DEBUG",
        },
    },
}
```

Then run Daphne normally:

```bash
daphne myapp.asgi:application
```

If you do not see `Lifespan startup complete` in the output, either your
application does not have a `"lifespan"` key in `ProtocolTypeRouter`, or
the handler raised before sending `startup.complete` — check the logs for
a `Lifespan startup failed` or fallback message. The fallback is triggered
when the application raises or exits before sending any startup response;
see [Fallback behaviour](reference.md#fallback-behaviour) in the reference
for the full details.
