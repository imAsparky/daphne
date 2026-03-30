# How to use ASGI lifespan hooks with Daphne

This guide shows how to wire startup and shutdown hooks into a Django
Channels application running on Daphne.


## Prerequisites

- Daphne 4.3 or later
- Django Channels 4.x
- A `ProtocolTypeRouter` in your `asgi.py`


## Step 1 — Write a lifespan handler

A lifespan handler is a plain ASGI callable that accepts a `lifespan` scope.
The simplest correct implementation:

```python
# myapp/lifespan.py

import logging

logger = logging.getLogger(__name__)


async def lifespan(scope, receive, send):
    assert scope["type"] == "lifespan"

    event = await receive()
    if event["type"] == "lifespan.startup":
        try:
            await on_startup()
        except Exception as exc:
            await send({"type": "lifespan.startup.failed", "message": str(exc)})
            raise
        await send({"type": "lifespan.startup.complete"})

    event = await receive()
    if event["type"] == "lifespan.shutdown":
        await on_shutdown()
        await send({"type": "lifespan.shutdown.complete"})


async def on_startup():
    logger.info("Server starting up — initialising resources")
    # initialise your async clients, connection pools, etc.


async def on_shutdown():
    logger.info("Server shutting down — releasing resources")
    # close clients, cancel background tasks, etc.
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
Lifespan shutdown complete
Server shutting down — releasing resources
Killed 0 pending application instances
```


## Signalling a fatal startup failure

If a startup hook cannot complete — for example a required external service
is unreachable — send `lifespan.startup.failed` and Daphne will log the
message and stop the process before accepting any connections:

```python
async def on_startup():
    if not await database_is_reachable():
        raise RuntimeError("Database unreachable at startup")
```

```
Lifespan startup failed: Database unreachable at startup
```

The process exits with a non-zero status, which is correct behaviour for a
container orchestrator such as Kubernetes — the pod will be restarted rather
than serving requests with a broken dependency.


## Sharing state between hooks and request handlers

Initialise singletons at module level and assign them in the startup hook:

```python
# myapp/clients.py

http_client = None


async def on_startup():
    global http_client
    import httpx
    http_client = httpx.AsyncClient()


async def on_shutdown():
    await http_client.aclose()
```

Request handlers import `http_client` from `myapp.clients`. Because
Daphne guarantees startup completes before the first request, the client
is always initialised when a handler reaches for it.


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
a `Lifespan startup failed` or fallback message.
