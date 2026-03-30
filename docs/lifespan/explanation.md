# Lifespan support in Daphne — explanation

## What the ASGI Lifespan protocol is

The [ASGI Lifespan sub-specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html)
defines a standard way for an ASGI server to notify an application when the
server process is starting up and when it is shutting down. It does this by
calling the application with a scope of type `lifespan` and then exchanging a
small number of events:

```
server → app   lifespan.startup
app    → server   lifespan.startup.complete   (or startup.failed)

server → app   lifespan.shutdown
app    → server   lifespan.shutdown.complete  (or shutdown.failed)
```

This gives the application a guaranteed hook to initialise shared resources
— connection pools, async HTTP clients, background tasks — before the first
request arrives, and to clean them up cleanly before the process exits.

The protocol has been part of the ASGI specification since 2019. Frameworks
such as Starlette and FastAPI rely on it. Django Channels routes it via
`ProtocolTypeRouter`.


## Why Daphne did not support it before

Daphne is built on Twisted, which has its own event loop model. Adding
lifespan required bridging Twisted's reactor startup and shutdown sequences
with asyncio coroutines in a way that is correct across all supported Python
versions (3.9–3.14). 

## How lifespan fits into Daphne's architecture

Daphne runs Twisted on top of Python's asyncio event loop via
`twisted.internet.asyncioreactor`. Every HTTP and WebSocket connection is
handled as a native asyncio coroutine scheduled with `asyncio.ensure_future`,
which is the mechanism lifespan uses too.

The implementation adds a `LifespanHandler` class that manages a single,
long-lived asyncio Task for the duration of the process:

```
reactor.run()
    └── callWhenRunning → _schedule_lifespan_startup
            └── asyncio.create_task(_lifespan_startup_then_listen)
                    ├── LifespanHandler.startup()   ← feeds lifespan.startup
                    │       └── awaits startup.complete from the app
                    └── ep.listen() × N             ← only now accept connections
```

On shutdown, a Twisted `"before shutdown"` system event trigger runs
`LifespanHandler.shutdown()` before `kill_all_applications()` cancels
in-flight connections. Because the trigger returns a
`defer.Deferred.fromFuture`, Twisted waits for the coroutine to complete
before proceeding — the same pattern used by `kill_all_applications` itself.


## The ordering guarantee

The critical invariant is:

> No TCP or Unix socket begins listening until `lifespan.startup.complete`
> has been received from the application.

This means any resource initialised in a startup hook — an async database
client, an OpenFGA connection, a background task — is fully ready before
the first request can arrive. There is no race window.


## Graceful fallback for apps that do not support lifespan

The ASGI spec requires that servers continue normally if an application does
not support the lifespan protocol. Daphne implements this by racing the
application task against the first message it sends. If the task exits or
raises before sending any response, lifespan is silently disabled and the
server starts normally. Applications that do support lifespan are unaffected.


## What is not in scope

Daphne's lifespan implementation is intentionally minimal. It does not
add retry logic, does not impose a startup timeout (relying instead on the
application to fail fast via `startup.failed`), and does not expose
lifespan events to the access log. These are application-level concerns.

Lifespan events are logged via Python's standard `logging` module (see the
log messages table in the reference). They do not appear in the NCSA-format
access log produced by `AccessLogGenerator`, which is reserved for
client-facing protocol events that carry a client address, path, and status
code. Lifespan has none of these fields.
