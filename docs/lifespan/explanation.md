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
handled as a native asyncio coroutine scheduled with `asyncio.create_task`,
which works because those callables are invoked from within a running asyncio
context. The lifespan task requires a different approach, explained below.

The implementation adds a `LifespanHandler` class that manages a single,
long-lived asyncio Task for the duration of the process:

```
reactor.run()
    └── callWhenRunning → _schedule_lifespan_startup
            └── asyncio.get_event_loop().create_task(_lifespan_startup_then_listen)
                    ├── LifespanHandler.startup()   ← feeds lifespan.startup
                    │       └── awaits startup.complete from the app
                    └── ep.listen() × N             ← only now accept connections
```

`_schedule_lifespan_startup` is a plain synchronous Twisted callback, not a
coroutine, so it cannot use `asyncio.create_task()` (which calls
`get_running_loop()` and raises `RuntimeError` when the asyncio loop is driven
by Twisted rather than `loop.run_forever()`). `asyncio.ensure_future()` avoids
that but has been deprecated since Python 3.10. The correct solution is
`asyncio.get_event_loop().create_task()`, which is safe as long as the global
event loop is set to the reactor's actual loop before the callback fires —
see the next section for how this is achieved.

On shutdown, a Twisted `"before shutdown"` system event trigger runs
`LifespanHandler.shutdown()` before `kill_all_applications()` cancels
in-flight connections. The trigger schedules the shutdown coroutine via
`asyncio.get_event_loop().create_task()` for the same reasons as startup,
then wraps the resulting Task in a `defer.Deferred.fromFuture` so Twisted
waits for completion before proceeding — the same pattern used by
`kill_all_applications` itself.


## Event loop identity across process contexts

Scheduling a task with `asyncio.get_event_loop().create_task()` is only correct
if `get_event_loop()` returns the same loop the Twisted reactor is actually
using. Ensuring this is harder than it looks.

**The naive approach and why it fails.** Daphne creates a module-level event
loop (`twisted_loop`) and installs it into the Twisted reactor at import time.
It is tempting to use `twisted_loop.create_task()` directly in the scheduling
callbacks — it avoids any deprecated API and requires no private attribute
access. This works correctly in the main process. However, Daphne's test suite
runs each integration test by forking a subprocess via `DaphneProcess`. Before
importing `daphne.server`, that subprocess calls `_reinstall_reactor()`, which
creates a fresh event loop and installs it into a new reactor. When
`daphne.server` is then reimported, the module-level `twisted_loop =
asyncio.new_event_loop()` line runs again, creating a second fresh loop that is
never installed into the reactor. At this point `twisted_loop` and the reactor's
actual loop are two different objects. Any task scheduled on `twisted_loop` is
placed on a loop that is never driven, so it never runs. The server starts
cleanly but never calls `listen_success`, `listening_addresses` is never
populated, and the subprocess times out without signalling readiness.

**Why not use `reactor._asyncioEventloop` directly?** `AsyncioSelectorReactor`
exposes no public method to retrieve its event loop. `_asyncioEventloop` is a
private attribute with no stability guarantee — a Twisted upgrade could rename
or remove it silently. Using it would trade a correctness bug for a
maintainability risk.

**The solution: `_daphne_loop`.** Daphne solves this by tracking the installed
loop itself, using only the public asyncio API. At module import time, after
each `asyncioreactor.install(twisted_loop)` call, `_daphne_loop` is set to
`twisted_loop` — Daphne installed it, so it knows it is the reactor's loop. In
the subprocess case where a reactor is already installed when the module is
reimported, `_daphne_loop = asyncio.get_event_loop()` retrieves the loop that
the installer already set via `asyncio.set_event_loop()` — the established
contract for asyncio reactor installation. `Server.run()` then calls
`asyncio.set_event_loop(_daphne_loop)` as a defensive reset before the reactor
starts, guarding against anything between import and `run()` (such as Django
setup) having changed the global loop. After that call, `asyncio.get_event_loop()`
in any Twisted callback reliably returns the reactor's actual loop — with no
private attribute access and no deprecated API anywhere in the implementation.


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
add retry logic and does not expose lifespan events to the access log.
These are application-level concerns.

Startup and shutdown timeouts are enforced (defaulting to 60 and 30 seconds
respectively) so that a hung application cannot prevent the server from
starting or stopping. Applications that need to signal a fatal startup
condition explicitly should send `lifespan.startup.failed` rather than
relying on the timeout.

Lifespan events are logged via Python's standard `logging` module (see the
log messages table in the reference). They do not appear in the NCSA-format
access log produced by `AccessLogGenerator`, which is reserved for
client-facing protocol events that carry a client address, path, and status
code. Lifespan has none of these fields.
