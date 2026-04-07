# Lifespan support — reference

This document covers Daphne's implementation of the
[ASGI Lifespan sub-specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html).

## Scope

Daphne calls the application with the following scope dict at process startup,
as defined in the [ASGI lifespan specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#scope):

```python
{
    "type": "lifespan",
    "asgi": {
        "version": "3.0",       # ASGI protocol version
        "spec_version": "2.0",  # lifespan sub-spec version; optional, defaults to "1.0"
    },
    "state": {},  # always present; populate during startup and Daphne will
                  # shallow-copy it into every HTTP and WebSocket scope
                  # (see Lifespan State section below)
}
```

The scope is passed exactly once per process. No per-request fields
(`headers`, `client`, `server`, etc.) are present.

`asgi["version"]` identifies the ASGI protocol generation (always `"3.0"` for
Daphne). `asgi["spec_version"]` identifies the lifespan sub-specification
revision; it is optional and defaults to `"1.0"` when absent. The current
revision is `"2.0"`, which added `startup.failed` and `shutdown.failed`
([version history](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#version-history)).
These two fields are independent and should not be confused.


## Lifespan state

The `"state"` key in the scope is defined by the
[ASGI lifespan spec](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#scope)
and allows the application to share objects — connection pools, HTTP clients,
caches — between the lifespan hooks and every subsequent HTTP/WebSocket request
handler, without using module-level globals.

When present, `scope["state"]` is an empty `dict` at startup. The application
populates it during the startup hook. Daphne then passes a **shallow copy** of
that dict as `scope["state"]` into every HTTP and WebSocket scope for the
lifetime of the process, so request handlers can read from it directly.

```python
# lifespan hook (write)
scope["state"]["db_pool"] = await create_pool(DATABASE_URL)

# HTTP handler (read)
pool = scope["state"]["db_pool"]
```

Because the server manages the copy, objects in state are always bound to the
correct event loop and process, with no risk of sharing stale connections
across processes or threads.

The how-to guide shows the full pattern.


## Receive events

Events sent from Daphne to the application via `receive()`, as defined in the
[ASGI lifespan specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#startup-receive-event).

### `lifespan.startup`

Sent immediately when the lifespan scope is opened, before any endpoint
begins listening for connections.

```python
{"type": "lifespan.startup"}
```

The application must not return from the lifespan callable before responding
to this event.

### `lifespan.shutdown`

Sent when the process is stopping, after all active connections have been
given the opportunity to close.

```python
{"type": "lifespan.shutdown"}
```


## Send events

Events sent from the application to Daphne via `send()`, as defined in the
[ASGI lifespan specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#startup-complete-send-event).

### `lifespan.startup.complete`

The application has completed startup successfully. Daphne will now begin
accepting connections.

```python
{"type": "lifespan.startup.complete"}
```

### `lifespan.startup.failed`

The application encountered a fatal error during startup. Daphne logs the
message and stops the process without accepting any connections.

```python
{
    "type": "lifespan.startup.failed",
    "message": "human-readable reason",   # optional, defaults to ""
}
```

Sending `startup.failed` is an **explicit failure signal** — it is not the
same as the fallback triggered by an unhandled exception. The application
should send this event when it has detected a condition it wants to report
clearly (e.g. a required service is unreachable), then raise to terminate
the lifespan task. See `LifespanHandler.startup()` below for how Daphne
handles the resulting `RuntimeError`.

### `lifespan.shutdown.complete`

The application has completed shutdown. Daphne proceeds to cancel remaining
connections and stop the reactor.

```python
{"type": "lifespan.shutdown.complete"}
```

### `lifespan.shutdown.failed`

The application encountered an error during shutdown. Daphne logs the message
and continues stopping the process.

```python
{
    "type": "lifespan.shutdown.failed",
    "message": "human-readable reason",   # optional, defaults to ""
}
```

Sending `shutdown.failed` does not prevent the process from stopping. It
signals that cleanup was incomplete — for example, a connection pool that
could not be drained — so operators have a clear log entry to act on.

### `send()` contract

Every value passed to `send()` must be a `dict`. Passing any other type raises
`TypeError` immediately, which propagates back to the application task. If this
happens before a startup response has been sent, Daphne treats it as a
fallback (lifespan not supported) and logs a `WARNING`. Message strings in the
`"message"` field are sanitised before logging — newlines are escaped and the
value is truncated to 500 characters — so it is safe to include diagnostic
detail there without risking log injection.


## Ordering guarantees

| Guarantee | Detail |
|---|---|
| No connections before startup | `ep.listen()` is not called until `startup.complete` is received |
| Shutdown before connection cancellation | The lifespan shutdown trigger fires before `kill_all_applications()` |
| Twisted waits for shutdown | The shutdown trigger returns a `Deferred`; Twisted blocks until it resolves |


## Fallback behaviour

If the application does not support the lifespan protocol, Daphne falls back
silently and starts normally. The fallback is triggered when:

- The application task raises any exception before sending a startup response
- The application task exits cleanly without sending a startup response

In both cases `_supported` is set to `False`, no further lifespan events are
sent, and a `DEBUG`-level log message is emitted.

Sending `lifespan.startup.failed` is **not** a fallback — it is an explicit
failure signal that stops the process.

### `ProtocolTypeRouter` and the fallback

When using Django Channels, if no `"lifespan"` key is present in
`ProtocolTypeRouter`, the router raises `ValueError` for the lifespan scope.
This exception is caught by the fallback mechanism (the task raised before
sending a startup response), so the server starts normally with lifespan
disabled. Users will see the `DEBUG` fallback log rather than any error.

To enable lifespan hooks, add an explicit `"lifespan"` entry to the router
(see the how-to guide).


## Shutdown timeout

Daphne waits up to **30 seconds** for `shutdown.complete` after sending
`lifespan.shutdown`. If the timeout expires, an error is logged and the
process continues stopping.

This is controlled by `lifespan_shutdown_timeout` on `Server` (default
30 seconds), which is passed through to `LifespanHandler` as
`shutdown_timeout`.

### Relationship to `application_close_timeout`

These are two distinct timeouts with different purposes:

| Timeout | Default | Controls |
|---|---|---|
| `lifespan_shutdown_timeout` | 30 s | How long Daphne waits for the app's lifespan shutdown hook to complete |
| `application_close_timeout` | 10 s | How long Daphne tolerates an individual HTTP/WebSocket connection instance running after its transport disconnects |

A generous `lifespan_shutdown_timeout` is appropriate because the app is
performing deliberate cleanup (draining pools, closing broker connections).
`application_close_timeout` governs potentially many misbehaving connection
instances and is kept tighter by default.


## `LifespanHandler` class

Internal class in `daphne.lifespan`. Not part of the public API but
documented here for contributors.

```python
LifespanHandler(application, startup_timeout=60, shutdown_timeout=30)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `application` | ASGI callable | required | The ASGI application |
| `startup_timeout` | `float` or `None` | `60` | Seconds to wait for a startup response. `None` disables the timeout entirely — the server will wait indefinitely, which is appropriate for applications with genuinely long startup work but risks a hung process if the application stalls. Passing `None` emits a `WARNING` at construction time so accidental misconfiguration is always visible in logs. In normal use this is set by `Server.lifespan_startup_timeout`. |
| `shutdown_timeout` | `float` | `30` | Seconds to wait for `shutdown.complete`. In normal use this is set by `Server.lifespan_shutdown_timeout` rather than relying on this default directly. |

### Methods

**`await startup()`**

Starts the lifespan task, sends `lifespan.startup`, and waits for the
application to respond. Raises `RuntimeError` if `startup.failed` is
received. Returns normally on `startup.complete` or graceful fallback.

The `RuntimeError` is caught by `Server._lifespan_startup_then_listen()`
without additional logging — `startup.failed` is already logged as `ERROR`
by this method before raising. Any other unexpected exception from `startup()`
is logged as `ERROR` by the caller with the full traceback.

**`await shutdown()`**

Sends `lifespan.shutdown` and waits for the application to respond. Never
raises — all errors are logged. Returns immediately if lifespan is not
supported or the task is already done.


## Server integration

This section describes how `Server` in `daphne.server` drives `LifespanHandler`.
It is intended for contributors and is not relevant to application authors.

### Task scheduling — why `loop.create_task()`

Daphne's `AsyncioSelectorReactor` owns the asyncio event loop and drives it
through Twisted's own internal iteration mechanisms rather than calling
`loop.run_forever()`. This means that at certain points — specifically inside
Twisted callbacks such as `reactor.callWhenRunning` and "before shutdown"
system event triggers — the loop exists and has been set as the current loop,
but Python's asyncio machinery does not consider it to be *running*.

This distinction matters for task scheduling:

| API | How it finds the loop | Works in Twisted callbacks? |
|---|---|---|
| `asyncio.create_task(coro)` | `get_running_loop()` — raises `RuntimeError` if no loop is running | No |
| `asyncio.ensure_future(coro)` | `get_event_loop()` — deprecated since Python 3.10 outside a running loop; raises `RuntimeError` in Python 3.14+ | No from Python 3.14 |
| `loop.create_task(coro)` | Direct reference to the loop object — no policy lookup needed | Yes |

`loop.create_task()` is the correct choice here: it is the low-level API
explicitly designed for framework code that already holds a loop reference,
it is not deprecated, and it works regardless of whether the loop is considered
running. The Python docs describe this section as intended for "authors of
lower-level code, libraries, and frameworks, who need finer control over the
event loop behavior" and demonstrate `loop.create_task()` as the appropriate
API when a direct loop reference is available.

Relevant Python documentation:

- [`loop.create_task()`](https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.create_task) — low-level task scheduling on a known loop
- [`asyncio.create_task()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task) — high-level equivalent, requires a running loop
- [`asyncio.ensure_future()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.ensure_future) — deprecated since 3.10 when no loop is running
- [`asyncio.get_event_loop()`](https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.get_event_loop) — deprecated since 3.10; raises `RuntimeError` in 3.14+

Daphne holds the loop reference as `reactor._asyncioEventloop`, which is set
up during `asyncioreactor.install()` at import time. Both lifespan scheduling
sites use it:

```python
# In _schedule_lifespan_startup (reactor.callWhenRunning callback)
reactor._asyncioEventloop.create_task(
    self._lifespan_startup_then_listen(),
    name="daphne.lifespan.startup",
)

# In _lifespan_shutdown (before-shutdown system event trigger)
reactor._asyncioEventloop.create_task(
    self._lifespan_handler.shutdown(),
    name="daphne.lifespan.shutdown",
)
```

Inside coroutines — where the loop genuinely is running — the standard
`asyncio.create_task()` is used instead (e.g. in `create_application` and
inside `LifespanHandler.startup()`).

### Startup sequencing

`Server.run()` registers the lifespan startup via `reactor.callWhenRunning`,
which fires once the reactor is live. The coroutine
`_lifespan_startup_then_listen()` then runs:

1. Constructs `LifespanHandler` and assigns it to `self._lifespan_handler`
2. Awaits `handler.startup()` — no endpoints are bound during this time
3. On `startup.complete`, calls `ep.listen()` for each configured endpoint
4. On `startup.failed` or any unexpected exception, calls `self.stop()`

This guarantees the ordering described in the [Ordering guarantees](#ordering-guarantees)
section: no connections are accepted until the application has confirmed it is
ready.

### State injection

After startup completes, `LifespanHandler.state` holds whatever the application
wrote into `scope["state"]` during its startup hook. `Server.create_application()`
injects a shallow copy into every HTTP and WebSocket scope before calling the
application. This implements the state propagation requirement defined in the
[ASGI lifespan spec](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#scope)
and formalised in the
[asgiref 3.7.0 typing update (May 2023)](https://github.com/django/asgiref/blob/main/CHANGELOG.txt):

```python
scope["state"] = (
    self._lifespan_handler.state.copy()
    if self._lifespan_handler is not None
    else {}
)
```

The copy is unconditional: if lifespan is not supported, `state` is always
`{}` in `LifespanHandler.__init__`, so every request still receives an empty
dict rather than no key at all. Applications can rely on `scope["state"]`
always being present.

### Shutdown sequencing

Two "before shutdown" system event triggers are registered in order:

```python
reactor.addSystemEventTrigger("before", "shutdown", self._lifespan_shutdown)
reactor.addSystemEventTrigger("before", "shutdown", self.kill_all_applications)
```

Twisted fires them in registration order. `_lifespan_shutdown` returns a
`Deferred` wrapping the shutdown coroutine; Twisted blocks on it before
proceeding to `kill_all_applications`. This ensures the application's cleanup
hook always runs before active connections are forcibly cancelled.


## Log messages

| Level | Message | When |
|---|---|---|
| `INFO` | `Lifespan startup complete` | `startup.complete` received |
| `INFO` | `Lifespan shutdown complete` | `shutdown.complete` received |
| `ERROR` | `Lifespan startup failed: <message>` | `startup.failed` received |
| `ERROR` | `Lifespan shutdown failed: <message>` | `shutdown.failed` received |
| `ERROR` | `Lifespan shutdown timed out after N seconds` | Shutdown timeout expired |
| `ERROR` | `Lifespan startup task was cancelled unexpectedly` | Task cancelled before startup |
| `ERROR` | `Unhandled error in lifespan startup: <exc>` | Unexpected exception in startup |
| `WARNING` | `Unexpected lifespan startup message type: <type>` | Unrecognised send event during startup |
| `WARNING` | `Unexpected lifespan shutdown message type: <type>` | Unrecognised send event during shutdown |
| `WARNING` | `Lifespan app sent a malformed startup message with no 'type' key` | `send()` called with a dict missing the `"type"` key during startup |
| `WARNING` | `Lifespan app sent a malformed shutdown message with no 'type' key` | `send()` called with a dict missing the `"type"` key during shutdown |
| `WARNING` | `lifespan startup_timeout is None; the server will wait indefinitely` | `LifespanHandler` constructed with `startup_timeout=None` |
| `WARNING` | `Application does not support lifespan protocol (task raised TypeError ...)` | `send()` called with a non-dict value |
| `DEBUG` | `Application does not support lifespan protocol (task raised <ExcType>), continuing without lifespan.` | Task raised an exception before startup |
| `DEBUG` | `Application does not support lifespan protocol (task exited without response), continuing without lifespan.` | Task exited cleanly without sending a startup response |
