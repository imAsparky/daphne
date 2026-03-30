# Lifespan support — reference

## Scope

Daphne calls the application with the following scope dict at process startup:

```python
{
    "type": "lifespan",
    "asgi": {"version": "3.0"},
}
```

The scope is passed exactly once per process. No per-request fields
(`headers`, `client`, `server`, etc.) are present.


## Receive events

Events sent from Daphne to the application via `receive()`.

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

Events sent from the application to Daphne via `send()`.

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

Sending `shutdown.failed` does not prevent the process from stopping.


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
LifespanHandler(application, shutdown_timeout=30)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `application` | ASGI callable | required | The ASGI application |
| `shutdown_timeout` | `float` | `30` | Seconds to wait for `shutdown.complete`. In normal use this is set by `Server.lifespan_shutdown_timeout` rather than relying on this default directly. |

### Methods

**`await startup()`**

Starts the lifespan task, sends `lifespan.startup`, and waits for the
application to respond. Raises `RuntimeError` if `startup.failed` is
received. Returns normally on `startup.complete` or graceful fallback.

**`await shutdown()`**

Sends `lifespan.shutdown` and waits for the application to respond. Never
raises — all errors are logged. Returns immediately if lifespan is not
supported or the task is already done.


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
| `DEBUG` | `Application does not support lifespan protocol (task raised <ExcType>), continuing without lifespan.` | Task raised an exception before startup |
| `DEBUG` | `Application does not support lifespan protocol (task exited without response), continuing without lifespan.` | Task exited cleanly without sending a startup response |
