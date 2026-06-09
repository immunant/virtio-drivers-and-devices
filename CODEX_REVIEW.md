# PR 33 deadlock-focused review

Scope: reviewed PR 33 (`perl/vsock-bridge-locking` at `4e0719e0`) against `master`
(`8f9454ed`). I focused on lock/liveness deadlocks in the vsock changes, and did
not review legacy `poll` except to avoid relying on it. `poll_direct` is covered.

## Findings

### High: `poll_direct` can consume a connection request with no rejection path

`poll_direct` silently drops connection requests for non-listening ports:

- `src/device/socket/connectionmanager.rs:637`
- `src/device/socket/connectionmanager.rs:643`
- `src/device/socket/connectionmanager.rs:645`
- `src/device/socket/connectionmanager.rs:650`

For listening ports it returns a `ConnectionRequest` with `new_connection`, but the
connection is deliberately not inserted into `inner.connections` until the caller
accepts it:

- `src/device/socket/connectionmanager.rs:653`
- `src/device/socket/connectionmanager.rs:659`
- `src/device/socket/connectionmanager.rs:660`

That leaves no way for a direct-mode caller to reject a request. The public
`force_close` path first looks up an already-registered connection, so it cannot
send an RST for the `new_connection` returned by `poll_direct` unless the caller
first accepts it:

- `src/device/socket/connectionmanager.rs:770`
- `src/device/socket/connectionmanager.rs:773`
- `src/device/socket/connectionmanager.rs:777`

The result is a protocol-level deadlock: a peer can send `REQUEST` and wait
forever for either `RESPONSE` or `RST`, while this side has already consumed the
request descriptor and returned `Ok(None)` or exposed a request that can only be
accepted.

Fix direction: make rejection an explicit direct-mode outcome. For example,
return the request to the caller even when policy will reject it and add a
`reject(Connection)`/action API, or insert a pending connection state that
`force_close` can use before acceptance. The actual RST send should happen after
`driver.poll` returns, not from inside the poll callback, so the RX/TX queue lock
is not held while `send_packet_to_queue` spins.

### Medium: `poll_direct` does not complete a peer-initiated shutdown

`poll_direct` special-cases only reset disconnects:

- `src/device/socket/connectionmanager.rs:671`
- `src/device/socket/connectionmanager.rs:676`

For `Disconnected { reason: Shutdown }`, it falls through the normal event path,
updates connection info, and returns the event:

- `src/device/socket/connectionmanager.rs:681`
- `src/device/socket/connectionmanager.rs:691`

However, the only built-in deferred shutdown completion path is in `recv`, and it
depends on `peer_requested_shutdown` being set:

- `src/device/socket/connectionmanager.rs:712`
- `src/device/socket/connectionmanager.rs:714`
- `src/device/socket/connectionmanager.rs:719`

`poll_direct` never sets that flag. If a shutdown arrives with buffered data, the
caller can drain the buffer but `recv` will not send the final RST. If no data is
buffered, the caller may not call `recv` at all. In both cases the peer can wait
indefinitely for the shutdown acknowledgement, and the local connection remains
registered unless the direct-mode caller knows to call `force_close` manually.

Fix direction: either make `poll_direct` preserve the manager's shutdown state
machine, or make the direct-mode contract explicit and provide a safe close action
for the caller. To preserve the current manager semantics without reintroducing
the lock/spin deadlock, compute any required close action while handling the event
but perform `driver.force_close` only after the queue poll call has returned and
the queue lock is dropped.

## Notes

I did not find a higher-level manager or connection lock still held across the
main queue-wait helpers in the direct send paths: `send`, `update_credit`,
`shutdown`, `force_close`, `connect`, and public `accept` all drop the manager
lock and, where applicable, connection lock before entering
`add_notify_wait_pop`/`wait_pop_add_notify`.

`cargo test --features spin` passes on this checkout.
