# Ultrareview findings — perl/vsock-bridge-locking

Scope: deadlocks and concurrency issues from the locking refactor (`&mut self` →
`&self` + `Send + Sync` + per-connection `Arc<L::Lock<Connection>>`). The pure
`poll` API is excluded since it is being replaced by `poll_direct`. Each finding
below was verified against the code.

## Real concurrency bugs

### 1. `send()` credit-check race → `tx_cnt` underflow
- Location: `src/device/socket/vsock.rs:538-557`, `522-536`
- `check_peer_buffer_is_sufficient` locks, checks `peer_free()`, drops the lock,
  and returns the `Arc`; `send` then re-locks to bump `tx_cnt`. Two threads
  sending on the same connection both pass the check and both add `len`, so
  `peer_free() = peer_buf_alloc - (tx_cnt - peer_fwd_cnt)` (plain `u32`)
  underflows → panic in debug, wrap to ~4 GiB in release, then the driver
  overruns the peer RX buffer until the next credit update.
- Enabled by the new `&self` + `Send + Sync` API; the old `&mut self` made it
  impossible.
- Fix: hold the connection guard across check-and-bump (single check-and-reserve
  under the lock).

### 2. `PciTransport::notify` / `HypPciTransport::notify` not thread-safe
- Location: `src/transport/pci.rs:250-261`, `src/transport/x86_64.rs:186-193`
- Both do a non-atomic `queue_select` write → `queue_notify_off` read → notify
  write. With `Transport: Send + Sync` (`src/transport/mod.rs:39`) and `&self`, a
  tx-send and an rx-poll can call `notify` concurrently (the rx/tx locks are
  independent by design), interleaving the triplet and delivering a notification
  to the wrong queue's register. MMIO is unaffected (single `volwrite`).
- Fix: cache `queue_notify_off` per queue at `queue_set` time (existing TODO), or
  guard the triplet with a per-transport lock.

### 3. `ack_interrupt(&self)` is UB
- Location: `src/device/socket/connectionmanager.rs:462-467`
- Does `(&raw const self.0.driver).cast_mut()`, and the downstream chain
  materializes `&mut transport` for a volatile write. `transport: T`
  (`src/device/socket/vsock.rs:234`) is a bare field — not behind `UnsafeCell` or
  a lock — so forming `&mut` from a `&self`-derived pointer is UB regardless of
  runtime exclusivity, and unfixable under `Send + Sync` (every `Arc::deref` is
  another live `&self`).
- Fix: wrap `transport` in `L::Lock<T>` (matches the rest of the PR), or revert to
  the old `unsafe fn ack_interrupt(ptr: *mut Self)`.

## Error-path regressions from the same refactor (not races, but real)

### 4. `has_pending_credit_request` stuck `true`
- Location: `src/device/socket/vsock.rs:550-554`
- Flag is set *before* `request_credit(connection)?`. On `QueueFull` no
  `CreditRequest` is sent, the peer never sends a `CreditUpdate` (the only thing
  that clears it, `src/device/socket/vsock.rs:74-76`), and every later `send`
  wedges with `InsufficientBufferSpaceInPeer`.
- Fix: set the flag only after `request_credit` returns `Ok`, or roll back on
  `Err`.

### 5. `force_close` / `recv` remove connection before RST
- Location: `src/device/socket/connectionmanager.rs:771-780` and `714-719`
- `swap_remove` runs before `driver.force_close(...)?`. On transport error the
  entry is gone but no RST was sent, and retry returns `NotConnected`, leaking the
  peer half. `connect()` already does this correctly (call first, remove on
  error).
- Fix: clone the `Arc`, call `force_close`, then `retain`/remove on `Ok`.

## Excluded deadlock (for the record)

### bug_001: poll/poll_direct AB-BA deadlock
- `poll()` locks `inner` then `rx`; `poll_direct()` locks `rx` then `inner`.
  Only triggers when both run concurrently — two `poll_direct` calls share lock
  order and are fine. Out of scope since `poll` is being retired, but note
  `wait_for_event` (`src/device/socket/connectionmanager.rs:746`) still calls
  `self.poll()`, so `poll` isn't fully dead. Cleanest resolution is to remove
  `poll`/`wait_for_event` rather than reorder locks.

## Nits (not concurrency)
- bug_004: `accept` doc says "device side only" but it's the driver-side flow
  (`src/device/socket/connectionmanager.rs:67-69`).
- bug_013: `let mut socket` in doctest, all methods now `&self`
  (`src/device/socket/connectionmanager.rs:31`).
- bug_012: `poll_direct` doesn't dedup retransmitted `ConnectionRequest`s
  (`src/device/socket/connectionmanager.rs:644-666`).
