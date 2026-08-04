# nonblocking-transmit: end-to-end transmit backpressure

Status: implemented (v1). See "As-built notes" at the end for where the
implementation deliberately simplifies this design.

## 1. Motivation

RustDDS runs a single event-loop thread (`dp_event_loop`) that both receives
and sends RTPS messages, sharing one `Rc<UDPSender>` between all writers,
readers and discovery. The current send path is fire-and-forget:

- The bound unicast sender socket is non-blocking, so on a full kernel send
  buffer `send_to` returns `WouldBlock` and the datagram is silently **dropped**
  (`send_to_udp_socket` swallows the error, `src/network/udp_sender.rs`).
- The multicast sender sockets are built from a `socket2::Socket` and handed to
  `mio_08::net::UdpSocket::from_std` **without** `set_nonblocking(true)`, so they
  are effectively **blocking**: a full kernel buffer stalls the entire event
  loop, freezing reception and all other endpoints.

Neither is acceptable under load. We want back-pressure that reaches all the way
from the network socket to the application `DataWriter::write()` call, so that a
slow network throttles producers instead of dropping data or blocking the loop.

## 2. Goals and constraints

- Non-blocking sender sockets, polled for **write readiness** in the event loop.
- A **per-socket round-robin queue** of writers willing to send bulk DATA. On a
  writable event the next willing sender is served, as is usual for non-blocking
  I/O.
- Back-pressure to `DataWriter`s using the existing full-send-buffer mechanism.
- **Control submessages must never be stalled or lost**: periodic HEARTBEATs,
  reader ACKNACKs, GAPs and discovery traffic keep the protocol live and must
  get out even while bulk data is congested.
- Keep the change to the control flow small: no new threads, no rewrite of the
  writer/reader state machines.

## 3. Two traffic classes

Outbound traffic is split into two classes with different policies.

- **Control / discovery** (HEARTBEAT, ACKNACK, GAP, SPDP/SEDP): small, latency
  sensitive, and self-healing, but placed in a **never-dropped high-priority
  queue** and drained before bulk data on every writable cycle.
- **Bulk user DATA / DATAFRAG** (including NACK-driven repair): flow-controlled
  through a per-socket round-robin queue and, when it cannot drain, propagated
  back as application back-pressure.

## 4. The OutboundScheduler

A new `OutboundScheduler`, held behind `Rc<RefCell<..>>` and shared by
`UDPSender` (for control enqueue) and `DPEventLoop` (for bulk serving, poll
reconciliation and writable handling).

Sockets are identified by:

```text
SocketId = Unicast | Multicast(usize)   // index into UDPSender.multicast_sockets
```

`UDPSender` maps a `Locator` (plus an `InterfaceSelector` for multicast, from the
interface-aware transmit feature) to a `SocketId` + `SocketAddr`, and exposes a
non-blocking send returning `Sent | WouldBlock`, plus the raw fd of each socket.

Per socket the scheduler holds:

- `control: VecDeque<Datagram>` where `Datagram = (SocketAddr, Bytes)`, bounded
  by **coalescing**:
  - HEARTBEAT coalesced by `(writer EntityId -> destination)` (newer supersedes
    older, since a heartbeat is a cumulative announcement),
  - ACKNACK coalesced by `(reader EntityId -> destination)` (newer supersedes),
  - GAP and discovery are FIFO.
  Nothing is dropped; coalescing keeps the queue naturally small. A
  high-watermark warning is logged if a socket is pathologically wedged.
- `bulk: VecDeque<EntityId>` of writers willing to send, plus a membership set to
  avoid duplicates (round-robin fairness).
- `writable_armed: bool` and a `dirty` flag used to reconcile poll interest.

### 4.1 Control path (callers unchanged)

`UDPSender::send_to_locator` / `send_to_locator_list` /
`send_to_multicast_locator_via` transparently enqueue into the target socket's
control queue and attempt an immediate non-blocking flush. Existing call sites in
`writer.rs`, `reader.rs` and discovery are unchanged; they simply no longer risk
blocking or silently dropping.

### 4.2 Bulk path

`Writer::process_pending` sends DATA/DATAFRAG through a send path that now
reports `WouldBlock`. On `WouldBlock` the writer stops advancing its TX cursor
and the event loop marks the writer willing on the blocked `SocketId`(s).

### 4.3 Serving on writable

On a writable event for a socket the scheduler:

1. Drains the **control** queue first (until empty or `WouldBlock`).
2. Serves **bulk** writers round-robin, each resuming from its own cursor for a
   bounded per-turn fragment batch, until `WouldBlock` or the queue is empty.

Writable interest is armed only while a socket has queued work and disarmed when
both queues drain, so we do not spin on the (almost always writable) UDP socket.

## 5. Large data: one sample, thousands of messages

A `CacheChange` whose serialized payload exceeds `data_max_size_serialized` is
expanded by `FragmentationIter` (`src/rtps/writer.rs`) into `num_frags` DATAFRAG
messages -- potentially thousands -- followed by a trailing HEARTBEAT, each sent
as an individual datagram. This is the case that most needs back-pressure and it
shapes two decisions.

### 5.1 Per-fragment resume cursor

A single sequence number no longer identifies a send position, because one
sample maps to thousands of datagrams. The writer's transmit position becomes:

```text
tx_cursor = (SequenceNumber, FragmentNumber)
```

When a DATAFRAG send returns `WouldBlock`, the writer records the exact
`(seq, frag)` it could not send and yields. On the next writable turn it rebuilds
a `FragmentationIter` starting at `frag` and continues. `num_frags` and
`fragment_size` are a deterministic function of the payload size, so any
fragment can be regenerated on demand; we therefore:

- never restart a huge sample from fragment 1 (that would livelock),
- never buffer thousands of pre-serialized messages -- the bulk queue holds only
  `EntityId`s plus the `(seq, frag)` cursor, and the payload lives exactly once
  as the `CacheChange` inside `WriterSendBuffer`,
- simply regenerate the one fragment that hit `WouldBlock` (cheap) instead of
  retaining the datagram.

The trailing HEARTBEAT for a fragmented sample is enqueued (into the control
queue) only once the cursor reaches the final fragment.

### 5.2 Fairness and control interleaving

Each writer emits only a **bounded batch of fragments per turn**, then yields so
the socket's control queue is re-drained and the next willing writer runs. Since
control is drained first on every writable cycle, HEARTBEATs and reader ACKNACKs
are never starved -- even while a multi-thousand-fragment sample streams out, and
even across several concurrently-sending large writers.

### 5.3 NACK-driven repair

The repair path (`handle_repair_data_send_worker`) also emits many DATAFRAGs when
a reader NACKs missing fragments. It reuses the same bulk queue and `(seq, frag)`
cursor treatment, so repair of large samples is likewise resumable and fair.

## 6. Application back-pressure

`WriterSendBuffer` already throttles reliable writers on the **unacked window**
(`has_room`, `src/rtps/writer_send_buffer.rs`). We add a second dimension: an
**unsent backlog** limit.

```rust
// WriterSendBuffer::Inner (new fields)
sent_frontier: SequenceNumber, // mirrors Writer's (seq) TX cursor
backlog_limit: usize,          // max unsent samples: last_seq - sent_frontier
```

- `has_room` additionally requires
  `i64::from(last_seq) - i64::from(sent_frontier) < backlog_limit`
  for non-built-in writers. Built-in/discovery writers stay exempt so discovery
  never stalls.
- `set_sent_frontier(sn)` is called by the `Writer` as its sequence-number
  cursor advances; it wakes parked producers when the frontier moves.

When a socket congests, the TX cursor stops advancing, `sent_frontier` stalls,
the unsent backlog fills, and `admit_blocking` / `try_admit` block or return
`WouldBlock` -- back-pressure reaching `DataWriter::write()` with **no new
application-facing API**.

Note the backlog is counted in **samples**, so a single giant fragmented sample
counts as one unsent sample but holds `sent_frontier` back until all its
fragments flush; even `backlog_limit = 1` makes the next `write()` wait until the
giant sample is fully on the wire.

### 6.1 Best-effort is opt-in (`WriteOptions::best_effort_may_block`)

DDS v1.4 section 2.2.2.4.2.11 ("write") does not permit `write` to block for
best-effort reliability. Applying the backlog back-pressure unconditionally to
best-effort writers would violate that, so it is **opt-in** per write:

- `WriteOptions::best_effort_may_block == false` (the default): a best-effort
  write is never throttled at admission (`has_room` returns true regardless of
  the backlog), and when the send socket returns `WouldBlock` the Writer
  **drops** the (rest of the) sample and advances (`process_pending` does not
  record blocked sockets, so no write-readiness back-pressure is armed). This is
  the spec-compliant, pre-back-pressure behavior.
- `WriteOptions::best_effort_may_block == true`: the backlog limit and the
  resume-on-writable back-pressure apply to this best-effort write exactly as for
  reliable writers, so `write()` may block until the socket drains.

Reliable writers always back-pressure regardless of this flag (their
`may_block = shared.reliable_writer || write_options.best_effort_may_block()`).

## 7. Event-loop and portability impact

The control flow grows by a small, bounded amount:

- One new event arm for sender-writable tokens (reusing the freed sender token
  slots in `src/rtps/constant.rs`) that calls `scheduler.on_writable(socket_id)`.
- One `scheduler.reconcile(&poll)` call per loop iteration to arm/disarm writable
  interest.
- `process_pending` and the DATA send path gain a `WouldBlock` return and a
  `(seq, frag)` cursor. Control-message call sites are untouched.

Poll integration: the event loop `Poll` is `mio_06` while sender sockets are
`mio_08`. Write readiness is registered by wrapping each sender socket's raw fd in
`mio_06::unix::EventedFd` (Unix; consistent with the existing Unix-only
`IP_PKTINFO` receive path). On non-Unix platforms the fallback is to skip
writable registration and retry queued sends at the end of each loop iteration
and on the shared timer tick -- still non-blocking, with control coalesced and
never dropped and bulk back-pressured.

## 8. Summary of changes

- `src/network/udp_sender.rs`: non-blocking multicast sockets; `SocketId`;
  locator -> `SocketId`/`SocketAddr` mapping; non-blocking send returning
  `Sent | WouldBlock`; raw-fd accessors; route control sends through the
  scheduler.
- `src/rtps/` new `OutboundScheduler` (per-socket control + bulk queues,
  coalescing, round-robin, reconcile/on_writable).
- `src/rtps/constant.rs`: sender-writable tokens + `TokenDecode` variant.
- `src/rtps/dp_event_loop.rs`: register/arm writable interest; writable event
  arm; per-iteration reconcile.
- `src/rtps/writer.rs`: `SampleCursor` (`Fresh` / `Frag(n)` / `Heartbeat`) TX
  cursor + `blocked_sockets`; `WouldBlock`-aware, resumable `process_pending`;
  `FragmentationIter::new_resume` yielding `(SampleCursor, Message)`;
  `send_message_to_readers(.., TrafficClass)` returning blocked sockets.
- `src/rtps/writer_send_buffer.rs`: `sent_frontier` + `backlog_limit` +
  `set_sent_frontier`.
- `src/rtps/outbound.rs`: `SocketId`, `SendOutcome`, `TrafficClass`, control
  `Datagram` queue types.
- `src/rtps/constant.rs`: `sender_writable_token` / `sender_writable_socket_id`
  in the fixed-token range `PTB+65..=PTB+79`.
- `src/rtps/dp_event_loop.rs`: `bulk_ready` round-robin + `writable_armed`;
  `on_socket_writable`, `reconcile_writable_interest` (EventedFd, unix),
  `service_outbound`.

## 9. As-built notes (v1)

The implementation follows this design with a few deliberate simplifications:

- Control queue is plain FIFO (never dropped) with a soft high-watermark
  warning (`CONTROL_QUEUE_WARN_LEN`). The coalescing of HEARTBEAT/ACKNACK by
  `(endpoint -> destination)` described in section 4 is a future optimization;
  in practice control traffic is low-rate and the FIFO stays small.
- Resume is message-granular. When a DATAFRAG hits WouldBlock we re-send that
  exact fragment (via `SampleCursor::Frag(n)`) on the next writable, to *all* of
  its destinations. At the congestion boundary a non-blocked destination may
  therefore receive a duplicate fragment; this is bounded (one or two repeats)
  and harmless (readers de-duplicate by fragment number). Per-route memoization
  to eliminate the duplicate is a possible future refinement.
- `backlog_limit` is set equal to the reliable `window_limit`
  (`src/dds/pubsub.rs`). For reliable writers the ack window is normally the
  binding constraint; the backlog is the binding constraint for best-effort
  writers (that opted into blocking) and under socket congestion.
- Best-effort back-pressure is opt-in via `WriteOptions::best_effort_may_block`
  (default `false`, see section 6.1). By default best-effort writes never block
  at admission and drop the sample on socket `WouldBlock` instead of holding it;
  the flag has no effect on reliable writers.
- NACK-driven repair (`handle_repair_data_send_worker`,
  `handle_repair_frags_send_worker`) sends DATA/DATAFRAG as bulk but simply
  drops on WouldBlock: the reader re-NACKs, so repair is self-healing and never
  buffers or stalls. Only the push path (`process_pending`) is resumable.
- Write-readiness registration uses `mio_06::unix::EventedFd` over the mio_08
  sender sockets' raw fds (level-triggered, armed on demand). On non-unix the
  loop falls back to draining the queues each iteration with a short poll
  timeout while anything is pending.
