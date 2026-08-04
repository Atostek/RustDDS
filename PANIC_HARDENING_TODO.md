# RustDDS panic hardening — audit follow-up

Audit date: 2026-07-08. Scope: library code under `src/` (excluding `src/test/**`,
`serialization_test.rs`, `examples/**`, and `#[cfg(test)]` modules).

## Done in this pass

### Placeholder APIs (compile-time warning + documented panic)

These methods are **not implemented**. Each is marked `#[deprecated(note = "placeholder only; will panic if called")]` and calls `unreachable!(...)` if invoked. Doc comments state they are placeholders.

| Location | Method |
|---|---|
| `dds/with_key/datawriter.rs` | `get_matched_subscriptions()` |
| `dds/with_key/datareader.rs` | `wait_for_historical_data()` |
| `dds/pubsub.rs` | `Publisher::suspend_publications()` |
| `dds/pubsub.rs` | `Publisher::resume_publications()` |
| `dds/pubsub.rs` | `Publisher::wait_for_acknowledgments()` |

`ParticipantAccessControl::set_listener` now returns `Err(...)` like the authentication plugin (no panic).

### Internal invariant messages

Several `panic!` sites now prefix the message with **"RustDDS internal bug:"** so a crash clearly indicates a library defect, not user misuse. See commits touching `rtps/reader.rs`, `rtps/writer.rs`, `discovery/discovery_db.rs`, `dds/participant.rs`, `dds/pubsub.rs`, `security/security_plugins.rs`, `dds/with_key/simpledatareader.rs`, `dds/with_key/datasample_cache.rs`.

### Already fixed elsewhere

- Duplicate receive timestamp in `datasample_cache` logs `error!` and overwrites instead of panicking.

### Commented-out placeholders (not compiled)

Still present inside block comments; no runtime effect:

- `DataWriter::{get_liveliness_lost_status, get_offered_incompatible_qos_status, get_publication_matched_status}` in `datawriter.rs`
- `Subscriber::lookup_datareader` in `pubsub.rs`
- `Topic::get_inconsistent_topic_status` in `topic.rs`

### P1 hardening (2026-07-08)

**Category D — fragment bounds (network input)**

- [`data_frag.rs`](/home/juhe/RustDDS/src/messages/submessages/data_frag.rs): reject `fragmentsInSubmessage < 1`, fragment span overflow, span beyond `expected_total`, and oversized payload vs claimed fragment run.
- [`fragment_assembler.rs`](/home/juhe/RustDDS/src/rtps/fragment_assembler.rs): `AssemblyBuffer::new` and `insert_frags` return `Option`/`bool`; invalid spans log and skip instead of panicking on `BitVec::set`.
- Unit tests: `data_frag::tests::*`, `fragment_assembler::tests::reject_fragment_span_beyond_total`, `fragment_assembler_rejects_span_beyond_total`.

**Category C — startup / lifecycle**

- [`discovery.rs`](/home/juhe/RustDDS/src/discovery/discovery.rs): `construct_topic_and_poll` uses `try_construct!`; discovery start handshake logs and returns instead of `.expect()`.
- [`dp_event_loop.rs`](/home/juhe/RustDDS/src/rtps/dp_event_loop.rs): `DPEventLoop::new` → `CreateResult<Self>`; runtime `poll()` failure breaks loop cleanly.
- [`participant.rs`](/home/juhe/RustDDS/src/dds/participant.rs): dp_event_loop ready handshake before returning `DomainParticipantInner`; discovery `join()` warns on panic instead of unwrap.
- [`udp_listener.rs`](/home/juhe/RustDDS/src/network/udp_listener.rs): `new_socket` propagates non-blocking / mio errors with `?`.
- [`secure_discovery.rs`](/home/juhe/RustDDS/src/discovery/secure_discovery.rs): missing Property QoS returns `SecurityResult::Err` (surfaces via `try_construct!` in discovery).

**Still open in Category C:** `secure_discovery.rs:2639` `.expect` on missing local participant data (internal state bug).

### P2 hardening (2026-07-08)

**Category D — send-path serialization**

- [`writer.rs`](/home/juhe/RustDDS/src/rtps/writer.rs) `send_message_to_readers`: `write_to_vec_fast` failure logs and skips send.
- [`reader.rs`](/home/juhe/RustDDS/src/rtps/reader.rs) `encode_and_send`: same for reader-originated messages.
- [`message.rs`](/home/juhe/RustDDS/src/rtps/message.rs): `try_submessage_content_length`; related sample identity serialize errors log and skip submessage; `data_size`/`sample_size` checked conversions; no `content_length as u16` truncation.
- [`writer.rs`](/home/juhe/RustDDS/src/rtps/writer.rs) fragment iterator: `u32::try_from(data_size)` instead of unwrap.
- [`rtps_reader_proxy.rs`](/home/juhe/RustDDS/src/rtps/rtps_reader_proxy.rs): `mark_all_frags_requested` uses checked `usize::try_from`.
- [`serialized_payload.rs`](/home/juhe/RustDDS/src/messages/submessages/elements/serialized_payload.rs): infallible `From<SerializedPayload> for Bytes` (manual header concat).

**Category F — public API documentation and footguns**

- [`Sample::unwrap`](/home/juhe/RustDDS/src/dds/with_key/datasample.rs): deprecated; use `value()` or match on variants.
- [`pubsub.rs`](/home/juhe/RustDDS/src/dds/pubsub.rs): `unwrap_or_new_entity_id` returns `CreateResult` (no panic when participant dropped).
- `# Panics` rustdoc on placeholder APIs, `Publisher`, `Subscriber`, `DomainParticipant`, `Sample::unwrap`, `AsyncWrite`.

---

## Category A — Internal invariants (panic acceptable)

These indicate a RustDDS logic error. Messages should stay explicit; no need to return `Result` unless recovery is meaningful.

| Item | File / area | Trigger | Notes |
|---|---|---|---|
| Topic name ≠ topic cache name | `rtps/reader.rs` | Mismatched `ReaderIngredients` | Message updated |
| Stateless + Reliable reader/writer | `rtps/reader.rs`, `rtps/writer.rs` | Internal `like_stateless` + Reliable QoS | Public API passes `false`; only internal misuse |
| `with_mutable_writer_proxy` re-insert | `rtps/reader.rs` | Worker closure violates contract | Message updated |
| Poisoned topic cache mutex | `rtps/reader.rs`, `simpledatareader.rs` | Prior panic while holding lock | Message updated |
| Poisoned DiscoveryDB RwLock | `discovery/discovery_db.rs`, `participant.rs` | Prior panic | Message updated |
| Poisoned InnerPublisher mutex | `dds/pubsub.rs` | Prior panic | Message updated |
| Poisoned SecurityPlugins mutex | `security/security_plugins.rs` | Prior panic | Message updated |
| Instance disappeared between select and access | `datasample_cache.rs` | Cache inconsistency | Message updated |
| SPDP built-in entity id assert | `discovery/spdp_participant_data.rs` | Non-built-in id passed to proxy helper | **Debug build only** (`assert!`) |
| `DisposeByKeyHash` in DATA_FRAG flags | `rtps/message.rs` | Internal message build bug | `unreachable!()`; `data_frag_msg` returns early for this variant |

---

## Category B — Mutex / lock `.unwrap()` (poison cascade)

Widespread pattern: `.lock().unwrap()`, `.read().unwrap()`, `.write().unwrap()` on:

- `DomainParticipant` inner (`dpi`)
- `WriterSendBuffer` + condvar waits
- `DDSCache` / topic caches
- Status channels (`statusevents.rs`, `mio_source.rs` with `mio_08`)
- ROS 2 node inner mutex

**Todo:** Prefer `lock()`/`read()`/`write()` match arms that log and abort gracefully, or propagate poison as `Err` on public APIs. Low urgency if Category A panics are rare.

---

## Category C — Startup / lifecycle `.expect()`

Most P1 items addressed — see **P1 hardening** above. Remaining:

| Item | File | Condition |
|---|---|---|
| Security: local participant data missing from DB | `discovery/secure_discovery.rs:2639` | Internal discovery state bug |

---

## Category D — Hot-path serialization / network `.unwrap()`

Fragment bounds (P1) and send-path serialization (P2) **done**. No remaining P2 items.

---

## Category E — Integer overflow

| Item | File | Notes |
|---|---|---|
| `Timestamp + Duration` | `structure/time.rs:147` | Plain `u64` add; wraps near 2106; debug may panic |
| `NumberSet` deserialize | `structure/sequence_number.rs` | Rejects `num_bits > 256` with error (good) |
| Release builds | crate-wide | No `#![deny(arithmetic_overflow)]`; unsigned ops wrap |

**Todo:** Use `checked_add` / `saturating_add` in `Timestamp + Duration` or document wrap as intentional RTPS semantics.

---

## Category F — User / API footguns (document or harden)

Most P2 items addressed — see **P2 hardening** above. Remaining:

| Item | File | Mitigation |
|---|---|---|
| `AsyncWrite` timeout `sample.take().unwrap()` | `datawriter.rs` | Internal invariant; documented on `AsyncWrite` |

---

## Category G — Commented / test-only (no action)

- Test helpers in `network/udp_sender.rs`, `network/udp_listener.rs`
- `rtps/message.rs` test module panics
- `discovery/sedp_messages.rs` test `panic!()`

---

## Suggested priority

1. ~~**P1:** Category D fragment bounds validation (network input)~~ **Done**
2. ~~**P1:** Category C discovery/participant startup `expect` → `Result`~~ **Done** (except secure_discovery internal `.expect`)
3. ~~**P2:** Category D send-path serialization unwrap → log + drop~~ **Done**
4. ~~**P2:** Category F public API panic documentation~~ **Done**
5. **P3:** Category B poison handling strategy (project-wide policy)
6. **P3:** Category E timestamp arithmetic
