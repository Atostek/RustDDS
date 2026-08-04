use std::{
  collections::BTreeMap,
  sync::{Arc, Condvar, Mutex},
  time::{Duration as StdDuration, Instant},
};
use core::task::Waker;

#[allow(unused_imports)]
use log::{debug, error, trace, warn};

use crate::{
  dds::{ddsdata::DDSData, with_key::datawriter::WriteOptions},
  structure::{cache_change::CacheChange, guid::GUID, sequence_number::SequenceNumber},
};

/// Result of an admission attempt into the [`WriterSendBuffer`].
pub(crate) enum Admission {
  /// The sample was admitted and given this sequence number. It now lives in
  /// the send buffer and is ready for the Writer (event loop) to transmit.
  Admitted(SequenceNumber),
  /// The reliable send window is full and no room became available within the
  /// allotted blocking time. The sample was *not* stored.
  WouldBlock,
}

// The actual shared state. Guarded by a single Mutex; the Condvar is signalled
// whenever the reliable acknowledgement frontier advances or the window grows,
// i.e. whenever a previously-blocked producer (or a `wait_for_acknowledgments`
// waiter) might be able to make progress.
struct Inner {
  // --- the send buffer proper (replaces the old HistoryBuffer) ---
  // Keyed by SequenceNumber, which is allocated here under the lock and is thus
  // strictly monotonic and unique. This is the single source of truth for the
  // Writer's outgoing samples (send, repair, fragmentation, eviction).
  changes: BTreeMap<SequenceNumber, CacheChange>,
  first_seq: SequenceNumber, // oldest still retained (default 1)
  last_seq: SequenceNumber,  // latest allocated (default 0 == nothing yet)

  // --- flow control ---
  // Maximum number of unacknowledged samples a reliable writer may have
  // outstanding before `write` must block/fail.
  window_limit: usize,
  // The acknowledgement frontier: the smallest `all_acked_before` over all
  // matched *reliable* readers. Only meaningful when `reliable_readers_present`.
  // Maintained by the Writer (event loop) via `set_acked_frontier`.
  acked_before: SequenceNumber,
  reliable_readers_present: bool,

  // nonblocking-transmit: the unsent backlog limit. The Writer advances
  // `sent_frontier` as it actually transmits samples; when the network socket
  // congests, `sent_frontier` stalls, the backlog `(sent_frontier, last_seq]`
  // fills, and admission blocks -- back-pressure reaching the application.
  // Applies to reliable *and* best-effort writers; built-in/discovery are
  // exempt so discovery never stalls.
  // (see src/rtps/nonblocking_transmit_design.md)
  backlog_limit: usize,
  // Highest sequence number the Writer has actually put on the wire.
  sent_frontier: SequenceNumber,

  // Hard upper bound on the number of retained samples for *non-blocking*
  // (best-effort, DDS-default) writes. Because such writes are never throttled
  // at admission (see `has_room`), and eviction otherwise only happens on the
  // periodic (6 s) cache-cleaning timer, the buffer would grow without bound
  // when the application produces faster than the socket drains. We therefore
  // apply KeepLast-style "newest wins" eviction directly on insert: once the
  // buffer exceeds `max_retain`, the oldest samples are dropped. Derived from
  // the writer's History depth / ResourceLimits (same value as `window_limit`).
  // Does NOT apply to reliable / opted-in writers, which are bounded by the
  // send window / unsent-backlog and must retain samples for repair.
  max_retain: usize,

  // Wakers of async producers / ack-waiters parked because the window was full
  // or acknowledgements were still pending. Drained (woken) on any advance.
  wakers: Vec<Waker>,
}

struct Shared {
  inner: Mutex<Inner>,
  // Signalled together with `wakers` whenever progress may be possible.
  progress: Condvar,
  // Fixed for the lifetime of the writer.
  writer_guid: GUID,
  reliable_writer: bool,
  is_builtin: bool,
  // True for VOLATILE durability (the DDS default). When true, a reliable writer
  // with no matched reliable reader trims KeepLast on insert (there is no
  // late-joiner to serve, so retaining unacked samples is pointless). When false
  // (TRANSIENT_LOCAL / TRANSIENT / PERSISTENT) the writer must retain samples for
  // late-joining readers, so pre-match trimming is disabled.
  volatile: bool,
  topic_name: String,
}

/// A shared, flow-controlled buffer of samples between a `DataWriter`
/// (producer, application threads) and its RTPS `Writer` (consumer, event
/// loop).
///
/// The producer side allocates a sequence number and inserts a sample only when
/// the reliable send window has room (synchronous admission, no round-trip).
/// The consumer side transmits unsent samples, advances the acknowledgement
/// frontier as ACKNACKs arrive, and evicts acknowledged samples. Both sides
/// hold a cloned handle to the same `Arc`.
#[derive(Clone)]
pub(crate) struct WriterSendBuffer {
  shared: Arc<Shared>,
}

impl WriterSendBuffer {
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    writer_guid: GUID,
    topic_name: String,
    reliable_writer: bool,
    is_builtin: bool,
    volatile: bool,
    window_limit: usize,
    backlog_limit: usize,
    max_retain: usize,
  ) -> Self {
    Self {
      shared: Arc::new(Shared {
        inner: Mutex::new(Inner {
          changes: BTreeMap::new(),
          first_seq: SequenceNumber::new(1),
          last_seq: SequenceNumber::new(0),
          window_limit: window_limit.max(1),
          acked_before: SequenceNumber::new(1),
          reliable_readers_present: false,
          backlog_limit: backlog_limit.max(1),
          sent_frontier: SequenceNumber::new(0),
          max_retain: max_retain.max(1),
          wakers: Vec::new(),
        }),
        progress: Condvar::new(),
        writer_guid,
        reliable_writer,
        is_builtin,
        volatile,
        topic_name,
      }),
    }
  }

  // --- predicates (must be called while holding the lock) ---

  // Is there room to admit one more sample right now?
  //
  // `may_block` is `true` for reliable writers and for best-effort writes that
  // opted in via `WriteOptions::best_effort_may_block`. When `false` (a
  // best-effort write that must not block, the DDS default) admission is never
  // throttled: congested samples are dropped later at the socket instead.
  fn has_room(shared: &Shared, inner: &Inner, may_block: bool) -> bool {
    if shared.is_builtin {
      // Built-in (discovery) writers must never stall.
      return true;
    }

    if !may_block {
      // Best-effort write that must not block (DDS v1.4 2.2.2.4.2.11 default).
      // Never throttle admission.
      return true;
    }

    // nonblocking-transmit: unsent-backlog limit. Applies to reliable writers
    // and to best-effort writers that opted in -- when the socket cannot drain,
    // the application is back-pressured instead of dropping. Samples in
    // (sent_frontier, last_seq].
    let unsent = i64::from(inner.last_seq) - i64::from(inner.sent_frontier);
    if unsent >= inner.backlog_limit as i64 {
      return false;
    }

    if !shared.reliable_writer || !inner.reliable_readers_present {
      // Best-effort and "no reliable reader yet" writers have no acknowledgement
      // window to wait on; the backlog limit above is their only throttle.
      return true;
    }
    // Number of unacknowledged samples in [acked_before, last_seq].
    let unacked = i64::from(inner.last_seq) - i64::from(inner.acked_before) + 1;
    unacked < inner.window_limit as i64
  }

  // Wake every parked producer / ack-waiter. Called after any state change that
  // could let someone make progress.
  fn wake_all(inner: &mut Inner, progress: &Condvar) {
    for w in inner.wakers.drain(..) {
      w.wake();
    }
    progress.notify_all();
  }

  // --- producer side ---

  /// Synchronous admission. Blocks the calling thread until there is room in
  /// the reliable send window, or `timeout` elapses. On success the sample is
  /// stored and its sequence number returned. Built-in / best-effort writers
  /// always admit immediately.
  pub fn admit_blocking(
    &self,
    write_options: WriteOptions,
    data: DDSData,
    timeout: Option<StdDuration>,
  ) -> Admission {
    let shared = &*self.shared;
    let mut inner = shared.inner.lock().unwrap();

    // Reliable writers always back-pressure; best-effort only if this write
    // opted in via `best_effort_may_block`.
    let may_block = shared.reliable_writer || write_options.best_effort_may_block();

    let deadline = timeout.map(|t| Instant::now() + t);
    loop {
      if Self::has_room(shared, &inner, may_block) {
        let seq = Self::insert_locked(shared, &mut inner, write_options, data, may_block);
        return Admission::Admitted(seq);
      }
      // Window full: wait for an acknowledgement to free up space.
      match deadline {
        None => {
          inner = shared.progress.wait(inner).unwrap();
        }
        Some(deadline) => {
          let now = Instant::now();
          if now >= deadline {
            return Admission::WouldBlock;
          }
          let (guard, _timeout_result) =
            shared.progress.wait_timeout(inner, deadline - now).unwrap();
          inner = guard;
        }
      }
    }
  }

  /// Non-blocking admission for async writers. On success returns the allocated
  /// sequence number. On a full window returns the (write_options, data) back
  /// so the caller can retry later, and registers `waker` to be woken when
  /// room becomes available.
  pub fn try_admit(
    &self,
    write_options: WriteOptions,
    data: DDSData,
    waker: &Waker,
  ) -> Result<SequenceNumber, (WriteOptions, DDSData)> {
    let shared = &*self.shared;
    let mut inner = shared.inner.lock().unwrap();
    let may_block = shared.reliable_writer || write_options.best_effort_may_block();
    if Self::has_room(shared, &inner, may_block) {
      Ok(Self::insert_locked(
        shared,
        &mut inner,
        write_options,
        data,
        may_block,
      ))
    } else {
      register_waker(&mut inner.wakers, waker);
      Err((write_options, data))
    }
  }

  fn insert_locked(
    shared: &Shared,
    inner: &mut Inner,
    write_options: WriteOptions,
    data: DDSData,
    may_block: bool,
  ) -> SequenceNumber {
    let seq = inner.last_seq.plus_1();
    let cc = CacheChange::new(shared.writer_guid, seq, write_options, data);
    inner.changes.insert(seq, cc);
    inner.last_seq = seq;

    // KeepLast "newest wins" bound. Applied on insert for:
    //  - non-blocking best-effort writes: never throttled at admission (`has_room`
    //    returns true), and the only other eviction path is the periodic
    //    cache-cleaning timer, which starves under a sustained flood -- so without
    //    this the buffer grows without bound (confirmed multi-GB leak);
    //  - reliable writers with NO matched reliable reader yet: there is nobody to
    //    repair to, so retaining unacknowledged samples is pointless. A reliable
    //    writer that produces flat-out before discovery completes (discovery is
    //    CPU-starved under load and can lag ~1 s) would otherwise race thousands of
    //    samples ahead, and when the reader finally matches it faces a huge
    //    unacknowledged backlog that must be recovered via repair at the slow (100
    //    ms) heartbeat cadence -- collapsing reliable throughput. Trimming to
    //    KeepLast here keeps only the recent samples (correct for volatile
    //    durability); once a reliable reader matches, `reliable_readers_present`
    //    flips true and we retain everything again for repair.
    // Built-in (discovery) writers are always exempt to never lose discovery data.
    let trim_keep_last = !shared.is_builtin
      && (!may_block
        || (shared.reliable_writer && shared.volatile && !inner.reliable_readers_present));
    if trim_keep_last {
      while inner.changes.len() > inner.max_retain {
        // BTreeMap keeps keys sorted, so the first entry is the oldest SN.
        let oldest = match inner.changes.keys().next().copied() {
          Some(sn) => sn,
          None => break,
        };
        inner.changes.remove(&oldest);
        inner.first_seq = oldest.plus_1();
      }
    }
    seq
  }

  // --- consumer side (Writer / event loop) ---

  /// Update the reliable acknowledgement frontier. `acked_before` is the
  /// minimum `all_acked_before` over all matched reliable readers, and
  /// `present` tells whether any reliable reader is currently matched. Wakes
  /// blocked producers / ack-waiters when the frontier advances or a reader
  /// goes away.
  pub fn set_acked_frontier(&self, acked_before: Option<SequenceNumber>) {
    let shared = &*self.shared;
    let mut inner = shared.inner.lock().unwrap();
    let advanced = match acked_before {
      Some(sn) => {
        let adv = sn > inner.acked_before || !inner.reliable_readers_present;
        inner.acked_before = sn;
        inner.reliable_readers_present = true;
        adv
      }
      None => {
        // No reliable readers (any more). Everything counts as acknowledged.
        let adv = inner.reliable_readers_present;
        inner.reliable_readers_present = false;
        adv
      }
    };
    if advanced {
      Self::wake_all(&mut inner, &shared.progress);
    }
  }

  /// nonblocking-transmit: advance the "actually transmitted" frontier. Called
  /// by the Writer as it puts samples on the wire. Wakes producers parked on a
  /// full unsent backlog when the frontier advances.
  pub fn set_sent_frontier(&self, sent_frontier: SequenceNumber) {
    let shared = &*self.shared;
    let mut inner = shared.inner.lock().unwrap();
    if sent_frontier > inner.sent_frontier {
      inner.sent_frontier = sent_frontier;
      Self::wake_all(&mut inner, &shared.progress);
    }
  }

  /// The sequence number of the latest allocated sample (0 if none yet).
  pub fn last_change_sequence_number(&self) -> SequenceNumber {
    self.shared.inner.lock().unwrap().last_seq
  }

  /// Number of samples currently retained in the buffer. Test-only.
  #[cfg(test)]
  pub fn retained_len(&self) -> usize {
    self.shared.inner.lock().unwrap().changes.len()
  }

  /// The lowest sequence number still retained (or the next-to-be-written if
  /// the buffer is empty).
  pub fn first_change_sequence_number(&self) -> SequenceNumber {
    self.shared.inner.lock().unwrap().first_seq
  }

  /// Fetch a clone of the sample with the given sequence number, if retained.
  /// Returns an owned `CacheChange` (a cheap `Bytes`-backed clone) so the
  /// caller can serialize and transmit without holding the lock.
  pub fn get_by_sn(&self, sn: SequenceNumber) -> Option<CacheChange> {
    self.shared.inner.lock().unwrap().changes.get(&sn).cloned()
  }

  /// Evict all samples with sequence number strictly less than `remove_before`.
  pub fn remove_changes_before(&self, remove_before: SequenceNumber) {
    let shared = &*self.shared;
    let mut inner = shared.inner.lock().unwrap();
    let count_before = inner.changes.len();
    inner.changes = inner.changes.split_off(&remove_before);
    if remove_before > inner.first_seq {
      inner.first_seq = remove_before;
    }
    let count_after = inner.changes.len();
    if count_before != count_after {
      debug!(
        "WriterSendBuffer: removed {} change(s) before {:?} topic={}",
        count_before - count_after,
        remove_before,
        shared.topic_name
      );
    }
  }

  // --- wait_for_acknowledgments support ---

  /// Has every matched reliable reader acknowledged everything up to and
  /// including `target`? Also true when there are no reliable readers.
  pub fn is_acked_through(&self, target: SequenceNumber) -> bool {
    let inner = self.shared.inner.lock().unwrap();
    !inner.reliable_readers_present || inner.acked_before > target
  }

  /// Synchronously wait until everything up to `target` is acknowledged, or
  /// `max_wait` elapses. Returns `true` if acknowledged.
  pub fn wait_for_acked_through(&self, target: SequenceNumber, max_wait: StdDuration) -> bool {
    let shared = &*self.shared;
    let mut inner = shared.inner.lock().unwrap();
    let deadline = Instant::now() + max_wait;
    loop {
      if !inner.reliable_readers_present || inner.acked_before > target {
        return true;
      }
      let now = Instant::now();
      if now >= deadline {
        return false;
      }
      let (guard, _to) = shared.progress.wait_timeout(inner, deadline - now).unwrap();
      inner = guard;
    }
  }

  /// Register `waker` to be notified when the acknowledgement frontier advances
  /// (used by the async `wait_for_acknowledgments` future).
  pub fn register_ack_waker(&self, waker: &Waker) {
    let mut inner = self.shared.inner.lock().unwrap();
    register_waker(&mut inner.wakers, waker);
  }
}

// Avoid storing duplicate wakers for the same task.
fn register_waker(wakers: &mut Vec<Waker>, waker: &Waker) {
  if !wakers.iter().any(|w| w.will_wake(waker)) {
    wakers.push(waker.clone());
  }
}

#[cfg(test)]
mod tests {
  use std::time::Duration as StdDuration;

  use super::*;
  use crate::{
    dds::{ddsdata::DDSData, with_key::datawriter::WriteOptionsBuilder},
    messages::submessages::elements::serialized_payload::SerializedPayload,
    structure::guid::GUID,
    RepresentationIdentifier,
  };

  fn sample() -> DDSData {
    DDSData::new(SerializedPayload::new(
      RepresentationIdentifier::CDR_LE,
      vec![0u8; 8],
    ))
  }

  fn admit_now(buf: &WriterSendBuffer, opts: WriteOptions) -> bool {
    matches!(
      buf.admit_blocking(opts, sample(), Some(StdDuration::ZERO)),
      Admission::Admitted(_)
    )
  }

  fn may_block_opts() -> WriteOptions {
    WriteOptionsBuilder::new()
      .best_effort_may_block(true)
      .build()
  }

  // nonblocking-transmit: a best-effort writer that opted in via
  // `best_effort_may_block` is throttled by the unsent-backlog limit once the
  // socket stops draining, and released when the Writer advances the sent
  // frontier.
  #[test]
  fn backlog_limit_backpressures_best_effort_when_may_block() {
    let buf = WriterSendBuffer::new(
      GUID::GUID_UNKNOWN,
      "t".to_string(),
      /* reliable_writer */ false,
      /* is_builtin */ false,
      /* volatile */ true,
      /* window_limit */ 1000,
      /* backlog_limit */ 2,
      /* max_retain */ 1000,
    );

    // Nothing sent yet: backlog fills after two admissions.
    assert!(admit_now(&buf, may_block_opts())); // seq 1
    assert!(admit_now(&buf, may_block_opts())); // seq 2
    assert!(!admit_now(&buf, may_block_opts())); // backlog full (2 unsent, limit 2)

    // The Writer transmits seq 1; backlog drops to 1, room opens for one more.
    buf.set_sent_frontier(SequenceNumber::new(1));
    assert!(admit_now(&buf, may_block_opts())); // seq 3
    assert!(!admit_now(&buf, may_block_opts())); // full again (2 unsent: seq
                                                 // 2,3)
  }

  // By default (`best_effort_may_block == false`, DDS v1.4 2.2.2.4.2.11) a
  // best-effort write is never throttled at admission, even when the backlog is
  // already full -- congested samples are dropped later at the socket instead.
  #[test]
  fn backlog_limit_ignored_for_best_effort_by_default() {
    let buf = WriterSendBuffer::new(
      GUID::GUID_UNKNOWN,
      "t".to_string(),
      /* reliable_writer */ false,
      /* is_builtin */ false,
      /* volatile */ true,
      /* window_limit */ 1000,
      /* backlog_limit */ 1,
      /* max_retain */ 1000,
    );
    // Default WriteOptions => best_effort_may_block == false => never blocks.
    assert!(admit_now(&buf, WriteOptions::default()));
    assert!(admit_now(&buf, WriteOptions::default()));
    assert!(admit_now(&buf, WriteOptions::default())); // still admitted despite
                                                       // full backlog
  }

  // Built-in (discovery) writers must never be throttled by the backlog.
  #[test]
  fn backlog_limit_exempts_builtin() {
    let buf = WriterSendBuffer::new(
      GUID::GUID_UNKNOWN,
      "t".to_string(),
      false,
      /* is_builtin */ true,
      /* volatile */ true,
      1000,
      /* backlog_limit */ 1,
      /* max_retain */ 1000,
    );
    assert!(admit_now(&buf, may_block_opts()));
    assert!(admit_now(&buf, may_block_opts()));
    assert!(admit_now(&buf, may_block_opts())); // still admitted despite tiny
                                                // backlog limit
  }

  // A best-effort (non-blocking, DDS-default) writer is never throttled at
  // admission, but the retained buffer must stay bounded by `max_retain`
  // (KeepLast "newest wins"): the leak fix. Every write is admitted, yet the
  // buffer never holds more than `max_retain` samples, and it retains the
  // newest ones (oldest are evicted on insert).
  #[test]
  fn best_effort_default_bounded_by_max_retain() {
    let max_retain = 4;
    let buf = WriterSendBuffer::new(
      GUID::GUID_UNKNOWN,
      "t".to_string(),
      /* reliable_writer */ false,
      /* is_builtin */ false,
      /* volatile */ true,
      /* window_limit */ 1000,
      /* backlog_limit */ 1000,
      max_retain,
    );

    for _ in 0..100 {
      // Default WriteOptions => best-effort, non-blocking => always admitted.
      assert!(admit_now(&buf, WriteOptions::default()));
      // Invariant after every insert: never exceeds the retain bound.
      assert!(buf.retained_len() <= max_retain);
    }

    // Exactly `max_retain` retained, and they are the newest sequence numbers.
    assert_eq!(buf.retained_len(), max_retain);
    let last = buf.last_change_sequence_number();
    assert_eq!(last, SequenceNumber::new(100));
    let first = buf.first_change_sequence_number();
    assert_eq!(first, SequenceNumber::new(100 - max_retain as i64 + 1));
    // Newest is retained, an evicted older one is gone.
    assert!(buf.get_by_sn(last).is_some());
    assert!(buf.get_by_sn(SequenceNumber::new(1)).is_none());
  }

  // A durable (non-VOLATILE) reliable writer must NOT drop on insert even before
  // a reader matches: it retains samples for late-joining readers. The
  // `max_retain` bound does not apply to it.
  #[test]
  fn durable_reliable_writer_not_trimmed_before_match() {
    let max_retain = 2;
    let buf = WriterSendBuffer::new(
      GUID::GUID_UNKNOWN,
      "t".to_string(),
      /* reliable_writer */ true,
      /* is_builtin */ false,
      /* volatile */ false, // TRANSIENT_LOCAL etc: keep for late joiners
      /* window_limit */ 1000,
      /* backlog_limit */ 1000,
      max_retain,
    );
    // No reliable readers matched yet => admission not window-throttled.
    for _ in 0..10 {
      assert!(admit_now(&buf, WriteOptions::default()));
    }
    // Durable path keeps everything for potential late-joiner delivery / repair
    // despite tiny max_retain.
    assert_eq!(buf.retained_len(), 10);
  }

  // A VOLATILE reliable writer with no matched reliable reader trims KeepLast on
  // insert: there is no late joiner to serve, so retaining unacknowledged samples
  // is pointless and would create a huge post-match repair backlog (the reliable
  // flat-out throughput fix). Once a reliable reader matches
  // (`set_acked_frontier(Some(..))`), trimming stops and everything is retained
  // for repair.
  #[test]
  fn volatile_reliable_writer_trimmed_until_reader_matches() {
    let max_retain = 4;
    let buf = WriterSendBuffer::new(
      GUID::GUID_UNKNOWN,
      "t".to_string(),
      /* reliable_writer */ true,
      /* is_builtin */ false,
      /* volatile */ true,
      /* window_limit */ 1000,
      /* backlog_limit */ 1000,
      max_retain,
    );
    // No reliable reader yet: admitted freely (backlog has room) but trimmed to
    // KeepLast so the buffer cannot balloon before discovery completes.
    for _ in 0..100 {
      assert!(admit_now(&buf, WriteOptions::default()));
      assert!(buf.retained_len() <= max_retain);
    }
    assert_eq!(buf.retained_len(), max_retain);

    // A reliable reader matches: from now on the writer retains everything for
    // repair (no trimming), even past max_retain.
    buf.set_acked_frontier(Some(SequenceNumber::new(101)));
    for _ in 0..10 {
      assert!(admit_now(&buf, WriteOptions::default()));
    }
    assert_eq!(buf.retained_len(), max_retain + 10);
  }
}
