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
  pub fn new(
    writer_guid: GUID,
    topic_name: String,
    reliable_writer: bool,
    is_builtin: bool,
    window_limit: usize,
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
          wakers: Vec::new(),
        }),
        progress: Condvar::new(),
        writer_guid,
        reliable_writer,
        is_builtin,
        topic_name,
      }),
    }
  }

  // --- predicates (must be called while holding the lock) ---

  // Is there room to admit one more sample right now?
  fn has_room(shared: &Shared, inner: &Inner) -> bool {
    if !shared.reliable_writer || shared.is_builtin || !inner.reliable_readers_present {
      // Best-effort, built-in (discovery) and "no reliable reader yet" writers
      // are never throttled: discovery and best-effort must not stall, and there
      // is no one whose acknowledgement we would be waiting for.
      return true;
    }
    // Number of samples in [acked_before, last_seq].
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

    let deadline = timeout.map(|t| Instant::now() + t);
    loop {
      if Self::has_room(shared, &inner) {
        let seq = Self::insert_locked(shared, &mut inner, write_options, data);
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
    if Self::has_room(shared, &inner) {
      Ok(Self::insert_locked(shared, &mut inner, write_options, data))
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
  ) -> SequenceNumber {
    let seq = inner.last_seq.plus_1();
    let cc = CacheChange::new(shared.writer_guid, seq, write_options, data);
    inner.changes.insert(seq, cc);
    inner.last_seq = seq;
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

  /// The sequence number of the latest allocated sample (0 if none yet).
  pub fn last_change_sequence_number(&self) -> SequenceNumber {
    self.shared.inner.lock().unwrap().last_seq
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
