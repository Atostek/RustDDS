// Internal polling primitives for the two reactors (dp_event_loop and Discovery).
//
// Sockets are registered with mio 1.2. Everything that is not a socket — command
// queues, writer doorbells, reader commands, discovery sample readiness — shares
// one `Waker` per poll. `wake()` is coalesced, so the token only means "drain
// the queues": the sender enqueues the payload (or entity id) and then wakes.
// Timers are an in-loop deadline map. The poll timeout is the sooner of the next
// deadline and the loop's idle cap. Nothing here is registered as a mio 0.6
// `Evented` source; user-visible sample and status channels stay mio-extras.

use std::{
  collections::BTreeMap,
  sync::{mpsc, Arc, OnceLock},
  time::{Duration, Instant},
};

use mio1::{Poll, Token, Waker};

use crate::structure::guid::EntityId;

/// One waker for one `Poll`. Share clones freely. `wake` before [`install`](Self::install)
/// is a no-op; the event loop drains its queues once after installing so anything
/// enqueued earlier is still observed.
#[derive(Clone)]
pub(crate) struct LoopWaker {
  slot: Arc<OnceLock<Waker>>,
}

impl LoopWaker {
  pub(crate) fn new() -> Self {
    Self {
      slot: Arc::new(OnceLock::new()),
    }
  }

  /// Register the single waker of `poll` under `token`. Debug builds assert this
  /// happens once: mio 1.2 allows only one `Waker` per `Poll`.
  pub(crate) fn install(&self, poll: &Poll, token: Token) -> std::io::Result<()> {
    let waker = Waker::new(poll.registry(), token)?;
    if self.slot.set(waker).is_err() {
      debug_assert!(false, "LoopWaker::install called twice for one poll");
    }
    Ok(())
  }

  pub(crate) fn wake(&self) {
    if let Some(waker) = self.slot.get() {
      if let Err(e) = waker.wake() {
        log::debug!("LoopWaker::wake: {e}");
      }
    }
  }
}

/// Bounded queue whose send wakes `waker`'s poll.
pub(crate) struct WakeSender<T> {
  tx: mpsc::SyncSender<T>,
  waker: LoopWaker,
}

impl<T> Clone for WakeSender<T> {
  fn clone(&self) -> Self {
    Self {
      tx: self.tx.clone(),
      waker: self.waker.clone(),
    }
  }
}

impl<T> WakeSender<T> {
  pub(crate) fn try_send(&self, value: T) -> Result<(), mpsc::TrySendError<T>> {
    match self.tx.try_send(value) {
      Ok(()) => {
        self.waker.wake();
        Ok(())
      }
      Err(e) => Err(e),
    }
  }

  pub(crate) fn send(&self, value: T) -> Result<(), mpsc::SendError<T>> {
    match self.tx.send(value) {
      Ok(()) => {
        self.waker.wake();
        Ok(())
      }
      Err(e) => Err(e),
    }
  }

  /// Retry a full bounded send for a short time. Wakes the poll on success.
  pub(crate) fn try_send_timeout(
    &self,
    value: T,
    timeout: Duration,
  ) -> Result<(), mpsc::TrySendError<T>> {
    let mut value = value;
    match self.try_send(value) {
      Ok(()) => Ok(()),
      Err(mpsc::TrySendError::Disconnected(v)) => Err(mpsc::TrySendError::Disconnected(v)),
      Err(mpsc::TrySendError::Full(v)) => {
        value = v;
        let start = Instant::now();
        let mut delay = Duration::from_micros(1);
        while start.elapsed() < timeout {
          std::thread::sleep(delay);
          match self.try_send(value) {
            Ok(()) => return Ok(()),
            Err(mpsc::TrySendError::Full(v)) => {
              value = v;
              delay = (delay * 2).min(Duration::from_millis(2));
            }
            Err(other) => return Err(other),
          }
        }
        Err(mpsc::TrySendError::Full(value))
      }
    }
  }
}

pub(crate) struct WakeReceiver<T> {
  rx: mpsc::Receiver<T>,
}

impl<T> WakeReceiver<T> {
  pub(crate) fn try_recv(&self) -> Result<T, mpsc::TryRecvError> {
    self.rx.try_recv()
  }
}

pub(crate) fn wake_channel<T>(
  capacity: usize,
  waker: &LoopWaker,
) -> (WakeSender<T>, WakeReceiver<T>) {
  let (tx, rx) = mpsc::sync_channel(capacity);
  (
    WakeSender {
      tx,
      waker: waker.clone(),
    },
    WakeReceiver { rx },
  )
}

/// Unbounded queue whose send wakes `waker`'s poll. Used for stop commands.
#[derive(Clone)]
pub(crate) struct WakeUnboundedSender<T> {
  tx: mpsc::Sender<T>,
  waker: LoopWaker,
}

impl<T> WakeUnboundedSender<T> {
  pub(crate) fn send(&self, value: T) -> Result<(), mpsc::SendError<T>> {
    match self.tx.send(value) {
      Ok(()) => {
        self.waker.wake();
        Ok(())
      }
      Err(e) => Err(e),
    }
  }
}

pub(crate) fn wake_channel_unbounded<T>(
  waker: &LoopWaker,
) -> (WakeUnboundedSender<T>, WakeReceiver<T>) {
  let (tx, rx) = mpsc::channel();
  (
    WakeUnboundedSender {
      tx,
      waker: waker.clone(),
    },
    WakeReceiver { rx },
  )
}

/// Unbounded "something became ready" queue. The payload is the reason (an
/// entity id or a discovery token). Duplicate reasons in one drain are collapsed.
#[derive(Clone)]
pub(crate) struct ReadySender<T> {
  tx: mpsc::Sender<T>,
  waker: LoopWaker,
}

impl<T> ReadySender<T> {
  pub(crate) fn waker(&self) -> LoopWaker {
    self.waker.clone()
  }

  pub(crate) fn notify(&self, value: T) {
    if self.tx.send(value).is_ok() {
      self.waker.wake();
    }
  }
}

pub(crate) struct ReadyReceiver<T> {
  rx: mpsc::Receiver<T>,
}

impl<T: Eq> ReadyReceiver<T> {
  /// Drain every queued reason, keeping the first occurrence of each.
  pub(crate) fn drain_unique(&self) -> Vec<T> {
    let mut out = Vec::new();
    while let Ok(value) = self.rx.try_recv() {
      if !out.contains(&value) {
        out.push(value);
      }
    }
    out
  }
}

pub(crate) fn ready_channel<T>(waker: &LoopWaker) -> (ReadySender<T>, ReadyReceiver<T>) {
  let (tx, rx) = mpsc::channel();
  (
    ReadySender {
      tx,
      waker: waker.clone(),
    },
    ReadyReceiver { rx },
  )
}

/// Bounded command queue plus a shared ready-queue of entity ids. A successful
/// send records `entity_id` so the event loop, which has only one waker, knows
/// which endpoint to drain.
/// Reader commands are not sent today (`ResetRequestedDeadlineStatus` is
/// unimplemented). The sender stays so a future send still wakes the right
/// reader.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct BoundSender<T> {
  tx: WakeSender<T>,
  ready: ReadySender<EntityId>,
  entity_id: EntityId,
}

#[allow(dead_code)]
impl<T> BoundSender<T> {
  pub(crate) fn new(tx: WakeSender<T>, ready: ReadySender<EntityId>, entity_id: EntityId) -> Self {
    Self {
      tx,
      ready,
      entity_id,
    }
  }

  pub(crate) fn try_send(&self, value: T) -> Result<(), mpsc::TrySendError<T>> {
    self.tx.try_send(value)?;
    self.ready.notify(self.entity_id);
    Ok(())
  }
}

/// Writer doorbell. Ringing enqueues the writer's entity id and wakes the
/// participant poll. Replaces the mio 0.6 `Registration` previously registered
/// under the writer token.
#[derive(Clone)]
pub(crate) struct Doorbell {
  entity_id: EntityId,
  ready: ReadySender<EntityId>,
}

impl Doorbell {
  pub(crate) fn new(entity_id: EntityId, ready: ReadySender<EntityId>) -> Self {
    Self { entity_id, ready }
  }

  pub(crate) fn ring(&self) {
    self.ready.notify(self.entity_id);
  }
}

/// In-loop deadlines. Not registered with `Poll`. `set_timeout` / `poll` keep
/// the call pattern readers, writers, and tests already use. Deadlines are
/// absolute `Instant`s, so there is no timer-wheel tick to round them up.
pub(crate) struct Timer<E> {
  next_id: u64,
  deadlines: BTreeMap<(Instant, u64), E>,
}

impl<E> Timer<E> {
  pub(crate) fn new() -> Self {
    Self {
      next_id: 0,
      deadlines: BTreeMap::new(),
    }
  }

  pub(crate) fn set_timeout(&mut self, delay: Duration, event: E) {
    let when = Instant::now()
      .checked_add(delay)
      .unwrap_or_else(Instant::now);
    let id = self.next_id;
    self.next_id = self.next_id.wrapping_add(1);
    self.deadlines.insert((when, id), event);
  }

  /// The earliest event whose deadline has passed, if any.
  pub(crate) fn poll(&mut self) -> Option<E> {
    let key = *self.deadlines.keys().next()?;
    if key.0 <= Instant::now() {
      self.deadlines.remove(&key)
    } else {
      None
    }
  }

  pub(crate) fn next_deadline(&self) -> Option<Instant> {
    self.deadlines.keys().next().map(|(when, _)| *when)
  }
}

pub(crate) type SharedTimer<E> = std::rc::Rc<std::cell::RefCell<Timer<E>>>;

pub(crate) fn new_shared_timer<E>() -> SharedTimer<E> {
  std::rc::Rc::new(std::cell::RefCell::new(Timer::new()))
}

/// Poll timeout: the sooner of `deadline` and `cap`. A deadline already due
/// yields a zero timeout so the loop drains it immediately.
pub(crate) fn timeout_until(deadline: Option<Instant>, cap: Duration) -> Duration {
  match deadline {
    Some(when) => {
      let now = Instant::now();
      if when <= now {
        Duration::ZERO
      } else {
        (when - now).min(cap)
      }
    }
    None => cap,
  }
}
