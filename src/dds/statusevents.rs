// Describe the communication status changes as events.
//
// These implement a mechanism equivalent to what is described in
// Section 2.2.4 Listeners, Conditions, and Wait-sets
//
// Communication statues are detailed in Figure 2.13 and tables in Section
// 2.2.4.1 in DDS Specification v1.4
use std::{
  io,
  pin::Pin,
  sync::{Arc, Mutex},
  task::{Context, Poll, Waker},
};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use futures::stream::{FusedStream, Stream};
#[cfg(feature = "mio_06")]
use mio_06::Evented;
#[cfg(feature = "mio_06")]
use mio_extras::channel as mio_channel;
#[cfg(feature = "mio_08")]
use mio_08::{event, Interest, Registry, Token};
use chrono::Utc;

use crate::{
  dds::{qos::QosPolicyId, topic::TopicData},
  discovery::SpdpDiscoveredParticipantData,
  messages::{protocol_version::ProtocolVersion, vendor_id::VendorId},
  mio_source::*,
  structure::guid::GuidPrefix,
  Duration, QosPolicies, GUID,
};
#[cfg(feature = "security")]
use crate::discovery::secure_discovery::AuthenticationStatus;

/// This trait corresponds to set_listener() of the Entity class in DDS spec.
/// Types implementing this trait can be registered to a poll and
/// polled for status events.
pub trait StatusEvented<'a, E, S>
where
  S: Stream<Item = E>,
  S: FusedStream,
{
  // The lifetime variable 'a marks the lifetime of the async stream object, if
  // such is requested. The stream object typically contains a reference to
  // self, so it is to ensure correct lifetimes.
  #[cfg(feature = "mio_06")]
  fn as_status_evented(&mut self) -> &dyn Evented; // This is for polling with mio-0.6.x
  #[cfg(feature = "mio_08")]
  fn as_status_source(&mut self) -> &mut dyn mio_08::event::Source; // This is for polling with mio-0.8.x
  fn as_status_source_mio1(&mut self) -> &mut dyn mio1::event::Source; // This is for polling with mio-1.2
  fn as_async_status_stream(&'a self) -> S;
  fn try_recv_status(&self) -> Option<E>;
}

// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------

// A bounded standard queue augmented with a poll channel. Mio 1.2 is always
// supported; a separate mio-extras unit channel keeps the legacy mio 0.6
// Evented implementation available without making it part of the core queue.

pub(crate) fn sync_status_channel<T>(
  capacity: usize,
) -> io::Result<(StatusChannelSender<T>, StatusChannelReceiver<T>)> {
  let (signal_receiver, signal_sender) = make_poll_channel()?;
  let (actual_sender, actual_receiver) = std::sync::mpsc::sync_channel(capacity);
  #[cfg(feature = "mio_06")]
  let (legacy_sender, legacy_receiver) = mio_channel::sync_channel(capacity);
  let waker = Arc::new(Mutex::new(None));
  Ok((
    StatusChannelSender {
      actual_sender,
      signal_sender,
      #[cfg(feature = "mio_06")]
      legacy_sender,
      waker: Arc::clone(&waker),
    },
    StatusChannelReceiver {
      actual_receiver: Mutex::new(actual_receiver),
      signal_receiver,
      #[cfg(feature = "mio_06")]
      legacy_receiver: Mutex::new(legacy_receiver),
      waker,
    },
  ))
}

// TODO: try to make this (and the Receiver) private types
#[derive(Clone)]
pub struct StatusChannelSender<T> {
  actual_sender: std::sync::mpsc::SyncSender<T>,
  signal_sender: PollEventSender,
  #[cfg(feature = "mio_06")]
  legacy_sender: mio_channel::SyncSender<()>,
  waker: Arc<Mutex<Option<Waker>>>,
}

pub struct StatusChannelReceiver<T> {
  actual_receiver: Mutex<std::sync::mpsc::Receiver<T>>,
  signal_receiver: PollEventSource,
  #[cfg(feature = "mio_06")]
  legacy_receiver: Mutex<mio_channel::Receiver<()>>,
  waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> StatusChannelSender<T> {
  #[cfg(feature = "mio_06")]
  /// Best-effort send. If there is no receiver, this will fail silently.
  pub fn try_send(&self, t: T) -> Result<(), mio_channel::TrySendError<T>> {
    self.try_send_impl(t).map_err(|error| match error {
      std::sync::mpsc::TrySendError::Full(value) => mio_channel::TrySendError::Full(value),
      std::sync::mpsc::TrySendError::Disconnected(value) => {
        mio_channel::TrySendError::Disconnected(value)
      }
    })
  }

  #[cfg(not(feature = "mio_06"))]
  /// Best-effort send. If there is no receiver, this will fail silently.
  pub fn try_send(&self, t: T) -> Result<(), std::sync::mpsc::TrySendError<T>> {
    self.try_send_impl(t)
  }

  pub(crate) fn try_send_impl(&self, t: T) -> Result<(), std::sync::mpsc::TrySendError<T>> {
    let mut w = self.waker.lock().unwrap(); // lock already at the beginning
    match self.actual_sender.try_send(t) {
      Ok(()) => {
        self.signal_sender.send();
        #[cfg(feature = "mio_06")]
        let _ = self.legacy_sender.try_send(());
        w.as_ref().map(|w| w.wake_by_ref());
        *w = None;
        Ok(())
      }
      Err(std::sync::mpsc::TrySendError::Full(_tt)) => {
        // Keep this at `trace!`: it is perfectly normal to fail due to a full
        // channel, because no-one is required to be listening to these. In
        // particular the built-in Discovery endpoints never drain their status
        // channels, so this fires once per matched remote endpoint. Raising it
        // to `warn!` (as commit 724950ca briefly did) spams production logs
        // unstoppably.
        trace!("StatusChannelSender cannot send new status changes, channel is full.");
        // It is perfectly normal to fail due to full channel, because
        // no-one is required to be listening to these.
        self.signal_sender.send(); // kick the receiver anyway
        #[cfg(feature = "mio_06")]
        let _ = self.legacy_sender.try_send(());
        w.as_ref().map(|w| w.wake_by_ref());
        *w = None;
        // We convert the Err to Ok, bause we do not consider this to be an error.
        // The caller loses the payload object (tt), even though it is not sent.
        Ok(())
      }
      Err(other_fail) => Err(other_fail),
    }
  }
}

impl<T> StatusChannelReceiver<T> {
  pub fn try_recv(&self) -> Result<T, std::sync::mpsc::TryRecvError> {
    // We do not manipulate waker here, because the
    // synchronous and asynchronous receiving are not supposed to be mixed.
    self.signal_receiver.drain();
    #[cfg(feature = "mio_06")]
    while self.legacy_receiver.lock().unwrap().try_recv().is_ok() {}
    self.actual_receiver.lock().unwrap().try_recv()
  }

  pub(crate) fn get_waker_update_lock(&self) -> std::sync::MutexGuard<'_, Option<Waker>> {
    self.waker.lock().unwrap()
  }
}

impl<'a, E> StatusEvented<'a, E, StatusReceiverStream<'a, E>> for StatusChannelReceiver<E> {
  #[cfg(feature = "mio_06")]
  fn as_status_evented(&mut self) -> &dyn Evented {
    self
  }

  #[cfg(feature = "mio_08")]
  fn as_status_source(&mut self) -> &mut dyn mio_08::event::Source {
    self
  }

  fn as_status_source_mio1(&mut self) -> &mut dyn mio1::event::Source {
    self
  }

  fn as_async_status_stream(&'a self) -> StatusReceiverStream<'a, E> {
    StatusReceiverStream {
      sync_receiver: self,
      terminated: AtomicBool::new(false),
    }
  }

  fn try_recv_status(&self) -> Option<E> {
    self.try_recv().ok()
  }
}

#[cfg(feature = "mio_06")]
impl<E> Evented for StatusChannelReceiver<E> {
  // We just delegate all the operations to notification_receiver, since it
  // already implements Evented
  fn register(
    &self,
    poll: &mio_06::Poll,
    token: mio_06::Token,
    interest: mio_06::Ready,
    opts: mio_06::PollOpt,
  ) -> io::Result<()> {
    self
      .legacy_receiver
      .lock()
      .unwrap()
      .register(poll, token, interest, opts)
  }

  fn reregister(
    &self,
    poll: &mio_06::Poll,
    token: mio_06::Token,
    interest: mio_06::Ready,
    opts: mio_06::PollOpt,
  ) -> io::Result<()> {
    self
      .legacy_receiver
      .lock()
      .unwrap()
      .reregister(poll, token, interest, opts)
  }

  fn deregister(&self, poll: &mio_06::Poll) -> io::Result<()> {
    self.legacy_receiver.lock().unwrap().deregister(poll)
  }
}

#[cfg(feature = "mio_08")]
impl<T> event::Source for StatusChannelReceiver<T> {
  fn register(&mut self, registry: &Registry, token: Token, interests: Interest) -> io::Result<()> {
    self.signal_receiver.register(registry, token, interests)
  }

  fn reregister(
    &mut self,
    registry: &Registry,
    token: Token,
    interests: Interest,
  ) -> io::Result<()> {
    self.signal_receiver.reregister(registry, token, interests)
  }

  fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
    self.signal_receiver.deregister(registry)
  }
}

impl<T> mio1::event::Source for StatusChannelReceiver<T> {
  fn register(
    &mut self,
    registry: &mio1::Registry,
    token: mio1::Token,
    interests: mio1::Interest,
  ) -> io::Result<()> {
    self.signal_receiver.register(registry, token, interests)
  }

  fn reregister(
    &mut self,
    registry: &mio1::Registry,
    token: mio1::Token,
    interests: mio1::Interest,
  ) -> io::Result<()> {
    self.signal_receiver.reregister(registry, token, interests)
  }

  fn deregister(&mut self, registry: &mio1::Registry) -> io::Result<()> {
    self.signal_receiver.deregister(registry)
  }
}

// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

// TODO: try to make private
pub struct StatusReceiverStream<'a, T> {
  sync_receiver: &'a StatusChannelReceiver<T>,
  terminated: AtomicBool,
}

impl<T> Stream for StatusReceiverStream<'_, T> {
  type Item = T;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    // debug!("poll_next");
    let mut w = self.sync_receiver.get_waker_update_lock();
    // lock already at the beginning, before try_recv
    match self.sync_receiver.try_recv() {
      Err(std::sync::mpsc::TryRecvError::Empty) => {
        // nothing available
        *w = Some(cx.waker().clone());
        Poll::Pending
      }
      Err(std::sync::mpsc::TryRecvError::Disconnected) => {
        self.terminated.store(true, Ordering::SeqCst);
        warn!("StatusReceiver channel disconnected");
        Poll::Ready(None)
      }
      Ok(t) => Poll::Ready(Some(t)), // got data
    }
  } // fn
}

impl<T> FusedStream for StatusReceiverStream<'_, T> {
  fn is_terminated(&self) -> bool {
    self.terminated.load(Ordering::SeqCst)
  }
}

// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DomainParticipantStatusEvent {
  ParticipantDiscovered {
    dpd: ParticipantDescription,
  },
  ParticipantLost {
    id: GuidPrefix,
    reason: LostReason,
  },
  InconsistentTopic {
    previous_topic_data: Box<TopicData>, // What was our ide aof the Topic
    previous_source: GUID,
    discovered_topic_data: Box<TopicData>, // What incoming Discovery data tells us about Topic
    discovery_source: GUID,                // Who sent the Discovery data
  },
  /// Discovery detects a new topic
  TopicDetected {
    name: String,
    type_name: String,
  },
  /// Topics are lost when there are no more known Readers or Writers in them.
  TopicLost {
    name: String,
  },
  /// New Reader detected (or created locally). Detection happens regardless of
  /// the remote being matched or not by a local Endpoint.
  ReaderDetected {
    reader: EndpointDescription,
  },
  /// New Writer detected
  WriterDetected {
    writer: EndpointDescription,
  },
  /// Remote Reader was lost (disconnected)
  ReaderLost {
    guid: GUID,
    reason: LostReason,
  },
  /// Remote Writer was lost (disconnected)
  WriterLost {
    guid: GUID,
    reason: LostReason,
  },
  RemoteReaderMatched {
    local_writer: GUID,
    remote_reader: GUID,
  },
  RemoteWriterMatched {
    local_reader: GUID,
    remote_writer: GUID,
  },
  RemoteReaderQosIncompatible {
    local_writer: GUID,
    remote_reader: GUID,
    requested_qos: Box<QosPolicies>,
    offered_qos: Box<QosPolicies>,
  },
  RemoteWriterQosIncompatible {
    local_reader: GUID,
    remote_writer: GUID,
    requested_qos: Box<QosPolicies>,
    offered_qos: Box<QosPolicies>,
  },
  #[cfg(feature = "security")]
  Authentication {
    participant: GuidPrefix,
    status: AuthenticationStatus,
  },
  /// The CA has revoked the identity of some Participant.
  /// We may be currently communicating with the Participant, or it may be
  /// unknown to us.
  // TODO:
  /// Not implemented, as we do not implement any identity revocation mechanism
  /// yet.
  #[cfg(feature = "security")]
  IdentityRevoked {
    participant: GuidPrefix,
  },
  /// Domain access permissions of some Participant have been revoked / changed.
  // TODO:
  /// Not implemented, as we do not implement any permissions revocation
  /// mechanism yet.
  #[cfg(feature = "security")]
  PermissionsRevoked {
    participant: GuidPrefix,
    // TODO: How to get more details on what was revoked, or was something added?
  },
}

/// Why some remote entity is considered to be no longer with us.
#[derive(Debug, Clone)]
pub enum LostReason {
  /// Participant announced via Discovery that it is leaving
  Disposed,
  /// Lease time exceeded => timeout
  Timeout {
    lease: Duration,   // What was the discovered lease duration
    elapsed: Duration, // How much time has actually elapsed from last contact
  },
}

/// This is a rewrite/summary of SpdpDiscoveredParticipantData from discovery.
///
/// The original is not used to avoid circular dependency between participant
/// and discovery. Some of the more technical details have been left out
#[derive(Debug, Clone)]
pub struct ParticipantDescription {
  pub updated_time: chrono::DateTime<Utc>,
  pub protocol_version: ProtocolVersion,
  pub vendor_id: VendorId,
  pub guid: GUID,
  pub lease_duration: Option<Duration>,
  pub entity_name: Option<String>,
  #[cfg(feature = "security")]
  pub supports_security: bool,
}

impl From<&SpdpDiscoveredParticipantData> for ParticipantDescription {
  fn from(dpd: &SpdpDiscoveredParticipantData) -> Self {
    ParticipantDescription {
      updated_time: dpd.updated_time,
      protocol_version: dpd.protocol_version,
      vendor_id: dpd.vendor_id,
      guid: dpd.participant_guid,
      lease_duration: dpd.lease_duration,
      entity_name: dpd.entity_name.clone(),
      #[cfg(feature = "security")]
      supports_security: dpd.supports_security(),
    }
  }
}

/// This is a summary of SubscriptionBuiltinTopicData /
/// PublicationBuiltinTopicData from discovery. The original is not used to
/// avoid circular dependency between participant and discovery.
#[derive(Debug, Clone)]
pub struct EndpointDescription {
  pub updated_time: chrono::DateTime<Utc>,
  pub guid: GUID,
  pub topic_name: String,
  pub type_name: String,
  pub qos: QosPolicies,
  pub user_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum DataReaderStatus {
  /// Sample was rejected, because resource limits would have been exceeded.
  SampleRejected {
    count: CountWithChange,
    last_reason: SampleRejectedStatusKind,
    // last_instance_key:
  },
  /// Remote Writer has become active or inactive.
  LivelinessChanged {
    alive_total: CountWithChange,
    not_alive_total: CountWithChange,
    // last_publication_key:
  },
  /// Deadline requested by this DataReader was missed.
  RequestedDeadlineMissed {
    count: CountWithChange,
    // last_instance_key:
  },
  /// This DataReader has requested a QoS policy that is incompatible with what
  /// is offered.
  RequestedIncompatibleQos {
    count: CountWithChange,
    last_policy_id: QosPolicyId,
    writer: GUID,
    requested_qos: Box<QosPolicies>,
    offered_qos: Box<QosPolicies>,
    //policies: Vec<QosPolicyCount>, // Not implemented
  },

  // DataAvailable variant is not implemented, as it seems to bring little additional value,
  // because the normal data waiting mechanism already uses the same mio::poll structure.
  /// A sample has been lost (never received).
  /// TODO: Implement this.
  /// * Check that the following interpretation is correct:
  /// * For a BEST_EFFORT reader: Whenever we skip ahead in SequenceNumber,
  ///   possibly because a message is lost, or messages arrive out of order.
  /// * For a RELIABLE reader: Whenever we skip ahead in SequenceNumbers that
  ///   are delivered via DataReader. The reason may be that we receive a
  ///   HEARTBEAT or GAP submessage indicating that some samples we are
  ///   expecting are not available.
  SampleLost { count: CountWithChange },

  /// The DataReader has found a DataWriter that matches the Topic and has
  /// compatible QoS, or has ceased to be matched with a DataWriter that was
  /// previously considered to be matched.
  SubscriptionMatched {
    total: CountWithChange,
    current: CountWithChange,
    writer: GUID,
    // last_publication_key:
  },
}

#[derive(Debug, Clone)]
pub enum DataWriterStatus {
  LivelinessLost {
    count: CountWithChange,
  },
  OfferedDeadlineMissed {
    count: CountWithChange,
    // last_instance_key:
  },
  OfferedIncompatibleQos {
    count: CountWithChange,
    last_policy_id: QosPolicyId,
    reader: GUID,
    requested_qos: Box<QosPolicies>,
    offered_qos: Box<QosPolicies>,
    //policies: Vec<QosPolicyCount>,  // Not implemented
  },
  PublicationMatched {
    total: CountWithChange,
    current: CountWithChange,
    reader: GUID,
    // last_subscription_key:
  },
}

/// Helper to contain same count actions across statuses
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct CountWithChange {
  // 2.3. Platform Specific Model defines these as "long", which appears to be 32-bit signed.
  count: i32,
  count_change: i32,
}

impl CountWithChange {
  pub(crate) fn new(count: i32, count_change: i32) -> Self {
    Self {
      count,
      count_change,
    }
  }

  // ??
  // same as "new" ?
  pub fn start_from(count: i32, count_change: i32) -> Self {
    Self {
      count,
      count_change,
    }
  }

  pub fn count(&self) -> i32 {
    self.count
  }

  pub fn count_change(&self) -> i32 {
    self.count_change
  }

  // does this make sense?
  // pub fn increase(&mut self) {
  //   self.count += 1;
  //   self.count_change += 1;
  // }
}

/// The PUBLICATION_MATCHED status of a DataWriter: how many DataReaders it is
/// matched with, that is, sends its samples to.
///
/// Returned by `DataWriter::get_publication_matched_status`. Each
/// `count_change()` is the change since the status was last read from the same
/// DataWriter.
///
/// See the tables of Section 2.2.4.1 in DDS Specification v1.4. Its
/// `last_subscription_handle` is not included: the
/// [`DataWriterStatus::PublicationMatched`] status event names the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationMatchedStatus {
  /// How many matches with a DataReader there have been since the DataWriter
  /// was created. This never decreases.
  pub total: CountWithChange,
  /// How many DataReaders the DataWriter is matched with now.
  pub current: CountWithChange,
}

/// How many remote endpoints a DataWriter or DataReader is matched with: the
/// counts of the PUBLICATION_MATCHED and SUBSCRIPTION_MATCHED statuses.
///
/// The RTPS Writer / Reader stores them whenever its set of matched endpoints
/// changes, and the DataWriter / DataReader reads them through a
/// [`MatchedCountsView`]. Unlike a status event, they are not queued, so they
/// are right also when nobody drains the status channel.
#[derive(Debug, Default)]
pub(crate) struct MatchedCounts {
  // The two are stored one after the other. A read that races with a match
  // change may see only one of them updated; the next read sees both.
  total: AtomicI32,
  current: AtomicI32,
}

impl MatchedCounts {
  /// `total`: how many matches there have been ever. `current`: how many
  /// endpoints are matched now.
  pub(crate) fn set(&self, total: i32, current: i32) {
    self.total.store(total, Ordering::Relaxed);
    self.current.store(current, Ordering::Relaxed);
  }
}

/// The DataWriter's / DataReader's end of [`MatchedCounts`]. It remembers the
/// counts it returned last, to tell how much they have changed since.
#[derive(Debug)]
pub(crate) struct MatchedCountsView {
  counts: Arc<MatchedCounts>,
  seen_total: AtomicI32,
  seen_current: AtomicI32,
}

impl MatchedCountsView {
  pub(crate) fn new(counts: Arc<MatchedCounts>) -> Self {
    Self {
      counts,
      seen_total: AtomicI32::new(0),
      seen_current: AtomicI32::new(0),
    }
  }

  /// The total and the current count, each with its change since the previous
  /// call.
  pub(crate) fn read(&self) -> (CountWithChange, CountWithChange) {
    let total = self.counts.total.load(Ordering::Relaxed);
    let current = self.counts.current.load(Ordering::Relaxed);
    let seen_total = self.seen_total.swap(total, Ordering::Relaxed);
    let seen_current = self.seen_current.swap(current, Ordering::Relaxed);
    (
      CountWithChange::new(total, total - seen_total),
      CountWithChange::new(current, current - seen_current),
    )
  }
}

// sample rejection reasons
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleRejectedStatusKind {
  NotRejected,
  ByInstancesLimit,
  BySamplesLimit,
  BySamplesPerInstanceLimit,
}

/* commented out for now, as it is not used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosPolicyCount {
  policy_id: QosPolicyId,
  count: i32,
}
*/

#[cfg(test)]
mod tests {
  use super::*;

  // Total and current count, each with its change, as a matched-status getter
  // returns them.
  fn status(total: (i32, i32), current: (i32, i32)) -> (CountWithChange, CountWithChange) {
    (
      CountWithChange::new(total.0, total.1),
      CountWithChange::new(current.0, current.1),
    )
  }

  // What the DataWriter / DataReader reads is what the RTPS endpoint stored
  // last, and the first read counts the change from "nothing matched".
  #[test]
  fn matched_counts_view_reads_the_counts_stored_last() {
    let counts = Arc::new(MatchedCounts::default());
    let view = MatchedCountsView::new(Arc::clone(&counts));
    assert_eq!(view.read(), status((0, 0), (0, 0)));

    counts.set(1, 1);
    counts.set(2, 2);
    assert_eq!(view.read(), status((2, 2), (2, 2)));
  }

  // A change is reported once: a second read with nothing new in between
  // shows the same counts and no change.
  #[test]
  fn matched_counts_view_reports_a_change_once() {
    let counts = Arc::new(MatchedCounts::default());
    let view = MatchedCountsView::new(Arc::clone(&counts));
    counts.set(1, 1);
    assert_eq!(view.read(), status((1, 1), (1, 1)));
    assert_eq!(view.read(), status((1, 0), (1, 0)));
  }

  // Changes between two reads add up: a new match and the loss of an old one
  // leave the current count where it was, while the total went up. A loss
  // alone is a negative change of the current count only.
  #[test]
  fn matched_counts_view_nets_the_changes_between_reads() {
    let counts = Arc::new(MatchedCounts::default());
    let view = MatchedCountsView::new(Arc::clone(&counts));
    counts.set(1, 1);
    view.read();

    counts.set(2, 2); // a second match
    counts.set(2, 1); // the first one is lost
    assert_eq!(view.read(), status((2, 1), (1, 0)));

    counts.set(2, 0); // the second one is lost too
    assert_eq!(view.read(), status((2, 0), (0, -1)));
  }
}
