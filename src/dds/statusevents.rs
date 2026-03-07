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
use mio_06::Evented;
use mio_extras::channel as mio_channel;
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
  fn as_status_evented(&mut self) -> &dyn Evented; // This is for polling with mio-0.6.x
  fn as_status_source(&mut self) -> &mut dyn mio_08::event::Source; // This is for polling with mio-0.8.x
  fn as_async_status_stream(&'a self) -> S;
  fn try_recv_status(&self) -> Option<E>;
}

// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------

// a wrapper(s) for mio_channel augmented with
// poll channel, so that it can be used together with mio-0.8
// This is only used so that a mio-0.6 channel can pose as a
// mio-0.8 event::Source.

pub(crate) fn sync_status_channel<T>(
  capacity: usize,
) -> io::Result<(StatusChannelSender<T>, StatusChannelReceiver<T>)> {
  let (signal_receiver, signal_sender) = make_poll_channel()?;
  let (actual_sender, actual_receiver) = mio_channel::sync_channel(capacity);
  let waker = Arc::new(Mutex::new(None));
  Ok((
    StatusChannelSender {
      actual_sender,
      signal_sender,
      waker: Arc::clone(&waker),
    },
    StatusChannelReceiver {
      actual_receiver: Mutex::new(actual_receiver),
      signal_receiver,
      waker,
    },
  ))
}

// TODO: try to make this (and the Receiver) private types
#[derive(Clone)]
pub struct StatusChannelSender<T> {
  actual_sender: mio_channel::SyncSender<T>,
  signal_sender: PollEventSender,
  waker: Arc<Mutex<Option<Waker>>>,
}

pub struct StatusChannelReceiver<T> {
  actual_receiver: Mutex<mio_channel::Receiver<T>>,
  signal_receiver: PollEventSource,
  waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> StatusChannelSender<T> {
  /// Best-effort send. If there is no receiver, this will fail silently.
  pub fn try_send(&self, t: T) -> Result<(), mio_channel::TrySendError<T>> {
    let mut w = self.waker.lock().unwrap(); // lock already at the beginning
    match self.actual_sender.try_send(t) {
      Ok(()) => {
        self.signal_sender.send();
        w.as_ref().map(|w| w.wake_by_ref());
        *w = None;
        Ok(())
      }
      Err(mio_channel::TrySendError::Full(_tt)) => {
        warn!("StatusChannelSender cannot send new status changes, channel is full.");
        // It is perfectly normal to fail due to full channel, because
        // no-one is required to be listening to these.
        self.signal_sender.send(); // kick the receiver anyway
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
    self.actual_receiver.lock().unwrap().try_recv()
  }

  pub(crate) fn get_waker_update_lock(&self) -> std::sync::MutexGuard<'_, Option<Waker>> {
    self.waker.lock().unwrap()
  }
}

impl<'a, E> StatusEvented<'a, E, StatusReceiverStream<'a, E>> for StatusChannelReceiver<E> {
  fn as_status_evented(&mut self) -> &dyn Evented {
    self
  }

  fn as_status_source(&mut self) -> &mut dyn mio_08::event::Source {
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
      .actual_receiver
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
      .actual_receiver
      .lock()
      .unwrap()
      .reregister(poll, token, interest, opts)
  }

  fn deregister(&self, poll: &mio_06::Poll) -> io::Result<()> {
    self.actual_receiver.lock().unwrap().deregister(poll)
  }
}

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

// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------
// -------------------------------------------------------------------------------

use std::sync::atomic::{AtomicBool, Ordering};

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
