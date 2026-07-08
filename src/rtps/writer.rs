use std::{
  cell::RefCell,
  cmp::{max, min},
  collections::{BTreeMap, BTreeSet},
  ops::Bound::Included,
  rc::Rc,
  sync::atomic,
};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use speedy::Endianness;
use mio_extras::channel::TrySendError;
use mio_06::{Ready, Registration, SetReadiness, Token};

use crate::{
  dds::{
    qos::{
      policy,
      policy::{History, Reliability},
      HasQoSPolicy, QosPolicies,
    },
    statusevents::{
      CountWithChange, DataWriterStatus, DomainParticipantStatusEvent, StatusChannelSender,
    },
  },
  messages::submessages::submessages::AckSubmessage,
  network::{udp_sender::UDPSender, util::IfAddr},
  polling::SharedTimer,
  rtps::{
    constant::{
      DEFAULT_WRITER_MAX_SAMPLES, FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE, FRAGMENT_SIZE,
      HEARTBEAT_PERIOD_FAST, HEARTBEAT_PERIOD_SLOW, HEARTBEAT_SUBMESSAGE_SERIALIZED_SIZE,
      NACK_RESPONSE_DELAY, NACK_SUPPRESSION_DURATION,
    },
    outbound::{SocketId, TrafficClass},
    rtps_reader_proxy::RtpsReaderProxy,
    timed_event::DpTimerEvent,
    transmit::{DefaultRouteSelector, InterfaceObservations, RouteKey},
    writer_send_buffer::WriterSendBuffer,
    Message, MessageBuilder,
  },
  structure::{
    cache_change::CacheChange,
    duration::Duration,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    locator::Locator,
    sequence_number::{FragmentNumber, SequenceNumber},
    time::Timestamp,
  },
};
#[cfg(feature = "security")]
use crate::{
  rtps::Submessage,
  security::{security_plugins::SecurityPluginsHandle, SecurityResult},
};
#[cfg(not(feature = "security"))]
use crate::no_security::SecurityPluginsHandle;

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum DeliveryMode {
  Unicast,
  Multicast,
}

/// nonblocking-transmit: how far into the current push-mode sample we have
/// transmitted. Lets a large (fragmented) sample resume from the exact point
/// where the socket last returned WouldBlock, instead of restarting.
/// (see src/rtps/nonblocking_transmit_design.md)
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(crate) enum SampleCursor {
  /// Nothing of this sample has been transmitted yet (fresh DATA, or the first
  /// DATAFRAG together with any leading GAP).
  Fresh,
  /// Resume DATAFRAG transmission starting at this fragment number, then the
  /// trailing HEARTBEAT.
  Frag(FragmentNumber),
  /// All fragments sent; only the trailing HEARTBEAT of a fragmented sample
  /// remains.
  Heartbeat,
}

/// Item 1 (DATA aggregation): outcome of attempting to coalesce one or more
/// consecutive unfragmented multicast-to-all samples into a single datagram.
pub(crate) enum BatchOutcome {
  /// The aggregated datagram was accepted by every socket. `last_seq` is the
  /// highest sequence number included in the batch.
  Sent { last_seq: SequenceNumber },
  /// A socket returned WouldBlock and back-pressure applies (reliable, or a
  /// best-effort sample that opted in to blocking). The whole datagram is
  /// all-or-nothing: `last_sent` is left unchanged so the batch is rebuilt and
  /// resent from its first sequence number on the next write-readiness wake.
  Blocked { blocked: BTreeSet<SocketId> },
  /// A socket returned WouldBlock for a best-effort batch that must not block;
  /// the whole datagram is dropped and we advance past it. `last_seq` is the
  /// highest sequence number in the dropped batch.
  Dropped { last_seq: SequenceNumber },
}

/// nonblocking-transmit: outcome of a resumable bulk send of one cache change.
pub(crate) enum SendProgress {
  /// The whole sample (all fragments + trailing HEARTBEAT) was transmitted.
  Complete,
  /// A socket returned WouldBlock. `cursor` is where to resume, `blocked` names
  /// the sockets to arm for write readiness.
  Blocked {
    cursor: SampleCursor,
    blocked: BTreeSet<SocketId>,
  },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimedEvent {
  Heartbeat,
  CacheCleaning,
  SendRepairData { to_reader: GUID },
  SendRepairFrags { to_reader: GUID },
}

// This is used to construct an actual Writer.
// Ingredients are sendable between threads, whereas the Writer is not.
pub(crate) struct WriterIngredients {
  pub guid: GUID,
  /// Shared, flow-controlled buffer of outgoing samples. The producer end is
  /// held by the `DataWriter`; this clone is the consumer end for the Writer.
  pub send_buffer: WriterSendBuffer,
  /// mio readiness "doorbell": the `DataWriter` rings it after admitting a
  /// sample so the event loop wakes and transmits. The Writer keeps the
  /// `Registration` end alive so it stays registered with the poll.
  pub doorbell_registration: Registration,
  /// A clone of the doorbell's `SetReadiness`, used by the Writer to reset the
  /// readiness to empty before draining, so edge-triggered re-arming works.
  pub doorbell: SetReadiness,
  pub topic_name: String,
  pub(crate) like_stateless: bool, // Usually false (see like_stateless attribute of Writer)
  pub qos_policies: QosPolicies,
  pub status_sender: StatusChannelSender<DataWriterStatus>,

  pub(crate) security_plugins: Option<SecurityPluginsHandle>,
}

pub(crate) struct Writer {
  pub endianness: Endianness,
  pub heartbeat_message_counter: atomic::AtomicI32,
  /// Configures the mode in which the
  /// Writer operates. If
  /// pushMode==true, then the Writer
  /// will push changes to the reader. If
  /// pushMode==false, changes will
  /// only be announced via heartbeats
  /// and only be sent as response to the
  /// request of a reader
  pub push_mode: bool,
  /// Protocol tuning parameter that
  /// allows the RTPS Writer to
  /// repeatedly announce the
  /// availability of data by sending a
  /// Heartbeat Message.
  pub heartbeat_period: Option<Duration>,
  /// Faster Heartbeat period used while some matched reader still has
  /// unacknowledged samples. `None` for BestEffort (no periodic Heartbeat).
  pub heartbeat_period_fast: Option<Duration>,
  /// duration to launch cache change remove from DDSCache
  pub cache_cleaning_period: Duration,
  /// Protocol tuning parameter that
  /// allows the RTPS Writer to delay
  /// the response to a request for data
  /// from a negative acknowledgment.
  pub nack_response_delay: std::time::Duration,
  pub nackfrag_response_delay: std::time::Duration,
  pub repairfrags_continue_delay: std::time::Duration,

  /// Protocol tuning parameter that
  /// allows the RTPS Writer to ignore
  /// requests for data from negative
  /// acknowledgments that arrive ‘too
  /// soon’ after the corresponding
  /// change is sent.
  // TODO: use this
  #[allow(dead_code)]
  pub nack_suppression_duration: std::time::Duration,

  /// Largest serialized payload advertised at writer creation (discovery
  /// metadata fallback). Transmit decisions use
  /// [`Self::max_unfragmented_serialized_payload`] instead.
  #[allow(dead_code)]
  pub data_max_size_serialized: usize,

  my_guid: GUID,
  /// mio readiness handle the event loop registers under `entity_token()`. The
  /// `DataWriter` rings the paired `SetReadiness` when it admits a sample.
  pub(crate) doorbell_registration: Registration,
  /// Used to reset the doorbell readiness to empty before draining pending
  /// samples (edge-triggered re-arming).
  doorbell: SetReadiness,

  /// The RTPS ReaderProxy class represents the information an RTPS
  /// StatefulWriter maintains on each matched RTPS Reader
  readers: BTreeMap<GUID, RtpsReaderProxy>,
  matched_readers_count_total: i32, // all matches ever, never decremented
  requested_incompatible_qos_count: i32, // how many times some Reader requested incompatible QoS

  // Sending mechanism
  udp_sender: Rc<UDPSender>,

  // Extra fixed unicast destinations that every outgoing message from this
  // writer is *also* sent to, bypassing route selection. Empty for all writers
  // except the built-in SPDP participant writer, which uses it for the
  // "localhost SPDP peers" (127.0.0.1:<well-known SPDP ports>) so same-host
  // participants discover each other with no external network. Unlike loopback
  // locators discovered from peers, these are unconditional (they are how we
  // bootstrap same-host discovery in the first place). See
  // `src/rtps/loopback_same_host_design.md`.
  extra_unicast_destinations: Vec<Locator>,

  // Whether route selection may prefer a same-host peer's loopback locator.
  // Mirrors the participant-builder `same_host_loopback` knob; `true` by
  // default. Passed into `DefaultRouteSelector` on every route resolution.
  prefer_loopback_same_host: bool,

  // Interface-aware transmit: per-remote observed receive interfaces/addresses,
  // shared (intra-thread) with the MessageReceiver that records them. Consulted
  // when (re)resolving each reader proxy's SendRoute.
  interface_observations: Rc<RefCell<InterfaceObservations>>,

  // Snapshot of the local interface table (shared, read-only), used to resolve
  // each matched reader's per-peer path-MTU budget when its locators change.
  local_interfaces: Rc<[IfAddr]>,

  // Minimum per-peer datagram-payload budget over all matched readers. The
  // aggregated / packed datagram is a single packet shared by every reader
  // (sent to `EntityId::UNKNOWN`), so it must fit the smallest reader's path
  // MTU. Recomputed whenever a reader is added, updated, or removed. Falls back
  // to `FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE` when there are no matched readers.
  min_datagram_payload: usize,

  // By default, this writer is a StatefulWriter (see RTPS spec section 8.4.9)
  // If like_stateless is true, then the writer mimics the behavior of a Best-Effort
  // StatelessWriter. This behavior is needed only for a single built-in discovery topic of
  // Secure DDS (topic DCPSParticipantStatelessMessage).
  // The basic idea in mimicking BestEffort & Stateless is:
  //  1. Make sure no heartbeats, acknacks, or anything related to Reliable behavior is processed
  //  2. Use the RtpsReaderProxies merely as locators, do not utilize/modify their state
  // Note that unlike the Best-Effort StatelessWriter in the specification, here we don't send
  // GAP messages. But this shouldn't matter since the expected remote Reader is also BestEffort &
  // Stateless, and therefore does not process GAP messages at all.
  like_stateless: bool,

  /// Writer can only read/write to this topic DDSHistoryCache.
  my_topic_name: String,

  /// Shared, flow-controlled buffer of outgoing samples. Filled by the
  /// `DataWriter` (admission + sequence numbering), drained/transmitted here.
  send_buffer: WriterSendBuffer,
  /// Highest sequence number this Writer has fully transmitted (push mode).
  /// Samples with `seq` in `(last_sent, send_buffer.last]` are pending send.
  last_sent: SequenceNumber,
  /// nonblocking-transmit: transmit progress within the sample `last_sent + 1`
  /// (the one currently being pushed). `Fresh` unless a large sample was
  /// interrupted mid-way by a full socket.
  sample_cursor: SampleCursor,
  /// nonblocking-transmit: sockets on which the last push attempt hit
  /// WouldBlock. Drained by the event loop, which enqueues this writer on those
  /// sockets' round-robin queues and arms write readiness.
  blocked_sockets: BTreeSet<SocketId>,

  /// Contains timer that needs to be set to timeout with duration of
  /// self.heartbeat_period timed_event_handler sends notification when timer
  /// is up via mio channel to poll in Dp_eventWrapper this also handles
  /// writers cache cleaning timeouts.
  pub(crate) timed_event_timer: SharedTimer<DpTimerEvent>,

  qos_policies: QosPolicies,

  // Used for sending status info about messages sent
  status_sender: StatusChannelSender<DataWriterStatus>,
  // offered_deadline_status: OfferedDeadlineMissedStatus,
  participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,

  security_plugins: Option<SecurityPluginsHandle>,
}

impl Writer {
  pub fn new(
    i: WriterIngredients,
    udp_sender: Rc<UDPSender>,
    timed_event_timer: SharedTimer<DpTimerEvent>,
    participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
    interface_observations: Rc<RefCell<InterfaceObservations>>,
    local_interfaces: Rc<[IfAddr]>,
  ) -> Self {
    // If writer should behave statelessly, only BestEffort QoS is currently
    // supported
    if i.like_stateless && i.qos_policies.is_reliable() {
      panic!(
        "RustDDS internal bug: attempted to create a stateless-like Writer with Reliable QoS"
      );
    }

    let heartbeat_period = i
      .qos_policies
      .reliability
      .and_then(|reliability| {
        if matches!(reliability, Reliability::Reliable { .. }) {
          Some(HEARTBEAT_PERIOD_SLOW)
        } else {
          None
        }
      })
      .map(|hbp| {
        // What is the logic here? Which spec section?
        if let Some(policy::Liveliness::ManualByTopic { lease_duration }) =
          i.qos_policies.liveliness
        {
          let std_dur = lease_duration;
          std_dur / 3
        } else {
          hbp
        }
      });

    // Faster Heartbeat period used while some reader is still behind. Never
    // slower than the (possibly liveliness-shortened) slow period.
    let heartbeat_period_fast = heartbeat_period.map(|slow| min(HEARTBEAT_PERIOD_FAST, slow));

    // TODO: Configuration value
    let cache_cleaning_period = Duration::from_secs(6);

    // Start periodic Heartbeat
    if let Some(period) = heartbeat_period {
      timed_event_timer.borrow_mut().set_timeout(
        std::time::Duration::from(period),
        DpTimerEvent::Writer {
          entity_id: i.guid.entity_id,
          event: TimedEvent::Heartbeat,
        },
      );
    }
    // start periodic cache cleaning
    timed_event_timer.borrow_mut().set_timeout(
      std::time::Duration::from(cache_cleaning_period),
      DpTimerEvent::Writer {
        entity_id: i.guid.entity_id,
        event: TimedEvent::CacheCleaning,
      },
    );

    Self {
      endianness: Endianness::LittleEndian,
      heartbeat_message_counter: atomic::AtomicI32::new(1),
      push_mode: true,
      heartbeat_period,
      heartbeat_period_fast,
      cache_cleaning_period,
      nack_response_delay: NACK_RESPONSE_DELAY, // default value from dp_event_loop
      nackfrag_response_delay: NACK_RESPONSE_DELAY, // default value from dp_event_loop
      repairfrags_continue_delay: std::time::Duration::from_millis(1),
      nack_suppression_duration: NACK_SUPPRESSION_DURATION,
      // Conservative fallback for any discovery advertisement of max sample size.
      data_max_size_serialized: FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE,
      my_guid: i.guid,
      doorbell_registration: i.doorbell_registration,
      doorbell: i.doorbell,
      readers: BTreeMap::new(),
      matched_readers_count_total: 0,
      requested_incompatible_qos_count: 0,
      udp_sender,
      extra_unicast_destinations: Vec::new(),
      prefer_loopback_same_host: true,
      interface_observations,
      local_interfaces,
      min_datagram_payload: FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE,
      my_topic_name: i.topic_name,
      send_buffer: i.send_buffer,
      last_sent: SequenceNumber::zero(),
      sample_cursor: SampleCursor::Fresh,
      blocked_sockets: BTreeSet::new(),
      timed_event_timer,
      like_stateless: i.like_stateless,
      qos_policies: i.qos_policies,
      status_sender: i.status_sender,
      participant_status_sender,

      security_plugins: i.security_plugins,
    }
  }

  /// To know when token represents a writer we should look entity attribute
  /// kind this entity token can be used in DataWriter -> Writer mio::channel.
  pub fn entity_token(&self) -> Token {
    self.guid().entity_id.as_token()
  }

  pub fn is_reliable(&self) -> bool {
    self.qos_policies.is_reliable()
  }

  /// Lists the known local (same DomainParticipant) ReaderProxies
  /// Note that local non-matching Readers are not here.
  pub fn local_readers(&self) -> Vec<EntityId> {
    let min = GUID::new_with_prefix_and_id(self.my_guid.prefix, EntityId::MIN);
    let max = GUID::new_with_prefix_and_id(self.my_guid.prefix, EntityId::MAX);

    self
      .readers
      .range((Included(min), Included(max)))
      .filter_map(|(guid, _)| {
        if guid.prefix == self.my_guid.prefix {
          Some(guid.entity_id)
        } else {
          None
        }
      })
      .collect()
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------
  // --------------------------------------------------------------

  // Schedule a timed event for this Writer on the event loop's shared timer.
  // The payload is tagged with this Writer's EntityId so the event loop can
  // route the fired event back to this Writer.
  fn schedule_timed_event(&self, after: std::time::Duration, event: TimedEvent) {
    self.timed_event_timer.borrow_mut().set_timeout(
      after,
      DpTimerEvent::Writer {
        entity_id: self.my_guid.entity_id,
        event,
      },
    );
  }

  // Handle a single timed event. The shared timer is drained by the event loop,
  // which dispatches each expired event to the addressed Writer.
  pub fn handle_timed_event(&mut self, event: TimedEvent) {
    match event {
      TimedEvent::Heartbeat => {
        let readers_behind = self.handle_heartbeat_tick(false);
        // ^^ false = This is automatic heartbeat by timer, not manual by application
        // call.
        // Adaptive period: reschedule sooner (fast) while some reader still has
        // unacknowledged data so repair is prompted quickly, and back off to the
        // slow period once everyone is caught up to keep idle traffic low.
        let next_period = if readers_behind {
          self.heartbeat_period_fast.or(self.heartbeat_period)
        } else {
          self.heartbeat_period
        };
        if let Some(period) = next_period {
          self.schedule_timed_event(std::time::Duration::from(period), TimedEvent::Heartbeat);
        }
      }
      TimedEvent::CacheCleaning => {
        self.handle_cache_cleaning();
        self.schedule_timed_event(
          std::time::Duration::from(self.cache_cleaning_period),
          TimedEvent::CacheCleaning,
        );
      }
      TimedEvent::SendRepairData {
        to_reader: reader_guid,
      } => {
        self.handle_repair_data_send(reader_guid);
        if let Some(rp) = self.lookup_reader_proxy_mut(reader_guid) {
          if rp.repair_mode {
            let delay_to_next_repair = self
              .qos_policies
              .deadline()
              .map_or_else(|| Duration::from_millis(1), |dl| dl.0)
              / 5;
            self.schedule_timed_event(
              std::time::Duration::from(delay_to_next_repair),
              TimedEvent::SendRepairData {
                to_reader: reader_guid,
              },
            );
          }
        }
      }
      TimedEvent::SendRepairFrags {
        to_reader: reader_guid,
      } => {
        self.handle_repair_frags_send(reader_guid);
        if let Some(rp) = self.lookup_reader_proxy_mut(reader_guid) {
          if rp.repair_frags_requested() {
            // more repair needed?
            self.schedule_timed_event(
              self.repairfrags_continue_delay,
              TimedEvent::SendRepairFrags {
                to_reader: reader_guid,
              },
            );
          } // if
        } // if let
      } // SendRepairFrags
    } // match
  } // fn

  /// This is called by dp_wrapper every time cacheCleaning message is received.
  fn handle_cache_cleaning(&mut self) {
    // Upper bound on retained samples. Use the Writer QoS ResourceLimits if it
    // specifies a positive max_samples; otherwise fall back to a generous
    // default so that a reliable Writer keeps unacknowledged samples available
    // for repair instead of evicting them eagerly. This is only a memory-safety
    // backstop, not a normal operating limit.
    let resource_limit = self
      .qos_policies
      .resource_limits()
      .map(|rl| rl.max_samples)
      .filter(|&max_samples| max_samples > 0)
      .map_or(DEFAULT_WRITER_MAX_SAMPLES, |max_samples| {
        max_samples as usize
      });

    match self.qos_policies.history {
      None => {
        // DDS Specification says this is the default History policy
        self.remove_all_acked_changes_but_keep_depth(Some(1), resource_limit);
      }
      Some(History::KeepAll) => {
        self.remove_all_acked_changes_but_keep_depth(None, resource_limit);
      }
      Some(History::KeepLast { depth: d }) => {
        self.remove_all_acked_changes_but_keep_depth(Some(d as usize), resource_limit);
      }
    }
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------
  // --------------------------------------------------------------
  // Per-peer UDP-payload budget (bytes for RTPS submessages in one datagram)
  // for a send: the specific reader's resolved path-MTU budget for a directed
  // send, or the writer-wide minimum over all matched readers for a
  // multicast-to-all send (one datagram shared by every reader).
  fn datagram_budget(&self, target_reader_opt: Option<&RtpsReaderProxy>) -> usize {
    match target_reader_opt {
      Some(reader) => reader.max_datagram_payload(),
      None => self.min_datagram_payload,
    }
  }

  /// Largest serialized payload that fits in one unfragmented DATA submessage
  /// within the per-peer datagram budget. Fragmentation is triggered only when
  /// the payload exceeds this, not when it exceeds [`FRAGMENT_SIZE`].
  fn max_unfragmented_serialized_payload(
    &self,
    target_reader_opt: Option<&RtpsReaderProxy>,
  ) -> usize {
    let budget = self.datagram_budget(target_reader_opt);
    let hb_reserve = if self.is_reliable() && !self.like_stateless {
      HEARTBEAT_SUBMESSAGE_SERIALIZED_SIZE
    } else {
      0
    };
    budget
      .saturating_sub(hb_reserve)
      .saturating_sub(DATA_SUBMESSAGE_OVERHEAD)
  }

  fn num_frags_and_frag_size(&self, payload_size: usize) -> (u32, u16) {
    let fragment_size = FRAGMENT_SIZE as u32;
    let data_size = payload_size as u32; // TODO: overflow check
                                         // Formula from RTPS spec v2.5 Section "8.3.8.3.5 Logical Interpretation"
    let num_frags = (data_size / fragment_size) + u32::from(data_size % fragment_size != 0); // rounding up
    debug!("Fragmenting {data_size} to {num_frags} x {fragment_size}");
    // TODO: Check fragment_size overflow
    (num_frags, fragment_size as u16)
  }

  // The DataWriter has admitted one or more new samples into the shared send
  // buffer and rung the doorbell. Transmit every sample we have not sent yet.
  //
  // nonblocking-transmit: this is also the resume entry point. When a socket
  // returns WouldBlock mid-way, we stop and remember exactly where (`last_sent`
  // + `sample_cursor`); the event loop re-invokes us on write readiness. Large
  // (fragmented) samples resume from the exact fragment, never restart. Blocked
  // sockets are recorded in `blocked_sockets` for the event loop to schedule.
  // Prune a completed / dropped / skipped sequence number from every matched
  // reader's `unsent_changes` set. Historically this was only done on the NACK
  // repair path (`mark_change_sent`), so a best-effort push (which never gets
  // ACKNACKs) left `unsent_changes` growing by one entry per sample forever.
  // Pruning here keeps the per-reader bookkeeping bounded on the push path too.
  fn mark_change_sent_to_all_readers(&mut self, sequence_number: SequenceNumber) {
    if self.like_stateless {
      return;
    }
    for reader in self.readers.values_mut() {
      reader.mark_change_sent(sequence_number);
    }
  }

  // Item 1: is this sample eligible for the DATA-coalescing fast path?
  // We only coalesce unfragmented samples that go to every matched reader
  // (multicast-to-all). Fragmented or single-reader samples keep the existing
  // per-sample / per-fragment path.
  fn is_aggregatable(&self, cc: &CacheChange) -> bool {
    cc.data_value.payload_size() <= self.max_unfragmented_serialized_payload(None)
      && cc.write_options.to_single_reader().is_none()
  }

  // Item 1: greedily coalesce consecutive unfragmented multicast-to-all samples
  // starting at `first_seq` (up to `last_available`) into a single RTPS
  // datagram, then send it once. Returns None if the first sample is not
  // eligible (so the caller uses the per-sample path); otherwise the batch
  // outcome. The datagram is all-or-nothing on WouldBlock.
  fn try_send_aggregated_batch(
    &mut self,
    first_seq: SequenceNumber,
    last_available: SequenceNumber,
  ) -> Option<BatchOutcome> {
    // Peek the first sample. If it is missing (evicted) or not eligible, defer
    // to the per-sample path, which knows how to skip / fragment / route it.
    let first_cc = self.send_buffer.get_by_sn(first_seq)?;
    if !self.is_aggregatable(&first_cc) {
      return None;
    }

    let is_reliable = self.is_reliable();
    // Reserve room for the single trailing HEARTBEAT (reliable only).
    let hb_reserve = if is_reliable && !self.like_stateless {
      HEARTBEAT_SUBMESSAGE_SERIALIZED_SIZE
    } else {
      0
    };

    let mut builder = MessageBuilder::new();
    let mut last_seq = first_seq;
    let mut count: i32 = 0;
    // The batch back-pressures (rather than dropping) if the writer is reliable
    // or any included best-effort sample opted in to blocking.
    let mut may_block = is_reliable;
    // Whether a preceding INFO_TS in this datagram is still "active". An INFO_TS
    // applies to every following DATA until the next INFO_TS, so a timestamped
    // sample followed by a non-timestamped one must emit an invalidating INFO_TS
    // to avoid the latter inheriting the former's timestamp.
    let mut ts_active = false;

    let mut seq = first_seq;
    while seq <= last_available {
      let Some(cc) = self.send_buffer.get_by_sn(seq) else {
        // A gap in the buffer (evicted). Stop the batch here and let the
        // per-sample path handle the missing sequence number next.
        break;
      };
      if !self.is_aggregatable(&cc) {
        break;
      }

      // Build this sample's INFO_TS? + DATA submessages and check the budget.
      let mut sample = MessageBuilder::new();
      let sample_ts_active = match cc.write_options.source_timestamp() {
        Some(src_ts) => {
          sample = sample.ts_msg(self.endianness, Some(src_ts));
          true
        }
        None => {
          if ts_active {
            // Invalidate the previous timestamp so this DATA is not stamped.
            sample = sample.ts_msg(self.endianness, None);
          }
          false
        }
      };
      sample = sample.data_msg(
        &cc,
        EntityId::UNKNOWN, // multicast-to-all
        self.my_guid,
        self.endianness,
        self.security_plugins.as_ref(),
      );

      // Always include the first sample (even if it alone exceeds the budget, so
      // we make progress); otherwise stop before overflowing the datagram. The
      // budget is the minimum per-peer path-MTU over all matched readers, since
      // this datagram is multicast to all of them.
      if count > 0
        && builder.len_serialized() + sample.submessage_bytes_len() + hb_reserve
          > self.min_datagram_payload
      {
        break;
      }

      builder.append(sample);
      ts_active = sample_ts_active;
      last_seq = seq;
      count += 1;
      if cc.write_options.best_effort_may_block() {
        may_block = true;
      }
      seq = seq.plus_1();
    }

    // One trailing HEARTBEAT for the whole datagram (reliable writers only).
    if hb_reserve > 0 {
      builder = builder.heartbeat_msg(
        self.entity_id(),
        self.send_buffer.first_change_sequence_number(),
        self.send_buffer.last_change_sequence_number(),
        self.next_heartbeat_count(),
        self.endianness,
        EntityId::UNKNOWN, // to all readers
        false,             // final_flag: request ACKNACK
        false,             // liveliness_flag
      );
    }

    let message = builder.add_header_and_build(self.my_guid.prefix);
    let blocked = self.send_message_to_readers(
      DeliveryMode::Multicast,
      message,
      &mut self.readers.values(),
      TrafficClass::Bulk,
    );

    if blocked.is_empty() {
      Some(BatchOutcome::Sent { last_seq })
    } else if may_block {
      Some(BatchOutcome::Blocked { blocked })
    } else {
      Some(BatchOutcome::Dropped { last_seq })
    }
  }

  pub fn process_pending(&mut self) {
    // Reset the doorbell to empty *before* reading the buffer state, so that any
    // sample admitted concurrently re-arms the (edge-triggered) doorbell and we
    // are woken again. The shared buffer's `last_seq` is the source of truth.
    let _ = self.doorbell.set_readiness(Ready::empty());

    loop {
      let last_available = self.send_buffer.last_change_sequence_number();
      if self.last_sent >= last_available {
        break;
      }
      let sequence_number = self.last_sent.plus_1();

      // Item 1 (DATA aggregation): coalesce several consecutive small samples
      // into one datagram to amortize the per-sample RTPS header + syscall.
      // Only engaged when starting a sample fresh (no mid-fragment resume),
      // in push mode, and with security off (per-reader crypto complicates
      // batching). `try_send_aggregated_batch` returns None when the first
      // sample is not eligible (fragmented or destined to a single reader), in
      // which case we fall through to the per-sample path below.
      if self.push_mode && self.sample_cursor == SampleCursor::Fresh && self.security_plugins.is_none()
      {
        match self.try_send_aggregated_batch(sequence_number, last_available) {
          Some(BatchOutcome::Sent { last_seq }) | Some(BatchOutcome::Dropped { last_seq }) => {
            // Coalesced samples are multicast-to-all, so per-reader `unsent`
            // bookkeeping is a no-op (notify + mark_sent cancel out); just
            // advance the frontier. Leaves `unsent_changes` empty on the push
            // path, exactly like the per-sample path after the Item 5 fix.
            self.last_sent = last_seq;
            self.sample_cursor = SampleCursor::Fresh;
            self.send_buffer.set_sent_frontier(last_seq);
            continue;
          }
          Some(BatchOutcome::Blocked { blocked }) => {
            // All-or-nothing: do not advance `last_sent`; the event loop resumes
            // us on write readiness and the batch is rebuilt from `sequence_number`.
            self.blocked_sockets.extend(blocked);
            return;
          }
          None => { /* first sample not aggregatable; use the per-sample path */ }
        }
      }

      // Fetch an owned clone of the sample (cheap, Bytes-backed) so we hold the
      // buffer lock only momentarily and can serialize/send without it.
      let Some(cc) = self.send_buffer.get_by_sn(sequence_number) else {
        // The sample is gone (e.g. evicted). Skip it; readers that need it will
        // be told via GAP during repair.
        self.last_sent = sequence_number;
        self.sample_cursor = SampleCursor::Fresh;
        self.send_buffer.set_sent_frontier(sequence_number);
        self.mark_change_sent_to_all_readers(sequence_number);
        continue;
      };
      let write_options = cc.write_options.clone();

      // Notify reader proxies once per sample (only when starting it fresh).
      if self.sample_cursor == SampleCursor::Fresh && !self.like_stateless {
        for reader in self.readers.values_mut() {
          reader.notify_new_cache_change(sequence_number);

          // If the data is meant for a single reader only, set others as pending
          // GAP for this sequence number.
          if let Some(single_reader_guid) = write_options.to_single_reader() {
            if reader.remote_reader_guid != single_reader_guid {
              reader.insert_pending_gap(sequence_number);
            }
          }
        }
      }

      if self.push_mode {
        // Send data (DATA or DATAFRAGs) and a Heartbeat, resuming from the
        // current fragment cursor. Compute the send in an inner scope so the
        // immutable borrow of `self.readers` (target reader) is released before
        // we mutate our cursor/frontier below.
        let cursor = self.sample_cursor;
        // Best-effort has no acknowledgement/repair semantics, so a heartbeat
        // per sample is pure overhead (an extra submessage built and sent for
        // every DATA). Only reliable writers piggyback a heartbeat here;
        // best-effort skips it entirely.
        let send_also_heartbeat = self.is_reliable();
        let (_fragmented, progress) = {
          let target_reader_opt = match write_options.to_single_reader() {
            Some(guid) => self.readers.get(&guid), // Sending only to this reader
            None => None,                          // Sending to all matched readers
          };
          // Built-in (discovery) writers carry low-volume, delivery-critical
          // SEDP/SPDP data. Their initial push must not be dropped on WouldBlock:
          // under a flat-out user writer the send socket is perpetually congested,
          // so a dropped DiscoveredWriterData would have to be recovered by the
          // reliable heartbeat/ACKNACK/repair chain - which is itself starved
          // (the shared timer barely fires under load), leaving the remote
          // endpoint permanently undiscovered. Route discovery pushes through the
          // never-dropped Control queue (strict priority in on_socket_writable)
          // so discovery completes regardless of user-data congestion. User
          // writers keep the flow-controlled Bulk path.
          let push_class = if self.my_guid.entity_id.kind().is_built_in() {
            TrafficClass::Control
          } else {
            TrafficClass::Bulk
          };
          self.send_cache_change_from(
            &cc,
            send_also_heartbeat,
            target_reader_opt,
            cursor,
            push_class,
          )
        };
        match progress {
          SendProgress::Complete => {
            self.last_sent = sequence_number;
            self.sample_cursor = SampleCursor::Fresh;
            self.send_buffer.set_sent_frontier(sequence_number);
            self.mark_change_sent_to_all_readers(sequence_number);
          }
          SendProgress::Blocked { cursor, blocked } => {
            // Reliable writers always back-pressure. Best-effort writers only if
            // this sample opted in via `best_effort_may_block`; otherwise the
            // DDS default applies and we drop the (rest of the) sample and move
            // on to fresher data (spec v1.4 2.2.2.4.2.11).
            if self.is_reliable() || write_options.best_effort_may_block() {
              // Stop here; resume on write readiness. Back-pressure to the
              // application follows from `sent_frontier` not advancing.
              self.sample_cursor = cursor;
              self.blocked_sockets.extend(blocked);
              return;
            }
            // Drop: advance past this sample without recording blocked sockets,
            // so no write-readiness back-pressure is applied. The unsent
            // remainder is discarded and later evicted by cache cleaning.
            self.last_sent = sequence_number;
            self.sample_cursor = SampleCursor::Fresh;
            self.send_buffer.set_sent_frontier(sequence_number);
            self.mark_change_sent_to_all_readers(sequence_number);
          }
        }
      } else {
        // Send Heartbeat only (control).
        // Readers will ask for the DATA with ACKNACK, if they are interested.
        let final_flag = false; // false = request that readers acknowledge with ACKNACK.
        let liveliness_flag = false; // This is not a manual liveliness assertion (DDS API call), but side-effect of
        let hb_message = MessageBuilder::new()
          .heartbeat_msg(
            self.entity_id(), // from Writer
            self.send_buffer.first_change_sequence_number(),
            self.send_buffer.last_change_sequence_number(),
            self.next_heartbeat_count(),
            self.endianness,
            EntityId::UNKNOWN, // to Reader
            final_flag,
            liveliness_flag,
          )
          .add_header_and_build(self.my_guid.prefix);
        self.send_message_to_readers(
          DeliveryMode::Multicast,
          hb_message,
          &mut self.readers.values(),
          TrafficClass::Control,
        );
        self.last_sent = sequence_number;
        self.sample_cursor = SampleCursor::Fresh;
        self.send_buffer.set_sent_frontier(sequence_number);
      }
    }
  }

  /// nonblocking-transmit: drain and return the sockets on which the last push
  /// attempt hit WouldBlock, so the event loop can enqueue this writer for a
  /// round-robin resume on write readiness.
  pub fn take_blocked_sockets(&mut self) -> BTreeSet<SocketId> {
    std::mem::take(&mut self.blocked_sockets)
  }

  // Returns a boolean telling if the data had to be fragmented. This one-shot
  // wrapper is used by the repair path (ACKNACK response). Repair is a
  // reliability action: it must actually reach the reader, so it goes through
  // the never-dropped `Control` queue rather than the best-effort `Bulk` path.
  // Dropping repair on WouldBlock livelocks under sustained send congestion -
  // the reader re-NACKs, the retransmit is dropped again, and reliable data
  // (e.g. builtin SEDP DiscoveredWriterData) never gets delivered while a
  // flat-out writer keeps the socket blocked.
  fn send_cache_change(
    &self,
    cc: &CacheChange,
    send_also_heartbeat: bool,
    target_reader_opt: Option<&RtpsReaderProxy>,
  ) -> bool {
    let (fragmentation_needed, _progress) = self.send_cache_change_from(
      cc,
      send_also_heartbeat,
      target_reader_opt,
      SampleCursor::Fresh,
      TrafficClass::Control,
    );
    fragmentation_needed
  }

  // Resumable bulk send of one cache change starting at `cursor`. Returns
  // whether the sample is fragmented and how far the send got (Complete, or
  // Blocked with the cursor to resume from and the sockets that blocked).
  fn send_cache_change_from(
    &self,
    cc: &CacheChange,
    send_also_heartbeat: bool,
    target_reader_opt: Option<&RtpsReaderProxy>,
    cursor: SampleCursor,
    class: TrafficClass,
  ) -> (bool, SendProgress) {
    // First make sure that if the data is meant for a single reader only, we do
    // not accidentally send it to everyone.
    if let Some(single_reader_guid) = cc.write_options.to_single_reader() {
      match target_reader_opt {
        None => {
          error!(
            "Data is meant for the single reader {single_reader_guid:?} but a proxy for this \
             reader was not provided. Not sending anything."
          );
          return (false, SendProgress::Complete);
        }
        Some(target_reader) => {
          if single_reader_guid != target_reader.remote_reader_guid {
            error!(
              "We were asked to send data meant for the reader {single_reader_guid:?} to a \
               different reader {:?}. Not gonna happen.",
              target_reader.remote_reader_guid
            );
            return (false, SendProgress::Complete);
          }
        }
      }
    }

    let messages_to_send =
      FragmentationIter::new_resume(self, cc, target_reader_opt, send_also_heartbeat, cursor);
    let fragmentation_needed = messages_to_send.fragmentation_needed();

    // Send the messages, either to all readers or just one, stopping at the
    // first message that a socket could not accept.
    for (resume_cursor, msg) in messages_to_send {
      let blocked = match target_reader_opt {
        None => self.send_message_to_readers(
          DeliveryMode::Multicast,
          msg,
          &mut self.readers.values(),
          class,
        ),
        Some(reader_proxy) => self.send_message_to_readers(
          DeliveryMode::Unicast,
          msg,
          &mut std::iter::once(reader_proxy),
          class,
        ),
      };
      if !blocked.is_empty() {
        return (
          fragmentation_needed,
          SendProgress::Blocked {
            cursor: resume_cursor,
            blocked,
          },
        );
      }
    }
    (fragmentation_needed, SendProgress::Complete)
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------
  // --------------------------------------------------------------

  /// This is called periodically.
  ///
  /// Returns `true` if some matched reader still has unacknowledged data (i.e.
  /// a HEARTBEAT was sent to prompt repair), so the caller can reschedule the
  /// next periodic heartbeat sooner. Returns `false` when all readers are
  /// caught up.
  pub fn handle_heartbeat_tick(&mut self, is_manual_assertion: bool) -> bool {
    if self.like_stateless {
      info!(
        "Ignoring handling heartbeat tick in a stateless-like Writer, since it currently supports \
         only BestEffort QoS. topic={:?}",
        self.my_topic_name
      );
      return false;
    }
    // Reliable Stateful Writer (that tracks Readers by ReaderProxy) will not set
    // the final flag.
    let final_flag = false;
    let liveliness_flag = is_manual_assertion; // RTPS spec "8.3.7.5 Heartbeat"

    trace!(
      "heartbeat tick in topic {:?} have {} readers",
      self.topic_name(),
      self.readers.len()
    );

    let first_change = self.send_buffer.first_change_sequence_number();
    let last_change = self.send_buffer.last_change_sequence_number();

    if self
      .readers
      .values()
      .all(|rp| last_change < rp.all_acked_before)
    {
      trace!("heartbeat tick: all readers have all available data.");
      false
    } else {
      // the interface to .heartbeat_msg is silly: we give ref to ourself
      // and that function then queries us.
      let hb_message = MessageBuilder::new()
        .ts_msg(self.endianness, Some(Timestamp::now()))
        .heartbeat_msg(
          self.entity_id(), // from Writer
          self.send_buffer.first_change_sequence_number(),
          self.send_buffer.last_change_sequence_number(),
          self.next_heartbeat_count(),
          self.endianness,
          EntityId::UNKNOWN, // to Reader
          final_flag,
          liveliness_flag,
        )
        .add_header_and_build(self.my_guid.prefix);

      debug!(
        "Writer {:?} topic={:} HEARTBEAT {:?} to {:?}",
        self.guid().entity_id,
        self.topic_name(),
        first_change,
        last_change,
      );

      // In the volatile key exchange topic we cannot send to multiple readers by any
      // means, so we handle that separately.
      if self.entity_id() == EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER {
        for rp in self.readers.values() {
          if last_change < rp.all_acked_before {
            // Everything we have has been acknowledged already. Do nothing.
          } else {
            self.send_control_to_readers(
              DeliveryMode::Unicast,
              hb_message.clone(),
              &mut std::iter::once(rp),
            );
          }
        }
      } else {
        // Normal case
        self.send_control_to_readers(
          DeliveryMode::Multicast,
          hb_message,
          &mut self.readers.values(),
        );
      }
      true
    }
  }

  /// When receiving an ACKNACK Message indicating a Reader is missing some data
  /// samples, the Writer must respond by either sending the missing data
  /// samples, sending a GAP message when the sample is not relevant, or
  /// sending a HEARTBEAT message when the sample is no longer available
  pub fn handle_ack_nack(
    &mut self,
    reader_guid_prefix: GuidPrefix,
    ack_submessage: &AckSubmessage,
  ) {
    // sanity check
    if !self.is_reliable() || self.like_stateless {
      // Stateless-like Writer currently supports only BestEffort QoS, so ignore
      // acknack also for it
      warn!(
        "Writer {:x?} is best effort or stateless-like! It should not handle acknack messages!",
        self.entity_id()
      );
      return;
    }

    match ack_submessage {
      AckSubmessage::AckNack(ref an) => {
        // Update the ReaderProxy
        let last_seq = self.send_buffer.last_change_sequence_number(); // to avoid borrow problems

        // sanity check requested sequence numbers
        if let Some(0) = an.reader_sn_state.iter().next().map(i64::from) {
          warn!("Request for SN zero! : {an:?}");
        }

        let reader_guid = GUID::new(reader_guid_prefix, an.reader_id);

        // sanity check
        if an.reader_sn_state.base() < SequenceNumber::from(1) {
          // This check is based on RTPS v2.5 Spec
          // Section "8.3.5.5 SequenceNumberSet" and
          // Section "8.3.8.1.3 Validity".
          // But apparently some RTPS implementations send ACKNACK with
          // reader_sn_state.base = 0 to indicate they have matched the writer,
          // so seeing these once per new writer should be ok.
          debug!(
            "ACKNACK SequenceNumberSet minimum must be >= 1, got {:?} from {:?} topic {:?}",
            an.reader_sn_state.base(),
            reader_guid,
            self.topic_name()
          );
        }

        let my_topic = self.my_topic_name.clone(); // for debugging

        // Built-in (discovery) writers must recover a missed sample promptly and
        // independently of the shared timer: under a flat-out user writer the
        // timer thread is CPU-starved and the deferred `SendRepairData` timeout
        // fires late or never, so a NACKed DiscoveredWriterData is never
        // retransmitted and the remote endpoint stays undiscovered. For built-in
        // writers we therefore repair synchronously, right here on the ACKNACK
        // event (which the event loop always services). Discovery is low-volume,
        // so responding immediately (no nack-batching delay) is cheap.
        let repair_immediately = self.my_guid.entity_id.kind().is_built_in();
        let mut do_immediate_repair = false;

        if let Some(reader_proxy) = self.lookup_reader_proxy_mut(reader_guid) {
          // Mark requested SNs as "unsent changes"

          //TODO: We should drop SNs in "pending gap" from unsent changes
          reader_proxy.handle_ack_nack(ack_submessage, last_seq);

          let reader_guid = reader_proxy.remote_reader_guid; // copy to avoid double mut borrow

          // Sanity Check: if the reader asked for something we did not even advertise
          // yet. TODO: This
          // checks the stored unset_changes, not presently received ACKNACK.
          if cfg!(debug_assertions) {
            if let Some(req_high) = reader_proxy.unsent_changes_iter().next_back() {
              if req_high > last_seq {
                warn!(
                  "ReaderProxy {:?} thinks we need to send {:?} but I have only up to {:?}",
                  reader_proxy.remote_reader_guid,
                  reader_proxy.unsent_changes_debug(),
                  last_seq
                );
              }
            }
            // Sanity Check 2
            if an.reader_sn_state.base() > last_seq.plus_1() {
              warn!(
                "ACKNACK from {:?} acks {:?}, but I have only up to {:?} count={:?} topic={:?}",
                reader_proxy.remote_reader_guid, an.reader_sn_state, last_seq, an.count, my_topic
              );
            }
            // Sanity check 3
            if let Some(max_req_sn) = an.reader_sn_state.iter().next_back() {
              if max_req_sn > last_seq {
                warn!(
                  "ACKNACK from {:?} requests {:?} but I have only up to {:?}",
                  reader_proxy.remote_reader_guid,
                  an.reader_sn_state.iter().collect::<Vec<SequenceNumber>>(),
                  last_seq
                );
              }
            }
          }

          // if we cannot send more data, we are done.
          // This is to prevent empty "repair data" messages from being sent.
          if reader_proxy.all_acked_before > last_seq {
            reader_proxy.repair_mode = false;
          } else {
            reader_proxy.repair_mode = true; // TODO: Is this correct? Do we need to repair immediately?
            if repair_immediately {
              // Built-in writer: repair now (see note above), not via the timer.
              do_immediate_repair = true;
            } else {
              // set repair timer to fire
              // Note: `reader_proxy` holds a mutable borrow of `self`, so we
              // cannot call the `&self` helper here; access disjoint fields
              // directly instead.
              self.timed_event_timer.borrow_mut().set_timeout(
                self.nack_response_delay,
                DpTimerEvent::Writer {
                  entity_id: self.my_guid.entity_id,
                  event: TimedEvent::SendRepairData {
                    to_reader: reader_guid,
                  },
                },
              );
            }
          }
        } // if have reader_proxy

        // See if we need to respond by GAP message
        if let Some(reader_proxy) = self.readers.get(&reader_guid) {
          if !reader_proxy.get_pending_gap().is_empty() {
            let gap_message = MessageBuilder::new()
              .gap_msg(
                reader_proxy.get_pending_gap(),
                self.my_guid.entity_id,
                self.endianness,
                reader_guid,
              )
              .add_header_and_build(self.my_guid.prefix);
            self.send_control_to_readers(
              DeliveryMode::Unicast,
              gap_message,
              &mut std::iter::once(reader_proxy),
            );
          }
        }

        // Built-in writers repair synchronously (see note above): the missed
        // sample is retransmitted now, on this ACKNACK event, without waiting
        // for the (starvable) shared timer. `handle_repair_data_send` detaches
        // and re-inserts the reader proxy internally, so it must run after the
        // borrows above are released.
        if do_immediate_repair {
          self.handle_repair_data_send(reader_guid);
        }
      } // AckNack
      AckSubmessage::NackFrag(ref nackfrag) => {
        // NackFrag is negative acknowledgement only, i.e. requesting missing fragments.
        let reader_guid = GUID::new(reader_guid_prefix, nackfrag.reader_id);
        if let Some(reader_proxy) = self.lookup_reader_proxy_mut(reader_guid) {
          reader_proxy.mark_frags_requested(nackfrag.writer_sn, &nackfrag.fragment_number_state);
        }
        self.schedule_timed_event(
          self.nackfrag_response_delay,
          TimedEvent::SendRepairFrags {
            to_reader: reader_guid,
          },
        );
      }
    }

    // Acknowledgement frontier may have advanced: push it into the shared send
    // buffer so any back-pressured producer (or `wait_for_acknowledgments`
    // waiter) can make progress.
    self.refresh_acked_frontier();
  }

  // Recompute the reliable acknowledgement frontier (the smallest
  // `all_acked_before` over all matched reliable readers) and publish it to the
  // shared send buffer. Called whenever acknowledgements arrive or the set of
  // matched readers changes. `None` means there are no reliable readers, so the
  // writer is never back-pressured.
  fn refresh_acked_frontier(&self) {
    if self.like_stateless {
      // Stateless-like writer is BestEffort: never throttle, never wait.
      self.send_buffer.set_acked_frontier(None);
      return;
    }
    let frontier = self
      .readers
      .values()
      .filter(|rp| rp.qos().is_reliable())
      .map(RtpsReaderProxy::acked_up_to_before)
      .min();
    self.send_buffer.set_acked_frontier(frontier);
  }

  // Send out missing data

  fn handle_repair_data_send(&mut self, to_reader: GUID) {
    if self.like_stateless {
      warn!(
        "Not sending repair data in a stateless-like Writer, since it currently supports only \
         BestEffort behavior. topic={:?}",
        self.my_topic_name
      );
      return;
    }
    // Note: here we remove the reader from our reader map temporarily.
    // Then we can mutate both the reader and other fields in self.
    // Doing a .get_mut() on the reader map would make self immutable.
    if let Some(mut reader_proxy) = self.readers.remove(&to_reader) {
      // We use a worker function to ensure that afterwards we can insert the
      // reader_proxy back. This technique ensures that all return paths lead to
      // re-insertion.
      self.handle_repair_data_send_worker(&mut reader_proxy);
      // insert reader back
      if let Some(rp) = self
        .readers
        .insert(reader_proxy.remote_reader_guid, reader_proxy)
      {
        // This should really not happen.
        error!("Reader proxy was duplicated somehow??? {rp:?}");
      }
    }
  }

  fn handle_repair_frags_send(&mut self, to_reader: GUID) {
    if self.like_stateless {
      warn!(
        "Not sending repair frags in a stateless-like Writer, since it currently supports only \
         BestEffort behavior. topic={:?}",
        self.my_topic_name
      );
      return;
    }

    // see similar function above
    if let Some(mut reader_proxy) = self.readers.remove(&to_reader) {
      self.handle_repair_frags_send_worker(&mut reader_proxy);
      if let Some(rp) = self
        .readers
        .insert(reader_proxy.remote_reader_guid, reader_proxy)
      {
        // this is an internal logic error, or maybe out of memory
        error!("Reader proxy was duplicated somehow??? (frags) {rp:?}");
      }
    }
  }

  fn handle_repair_data_send_worker(&mut self, reader_proxy: &mut RtpsReaderProxy) {
    // Note: The reader_proxy is now removed from readers map
    let reader_guid = reader_proxy.remote_reader_guid;

    debug!(
      "Repair data send to {reader_guid:?} due to ACKNACK. ReaderProxy Unsent changes: {:?}",
      reader_proxy.unsent_changes_debug()
    );

    if let Some(unsent_sn) = reader_proxy.first_unsent_change() {
      // There are unsent changes.
      let mut no_longer_relevant: BTreeSet<SequenceNumber> = BTreeSet::new();
      let mut all_irrelevant_before = None;

      // If we have set the reader as pending GAP for the unsent sequence number,
      // just send a GAP message, not DATA.
      let pending_gaps = reader_proxy.get_pending_gap();

      // Check what we actually have in store
      let first_available = self.send_buffer.first_change_sequence_number();
      if unsent_sn < first_available {
        // Reader is requesting older than what we actually have. Notify that they are
        // gone.
        all_irrelevant_before = Some(first_available);
      }

      // If all_irrelevant_before is still None, then TopicCache has SNs that are
      // less than equal to the requested "unsent_sn". But might not have that exact
      // SN.
      if pending_gaps.contains(&unsent_sn) || all_irrelevant_before.is_some() {
        no_longer_relevant.extend(pending_gaps);
      } else {
        // Reader not pending gap on unsent_sn. Get the cache change from the send
        // buffer
        if let Some(cc) = self.send_buffer.get_by_sn(unsent_sn) {
          // The cache change was found. Send it to the reader
          let data_was_fragmented = self.send_cache_change(&cc, false, Some(reader_proxy));

          if data_was_fragmented {
            // Mark the reader as having requested all frags
            let (num_frags, _frag_size) =
              self.num_frags_and_frag_size(cc.data_value.payload_size());
            reader_proxy.mark_all_frags_requested(unsent_sn, num_frags);

            // Set a timer to send repair frags if needed
            self.schedule_timed_event(
              self.repairfrags_continue_delay,
              TimedEvent::SendRepairFrags {
                to_reader: reader_guid,
              },
            );
          }
          // mark as sent
          reader_proxy.mark_change_sent(unsent_sn);
        } else {
          // Did not find a cache change for the sequence number. Mark for GAP.
          no_longer_relevant.insert(unsent_sn);
          // Try to find a reason why and log about it
          if unsent_sn < first_available {
            info!(
              "Reader {:?} requested too old data {:?}. I have only from {:?}. Topic {:?}",
              reader_proxy, unsent_sn, first_available, self.my_topic_name
            );
          } else {
            // we are running out of excuses
            error!(
              "handle_repair_data_send_worker {:?} seq.number {:?} missing. first_change={:?}",
              self.my_guid, unsent_sn, first_available
            );
          }
        }
      }

      // Send a GAP if we marked a sequence number as no longer relevant
      if !no_longer_relevant.is_empty() || all_irrelevant_before.is_some() {
        let mut gap_msg = MessageBuilder::new().dst_submessage(self.endianness, reader_guid.prefix);
        if let Some(all_irrelevant_before) = all_irrelevant_before {
          gap_msg = gap_msg.gap_msg_before(
            all_irrelevant_before,
            self.entity_id(),
            self.endianness,
            reader_guid,
          );
          reader_proxy.remove_from_unsent_set_all_before(all_irrelevant_before);
        }
        if !no_longer_relevant.is_empty() {
          gap_msg = gap_msg.gap_msg(
            &no_longer_relevant,
            self.entity_id(),
            self.endianness,
            reader_guid,
          );
          no_longer_relevant
            .iter()
            .for_each(|sn| reader_proxy.mark_change_sent(*sn));
        }
        let gap_msg = gap_msg.add_header_and_build(self.my_guid.prefix);

        self.send_control_to_readers(
          DeliveryMode::Unicast,
          gap_msg,
          &mut std::iter::once(&*reader_proxy),
        );
      } // if sending GAP
    } else {
      // Unsent list is empty. Switch off repair mode.
      reader_proxy.repair_mode = false;
    }
  } // fn

  fn handle_repair_frags_send_worker(
    &mut self,
    reader_proxy: &mut RtpsReaderProxy, /* This is mutable proxy temporarily detached from the
                                         * set of reader proxies */
  ) {
    // Decide the (max) number of frags to be sent
    let max_send_count = 8;

    let reader_guid = reader_proxy.remote_reader_guid;

    // Get (an iterator to) frags requested but not yet sent
    // reader_proxy.
    // Iterate over frags to be sent
    for (seq_num, frag_num) in reader_proxy.frags_requested_iterator().take(max_send_count) {
      // Sanity check request
      // ^^^ TODO

      if let Some(cache_change) = self.send_buffer.get_by_sn(seq_num) {
        // If the data is meant for a single reader only, make sure it is the one we're
        // about to send frags to.
        if let Some(single_reader_guid) = cache_change.write_options.to_single_reader() {
          if single_reader_guid != reader_guid {
            error!(
              "We were asked to send datafrags meant for the reader {single_reader_guid:?} to a \
               different reader {reader_guid:?}. Not gonna happen."
            );
            return;
          }
        }

        // Generate datafrag message
        let mut message_builder = MessageBuilder::new();
        if let Some(src_ts) = cache_change.write_options.source_timestamp() {
          message_builder = message_builder.ts_msg(self.endianness, Some(src_ts));
        }

        let fragment_size: u32 = FRAGMENT_SIZE as u32;
        let data_size: u32 = cache_change.data_value.payload_size() as u32; // TODO: overflow check

        message_builder = message_builder.data_frag_msg(
          &cache_change,
          reader_guid.entity_id, // reader
          self.my_guid,          // writer
          frag_num,
          // Repair responds to specifically-NACKed fragment numbers (possibly
          // non-contiguous), so retransmit one fragment per submessage.
          1,
          fragment_size as u16, // TODO: overflow check
          data_size,
          self.endianness,
          self.security_plugins.as_ref(),
        );

        // Repair frags are a reliability action (response to a NACK_FRAG), so
        // they must actually reach the reader. Use the never-dropped `Control`
        // queue: dropping repair on WouldBlock livelocks under sustained send
        // congestion (reader re-NACKs, retransmit dropped again, forever).
        let _blocked = self.send_message_to_readers(
          DeliveryMode::Unicast,
          message_builder.add_header_and_build(self.my_guid.prefix),
          &mut std::iter::once(&*reader_proxy),
          TrafficClass::Control,
        );
      } else {
        error!(
          "handle_repair_frags_send_worker: {:?} missing from send buffer. topic={:?}",
          seq_num, self.my_topic_name
        );
        // TODO: Should we send a GAP message then?
      }

      reader_proxy.mark_frag_sent(seq_num, &frag_num);
    } // for
  } // fn

  /// Removes permanently cacheChanges from DDSCache.
  /// CacheChanges can be safely removed only if they are acked by all readers.
  /// (Reliable) Depth is QoS policy History depth.
  /// Returns SequenceNumbers of removed CacheChanges
  /// This is called repeatedly by handle_cache_cleaning action.
  fn remove_all_acked_changes_but_keep_depth(
    &mut self,
    depth: Option<usize>,
    resource_limit: usize,
  ) {
    let first_keeper = if !self.like_stateless {
      // Regular stateful writer behavior
      // All readers have acked up to this point (SequenceNumber)
      let acked_by_all_readers = self
        .readers
        .values()
        .map(RtpsReaderProxy::acked_up_to_before)
        .min()
        .unwrap_or_else(SequenceNumber::zero);
      // If all readers have acked all up to before 5, and depth is 5, we need
      // to keep samples 0..4, i.e. from acked_up_to_before - depth .
      let depth_keeper = if let Some(depth) = depth {
        max(
          acked_by_all_readers - SequenceNumber::from(depth),
          self.send_buffer.first_change_sequence_number(),
        )
      } else {
        // try to keep all
        self.send_buffer.first_change_sequence_number()
      };
      // Never evict samples that some matched (reliable) reader has not yet
      // acknowledged: clamp the keeper to at most the all-acked point so that
      // unacknowledged samples remain available for repair. The resource-limit
      // backstop below still bounds memory if a reader falls hopelessly behind.
      min(depth_keeper, acked_by_all_readers)
    } else {
      // Stateless-like writer currently supports only BestEffort behavior, so here we
      // make it explicit that it does not care about acked sequence numbers
      let depth = depth.unwrap_or(0);
      max(
        self.send_buffer.last_change_sequence_number() - SequenceNumber::from(depth),
        self.send_buffer.first_change_sequence_number(),
      )
    };
    // Memory-safety backstop: never retain more than `resource_limit` samples,
    // even if that forces eviction of still-unacknowledged data.
    let first_keeper = max(
      max(
        first_keeper,
        self.send_buffer.last_change_sequence_number() - SequenceNumber::from(resource_limit),
      ),
      SequenceNumber::zero(),
    );
    debug!(
      "WriterSendBuffer: cleaning before {first_keeper:?} topic={:?}",
      self.topic_name()
    );
    // actual cleaning
    self.send_buffer.remove_changes_before(first_keeper);
  }

  pub(crate) fn next_heartbeat_count(&self) -> i32 {
    self
      .heartbeat_message_counter
      .fetch_add(1, atomic::Ordering::SeqCst)
  }

  #[cfg(feature = "security")]
  fn security_encode(
    &self,
    message: Message,
    readers: &[&RtpsReaderProxy],
  ) -> SecurityResult<Message> {
    // If we have security plugins, use them, otherwise pass through
    if let Some(security_plugins_handle) = &self.security_plugins {
      // Get the source and destination GUIDs
      let source_guid = self.guid();
      let destination_guid_list: Vec<GUID> = readers
        .iter()
        .map(|reader_proxy| reader_proxy.remote_reader_guid)
        .collect();
      // Destructure
      let Message {
        header,
        submessages,
      } = message;

      // Encode submessages
      SecurityResult::<Vec<Vec<Submessage>>>::from_iter(submessages.iter().map(|submessage| {
        security_plugins_handle
          .get_plugins()
          .encode_datawriter_submessage(submessage.clone(), &source_guid, &destination_guid_list)
          // Convert each encoding output to a Vec of 1 or 3 submessages
          .map(Vec::from)
      }))
      // Flatten and convert back to Message
      .map(|encoded_submessages| Message {
        header,
        submessages: encoded_submessages.concat(),
      })
      // Encode message
      .and_then(|message| {
        // Convert GUIDs to GuidPrefixes
        let source_guid_prefix = source_guid.prefix;
        let destination_guid_prefix_list: Vec<GuidPrefix> = destination_guid_list
          .iter()
          .map(|guid| guid.prefix)
          .collect();
        // Encode message
        security_plugins_handle.get_plugins().encode_message(
          message,
          &source_guid_prefix,
          &destination_guid_prefix_list,
        )
      })
    } else {
      Ok(message)
    }
  }

  // nonblocking-transmit: `class` selects the queueing policy. `Control`
  // datagrams go through the never-dropped per-socket control queue; `Bulk`
  // datagrams are attempted non-blocking and the sockets that returned
  // WouldBlock are returned so the caller can stop and arm write readiness.
  // Returns the set of blocked sockets (always empty for `Control`).
  fn send_message_to_readers(
    &self,
    preferred_mode: DeliveryMode,
    message: Message,
    readers: &mut dyn Iterator<Item = &RtpsReaderProxy>,
    class: TrafficClass,
  ) -> BTreeSet<SocketId> {
    // Interface-aware transmit (see src/rtps/transmit_design.md): each reader
    // carries a pre-resolved `SendRoute`. When the route is known we emit a
    // single datagram per distinct destination (`RouteKey`), targeting one
    // interface for multicast. When the route is unknown/ambiguous we fall back
    // to the legacy path (send to every advertised locator on every interface)
    // so reachability is preserved.

    // Only the security path needs the readers materialized into a slice (to
    // pass to `security_encode`). In the default (non-security) build we iterate
    // the incoming iterator directly below, avoiding a per-sample Vec
    // allocation on the send hot path.
    #[cfg(feature = "security")]
    let readers = readers.collect::<Vec<_>>();

    let mut blocked: BTreeSet<SocketId> = BTreeSet::new();

    #[cfg(feature = "security")]
    let encoded = self.security_encode(message, &readers);
    #[cfg(not(feature = "security"))]
    let encoded: Result<Message, ()> = Ok(message);

    match encoded {
      Ok(message) => {
        let buffer = message.write_to_vec_fast(self.endianness).unwrap();

        // De-duplication of narrowed (interface-aware) sends across readers.
        let mut sent_routes: BTreeSet<RouteKey> = BTreeSet::new();
        // De-duplication of legacy (all-interface) sends across readers.
        let mut sent_legacy: BTreeSet<Locator> = BTreeSet::new();

        macro_rules! emit_multicast {
          ($mc:expr, $iface:expr) => {
            if sent_routes.insert(RouteKey::Multicast($mc, $iface)) {
              match class {
                TrafficClass::Control => self
                  .udp_sender
                  .send_to_multicast_locator_via(&buffer, &$mc, &$iface),
                TrafficClass::Bulk => blocked.extend(
                  self
                    .udp_sender
                    .try_send_to_multicast_locator_via(&buffer, &$mc, &$iface),
                ),
              }
            } else {
              trace!("Already sent to multicast {:?} via {:?}", $mc, $iface);
            }
          };
        }
        macro_rules! emit_unicast {
          ($uc:expr) => {
            if sent_routes.insert(RouteKey::Unicast($uc)) {
              match class {
                TrafficClass::Control => self.udp_sender.send_to_locator(&buffer, &$uc),
                TrafficClass::Bulk => {
                  blocked.extend(self.udp_sender.try_send_to_locator(&buffer, &$uc));
                }
              }
            } else {
              trace!("Already sent to unicast {:?}", $uc);
            }
          };
        }
        macro_rules! send_legacy {
          ($locs:expr) => {
            for loc in $locs.iter() {
              if sent_legacy.insert(*loc) {
                match class {
                  TrafficClass::Control => self.udp_sender.send_to_locator(&buffer, loc),
                  TrafficClass::Bulk => {
                    blocked.extend(self.udp_sender.try_send_to_locator(&buffer, loc));
                  }
                }
              } else {
                trace!("Already sent to {:?}", loc);
              }
            }
          };
        }

        for reader in readers {
          let route = reader.send_route();

          if route.fallback {
            // Unknown/ambiguous route: preserve reachability using the legacy
            // all-locators/all-interfaces path with the original precedence.
            match (
              preferred_mode,
              reader
                .unicast_locator_list
                .iter()
                .find(|l| Locator::is_udp(l)),
              reader
                .multicast_locator_list
                .iter()
                .find(|l| Locator::is_udp(l)),
            ) {
              (DeliveryMode::Multicast, _, Some(_)) => send_legacy!(reader.multicast_locator_list),
              (DeliveryMode::Unicast, Some(_), _) => send_legacy!(reader.unicast_locator_list),
              (_, _, Some(_)) => send_legacy!(reader.multicast_locator_list),
              (_, Some(_), _) => send_legacy!(reader.unicast_locator_list),
              (_, None, None) => warn!("send_message_to_readers: No locators for {reader:?}"),
            }
            continue;
          }

          // Narrowed route: reuse the multicast/unicast preference precedence.
          match (preferred_mode, route.multicast, route.unicast) {
            (DeliveryMode::Multicast, Some((mc, iface)), _) => emit_multicast!(mc, iface),
            (DeliveryMode::Unicast, _, Some(uc)) => emit_unicast!(uc),
            (_, _, Some(uc)) => emit_unicast!(uc),
            (_, Some((mc, iface)), _) => emit_multicast!(mc, iface),
            (_, None, None) => {
              warn!("send_message_to_readers: resolved route has no destination for {reader:?}");
            }
          }
        }

        // Fixed extra unicast destinations (SPDP localhost peers): send the same
        // datagram unconditionally, deduplicated against everything already sent.
        send_legacy!(self.extra_unicast_destinations);
      }
      Err(e) => error!("Failed to send message to readers. Encoding failed: {e:?}"),
    }
    blocked
  }

  // Kept for readability at call sites that fire a single control message and
  // do not care about back-pressure (heartbeats, GAPs, repair control).
  fn send_control_to_readers(
    &self,
    preferred_mode: DeliveryMode,
    message: Message,
    readers: &mut dyn Iterator<Item = &RtpsReaderProxy>,
  ) {
    let _ = self.send_message_to_readers(preferred_mode, message, readers, TrafficClass::Control);
  }

  #[allow(dead_code)] // symmetry with send_control_to_readers; reserved for future direct bulk sends
  fn send_bulk_to_readers(
    &self,
    preferred_mode: DeliveryMode,
    message: Message,
    readers: &mut dyn Iterator<Item = &RtpsReaderProxy>,
  ) -> BTreeSet<SocketId> {
    self.send_message_to_readers(preferred_mode, message, readers, TrafficClass::Bulk)
  }

  // Send status to DataWriter or however is listening
  fn send_status(&self, status: DataWriterStatus) {
    self
      .status_sender
      .try_send(status)
      .unwrap_or_else(|e| match e {
        TrySendError::Full(_) => (), // This is normal in case there is no receiver
        TrySendError::Disconnected(_) => {
          debug!("send_status - status receiver is disconnected");
        }
        TrySendError::Io(e) => {
          warn!("send_status - io error {e:?}");
        }
      });
  }

  /// Set the fixed unicast destinations every outgoing message is also sent to
  /// (in addition to matched readers), bypassing route selection. Used only for
  /// the built-in SPDP writer's "localhost SPDP peers". See
  /// [`Self::extra_unicast_destinations`].
  pub fn set_extra_unicast_destinations(&mut self, locators: Vec<Locator>) {
    self.extra_unicast_destinations = locators;
  }

  /// Enable/disable preferring a same-host peer's loopback locator during route
  /// selection. See the participant-builder `same_host_loopback` knob.
  pub fn set_prefer_loopback_same_host(&mut self, enabled: bool) {
    self.prefer_loopback_same_host = enabled;
  }

  pub fn update_reader_proxy(
    &mut self,
    reader_proxy: &RtpsReaderProxy,
    requested_qos: &QosPolicies,
  ) {
    debug!(
      "update_reader_proxy topic={:?} reader_proxy={reader_proxy:?}",
      self.my_topic_name
    );
    match self.qos_policies.compliance_failure_wrt(requested_qos) {
      // matched QoS
      None => {
        let new_reader = self.matched_reader_update(reader_proxy);
        // A (possibly new) reliable reader changes the acknowledgement frontier
        // and thus the back-pressure window.
        self.refresh_acked_frontier();
        if new_reader {
          self.matched_readers_count_total += 1;
          self.send_status(DataWriterStatus::PublicationMatched {
            // total: How many matches have been detected ever?
            total: CountWithChange::new(self.matched_readers_count_total, 1),
            // current: How many readers we are matched with?
            current: CountWithChange::new(self.readers.len() as i32, 1),
            reader: reader_proxy.remote_reader_guid,
          });
          self.send_participant_status(DomainParticipantStatusEvent::RemoteReaderMatched {
            local_writer: self.my_guid,
            remote_reader: reader_proxy.remote_reader_guid,
          });
          // Reliable: send an initial HEARTBEAT to the newly matched reader so it
          // can request the samples we already hold (repair/late-join). This runs
          // synchronously on the discovery-match event in the event loop, so it
          // does NOT depend on the periodic heartbeat timer - which is CPU-starved
          // under a flat-out user writer, leaving the timer to fire seldom or
          // never. Without this prompt, a reader that matches after our initial
          // send burst is never told what we have and never NACKs, so reliable
          // data (notably builtin SEDP DiscoveredWriterData) is never delivered
          // and the endpoints stay unmatched. Unicast to just the new reader.
          if self.is_reliable() && !self.like_stateless {
            let new_reader_guid = reader_proxy.remote_reader_guid;
            let first = self.send_buffer.first_change_sequence_number();
            let last = self.send_buffer.last_change_sequence_number();
            let hb_message = MessageBuilder::new()
              .ts_msg(self.endianness, Some(Timestamp::now()))
              .heartbeat_msg(
                self.entity_id(),
                first,
                last,
                self.next_heartbeat_count(),
                self.endianness,
                EntityId::UNKNOWN,
                false, // final_flag: require the reader to respond with ACKNACK
                false, // liveliness_flag
              )
              .add_header_and_build(self.my_guid.prefix);
            if let Some(rp) = self.readers.get(&new_reader_guid) {
              self.send_control_to_readers(
                DeliveryMode::Unicast,
                hb_message,
                &mut std::iter::once(rp),
              );
            }
          }
          info!(
            "Matched new remote reader on topic={:?} reader={:?}",
            self.topic_name(),
            reader_proxy.remote_reader_guid
          );
          debug!("Reader details: {:?}", reader_proxy);
        }
      }
      Some(bad_policy_id) => {
        // QoS not compliant :(
        warn!(
          "update_reader_proxy - QoS mismatch {:?} topic={:?}",
          bad_policy_id,
          self.topic_name()
        );
        info!(
          "Reader QoS={:?} Writer QoS={:?}",
          requested_qos, self.qos_policies
        );

        self.requested_incompatible_qos_count += 1;
        self.send_status(DataWriterStatus::OfferedIncompatibleQos {
          count: CountWithChange::new(self.requested_incompatible_qos_count, 1),
          last_policy_id: bad_policy_id,
          reader: reader_proxy.remote_reader_guid,
          requested_qos: Box::new(requested_qos.clone()),
          offered_qos: Box::new(self.qos_policies.clone()),
        });
        self.send_participant_status(DomainParticipantStatusEvent::RemoteReaderQosIncompatible {
          local_writer: self.my_guid,
          remote_reader: reader_proxy.remote_reader_guid,
          requested_qos: Box::new(requested_qos.clone()),
          offered_qos: Box::new(self.qos_policies.clone()),
        });
      }
    } // match
  }

  // Update the given reader proxy. Preserve data we are tracking.
  // return value: true = reader was new, false = reader was previously known
  fn matched_reader_update(&mut self, updated_reader_proxy: &RtpsReaderProxy) -> bool {
    let mut is_new = false;
    let is_volatile = self.qos().is_volatile(); // Get this in advance to work with the borrow checker
                                                // Capture the interface set once; resolution consults current observations.
    let multicast_ifaces = self.udp_sender.multicast_interfaces();
    let selector = DefaultRouteSelector::new(self.prefer_loopback_same_host);
    self
      .readers
      .entry(updated_reader_proxy.remote_reader_guid)
      .and_modify(|rp| {
        rp.update(updated_reader_proxy, &self.my_topic_name);
        // Locators may have changed; refresh the interface-aware send route and
        // the per-peer path-MTU budget.
        rp.resolve_send_route(
          &self.interface_observations.borrow(),
          &multicast_ifaces,
          &selector,
        );
        rp.resolve_path_mtu(&self.local_interfaces);
      })
      .or_insert_with(|| {
        is_new = true;
        let mut new_proxy = updated_reader_proxy.clone();
        // Ensure loopback stays in the gated bucket even for proxies that
        // arrive with it inline (e.g. the built-in get_builtin_reader_proxy path).
        new_proxy.normalize_loopback();
        if is_volatile {
          // With Durabilty::Volatile QoS we won't send the sequence numbers which existed
          // before matching with this reader. Therefore we set the reader as pending GAP
          // for all existing sequence numbers
          new_proxy.set_pending_gap_up_to(self.send_buffer.last_change_sequence_number());
        }
        new_proxy.resolve_send_route(
          &self.interface_observations.borrow(),
          &multicast_ifaces,
          &selector,
        );
        new_proxy.resolve_path_mtu(&self.local_interfaces);
        new_proxy
      });
    // A reader was added or its locators changed: refresh the writer-wide
    // minimum datagram budget used for packing.
    self.recompute_min_datagram_payload();
    is_new
  }

  /// Recompute [`min_datagram_payload`](Self::min_datagram_payload) as the
  /// minimum per-peer budget over all matched readers. The aggregated/packed
  /// datagram is one packet multicast to every reader, so it must fit the
  /// smallest path MTU. With no matched readers, fall back to the default.
  fn recompute_min_datagram_payload(&mut self) {
    self.min_datagram_payload = self
      .readers
      .values()
      .map(RtpsReaderProxy::max_datagram_payload)
      .min()
      .unwrap_or(FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE);
  }

  /// Refresh the [`SendRoute`](crate::rtps::transmit::SendRoute) of every
  /// matched reader belonging to `prefix`. Called when fresh interface
  /// observations for that participant may have arrived (e.g. periodic SPDP).
  pub fn recompute_routes_for(&mut self, prefix: GuidPrefix) {
    let multicast_ifaces = self.udp_sender.multicast_interfaces();
    let selector = DefaultRouteSelector::new(self.prefer_loopback_same_host);
    {
      let observations = self.interface_observations.borrow();
      for rp in self.readers.values_mut() {
        if rp.remote_reader_guid.prefix == prefix {
          rp.resolve_send_route(&observations, &multicast_ifaces, &selector);
          rp.resolve_path_mtu(&self.local_interfaces);
        }
      }
    }
    self.recompute_min_datagram_payload();
  }

  fn matched_reader_remove(&mut self, guid: GUID) -> Option<RtpsReaderProxy> {
    let removed = self.readers.remove(&guid);
    if let Some(ref removed_reader) = removed {
      info!(
        "Removed reader proxy. topic={:?} reader={:?}",
        self.topic_name(),
        removed_reader.remote_reader_guid,
      );
      debug!("Removed reader proxy details: {removed_reader:?}");
    }
    #[cfg(feature = "security")]
    if let Some(security_plugins_handle) = &self.security_plugins {
      security_plugins_handle
        .get_plugins()
        .unregister_remote_reader(&self.my_guid, &guid)
        .unwrap_or_else(|e| error!("{e}"));
    }
    removed
  }

  pub fn reader_lost(&mut self, guid: GUID) {
    if self.readers.contains_key(&guid) {
      info!(
        "reader_lost topic={:?} reader={:?}",
        self.topic_name(),
        guid
      );
      self.matched_reader_remove(guid);
      // Removing a reader may relax (raise) the writer-wide minimum budget.
      self.recompute_min_datagram_payload();
      // self.matched_readers_count_total -= 1; // this never decreases
      self.send_status(DataWriterStatus::PublicationMatched {
        total: CountWithChange::new(self.matched_readers_count_total, 0),
        current: CountWithChange::new(self.readers.len() as i32, -1),
        reader: guid,
      });
    }
    // A matched reader going away may complete a pending wait_for_acknowledgments
    // and may relax back-pressure: recompute the acknowledgement frontier.
    self.refresh_acked_frontier();
  }

  // Entire remote participant was lost.
  // Remove all remote readers belonging to it.
  pub fn participant_lost(&mut self, guid_prefix: GuidPrefix) {
    let lost_readers: Vec<GUID> = self
      .readers
      .range(guid_prefix.range())
      .map(|(g, _)| *g)
      .collect();
    for reader in lost_readers {
      self.reader_lost(reader);
    }
  }

  fn lookup_reader_proxy_mut(&mut self, guid: GUID) -> Option<&mut RtpsReaderProxy> {
    self.readers.get_mut(&guid)
  }

  pub fn topic_name(&self) -> &String {
    &self.my_topic_name
  }

  fn send_participant_status(&self, event: DomainParticipantStatusEvent) {
    self
      .participant_status_sender
      .try_send(event)
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));
  }

  // TODO
  // This is placeholder for not-yet-implemented feature.
  //
  // pub fn reset_offered_deadline_missed_status(&mut self) {
  //   self.offered_deadline_status.reset_change();
  // }
}

impl RTPSEntity for Writer {
  fn guid(&self) -> GUID {
    self.my_guid
  }
}

impl HasQoSPolicy for Writer {
  fn qos(&self) -> QosPolicies {
    self.qos_policies.clone()
  }
}

// Serialized overhead of one DATA submessage excluding its serialized payload
// and inline QoS. Conservative fixed fields only; underestimating inline QoS
// causes earlier fragmentation (safe), never datagram overflow.
const DATA_SUBMESSAGE_OVERHEAD: usize = 48;

// Serialized overhead of one DATAFRAG submessage excluding its payload:
// submessage header (4) + extraFlags (2) + octetsToInlineQos (2) + readerId (4)
// + writerId (4) + writerSN (8) + fragmentStartingNum (4) +
// fragmentsInSubmessage (2) + fragmentSize (2) + sampleSize (4) = 36 bytes.
// Inline QoS (e.g. related sample identity) would add more; ignoring it only
// risks a slight overestimate, which degrades to benign IP fragmentation rather
// than data loss.
const DATAFRAG_SUBMESSAGE_OVERHEAD: usize = 36;

// Adaptive packing: how many contiguous fragments (starting at 1-based `start`)
// to place in a single DATAFRAG submessage so the datagram stays within
// `budget` bytes. `header_len` is the datagram size already committed (RTPS
// header + optional INFO_TS/INFO_DST). The fragment *size* is constant; only
// the count adapts. Always returns at least 1 (we emit progress even if a lone
// fragment exceeds the budget, letting IP fragmentation handle the overflow).
fn frags_per_datafrag(
  header_len: usize,
  budget: usize,
  start: u32,
  num_frags: u32,
  fragment_size: u16,
  data_size: usize,
) -> u32 {
  let fragment_size = fragment_size as usize;
  let remaining_frags = num_frags - (start - 1);
  // Bytes available in this datagram for the DATAFRAG payload.
  let payload_cap = budget
    .saturating_sub(header_len)
    .saturating_sub(DATAFRAG_SUBMESSAGE_OVERHEAD);
  // Bytes from the first fragment of this run to the end of the sample.
  let start_byte = (start as usize - 1) * fragment_size;
  let bytes_to_end = data_size.saturating_sub(start_byte);

  let k = if payload_cap >= bytes_to_end {
    // The rest of the sample (including a shorter final fragment) fits.
    remaining_frags
  } else {
    // Only whole fragments fit; take as many as the budget allows (>= 1).
    ((payload_cap / fragment_size) as u32).max(1)
  };
  k.min(remaining_frags)
}

struct FragmentationIter<'a> {
  writer: &'a Writer,
  cache_change: &'a CacheChange,
  target_reader_opt: Option<&'a RtpsReaderProxy>,
  reader_entity_id: EntityId,
  send_heartbeat: bool,
  finished: bool,
  state: FragmentationIterState,
}

impl<'a> FragmentationIter<'a> {
  // nonblocking-transmit: build an iterator that resumes at `cursor`. `Fresh`
  // yields everything (leading GAP for a single reader, all DATAFRAGs, trailing
  // HEARTBEAT, or a single DATA for an unfragmented sample). `Frag(n)` skips the
  // GAP and earlier fragments and resumes at fragment `n`. `Heartbeat` yields
  // only the trailing HEARTBEAT.
  fn new_resume(
    writer: &'a Writer,
    cache_change: &'a CacheChange,
    target_reader_opt: Option<&'a RtpsReaderProxy>,
    send_heartbeat: bool,
    cursor: SampleCursor,
  ) -> Self {
    // The EntityId of the destination
    let reader_entity_id =
      target_reader_opt.map_or(EntityId::UNKNOWN, |p| p.remote_reader_guid.entity_id);

    let data_size = cache_change.data_value.payload_size();
    let fragmentation_needed =
      data_size > writer.max_unfragmented_serialized_payload(target_reader_opt);

    let state = if fragmentation_needed {
      let fragmented = match cursor {
        SampleCursor::Fresh => FragmentedState::TargetReader,
        SampleCursor::Frag(start) => {
          let (num_frags, fragment_size) = writer.num_frags_and_frag_size(data_size);
          FragmentedState::Fragments {
            next: u32::from(start),
            num_frags,
            fragment_size,
          }
        }
        SampleCursor::Heartbeat => FragmentedState::Heartbeat,
      };
      FragmentationIterState::Fragmented(fragmented, data_size)
    } else {
      FragmentationIterState::Unfragmented
    };

    Self {
      writer,
      cache_change,
      target_reader_opt,
      state,
      reader_entity_id,
      finished: false,
      send_heartbeat,
    }
  }

  fn fragmentation_needed(&self) -> bool {
    matches!(self.state, FragmentationIterState::Fragmented(..))
  }
}

enum FragmentationIterState {
  Fragmented(FragmentedState, usize),
  Unfragmented,
}

enum FragmentedState {
  TargetReader,
  // Adaptive-packing DATAFRAG cursor: the next 1-based fragment number to send,
  // the total fragment count, and the (constant) fragment size. Each `next()`
  // emits one datagram carrying a single DATAFRAG submessage that packs as many
  // contiguous fragments as the destination's path-MTU budget allows.
  Fragments {
    next: u32,
    num_frags: u32,
    fragment_size: u16,
  },
  Heartbeat,
}

impl<'a> Iterator for FragmentationIter<'a> {
  // nonblocking-transmit: each item carries the cursor to resume from should
  // this message be the one that a socket cannot accept.
  type Item = (SampleCursor, Message);
  fn next(&mut self) -> Option<Self::Item> {
    if self.finished {
      return None;
    }

    let cc = self.cache_change;
    let writer = self.writer;
    let target_reader_opt = self.target_reader_opt;
    let reader_entity_id = self.reader_entity_id;
    let send_heartbeat = self.send_heartbeat;

    match &mut self.state {
      FragmentationIterState::Fragmented(state, data_size) => {
        // fragmentation_needed: We need to send DATAFRAGs
        match state {
          FragmentedState::TargetReader => {
            let (num_frags, fragment_size) = writer.num_frags_and_frag_size(*data_size);
            *state = FragmentedState::Fragments {
              next: 1,
              num_frags,
              fragment_size,
            };

            // If sending to a single reader, add a GAP message with pending gaps if any
            if let Some(reader) = target_reader_opt {
              if !reader.get_pending_gap().is_empty() {
                let gap_msg = MessageBuilder::new()
                  .dst_submessage(writer.endianness, reader.remote_reader_guid.prefix)
                  .gap_msg(
                    reader.get_pending_gap(),
                    writer.entity_id(),
                    writer.endianness,
                    reader.remote_reader_guid,
                  )
                  .add_header_and_build(writer.my_guid.prefix);
                // Leading GAP: if it blocks, resume from Fresh (re-send GAP too).
                return Some((SampleCursor::Fresh, gap_msg));
              }
            }
            self.next()
          }
          FragmentedState::Fragments {
            next,
            num_frags,
            fragment_size,
          } => {
            if *next <= *num_frags {
              let start = *next;
              let fragment_size = *fragment_size;
              let num_frags = *num_frags;
              let data_size = *data_size;

              let mut message_builder = MessageBuilder::new(); // fresh builder

              if let Some(src_ts) = cc.write_options.source_timestamp() {
                // Add timestamp (applies to the DATAFRAG that follows).
                message_builder = message_builder.ts_msg(writer.endianness, Some(src_ts));
              }

              if let Some(reader) = target_reader_opt {
                // Add info_destination
                message_builder = message_builder
                  .dst_submessage(writer.endianness, reader.remote_reader_guid.prefix);
              }

              // Per-peer datagram budget: the destination reader's path-MTU
              // budget for a directed send, or the writer-wide minimum (over all
              // matched readers) for a multicast-to-all send.
              let budget = writer.datagram_budget(target_reader_opt);
              // How many contiguous fragments (starting at `start`) fit in one
              // DATAFRAG submessage within the remaining datagram budget. Fragment
              // *size* is constant (RTPS rule); only the *count* per submessage
              // adapts to the path MTU.
              let k = frags_per_datafrag(
                message_builder.len_serialized(),
                budget,
                start,
                num_frags,
                fragment_size,
                data_size,
              );

              message_builder = message_builder.data_frag_msg(
                cc,
                reader_entity_id, // reader
                writer.my_guid,
                FragmentNumber::new(start),
                k as u16,
                fragment_size,
                data_size.try_into().unwrap(),
                writer.endianness,
                writer.security_plugins.as_ref(),
              );

              *next = start + k;
              let datafrag_msg = message_builder.add_header_and_build(writer.my_guid.prefix);
              // If this datagram blocks, resume from its first fragment next time
              // (the same K is recomputed deterministically).
              return Some((SampleCursor::Frag(FragmentNumber::new(start)), datafrag_msg));
            }
            *state = FragmentedState::Heartbeat;
            self.next()
          }
          FragmentedState::Heartbeat => {
            self.finished = true;

            // Add HEARTBEAT message if needed
            if send_heartbeat && !writer.like_stateless {
              let final_flag = false; // false = request that readers acknowledge with ACKNACK.
              let liveliness_flag = false; // This is not a manual liveliness assertion (DDS API call), but side-effect of
                                           // writing new data.
              let hb_msg = MessageBuilder::new()
                .heartbeat_msg(
                  writer.entity_id(), // from Writer
                  writer.send_buffer.first_change_sequence_number(),
                  writer.send_buffer.last_change_sequence_number(),
                  writer.next_heartbeat_count(),
                  writer.endianness,
                  reader_entity_id, // to Reader
                  final_flag,
                  liveliness_flag,
                )
                .add_header_and_build(writer.my_guid.prefix);
              // Trailing HEARTBEAT: if it blocks, resume from Heartbeat only.
              return Some((SampleCursor::Heartbeat, hb_msg));
            }
            None
          }
        }
      }
      FragmentationIterState::Unfragmented => {
        // We can send DATA
        let mut message_builder = MessageBuilder::new();

        // If DataWriter sent us a source timestamp, then add that.
        // Timestamp has to go before Data to have effect on Data.
        if let Some(src_ts) = cc.write_options.source_timestamp() {
          message_builder = message_builder.ts_msg(writer.endianness, Some(src_ts));
        }

        if let Some(reader) = target_reader_opt {
          // Add info_destination
          message_builder =
            message_builder.dst_submessage(writer.endianness, reader.remote_reader_guid.prefix);

          // If the reader is pending GAPs on any sequence numbers, add a GAP
          if !reader.get_pending_gap().is_empty() {
            message_builder = message_builder.gap_msg(
              reader.get_pending_gap(),
              writer.entity_id(),
              writer.endianness,
              reader.remote_reader_guid,
            );
          }
        }

        // Add the DATA submessage
        message_builder = message_builder.data_msg(
          cc,
          reader_entity_id,
          writer.my_guid,
          writer.endianness,
          writer.security_plugins.as_ref(),
        );

        // Add HEARTBEAT if needed
        if send_heartbeat && !writer.like_stateless {
          let final_flag = false; // false = request that readers acknowledge with ACKNACK.
          let liveliness_flag = false; // This is not a manual liveliness assertion (DDS API call), but side-effect of
                                       // writing new data.
          message_builder = message_builder.heartbeat_msg(
            writer.entity_id(),
            writer.send_buffer.first_change_sequence_number(),
            writer.send_buffer.last_change_sequence_number(),
            writer.next_heartbeat_count(),
            writer.endianness,
            reader_entity_id, // to Reader
            final_flag,
            liveliness_flag,
          );
        }

        let data_message = message_builder.add_header_and_build(writer.my_guid.prefix);
        self.finished = true;
        // Unfragmented DATA (+HEARTBEAT): if it blocks, resume from Fresh.
        Some((SampleCursor::Fresh, data_message))
      }
    }
  }
}

// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::thread;

  use byteorder::LittleEndian;
  use log::info;

  use super::frags_per_datafrag;

  // At a 1500-byte-MTU budget, a two-fragment "1 KB" sample (its serialized form
  // is slightly over the 1024-byte fragment size) packs BOTH fragments into one
  // DATAFRAG submessage, so it goes out in a single datagram instead of two.
  #[test]
  fn small_mtu_packs_1k_sample_into_one_datafrag() {
    // header_len 20 (RTPS header only), budget 1452, fragment size 1024,
    // sample 1036 bytes => 2 fragments.
    assert_eq!(frags_per_datafrag(20, 1452, 1, 2, 1024, 1036), 2);
  }

  // A large sample at a 1500-byte MTU can only fit one 1024-byte fragment per
  // datagram (2 * 1024 would overflow), so K collapses to 1.
  #[test]
  fn small_mtu_large_sample_one_fragment_per_datagram() {
    assert_eq!(frags_per_datafrag(20, 1452, 1, 10, 1024, 10240), 1);
  }

  // A jumbo-frame budget packs several whole fragments per DATAFRAG.
  #[test]
  fn jumbo_mtu_packs_several_fragments() {
    // budget 8952 (9000 MTU - 48), header 20 => payload_cap 8900 => 8 * 1024.
    assert_eq!(frags_per_datafrag(20, 8952, 1, 10, 1024, 10240), 8);
  }

  // The final run (including the shorter tail fragment) is taken whole when it
  // fits.
  #[test]
  fn tail_run_includes_partial_last_fragment() {
    // Fragments 9 and 10 of a 10240-byte sample: 1024 + (partial) with a large
    // budget => both fit.
    assert_eq!(frags_per_datafrag(20, 100_000, 9, 10, 1024, 10240), 2);
  }

  // A pathologically small budget still emits at least one fragment (progress),
  // accepting benign IP fragmentation for the overflow.
  #[test]
  fn tiny_budget_still_emits_one_fragment() {
    assert_eq!(frags_per_datafrag(20, 100, 1, 5, 1024, 5120), 1);
  }

  // K never exceeds the number of fragments actually remaining.
  #[test]
  fn k_capped_by_remaining_fragments() {
    assert_eq!(frags_per_datafrag(20, 100_000, 1, 3, 1024, 2600), 3);
  }

  use crate::{
    dds::{
      participant::DomainParticipant, qos::QosPolicies, topic::TopicKind,
      with_key::datawriter::DataWriter,
    },
    serialization::CDRSerializerAdapter,
    test::random_data::*,
  };

  #[test]
  fn test_writer_receives_datawriter_cache_change_notifications() {
    let domain_participant = DomainParticipant::new(0).expect("Failed to create participant");
    let qos = QosPolicies::qos_none();
    let _default_dw_qos = QosPolicies::qos_none();

    let publisher = domain_participant
      .create_publisher(&qos)
      .expect("Failed to create publisher");
    let topic = domain_participant
      .create_topic(
        "Aasii".to_string(),
        "Huh?".to_string(),
        &qos,
        TopicKind::WithKey,
      )
      .expect("Failed to create topic");
    let data_writer: DataWriter<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>> =
      publisher
        .create_datawriter(&topic, None)
        .expect("Failed to create datawriter");

    let data = RandomData {
      a: 4,
      b: "Fobar".to_string(),
    };

    let data2 = RandomData {
      a: 2,
      b: "Fobar".to_string(),
    };

    let data3 = RandomData {
      a: 3,
      b: "Fobar".to_string(),
    };

    let write_result = data_writer.write(data, None);
    info!("writerResult:  {write_result:?}");

    data_writer
      .write(data2, None)
      .expect("Unable to write data");

    info!("writerResult:  {write_result:?}");
    let write_result = data_writer.write(data3, None);

    thread::sleep(std::time::Duration::from_millis(100));
    info!("writerResult:  {write_result:?}");
  }
}
