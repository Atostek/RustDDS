use std::{
  collections::BTreeMap,
  fmt, iter,
  rc::Rc,
  sync::{Arc, Mutex, MutexGuard},
  task::Waker,
  time::Duration as StdDuration,
};

use mio_06::Token;
use mio_extras::{channel as mio_channel, timer::Timer};
use log::{debug, error, info, trace, warn};
use enumflags2::BitFlags;
use speedy::{Endianness, Writable};

use crate::{
  dds::{
    ddsdata::DDSData,
    qos::{policy, HasQoSPolicy, QosPolicies},
    statusevents::{
      CountWithChange, DataReaderStatus, DomainParticipantStatusEvent, StatusChannelSender,
    },
    with_key::{
      datawriter::{WriteOptions, WriteOptionsBuilder},
      simpledatareader::ReaderCommand,
    },
  },
  messages::{
    header::Header,
    protocol_id::ProtocolId,
    protocol_version::ProtocolVersion,
    submessages::{
      elements::{
        inline_qos::InlineQos, parameter_list::ParameterList, serialized_payload::SerializedPayload,
      },
      submessages::*,
    },
    vendor_id::VendorId,
  },
  mio_source,
  network::udp_sender::UDPSender,
  rtps::{
    fragment_assembler::FragmentAssembler, message_receiver::MessageReceiverState,
    rtps_writer_proxy::RtpsWriterProxy, Message,
  },
  structure::{
    cache_change::{CacheChange, ChangeKind},
    dds_cache::TopicCache,
    duration::Duration,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    locator::Locator,
    sequence_number::{FragmentNumber, FragmentNumberSet, SequenceNumber, SequenceNumberSet},
    time::Timestamp,
  },
};
#[cfg(feature = "security")]
use super::Submessage;
#[cfg(feature = "security")]
use crate::security::{security_plugins::SecurityPluginsHandle, SecurityResult};
#[cfg(not(feature = "security"))]
use crate::no_security::SecurityPluginsHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimedEvent {
  DeadlineMissedCheck,
}

// Some pieces necessary to construct a reader.
// These can be sent between threads, whereas a Reader cannot.
pub(crate) struct ReaderIngredients {
  pub guid: GUID,
  pub notification_sender: mio_channel::SyncSender<()>,
  pub status_sender: StatusChannelSender<DataReaderStatus>,
  pub topic_name: String,
  pub(crate) topic_cache_handle: Arc<Mutex<TopicCache>>, /* A handle to the topic cache in DDS
                                                          * cache */
  pub(crate) like_stateless: bool, // Usually false (see like_stateless attribute of Reader)
  pub qos_policy: QosPolicies,
  pub data_reader_command_receiver: mio_channel::Receiver<ReaderCommand>,
  pub(crate) data_reader_waker: Arc<Mutex<Option<Waker>>>,
  pub(crate) poll_event_sender: mio_source::PollEventSender,

  pub(crate) security_plugins: Option<SecurityPluginsHandle>,
}

impl ReaderIngredients {
  pub fn alt_entity_token(&self) -> Token {
    self.guid.entity_id.as_alt_token()
  }
}

impl fmt::Debug for ReaderIngredients {
  // Need manual implementation, because channels cannot be Debug formatted.
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Reader")
      .field("my_guid", &self.guid)
      .field("topic_name", &self.topic_name)
      .field("qos_policy", &self.qos_policy)
      .finish()
  }
}

pub(crate) struct Reader {
  // Should the instant be sent?
  notification_sender: mio_channel::SyncSender<()>,
  status_sender: StatusChannelSender<DataReaderStatus>,
  udp_sender: Rc<UDPSender>,

  // By default, this reader is a StatefulReader (see RTPS spec section 8.4.12)
  // If like_stateless is true, then the reader mimics the behavior of a StatelessReader
  // This means for example that the reader does not keep track of matched remote writers.
  // This behavior is needed only for a single built-in discovery topic of Secure DDS
  // (topic DCPSParticipantStatelessMessage)
  like_stateless: bool,
  // Reliability Qos. Note that a StatelessReader cannot be Reliable (RTPS Spec: Section 8.4.11.2)
  reliability: policy::Reliability,
  // Reader stores a pointer to a mutex on the topic cache
  topic_cache: Arc<Mutex<TopicCache>>,

  #[cfg(test)]
  seqnum_instant_map: BTreeMap<SequenceNumber, Timestamp>,

  topic_name: String,
  qos_policy: QosPolicies,

  my_guid: GUID,

  heartbeat_response_delay: StdDuration,

  // TODO: Implement (use) this
  #[allow(dead_code)]
  heartbeat_suppression_duration: StdDuration,

  received_heartbeat_count: i32,

  fragment_assemblers: BTreeMap<GUID, FragmentAssembler>,
  last_fragment_garbage_collect: Timestamp,
  matched_writers: BTreeMap<GUID, RtpsWriterProxy>,
  writer_match_count_total: i32, // total count, never decreases

  requested_deadline_missed_count: i32,
  offered_incompatible_qos_count: i32,

  pub(crate) timed_event_timer: Timer<TimedEvent>,
  pub(crate) data_reader_command_receiver: mio_channel::Receiver<ReaderCommand>,
  data_reader_waker: Arc<Mutex<Option<Waker>>>,
  poll_event_sender: mio_source::PollEventSender,

  participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,

  #[allow(dead_code)] // to avoid warning if no security feature
  security_plugins: Option<SecurityPluginsHandle>,
}

// If we are assembling a fragment, but it does not receive any updates
// for this time, the AssemblyBuffer is just dropped.
const FRAGMENT_ASSEMBLY_TIMEOUT: Duration = Duration::from_secs(10);
// minimum interval (max frequency) of AssemblyBuffer GC
const MIN_FRAGMENT_GC_INTERVAL: Duration = Duration::from_secs(2);

impl Reader {
  pub(crate) fn new(
    i: ReaderIngredients,
    udp_sender: Rc<UDPSender>,
    timed_event_timer: Timer<TimedEvent>,
    participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
  ) -> Self {
    // Verify that the topic cache corresponds to the topic of the Reader
    let topic_cache_name = i.topic_cache_handle.lock().unwrap().topic_name();
    if i.topic_name != topic_cache_name {
      panic!(
        "Topic name = {} and topic cache name = {} not equal when creating a Reader",
        i.topic_name, topic_cache_name
      );
    }

    // If reader should be stateless, only BestEffort QoS is supported
    if i.like_stateless && i.qos_policy.is_reliable() {
      panic!("Attempted to create a stateless Reader with other than BestEffort reliability");
    }

    Self {
      notification_sender: i.notification_sender,
      status_sender: i.status_sender,
      udp_sender,
      like_stateless: i.like_stateless,
      reliability: i
        .qos_policy
        .reliability() // use qos specification
        .unwrap_or(policy::Reliability::BestEffort), // or default to BestEffort
      topic_cache: i.topic_cache_handle,
      topic_name: i.topic_name,
      qos_policy: i.qos_policy,

      #[cfg(test)]
      seqnum_instant_map: BTreeMap::new(),
      my_guid: i.guid,

      heartbeat_response_delay: StdDuration::new(0, 500_000_000), // 0,5sec
      heartbeat_suppression_duration: StdDuration::new(0, 0),
      received_heartbeat_count: 0,
      fragment_assemblers: BTreeMap::new(),
      last_fragment_garbage_collect: Timestamp::now(),
      matched_writers: BTreeMap::new(),
      writer_match_count_total: 0,
      requested_deadline_missed_count: 0,
      offered_incompatible_qos_count: 0,
      timed_event_timer,
      data_reader_command_receiver: i.data_reader_command_receiver,
      data_reader_waker: i.data_reader_waker,
      poll_event_sender: i.poll_event_sender,
      participant_status_sender,

      security_plugins: i.security_plugins,
    }
  }
  // TODO: check if it's necessary to implement different handlers for discovery
  // and user messages

  /// To know when token represents a reader we should look entity attribute
  /// kind
  pub fn entity_token(&self) -> Token {
    self.guid().entity_id.as_token()
  }

  pub fn set_requested_deadline_check_timer(&mut self) {
    if let Some(deadline) = self.qos_policy.deadline {
      debug!(
        "GUID={:?} set_requested_deadline_check_timer: {:?}",
        self.my_guid,
        deadline.0.to_std()
      );
      self
        .timed_event_timer
        .set_timeout(deadline.0.to_std(), TimedEvent::DeadlineMissedCheck);
    } else {
      trace!(
        "GUID={:?} - no deadline policy - do not set set_requested_deadline_check_timer",
        self.my_guid
      );
    }
  }

  pub fn send_status_change(&self, change: DataReaderStatus) {
    match self.status_sender.try_send(change) {
      Ok(()) => (), // expected result
      Err(mio_channel::TrySendError::Full(_)) => {
        trace!("Reader cannot send new status changes, datareader is full.");
        // It is perfectly normal to fail due to full channel, because
        // no-one is required to be listening to these.
      }
      Err(mio_channel::TrySendError::Disconnected(_)) => {
        // If we get here, our DataReader has died. The Reader should now dispose
        // itself. Or possibly it has lost the receiver object, which is sort of
        // sloppy, but does not necessarily mean the end of the world.
        // TODO: Implement Reader disposal.
        info!("send_status_change - cannot send status, DataReader Disconnected.");
      }
      Err(mio_channel::TrySendError::Io(e)) => {
        error!("send_status_change - cannot send status: {e:?}");
      }
    }
  }

  fn send_participant_status(&self, event: DomainParticipantStatusEvent) {
    self
      .participant_status_sender
      .try_send(event)
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));
  }

  // The deadline that the DataReader was expecting through its QosPolicy
  // DEADLINE was not respected for a specific instance
  // if statusChange is returned it should be send to DataReader
  // this calculation should be repeated every self.qos_policy.deadline
  fn calculate_if_requested_deadline_is_missed(&mut self) -> Vec<DataReaderStatus> {
    debug!("calculate_if_requested_deadline_is_missed");

    let deadline_duration = match self.qos_policy.deadline {
      None => return vec![],
      Some(policy::Deadline(deadline_duration)) => deadline_duration,
    };

    let mut changes: Vec<DataReaderStatus> = vec![];
    let now = Timestamp::now();
    for writer_proxy in self.matched_writers.values_mut() {
      if let Some(last_change) = writer_proxy.last_change_timestamp() {
        let since_last = now.duration_since(last_change);
        // if time singe last received message is greater than deadline increase status
        // and return notification.
        trace!("Comparing deadlines: {since_last:?} - {deadline_duration:?}");
        if since_last > deadline_duration {
          debug!("Deadline missed: {since_last:?} - {deadline_duration:?}");
          self.requested_deadline_missed_count += 1;
          changes.push(DataReaderStatus::RequestedDeadlineMissed {
            count: CountWithChange::start_from(self.requested_deadline_missed_count, 1),
          });
        }
      } else {
        // no messages received ever so deadline must be missed.
        // TODO: But what if the Reader or WriterProxy was just created?
        self.requested_deadline_missed_count += 1;
        changes.push(DataReaderStatus::RequestedDeadlineMissed {
          count: CountWithChange::start_from(self.requested_deadline_missed_count, 1),
        });
      }
    } // for
    changes
  } // fn

  pub fn handle_timed_event(&mut self) {
    while let Some(e) = self.timed_event_timer.poll() {
      match e {
        TimedEvent::DeadlineMissedCheck => {
          self.handle_requested_deadline_event();
          self.set_requested_deadline_check_timer(); // re-prime timer
        }
      }
    }
  }

  pub fn process_command(&mut self) {
    trace!("process_command {:?}", self.my_guid);
    loop {
      use std::sync::mpsc::TryRecvError;
      match self.data_reader_command_receiver.try_recv() {
        Ok(ReaderCommand::ResetRequestedDeadlineStatus) => {
          warn!("RESET_REQUESTED_DEADLINE_STATUS not implemented!");
          // TODO: This should be implemented.
        }
        // Disconnected is normal when terminating
        Err(TryRecvError::Disconnected) => {
          trace!("DataReader disconnected");
          break;
        }
        Err(TryRecvError::Empty) => {
          warn!("There was no command. Spurious command event??");
          break;
        }
      }
    }
  }

  fn handle_requested_deadline_event(&mut self) {
    debug!("handle_requested_deadline_event");
    for missed_deadline in self.calculate_if_requested_deadline_is_missed() {
      self.send_status_change(missed_deadline);
    }
  }

  // TODO Used for test/debugging purposes
  #[cfg(test)]
  pub fn history_cache_change_data(&self, sequence_number: SequenceNumber) -> Option<DDSData> {
    let topic_cache = self.acquire_the_topic_cache_guard();
    let cc = self
      .seqnum_instant_map
      .get(&sequence_number)
      .and_then(|i| topic_cache.get_change(i));

    debug!("history cache !!!! {cc:?}");

    cc.map(|cc| cc.data_value.clone())
  }

  // TODO Used for test/debugging purposes
  #[cfg(test)]
  pub fn history_cache_sequence_start_and_end_numbers(&self) -> Vec<SequenceNumber> {
    if self.seqnum_instant_map.is_empty() {
      vec![]
    } else {
      let start = self.seqnum_instant_map.iter().min().unwrap().0;
      let end = self.seqnum_instant_map.iter().max().unwrap().0;
      vec![*start, *end]
    }
  }

  // updates or adds a new writer proxy, doesn't touch changes
  pub fn update_writer_proxy(&mut self, proxy: RtpsWriterProxy, offered_qos: &QosPolicies) {
    if self.like_stateless {
      debug!(
        "Attempted to update writer proxy for stateless reader. Ignoring. topic={:?}",
        self.topic_name
      );
      return;
    }

    debug!("update_writer_proxy topic={:?}", self.topic_name);
    let writer = proxy.remote_writer_guid;

    match offered_qos.compliance_failure_wrt(&self.qos_policy) {
      None => {
        // success, update or insert
        let count_change = self.matched_writer_update(proxy);
        if count_change > 0 {
          self.writer_match_count_total += count_change;
          self.send_status_change(DataReaderStatus::SubscriptionMatched {
            total: CountWithChange::new(self.writer_match_count_total, count_change),
            current: CountWithChange::new(self.matched_writers.len() as i32, count_change),
            writer,
          });
          self.send_participant_status(DomainParticipantStatusEvent::RemoteWriterMatched {
            local_reader: self.my_guid,
            remote_writer: writer,
          });

          info!(
            "Matched new remote writer on topic={:?} writer={:?}",
            self.topic_name, writer
          );
        }
      }
      Some(bad_policy_id) => {
        // no QoS match.
        self.offered_incompatible_qos_count += 1;
        self.send_status_change(DataReaderStatus::RequestedIncompatibleQos {
          count: CountWithChange::new(self.offered_incompatible_qos_count, 1),
          last_policy_id: bad_policy_id,
          writer,
          requested_qos: Box::new(self.qos_policy.clone()),
          offered_qos: Box::new(offered_qos.clone()),
        });
        self.send_participant_status(DomainParticipantStatusEvent::RemoteWriterQosIncompatible {
          local_reader: self.my_guid,
          remote_writer: writer,
          requested_qos: Box::new(self.qos_policy.clone()),
          offered_qos: Box::new(offered_qos.clone()),
        });

        warn!("update_writer_proxy - QoS mismatch {bad_policy_id:?}");
        info!(
          "update_writer_proxy - QoS mismatch: topic={:?} requested={:?}  offered={:?}",
          self.topic_name, &self.qos_policy, offered_qos
        );
      }
    }
  }

  // return value counts how many new proxies were added
  fn matched_writer_update(&mut self, proxy: RtpsWriterProxy) -> i32 {
    if let Some(op) = self.matched_writer_mut(proxy.remote_writer_guid) {
      op.update_contents(proxy);
      0
    } else {
      self.matched_writers.insert(proxy.remote_writer_guid, proxy);
      1
    }
  }

  pub fn remove_writer_proxy(&mut self, writer_guid: GUID) {
    if self.matched_writers.contains_key(&writer_guid) {
      self.matched_writers.remove(&writer_guid);
      #[cfg(feature = "security")]
      if let Some(security_plugins_handle) = &self.security_plugins {
        security_plugins_handle
          .get_plugins()
          .unregister_remote_writer(&self.my_guid, &writer_guid)
          .unwrap_or_else(|e| error!("{e}"));
      }
      self.send_status_change(DataReaderStatus::SubscriptionMatched {
        total: CountWithChange::new(self.writer_match_count_total, 0),
        current: CountWithChange::new(self.matched_writers.len() as i32, -1),
        writer: writer_guid,
      });
    }
  }

  // Entire remote participant was lost.
  // Remove all remote writers belonging to it.
  pub fn participant_lost(&mut self, guid_prefix: GuidPrefix) {
    let lost_writers: Vec<GUID> = self
      .matched_writers
      .range(guid_prefix.range())
      .map(|(g, _)| *g)
      .collect();
    for writer in lost_writers {
      self.remove_writer_proxy(writer);
    }
  }

  pub fn contains_writer(&self, entity_id: EntityId) -> bool {
    if !self.like_stateless {
      self
        .matched_writers
        .iter()
        .any(|(&g, _)| g.entity_id == entity_id)
    } else {
      // Making it explicit: stateless reader does not contain any writers
      false
    }
  }

  #[cfg(test)]
  pub(crate) fn matched_writer_add(
    &mut self,
    remote_writer_guid: GUID,
    remote_group_entity_id: EntityId,
    unicast_locator_list: Vec<Locator>,
    multicast_locator_list: Vec<Locator>,
    qos: &QosPolicies,
  ) {
    let proxy = RtpsWriterProxy::new(
      remote_writer_guid,
      unicast_locator_list,
      multicast_locator_list,
      remote_group_entity_id,
    );
    self.update_writer_proxy(proxy, qos);
  }

  fn matched_writer(&self, remote_writer_guid: GUID) -> Option<&RtpsWriterProxy> {
    self.matched_writers.get(&remote_writer_guid)
  }

  fn matched_writer_mut(&mut self, remote_writer_guid: GUID) -> Option<&mut RtpsWriterProxy> {
    self.matched_writers.get_mut(&remote_writer_guid)
  }

  // handles regular data message and updates history cache
  pub fn handle_data_msg(
    &mut self,
    data: Data,
    data_flags: BitFlags<DATA_Flags>,
    mr_state: &MessageReceiverState,
  ) {
    // trace!("handle_data_msg entry");
    let receive_timestamp = Timestamp::now();

    // parse write_options out of the message
    let mut write_options_b = WriteOptionsBuilder::new();
    // Check if we have s source timestamp
    if let Some(source_timestamp) = mr_state.source_timestamp {
      write_options_b = write_options_b.source_timestamp(source_timestamp);
    }
    // Check if the message specifies a related_sample_identity
    let representation_identifier = DATA_Flags::cdr_representation_identifier(data_flags);
    if let Some(related_sample_identity) =
      data.inline_qos.as_ref().and_then(|inline_qos_parameters| {
        InlineQos::related_sample_identity(inline_qos_parameters, representation_identifier)
          .unwrap_or_else(|e| {
            error!("Deserializing related_sample_identity: {:?}", &e);
            None
          })
      })
    {
      write_options_b = write_options_b.related_sample_identity(related_sample_identity);
    }

    let writer_guid = GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, data.writer_id);
    let writer_seq_num = data.writer_sn; // for borrow checker

    match self.data_to_dds_data(data, data_flags) {
      Ok(dds_data) => self.process_received_data(
        dds_data,
        receive_timestamp,
        write_options_b.build(),
        writer_guid,
        writer_seq_num,
      ),
      Err(e) => debug!("Parsing DATA to DDSData failed: {e}"),
    }
  }

  pub fn handle_datafrag_msg(
    &mut self,
    datafrag: &DataFrag,
    datafrag_flags: BitFlags<DATAFRAG_Flags>,
    mr_state: &MessageReceiverState,
  ) {
    let writer_guid = GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, datafrag.writer_id);
    let seq_num = datafrag.writer_sn;
    let receive_timestamp = Timestamp::now();
    //trace!("DATAFRAG received topic={:?}", self.topic_name);

    // check if this submessage is expired already
    // TODO: Maybe this check is in the wrong place altogether? It should be
    // done when Datareader fetches data for the application.
    if let (Some(source_timestamp), Some(lifespan)) =
      (mr_state.source_timestamp, self.qos().lifespan)
    {
      let elapsed = receive_timestamp.duration_since(source_timestamp);
      if lifespan.duration < elapsed {
        info!(
          "DataFrag {:?} from {:?} lifespan exceeded. duration={:?} elapsed={:?}",
          seq_num, writer_guid, lifespan.duration, elapsed
        );
        return;
      }
    }

    // parse write_options out of the message
    // TODO: This is almost duplicate code from DATA processing
    let mut write_options_b = WriteOptionsBuilder::new();
    // Check if we have a source timestamp
    if let Some(source_timestamp) = mr_state.source_timestamp {
      write_options_b = write_options_b.source_timestamp(source_timestamp);
    }
    // Check if the message specifies a related_sample_identity
    let representation_identifier = DATAFRAG_Flags::cdr_representation_identifier(datafrag_flags);
    if let Some(related_sample_identity) =
      datafrag
        .inline_qos
        .as_ref()
        .and_then(|inline_qos_parameters| {
          InlineQos::related_sample_identity(inline_qos_parameters, representation_identifier)
            .unwrap_or_else(|e| {
              error!("Deserializing related_sample_identity: {:?}", &e);
              None
            })
        })
    {
      write_options_b = write_options_b.related_sample_identity(related_sample_identity);
    }

    // Feed to fragment assembler ...
    let writer_seq_num = datafrag.writer_sn; // for borrow checker
    let completed_dds_data = self
      .fragment_assembler_mutable(writer_guid, datafrag.fragment_size)
      .new_datafrag(datafrag, datafrag_flags);

    // ... and continue processing, if data was completed.
    if let Some(dds_data) = completed_dds_data {
      // Source timestamp (if any) will be the timestamp of the last fragment (that
      // completes the sample).
      self.process_received_data(
        dds_data,
        receive_timestamp,
        write_options_b.build(),
        writer_guid,
        writer_seq_num,
      );
    } else {
      self.garbage_collect_fragments();
    }
  }

  fn fragment_assembler_mutable(
    &mut self,
    writer_guid: GUID,
    frag_size: u16,
  ) -> &mut FragmentAssembler {
    self
      .fragment_assemblers
      .entry(writer_guid)
      .or_insert_with(|| FragmentAssembler::new(frag_size))
  }

  fn garbage_collect_fragments(&mut self) {
    // If GC time/packet limit has been exceeded, iterate through
    // fragment assemblers and discard those assembly buffers whose
    // creation / modification timestamps look like it is no longer receiving
    // data and can therefore be discarded.
    let now = Timestamp::now();
    if now - self.last_fragment_garbage_collect > MIN_FRAGMENT_GC_INTERVAL {
      self.last_fragment_garbage_collect = now;

      let expire_before = now - FRAGMENT_ASSEMBLY_TIMEOUT;

      self
        .fragment_assemblers
        .iter_mut()
        .for_each(|(writer, fa)| {
          debug!("AssemblyBuffer GC writer {writer:?}");
          fa.garbage_collect_before(expire_before);
        });
    } else {
      trace!("Not yet AssemblyBuffer GC time.");
    }
  }

  fn missing_frags_for(
    &self,
    writer_guid: GUID,
    seq: SequenceNumber,
  ) -> Box<dyn '_ + Iterator<Item = FragmentNumber>> {
    self.fragment_assemblers.get(&writer_guid).map_or_else(
      || Box::new(iter::empty()) as Box<dyn Iterator<Item = FragmentNumber>>,
      |fa| fa.missing_frags_for(seq),
    )
  }

  fn is_frag_partially_received(&self, writer_guid: GUID, seq: SequenceNumber) -> bool {
    self
      .fragment_assemblers
      .get(&writer_guid)
      .is_some_and(|fa| fa.is_partially_received(seq))
  }

  // common parts of processing DATA or a completed DATAFRAG (when all frags are
  // received)
  fn process_received_data(
    &mut self,
    dds_data: DDSData,
    receive_timestamp: Timestamp,
    write_options: WriteOptions,
    writer_guid: GUID,
    writer_sn: SequenceNumber,
  ) {
    trace!(
      "handle_data_msg from {:?} seq={:?} topic={:?} reliability={:?} stateless={:?}",
      &writer_guid,
      writer_sn,
      self.topic_name,
      self.reliability,
      self.like_stateless,
    );
    if !self.like_stateless {
      let my_entity_id = self.my_guid.entity_id; // to please borrow checker
      if let Some(writer_proxy) = self.matched_writer_mut(writer_guid) {
        if writer_proxy.should_ignore_change(writer_sn) {
          // change already present
          trace!("handle_data_msg already have this seq={writer_sn:?}");
          if my_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_READER {
            debug!("Accepting duplicate message to participant reader.");
            // This is an attempted workaround to eProsima FastRTPS not
            // incrementing sequence numbers. (eProsima shapes demo 2.1.0 from
            // 2021)
          } else {
            return;
          }
        }
        // Add the change and get the instant
        writer_proxy.received_changes_add(writer_sn, receive_timestamp);
      } else {
        // no writer proxy found
        debug!(
          "handle_data_msg in stateful Reader {:?} has no writer proxy for {:?} topic={:?}",
          my_entity_id, writer_guid, self.topic_name,
        );
        // This is normal if the DATA was broadcast, but it was from another topic.
        // We just ignore the data in such a case
        // ... unless it is Discovery traffic.
        if writer_guid.entity_id.entity_kind.is_user_defined() {
          return;
        }
      }
    } else {
      // stateless reader: nothing to do before making cache change
    }

    self.make_cache_change(
      dds_data,
      receive_timestamp,
      write_options,
      writer_guid,
      writer_sn,
    );

    // Add to own track-keeping data structure
    #[cfg(test)]
    self.seqnum_instant_map.insert(writer_sn, receive_timestamp);

    self.notify_cache_change();
  }

  fn data_to_dds_data(
    &self,
    data: Data,
    data_flags: BitFlags<DATA_Flags>,
  ) -> Result<DDSData, String> {
    let representation_identifier = DATA_Flags::cdr_representation_identifier(data_flags);

    match (
      data.serialized_payload,
      data_flags.contains(DATA_Flags::Data),
      data_flags.contains(DATA_Flags::Key),
    ) {
      (Some(serialized_payload), true, false) => {
        // data
        Ok(DDSData::new(
          SerializedPayload::from_bytes(&serialized_payload).map_err(|e| format!("{e:?}"))?,
        ))
      }

      (Some(serialized_payload), false, true) => {
        // key
        Ok(DDSData::new_disposed_by_key(
          Self::deduce_change_kind(&data.inline_qos, false, representation_identifier),
          SerializedPayload::from_bytes(&serialized_payload).map_err(|e| format!("{e:?}"))?,
        ))
      }

      (None, false, false) => {
        // no data, no key. Maybe there is inline QoS?
        // At least we should find key hash, or we do not know WTF the writer is talking
        // about
        let key_hash = if let Some(h) = data.inline_qos.as_ref().and_then(|inline_qos_parameters| {
          InlineQos::key_hash(inline_qos_parameters).unwrap_or_else(|e| {
            error!("Deserializing key_hash: {:?}", &e);
            None
          })
        }) {
          Ok(h)
        } else {
          info!("Received DATA that has no payload and no key_hash inline QoS - discarding");
          // Note: This case is normal when handling coherent sets.
          // The coherent set end marker is sent as DATA with no payload and not key, only
          // Inline QoS.
          Err("DATA with no contents".to_string())
        }?;
        // now, let's try to determine what is the dispose reason
        let change_kind =
          Self::deduce_change_kind(&data.inline_qos, false, representation_identifier);
        info!(
          "status change by Inline QoS: topic={:?} change={:?}",
          self.topic_name, change_kind
        );
        Ok(DDSData::new_disposed_by_key_hash(change_kind, key_hash))
      }

      (Some(_), true, true) => {
        // payload cannot be both key and data.
        // RTPS Spec 9.4.5.3.1 Flags in the Submessage Header says
        // "D=1 and K=1 is an invalid combination in this version of the protocol."
        warn!("Got DATA that claims to be both data and key - discarding.");
        Err("Ambiguous data/key received.".to_string())
      }

      (Some(_), false, false) => {
        // data but no data? - this should not be possible
        warn!("make_cache_change - Flags says no data or key, but got payload!");
        Err("DATA message has mystery contents".to_string())
      }
      (None, true, _) | (None, _, true) => {
        warn!("make_cache_change - Where is my SerializedPayload?");
        Err("DATA message contents missing".to_string())
      }
    }
  }

  // helper to work with mutable proxy
  fn with_mutable_writer_proxy<F, U>(&mut self, writer_guid: GUID, worker: F) -> Option<U>
  where
    F: FnOnce(&mut Self, &mut RtpsWriterProxy) -> U,
  {
    match self.matched_writers.remove(&writer_guid) {
      None => {
        error!("Writer proxy {writer_guid:?} not found");
        None
      }
      Some(mut wp) => {
        let res = worker(self, &mut wp);
        let x = self.matched_writers.insert(writer_guid, wp); // re-insert
        if x.is_some() {
          panic!("with_mutable_writer_proxy: Worker inserted writer proxy behind my back!")
        }
        Some(res)
      }
    }
  }

  // Returns if responding with ACKNACK?
  // TODO: Return value seems to go unused in callers.
  // ...except in test cases, but not sure if this is strictly necessary to have.
  pub fn handle_heartbeat_msg(
    &mut self,
    heartbeat: &Heartbeat,
    final_flag_set: bool,
    mr_state: &MessageReceiverState,
  ) -> bool {
    let writer_guid =
      GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, heartbeat.writer_id);

    if self.reliability == policy::Reliability::BestEffort || self.like_stateless {
      debug!(
        "HEARTBEAT from {:?}, but this Reader is BestEffort or stateless. Ignoring. topic={:?} \
         reader={:?}",
        writer_guid, self.topic_name, self.my_guid
      );
      // BestEffort Reader reacts only to DATA and GAP
      // See RTPS Spec Section "8.4.11 RTPS StatelessReader Behavior":
      // Figure 8.23 - Behavior of the Best-Effort StatefulReader with respect to each
      // matched Writer and
      // Figure 8.22 - Behavior of the Best-Effort StatelessReader
      return false;
    }

    if !self.matched_writers.contains_key(&writer_guid) {
      debug!(
        "HEARTBEAT from {:?}, but no writer proxy available. topic={:?} reader={:?}",
        writer_guid, self.topic_name, self.my_guid
      );
      return false;
    }
    // sanity check
    if heartbeat.first_sn < SequenceNumber::default() {
      warn!(
        "Writer {:?} advertised SequenceNumbers from {:?} to {:?}!",
        writer_guid, heartbeat.first_sn, heartbeat.last_sn
      );
    }

    self
      .with_mutable_writer_proxy(writer_guid, |this, writer_proxy| {
        // Note: This is worker closure. Use `this` instead of `self`.

        if heartbeat.count <= writer_proxy.received_heartbeat_count {
          // This heartbeat was already seen an processed.
          return false;
        }
        writer_proxy.received_heartbeat_count = heartbeat.count;

        // remove changes until first_sn.
        writer_proxy.irrelevant_changes_up_to(heartbeat.first_sn);

        let marker_moved = this
          .acquire_the_topic_cache_guard()
          .mark_reliably_received_before(writer_guid, writer_proxy.all_ackable_before());
        if marker_moved {
          this.notify_cache_change();
        }

        // let received_before = writer_proxy.all_ackable_before();
        let reader_id = this.entity_id();

        // See if ACKNACK is needed, and generate one.
        let missing_seqnums = writer_proxy.missing_seqnums(heartbeat.first_sn, heartbeat.last_sn);

        // Interpretation of final flag in RTPS spec
        // 8.4.2.3.1 Readers must respond eventually after receiving a HEARTBEAT with
        // final flag not set
        //
        // Upon receiving a HEARTBEAT Message with final flag not set, the Reader must
        // respond with an ACKNACK Message. The ACKNACK Message may acknowledge
        // having received all the data samples or may indicate that some data
        // samples are missing. The response may be delayed to avoid message storms.

        if !missing_seqnums.is_empty() || !final_flag_set {
          let mut partially_received = Vec::new();
          // report of what we have.
          // We claim to have received all SNs before "base" and produce a set of missing
          // sequence numbers that are >= base.
          let reader_sn_state = match missing_seqnums.first() {
            Some(&first_missing) => {
              // Here we assume missing_seqnums are returned in order.
              // Limit the set to maximum that can be sent in acknack submessage.

              SequenceNumberSet::from_base_and_set(
                first_missing,
                &missing_seqnums
                  .iter()
                  .copied()
                  .take_while(|sn| sn < &(first_missing + SequenceNumber::new(256)))
                  .filter(|sn| {
                    if this.is_frag_partially_received(writer_guid, *sn) {
                      partially_received.push(*sn);
                      false
                    } else {
                      true
                    }
                  })
                  .collect(),
              )
            }

            // Nothing missing. Report that we have all we have.
            None => SequenceNumberSet::new_empty(writer_proxy.all_ackable_before()),
          };

          let response_ack_nack = AckNack {
            reader_id,
            writer_id: heartbeat.writer_id,
            reader_sn_state,
            count: writer_proxy.next_ack_nack_sequence_number(),
          };

          // Sanity check
          //
          // Wrong. This sanity check is invalid. The condition
          // ack_base > heartbeat.last_sn + 1
          // May be legitimately true, if there are some changes available, and a GAP
          // after that. E.g. HEARTBEAT 1..8 and GAP 9..10. Then acknack_base == 11
          // and 11 > 8 + 1.
          //
          //
          // if response_ack_nack.reader_sn_state.base() > heartbeat.last_sn +
          // SequenceNumber::new(1) {   error!(
          //     "OOPS! AckNack sanity check tripped: HEARTBEAT = {:?} ACKNACK = {:?}
          // missing_seqnums = {:?} all_ackable_before = {:?} writer={:?}",
          //     &heartbeat, &response_ack_nack, missing_seqnums,
          // writer_proxy.all_ackable_before(), writer_guid,   );
          // }

          // The acknack can be sent now or later. The rest of the RTPS message
          // needs to be constructed. p. 48
          let acknack_flags = BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Endianness)
            | BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Final);

          let nackfrag_flags = BitFlags::<NACKFRAG_Flags>::from_flag(NACKFRAG_Flags::Endianness);

          // send NackFrags, if any
          let mut nackfrags = Vec::new();
          for sn in partially_received {
            let count = writer_proxy.next_ack_nack_sequence_number();
            let mut missing_frags = this.missing_frags_for(writer_guid, sn);
            let first_missing = missing_frags.next();
            if let Some(first) = first_missing {
              let missing_frags_set = iter::once(first).chain(missing_frags).collect(); // "undo" the .next() above
              let nf = NackFrag {
                reader_id,
                writer_id: writer_proxy.remote_writer_guid.entity_id,
                writer_sn: sn,
                fragment_number_state: FragmentNumberSet::from_base_and_set(
                  first,
                  &missing_frags_set,
                ),
                count,
              };
              nackfrags.push(nf);
            } else {
              error!("The dog ate my missing fragments.");
              // Really, this should not happen, as we are above checking
              // that this SN is really partially (and not fully) received.
            }
          }

          // Decide where should we send a reply, i.e. ACKNACK
          let reply_locators = match mr_state.unicast_reply_locator_list {
            [] | [Locator::Invalid] => &writer_proxy.unicast_locator_list,
            //TODO: What is writer_proxy has an empty list?
            others => others,
          };

          if !nackfrags.is_empty() {
            this.send_nackfrags_to(
              nackfrag_flags,
              nackfrags,
              InfoDestination {
                guid_prefix: mr_state.source_guid_prefix,
              },
              reply_locators,
              writer_guid,
            );
          }

          this.send_acknack_to(
            acknack_flags,
            response_ack_nack,
            InfoDestination {
              guid_prefix: mr_state.source_guid_prefix,
            },
            reply_locators,
            writer_guid,
          );

          return true;
        }

        false
      }) // worker fn
      .unwrap_or(false) // default false: no writer_proxy -> no acknack
  } // fn

  pub fn handle_gap_msg(&mut self, gap: &Gap, mr_state: &MessageReceiverState) {
    // ATM all things related to groups is ignored. TODO?

    let writer_guid = GUID::new_with_prefix_and_id(mr_state.source_guid_prefix, gap.writer_id);

    if self.like_stateless {
      debug!(
        "GAP from {:?}, but reader is stateless. Ignoring. topic={:?} reader={:?}",
        writer_guid, self.topic_name, self.my_guid
      );
      return;
    }
    let all_ackable_before;
    {
      let writer_proxy = if let Some(wp) = self.matched_writer_mut(writer_guid) {
        wp
      } else {
        info!(
          "GAP from {:?}, but no writer proxy available. topic={:?} reader={:?}",
          writer_guid, self.topic_name, self.my_guid
        );
        return;
      };

      // Check validity of the GAP message (Section 8.3.8.4.3)
      if gap.gap_start <= SequenceNumber::new(0) {
        debug!(
          "Invalid GAP from {:?}: gap_start={:?} is zero or negative. topic={:?} reader={:?}",
          writer_guid, gap.gap_start, self.topic_name, self.my_guid
        );
        return;
      }
      if gap.gap_list.base() <= SequenceNumber::new(0) {
        debug!(
          "Invalid GAP from {:?}: minimum of gap_list (={:?}) is zero or negative. topic={:?} \
           reader={:?}",
          writer_guid,
          gap.gap_list.base(),
          self.topic_name,
          self.my_guid
        );
        return;
      }
      // TODO: check that maximum(gap_list) - minimum(gap_list) < 256 ?

      // Irrelevant sequence numbers communicated in the Gap message are
      // composed of two groups:
      //   1. All sequence numbers in the range gapStart <= sequence_number <
      // gapList.base
      writer_proxy.irrelevant_changes_range(gap.gap_start, gap.gap_list.base());

      //   2. All the sequence numbers that appear explicitly listed in the gapList.
      //      Note that gapList.base may or may not be included in gapList; its
      //      inclusion is determined by the bitmap, as with the other sequence
      //      numbers
      for seq_num in gap.gap_list.iter() {
        writer_proxy.set_irrelevant_change(seq_num);
      }
      all_ackable_before = writer_proxy.all_ackable_before();
    }

    // Get the topic cache and mark progress
    let marker_moved = self
      .acquire_the_topic_cache_guard()
      .mark_reliably_received_before(writer_guid, all_ackable_before);

    // Receiving a GAP could make a Reliable stream.
    // E.g. we had #2, but were missing #1. Now GAP says that #1 does not exist.
    // Then a Reliable Datareader
    if marker_moved {
      self.notify_cache_change();
    }
    // able to move forward, i.e. hand over data to application, if
    // we now know that nothing is missng from the past.

    // TODO: If receiving GAP actually moved the reliably received mark forward
    // in the Topic Cache, then we should generate a SAMPLE_LOST status event
    // from our Datareader (DDS Spec Section 2.2.4.1)
    //
    // If the the GAP message contained filteredCount (RTPS spec v2.5 Table
    // 8.43), then some of the not-available messages should not be treated
    // as "lost" but "filtered".
  }

  pub fn handle_heartbeatfrag_msg(
    &mut self,
    heartbeatfrag: &HeartbeatFrag,
    _mr_state: &MessageReceiverState,
  ) {
    info!(
      "HeartbeatFrag handling not implemented. topic={:?}   {:?}",
      self.topic_name, heartbeatfrag
    );
  }

  // This is used to determine exact change kind in case we do not get a data
  // payload in DATA submessage
  fn deduce_change_kind(
    inline_qos: &Option<ParameterList>,
    no_writers: bool,
    representation_identifier: RepresentationIdentifier,
  ) -> ChangeKind {
    match inline_qos.as_ref().and_then(|inline_qos_parameters| {
      InlineQos::status_info(inline_qos_parameters, representation_identifier).map_or_else(
        |e| {
          error!("Deserializing status_info: {:?}", &e);
          None
        },
        Some,
      )
    }) {
      Some(si) => si.change_kind(), // get from inline QoS
      // TODO: What if si.change_kind() gives ALIVE ??
      None => {
        if no_writers {
          ChangeKind::NotAliveUnregistered
        } else {
          ChangeKind::NotAliveDisposed
        } // TODO: Is this reasonable default?
      }
    }
  }

  // Convert DATA submessage into a CacheChange and update history cache
  fn make_cache_change(
    &mut self,
    data: DDSData,
    receive_timestamp: Timestamp,
    write_options: WriteOptions,
    writer_guid: GUID,
    writer_sn: SequenceNumber,
  ) {
    let cache_change = CacheChange::new(writer_guid, writer_sn, write_options, data);

    // Get the topic cache
    let mut tc = self.acquire_the_topic_cache_guard();

    tc.add_change(&receive_timestamp, cache_change);
    // Mark seqnums as received if not behaving statelessly
    if !self.like_stateless {
      self.matched_writer(writer_guid).map(|wp| {
        tc.mark_reliably_received_before(writer_guid, wp.all_ackable_before());
        // Here we do not need to notify waiting DataReader, because
        // the upper call level from here does it.
      });
    }
  }

  // notifies DataReaders (or any listeners that history cache has changed for
  // this reader) likely use of mio channel
  pub fn notify_cache_change(&mut self) {
    // async notify mechanism
    self
      .data_reader_waker
      .lock()
      .unwrap() // TODO: unwrap
      .take() // Take to nullify the reference
      .map(|w| w.wake_by_ref()); // If Some, call wake_by_ref

    // mio-0.8 notify
    self.poll_event_sender.send();

    // mio-0.6 notify
    match self.notification_sender.try_send(()) {
      Ok(()) => (),
      Err(mio_channel::TrySendError::Full(_)) => (),
      // This is harmless. There is a notification in already.
      Err(mio_channel::TrySendError::Disconnected(_)) => {
        // If we get here, our DataReader has died. The Reader should now
        // dispose itself. TODO: Implement Reader disposal.
      }
      Err(mio_channel::TrySendError::Io(_)) => {
        // TODO: What does this mean? Can we ever get here?
      }
    }
  }

  #[cfg(not(feature = "security"))]
  fn encode_and_send(
    &self,
    message: Message,
    _destination_guid: GUID,
    dst_locator_list: &[Locator],
  ) {
    let bytes = message
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .unwrap(); //TODO!
    let _dummy = message; // consume it to avoid clippy warning
    self
      .udp_sender
      .send_to_locator_list(&bytes, dst_locator_list);
  }

  #[cfg(feature = "security")]
  fn encode_and_send(
    &self,
    message: Message,
    destination_guid: GUID,
    dst_locator_list: &[Locator],
  ) {
    match self.security_encode(message, destination_guid) {
      Ok(message) => {
        let bytes = message
          .write_to_vec_with_ctx(Endianness::LittleEndian)
          .unwrap(); //TODO!!
        self
          .udp_sender
          .send_to_locator_list(&bytes, dst_locator_list);
      }
      Err(e) => error!("Failed to send message to writers. Encoding failed: {e:?}"),
    }
  }

  #[cfg(feature = "security")]
  fn security_encode(&self, message: Message, destination_guid: GUID) -> SecurityResult<Message> {
    // If we have security plugins, use them, otherwise pass through
    if let Some(security_plugins_handle) = &self.security_plugins {
      // Get the source GUID
      let source_guid = self.guid();
      // Destructure
      let Message {
        header,
        submessages,
      } = message;

      // Encode submessages
      SecurityResult::<Vec<Vec<Submessage>>>::from_iter(submessages.iter().map(|submessage| {
        security_plugins_handle
          .get_plugins()
          .encode_datareader_submessage(submessage.clone(), &source_guid, &[destination_guid])
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
        let destination_guid_prefix = destination_guid.prefix;
        // Encode message
        security_plugins_handle.get_plugins().encode_message(
          message,
          &source_guid_prefix,
          &[destination_guid_prefix],
        )
      })
    } else {
      Ok(message)
    }
  }

  fn send_acknack_to(
    &self,
    flags: BitFlags<ACKNACK_Flags>,
    acknack: AckNack,
    info_dst: InfoDestination,
    dst_locator_list: &[Locator],
    destination_guid: GUID,
  ) {
    let infodst_flags =
      BitFlags::<INFODESTINATION_Flags>::from_flag(INFODESTINATION_Flags::Endianness);

    let mut message = Message::new(Header {
      protocol_id: ProtocolId::default(),
      protocol_version: ProtocolVersion::THIS_IMPLEMENTATION,
      vendor_id: VendorId::THIS_IMPLEMENTATION,
      guid_prefix: self.my_guid.prefix,
    });

    message.add_submessage(info_dst.create_submessage(infodst_flags));

    message.add_submessage(acknack.create_submessage(flags));

    self.encode_and_send(message, destination_guid, dst_locator_list);
  }

  fn send_nackfrags_to(
    &self,
    flags: BitFlags<NACKFRAG_Flags>,
    nackfrags: Vec<NackFrag>,
    info_dst: InfoDestination,
    dst_locator_list: &[Locator],
    destination_guid: GUID,
  ) {
    let infodst_flags =
      BitFlags::<INFODESTINATION_Flags>::from_flag(INFODESTINATION_Flags::Endianness);

    let mut message = Message::new(Header {
      protocol_id: ProtocolId::default(),
      protocol_version: ProtocolVersion::THIS_IMPLEMENTATION,
      vendor_id: VendorId::THIS_IMPLEMENTATION,
      guid_prefix: self.my_guid.prefix,
    });

    message.add_submessage(info_dst.create_submessage(infodst_flags));

    for nf in nackfrags {
      message.add_submessage(nf.create_submessage(flags));
    }

    self.encode_and_send(message, destination_guid, dst_locator_list);
  }

  pub fn send_preemptive_acknacks(&mut self) {
    if self.like_stateless {
      info!(
        "Attempted to send pre-emptive acknacks in a stateless Reader, which does not support \
         them. Ignoring. topic={:?}",
        self.topic_name
      );
      return;
    }

    let flags = BitFlags::<ACKNACK_Flags>::from_flag(ACKNACK_Flags::Endianness);
    // Do not set final flag --> we are requesting immediate heartbeat from writers.

    // Detach the writer proxy set. This is a way to avoid multiple &mut self
    let mut writer_proxies = std::mem::take(&mut self.matched_writers);

    let reader_id = self.entity_id();
    for (_, writer_proxy) in writer_proxies
      .iter_mut()
      .filter(|(_, p)| p.no_changes_received())
    {
      let acknack_count = writer_proxy.next_ack_nack_sequence_number();
      let RtpsWriterProxy {
        remote_writer_guid,
        unicast_locator_list,
        ..
      } = writer_proxy;
      self.send_acknack_to(
        flags,
        AckNack {
          reader_id,
          writer_id: remote_writer_guid.entity_id,
          reader_sn_state: SequenceNumberSet::new_empty(SequenceNumber::new(1)),
          count: acknack_count,
        },
        InfoDestination {
          guid_prefix: remote_writer_guid.prefix,
        },
        unicast_locator_list,
        *remote_writer_guid,
      );
    }
    // put writer proxies back
    self.matched_writers = writer_proxies;
  }

  pub fn topic_name(&self) -> &String {
    &self.topic_name
  }

  fn acquire_the_topic_cache_guard(&self) -> MutexGuard<'_, TopicCache> {
    self.topic_cache.lock().unwrap_or_else(|e| {
      panic!(
        "The topic cache of topic {} is poisoned. Error: {}",
        &self.topic_name, e
      )
    })
  }
} // impl Reader

impl HasQoSPolicy for Reader {
  fn qos(&self) -> QosPolicies {
    self.qos_policy.clone()
  }
}

impl RTPSEntity for Reader {
  fn guid(&self) -> GUID {
    self.my_guid
  }
}

impl fmt::Debug for Reader {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Reader")
      .field("notification_sender, dds_cache", &"can't print".to_string())
      .field("topic_name", &self.topic_name)
      .field("my_guid", &self.my_guid)
      .field("heartbeat_response_delay", &self.heartbeat_response_delay)
      .field("received_heartbeat_count", &self.received_heartbeat_count)
      .finish()
  }
}

#[cfg(test)]
mod tests {
  use std::sync::RwLock;

  use crate::{
    dds::{qos::policy::Reliability, statusevents::sync_status_channel, typedesc::TypeDesc},
    structure::{dds_cache::DDSCache, guid::EntityKind},
    QosPolicyBuilder,
  };
  use super::*;

  #[test]
  fn reader_sends_notification_when_receiving_data() {
    // 1. Create a reader
    // Create the DDS cache and a topic
    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    let topic_name = "test_name";
    let qos_policy = QosPolicies::qos_none();

    let topic_cache_handle = dds_cache.write().unwrap().add_new_topic(
      topic_name.to_string(),
      TypeDesc::new("test_type".to_string()),
      &qos_policy,
    );

    // Create notification mechanisms
    // mio-0.6 channel:
    let (notification_sender, notification_receiver) = mio_channel::sync_channel::<()>(100);
    // mio-0.8 event source and sender:
    let (_notification_event_source, notification_event_sender) =
      mio_source::make_poll_channel().unwrap();
    // async notification waker
    let data_reader_waker = Arc::new(Mutex::new(None));

    // Create status channel
    let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    // Create reader command channel
    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    // Then finally create the reader
    let reader_guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    let reader_ing = ReaderIngredients {
      guid: reader_guid,
      notification_sender,
      status_sender,
      topic_name: topic_name.to_string(),
      topic_cache_handle,
      like_stateless: false,
      qos_policy,
      data_reader_command_receiver: reader_command_receiver,
      data_reader_waker,
      poll_event_sender: notification_event_sender,
      security_plugins: None,
    };
    let mut reader = Reader::new(
      reader_ing,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
      participant_status_sender,
    );

    // 2. Add info of a matched writer to the reader
    let writer_guid = GUID::dummy_test_guid(EntityKind::WRITER_NO_KEY_USER_DEFINED);

    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.to_vec(),
      mr_state.multicast_reply_locator_list.to_vec(),
      &QosPolicies::qos_none(),
    );

    // 3. Create data that the matched writer supposedly sent to the reader
    let data = Data {
      reader_id: reader_guid.entity_id,
      writer_id: writer_guid.entity_id,
      ..Data::default()
    };
    let data_flags = BitFlags::<DATA_Flags>::from_flag(DATA_Flags::Data);

    // 4. Feed the data for the reader to handle
    reader.handle_data_msg(data, data_flags, &mr_state);

    // 5. Verify that the reader sends a notification about the new data
    assert!(
      notification_receiver.try_recv().is_ok(),
      "Reader did not send a notification through the mio-0.6 channel"
    );
    // TODO: Should the other notification mechanisms (mio-0.8 & async) be also
    // checked?
  }

  #[test]
  fn reader_sends_data_to_topic_cache() {
    // 1. Create a reader
    // Create the DDS cache and a topic
    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    let topic_name = "test_name";
    let qos_policy = QosPolicies::qos_none();

    let topic_cache_handle = dds_cache.write().unwrap().add_new_topic(
      topic_name.to_string(),
      TypeDesc::new("test_type".to_string()),
      &qos_policy,
    );

    // Create mechanisms for notifications, statuses & commands
    let (notification_sender, _notification_receiver) = mio_channel::sync_channel::<()>(100);
    let (_notification_event_source, notification_event_sender) =
      mio_source::make_poll_channel().unwrap();
    let data_reader_waker = Arc::new(Mutex::new(None));

    let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    // Then create the reader
    let reader_guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    let reader_ing = ReaderIngredients {
      guid: reader_guid,
      notification_sender,
      status_sender,
      topic_name: topic_name.to_string(),
      topic_cache_handle: topic_cache_handle.clone(),
      like_stateless: false,
      qos_policy,
      data_reader_command_receiver: reader_command_receiver,
      data_reader_waker,
      poll_event_sender: notification_event_sender,
      security_plugins: None,
    };
    let mut reader = Reader::new(
      reader_ing,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
      participant_status_sender,
    );

    // 2. Add info of a matched writer to the reader
    let writer_guid = GUID::dummy_test_guid(EntityKind::WRITER_NO_KEY_USER_DEFINED);

    let source_timestamp = Timestamp::INVALID;
    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      source_timestamp: Some(source_timestamp),
      ..Default::default()
    };

    reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.to_vec(),
      mr_state.multicast_reply_locator_list.to_vec(),
      &QosPolicies::qos_none(),
    );

    // 3. Create data that the matched writer supposedly sent to the reader
    let data = Data {
      reader_id: reader_guid.entity_id,
      writer_id: writer_guid.entity_id,
      ..Data::default()
    };
    let data_flags = BitFlags::<DATA_Flags>::from_flag(DATA_Flags::Data);
    let sequence_num = data.writer_sn;

    // 4. Feed the data for the reader to handle
    reader.handle_data_msg(data.clone(), data_flags, &mr_state);

    // 5. Verify that the reader sent the data to the topic cache
    let topic_cache = topic_cache_handle.lock().unwrap();

    let cc_from_cache = topic_cache
      .get_change(reader.seqnum_instant_map.get(&sequence_num).unwrap())
      .expect("No cache change in topic cache");

    // 6. Verify that the content of the cache change is as expected
    // Construct a cache change with the expected content
    let dds_data = DDSData::new(data.unwrap_serialized_payload());
    let cc_locally_built = CacheChange::new(
      writer_guid,
      sequence_num,
      WriteOptions::from(Some(source_timestamp)),
      dds_data,
    );

    assert_eq!(
      cc_from_cache, &cc_locally_built,
      "The content of the cache change in the topic cache not as expected"
    );
  }

  #[test]
  fn reader_handles_heartbeats() {
    // 1. Create a reader for a topic with Reliable QoS
    // Create the DDS cache and the topic
    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    let topic_name = "test_name";
    let reliable_qos = QosPolicyBuilder::new()
      .reliability(Reliability::Reliable {
        max_blocking_time: Duration::from_millis(100),
      })
      .build();

    let topic_cache_handle = dds_cache.write().unwrap().add_new_topic(
      topic_name.to_string(),
      TypeDesc::new("test_type".to_string()),
      &reliable_qos,
    );

    // Create mechanisms for notifications, statuses & commands
    let (notification_sender, _notification_receiver) = mio_channel::sync_channel::<()>(100);
    let (_notification_event_source, notification_event_sender) =
      mio_source::make_poll_channel().unwrap();
    let data_reader_waker = Arc::new(Mutex::new(None));

    let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    // Then create the reader
    let reader_guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    let reader_ing = ReaderIngredients {
      guid: reader_guid,
      notification_sender,
      status_sender,
      topic_name: topic_name.to_string(),
      topic_cache_handle,
      like_stateless: false,
      qos_policy: reliable_qos.clone(),
      data_reader_command_receiver: reader_command_receiver,
      data_reader_waker,
      poll_event_sender: notification_event_sender,
      security_plugins: None,
    };
    let mut reader = Reader::new(
      reader_ing,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
      participant_status_sender,
    );

    // 2. Add info of a matched writer to the reader
    let writer_guid = GUID::dummy_test_guid(EntityKind::WRITER_NO_KEY_USER_DEFINED);

    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.to_vec(),
      mr_state.multicast_reply_locator_list.to_vec(),
      &reliable_qos,
    );

    // 3. Send an initial heartbeat from the new writer, reader should not respond
    // with acknack first_sn: 1, last_sn: 0 to indicate no samples available
    let hb_new = Heartbeat {
      reader_id: reader.entity_id(),
      writer_id: writer_guid.entity_id,
      first_sn: SequenceNumber::new(1),
      last_sn: SequenceNumber::new(0),
      count: 1,
    };
    assert!(!reader.handle_heartbeat_msg(&hb_new, true, &mr_state)); // should be false, no ack

    // 4. Send the first proper heartbeat, reader should respond with acknack
    let hb_one = Heartbeat {
      reader_id: reader.entity_id(),
      writer_id: writer_guid.entity_id,
      first_sn: SequenceNumber::new(1), // Only one in writers cache
      last_sn: SequenceNumber::new(1),
      count: 2,
    };
    assert!(reader.handle_heartbeat_msg(&hb_one, false, &mr_state)); // Should send an ack_nack

    // 5. Send a duplicate of the first heartbeat, reader should not respond with
    // acknack
    let hb_one2 = hb_one.clone();
    assert!(!reader.handle_heartbeat_msg(&hb_one2, false, &mr_state)); // No acknack

    // 6. Send a second proper heartbeat, reader should respond with acknack
    let hb_2 = Heartbeat {
      reader_id: reader.entity_id(),
      writer_id: writer_guid.entity_id,
      first_sn: SequenceNumber::new(1), // writer has last 2 in cache
      last_sn: SequenceNumber::new(3),  // writer has written 3 samples
      count: 3,
    };
    assert!(reader.handle_heartbeat_msg(&hb_2, false, &mr_state)); // Should send an ack_nack

    // 7. Count of acknack sent should be 2
    // The count is verified from the writer proxy
    let writer_proxy = reader
      .matched_writer(writer_guid)
      .expect("Did not find a matched writer");
    assert_eq!(writer_proxy.sent_ack_nack_count, 2);
  }

  #[test]
  fn reader_handles_gaps() {
    // 1. Create a reader
    // Create the DDS cache and a topic
    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    let topic_name = "test_name";
    let qos_policy = QosPolicies::qos_none();

    let topic_cache_handle = dds_cache.write().unwrap().add_new_topic(
      topic_name.to_string(),
      TypeDesc::new("test_type".to_string()),
      &qos_policy,
    );

    // Create mechanisms for notifications, statuses & commands
    let (notification_sender, _notification_receiver) = mio_channel::sync_channel::<()>(100);
    let (_notification_event_source, notification_event_sender) =
      mio_source::make_poll_channel().unwrap();
    let data_reader_waker = Arc::new(Mutex::new(None));

    let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    // Then create the reader
    let reader_guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    let reader_ing = ReaderIngredients {
      guid: reader_guid,
      notification_sender,
      status_sender,
      topic_name: topic_name.to_string(),
      topic_cache_handle,
      like_stateless: false,
      qos_policy,
      data_reader_command_receiver: reader_command_receiver,
      data_reader_waker,
      poll_event_sender: notification_event_sender,
      security_plugins: None,
    };
    let mut reader = Reader::new(
      reader_ing,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
      participant_status_sender,
    );

    // 2. Add info of a matched writer to the reader
    let writer_guid = GUID::dummy_test_guid(EntityKind::WRITER_NO_KEY_USER_DEFINED);

    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.to_vec(),
      mr_state.multicast_reply_locator_list.to_vec(),
      &QosPolicies::qos_none(),
    );

    // 3. Feed the reader a gap message which marks sequence numbers 1-2 & 4 as
    // irrelevant
    let gap_start = SequenceNumber::new(1);
    let gap_list_base = SequenceNumber::new(3);
    let mut gap_list = SequenceNumberSet::new(gap_list_base, 7);
    gap_list.test_insert(SequenceNumber::new(4));

    let gap = Gap {
      reader_id: reader.entity_id(),
      writer_id: writer_guid.entity_id,
      gap_start,
      gap_list,
    };
    reader.handle_gap_msg(&gap, &mr_state);

    // 4. Verify that the writer proxy reports seqnums below 3 as ackable
    // This should be the case since seqnums 1-2 were marked as irrelevant
    assert_eq!(
      reader
        .matched_writer(writer_guid)
        .unwrap()
        .all_ackable_before(),
      SequenceNumber::new(3)
    );

    // 5. Feed the reader a data message with sequence number 3
    let data = Data {
      writer_id: writer_guid.entity_id,
      writer_sn: SequenceNumber::new(3),
      ..Default::default()
    };
    let data_flags = BitFlags::<DATA_Flags>::from_flag(DATA_Flags::Data);

    reader.handle_data_msg(data, data_flags, &mr_state);

    // 6. Verify that the writer proxy reports seqnums below 5 as ackable
    // This should be the case since reader received data with seqnum 3 and seqnum 4
    // was marked irrelevant before
    assert_eq!(
      reader
        .matched_writer(writer_guid)
        .unwrap()
        .all_ackable_before(),
      SequenceNumber::new(5)
    );

    // 7. Feed the reader a gap message which marks the sequence number 5 as
    // irrelevant
    let gap_start = SequenceNumber::new(5);
    let gap_list_base = SequenceNumber::new(5);
    let mut gap_list = SequenceNumberSet::new(gap_list_base, 7);
    gap_list.test_insert(SequenceNumber::new(5));

    let gap = Gap {
      reader_id: reader.entity_id(),
      writer_id: writer_guid.entity_id,
      gap_start,
      gap_list,
    };
    reader.handle_gap_msg(&gap, &mr_state);

    // 8. Verify that the writer proxy reports seqnums below 6 as ackable
    assert_eq!(
      reader
        .matched_writer(writer_guid)
        .unwrap()
        .all_ackable_before(),
      SequenceNumber::new(6)
    );
  }

  #[test]
  fn stateless_reader_does_not_contain_writer_proxies() {
    // 1. Create a stateless-like reader
    // Create the DDS cache and a topic
    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    let topic_name = "test_name";
    let qos_policy = QosPolicies::builder()
      .reliability(Reliability::BestEffort) // Stateless needs to be BestEffort
      .build();

    let topic_cache_handle = dds_cache.write().unwrap().add_new_topic(
      topic_name.to_string(),
      TypeDesc::new("test_type".to_string()),
      &qos_policy,
    );

    // Create mechanisms for notifications, statuses & commands
    let (notification_sender, _notification_receiver) = mio_channel::sync_channel::<()>(100);
    let (_notification_event_source, notification_event_sender) =
      mio_source::make_poll_channel().unwrap();
    let data_reader_waker = Arc::new(Mutex::new(None));

    let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    let like_stateless = true;
    let reader_guid = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    let reader_ing = ReaderIngredients {
      guid: reader_guid,
      notification_sender,
      status_sender,
      topic_name: topic_name.to_string(),
      topic_cache_handle,
      like_stateless,
      qos_policy,
      data_reader_command_receiver: reader_command_receiver,
      data_reader_waker,
      poll_event_sender: notification_event_sender,
      security_plugins: None,
    };
    let mut reader = Reader::new(
      reader_ing,
      Rc::new(UDPSender::new(0).unwrap()),
      mio_extras::timer::Builder::default().build(),
      participant_status_sender,
    );

    // 2. Attempt to add info of a matched writer to the reader
    let writer_guid = GUID::dummy_test_guid(EntityKind::WRITER_NO_KEY_USER_DEFINED);

    let mr_state = MessageReceiverState {
      source_guid_prefix: writer_guid.prefix,
      ..Default::default()
    };

    reader.matched_writer_add(
      writer_guid,
      EntityId::UNKNOWN,
      mr_state.unicast_reply_locator_list.to_vec(),
      mr_state.multicast_reply_locator_list.to_vec(),
      &QosPolicies::qos_none(),
    );

    // 3. Verify that the reader does not contain a writer proxy for the writer that
    // we attempted to add
    assert!(reader.matched_writer(writer_guid).is_none());
  }
}
