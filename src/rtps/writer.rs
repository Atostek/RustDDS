use std::{
  cmp::max,
  collections::{BTreeMap, BTreeSet},
  ops::Bound::Included,
  rc::Rc,
  sync::{atomic, Arc, Mutex},
};
use core::task::Waker;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use speedy::{Endianness, Writable};
use mio_extras::{
  channel::{self as mio_channel, TrySendError},
  timer::Timer,
};
use mio_06::Token;

use crate::{
  dds::{
    ddsdata::DDSData,
    qos::{
      policy,
      policy::{History, Reliability},
      HasQoSPolicy, QosPolicies,
    },
    statusevents::{
      CountWithChange, DataWriterStatus, DomainParticipantStatusEvent, StatusChannelSender,
    },
    with_key::datawriter::WriteOptions,
  },
  messages::submessages::submessages::AckSubmessage,
  network::udp_sender::UDPSender,
  rtps::{
    constant::{NACK_RESPONSE_DELAY, NACK_SUPPRESSION_DURATION},
    rtps_reader_proxy::RtpsReaderProxy,
    Message, MessageBuilder,
  },
  structure::{
    cache_change::CacheChange,
    duration::Duration,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    locator::Locator,
    sequence_number::{FragmentNumber, FragmentNumberRange, SequenceNumber},
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
  pub writer_command_receiver: mio_channel::Receiver<WriterCommand>,
  pub writer_command_receiver_waker: Arc<Mutex<Option<Waker>>>,
  pub topic_name: String,
  pub(crate) like_stateless: bool, // Usually false (see like_stateless attribute of Writer)
  pub qos_policies: QosPolicies,
  pub status_sender: StatusChannelSender<DataWriterStatus>,

  pub(crate) security_plugins: Option<SecurityPluginsHandle>,
}

impl WriterIngredients {
  /// This token is used in timed event mio::channel HeartbeatHandler ->
  /// dpEventWrapper
  pub fn alt_entity_token(&self) -> Token {
    self.guid.entity_id.as_alt_token()
  }
}

struct AckWaiter {
  wait_until: SequenceNumber,
  complete_channel: StatusChannelSender<()>,
  readers_pending: BTreeSet<GUID>,
}

impl AckWaiter {
  pub fn notify_wait_complete(&self) {
    // it is normal for the send to fail, because receiver may have timed out
    let _ = self.complete_channel.try_send(());
  }
  pub fn reader_acked_or_lost(&mut self, guid: GUID, acked_before: Option<SequenceNumber>) -> bool // true = waiting complete
  {
    match acked_before {
      None => {
        self.readers_pending.remove(&guid);
      }
      Some(acked_before) if self.wait_until < acked_before => {
        self.readers_pending.remove(&guid);
      }
      Some(_) => (),
    }

    // if the set of waiters is empty, then wait is complete
    self.readers_pending.is_empty()
  }
}

// helper struct for Writer
struct HistoryBuffer {
  first_seq: SequenceNumber, // oldest not removed. Default is 1.
  last_seq: SequenceNumber,  // latest added. Default is 0.

  /// Maps this writers local sequence numbers to DDSHistoryCache instants.
  /// Useful when negative acknack is received.
  sequence_number_to_instant: BTreeMap<SequenceNumber, Timestamp>,

  /// History biffer for serving late-joining Readers nad ACKNACKs.
  /// Maintains as many samples as QoS policies History and Resource Limits
  /// specifiy.
  history_buffer: BTreeMap<Timestamp, CacheChange>,

  // topic name is just for debugging
  topic_name: String,
}

impl HistoryBuffer {
  fn new(topic_name: String) -> Self {
    HistoryBuffer {
      first_seq: SequenceNumber::new(1),
      last_seq: SequenceNumber::new(0), // Indicates that we have nothing yet
      sequence_number_to_instant: BTreeMap::new(),
      history_buffer: BTreeMap::new(),
      topic_name,
    }
  }

  /// Internal counter used to assign
  /// increasing sequence number to
  /// each change made by the Writer
  fn last_change_sequence_number(&self) -> SequenceNumber {
    self.last_seq
  }

  /// If samples are available in the Writer, identifies the first (lowest)
  /// sequence number that is available in the Writer.
  /// If no samples are available in the Writer, identifies the lowest
  /// sequence number that is yet to be written by the Writer
  fn first_change_sequence_number(&self) -> SequenceNumber {
    self.first_seq
  }

  fn get_change(&self, ts: Timestamp) -> Option<&CacheChange> {
    self.history_buffer.get(&ts)
  }

  fn get_by_sn(&self, sn: SequenceNumber) -> Option<&CacheChange> {
    self
      .sequence_number_to_instant
      .get(&sn)
      .and_then(|ts| self.get_change(*ts))
  }

  fn add_change(&mut self, timestamp: Timestamp, new_cache_change: CacheChange) {
    let new_seq = new_cache_change.sequence_number;

    // actual insert
    let had_already_same = self.history_buffer.insert(timestamp, new_cache_change);
    if had_already_same.is_some() {
      // This should really not happen.
      error!(
        "HistoryBuffer: Tried to insert CacheChange with duplicate key. Discarding old sample."
      );
    }
    // also update SeqNo map
    self.sequence_number_to_instant.insert(new_seq, timestamp);

    if new_seq > self.last_change_sequence_number() {
      self.last_seq = new_seq;
    } else {
      error!("HistoryBuffer: Tried to add changes out of SequenceNumber order.");
    }
  }

  fn remove_changes_before(&mut self, remove_before_seq: SequenceNumber) {
    if let Some(remove_before) = self.sequence_number_to_instant.get(&remove_before_seq) {
      let count_before = self.history_buffer.len();
      self.history_buffer = self.history_buffer.split_off(remove_before);
      self.sequence_number_to_instant = self
        .sequence_number_to_instant
        .split_off(&remove_before_seq);
      // debug printout
      let count_after = self.history_buffer.len();
      debug!(
        "HistoryBuffer: remove_changes_before. count before={} after={}, topic={}",
        count_before, count_after, self.topic_name
      );
      if remove_before_seq >= self.first_seq {
        self.first_seq = remove_before_seq; // update
      } else {
        error!(
          "HistoryBuffer: Trying to remove before the first SequenceNumber. But how did we find \
           it in the sequence_number_to_instant map? Looks like a bug."
        );
      }
    } else {
      // Number 1 is a special case. If the buffer is empty, then it is not found.
      if remove_before_seq != SequenceNumber::new(1) {
        warn!(
          "HistoryBuffer: remove_changes_before. Cannot find {:?} first={:?} last={:?} topic={}",
          remove_before_seq, self.first_seq, self.last_seq, self.topic_name
        );
      }
    }
  }
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

  /// The maximum size of any
  /// SerializedPayload that may be sent by the Writer.
  /// This is used to decide when to send DATA or DATAFRAG.
  /// Supposedly "fragment size" limitations apply here, so must be <= 64k.
  /// RTPS spec v2.5 Section "8.4.14.1 Large Data"
  // Note: Writer can choose the max size at initialization, but is not allowed to change it later.
  // RTPS spec v2.5 Section 8.4.14.1.1:
  // "The fragment size must be fixed for a given Writer and is identical for all remote Readers"
  pub data_max_size_serialized: usize,

  my_guid: GUID,
  pub(crate) writer_command_receiver: mio_channel::Receiver<WriterCommand>,
  writer_command_receiver_waker: Arc<Mutex<Option<Waker>>>,

  /// The RTPS ReaderProxy class represents the information an RTPS
  /// StatefulWriter maintains on each matched RTPS Reader
  readers: BTreeMap<GUID, RtpsReaderProxy>,
  matched_readers_count_total: i32, // all matches ever, never decremented
  requested_incompatible_qos_count: i32, // how many times some Reader requested incompatible QoS

  // Sending mechanism
  udp_sender: Rc<UDPSender>,

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

  history_buffer: HistoryBuffer,

  /// Contains timer that needs to be set to timeout with duration of
  /// self.heartbeat_period timed_event_handler sends notification when timer
  /// is up via mio channel to poll in Dp_eventWrapper this also handles
  /// writers cache cleaning timeouts.
  pub(crate) timed_event_timer: Timer<TimedEvent>,

  qos_policies: QosPolicies,

  // Used for sending status info about messages sent
  status_sender: StatusChannelSender<DataWriterStatus>,
  // offered_deadline_status: OfferedDeadlineMissedStatus,
  ack_waiter: Option<AckWaiter>,
  participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,

  security_plugins: Option<SecurityPluginsHandle>,
}

pub enum WriterCommand {
  // TODO: try to make this more private, like pub(crate)
  DDSData {
    ddsdata: DDSData,
    write_options: WriteOptions,
    sequence_number: SequenceNumber,
  },
  WaitForAcknowledgments {
    all_acked: StatusChannelSender<()>,
  },
  // ResetOfferedDeadlineMissedStatus { writer_guid: GUID },
}

impl Writer {
  pub fn new(
    i: WriterIngredients,
    udp_sender: Rc<UDPSender>,
    mut timed_event_timer: Timer<TimedEvent>,
    participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
  ) -> Self {
    // If writer should behave statelessly, only BestEffort QoS is currently
    // supported
    if i.like_stateless && i.qos_policies.is_reliable() {
      panic!("Attempted to create a stateless-like Writer with other than BestEffort reliability");
    }

    let heartbeat_period = i
      .qos_policies
      .reliability
      .and_then(|reliability| {
        if matches!(reliability, Reliability::Reliable { .. }) {
          Some(Duration::from_secs(1))
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

    // TODO: Configuration value
    let cache_cleaning_period = Duration::from_secs(6);

    // Start periodic Heartbeat
    if let Some(period) = heartbeat_period {
      timed_event_timer.set_timeout(std::time::Duration::from(period), TimedEvent::Heartbeat);
    }
    // start periodic cache cleaning
    timed_event_timer.set_timeout(
      std::time::Duration::from(cache_cleaning_period),
      TimedEvent::CacheCleaning,
    );

    Self {
      endianness: Endianness::LittleEndian,
      heartbeat_message_counter: atomic::AtomicI32::new(1),
      push_mode: true,
      heartbeat_period,
      cache_cleaning_period,
      nack_response_delay: NACK_RESPONSE_DELAY, // default value from dp_event_loop
      nackfrag_response_delay: NACK_RESPONSE_DELAY, // default value from dp_event_loop
      repairfrags_continue_delay: std::time::Duration::from_millis(1),
      nack_suppression_duration: NACK_SUPPRESSION_DURATION,
      data_max_size_serialized: 1024,
      // ^^ TODO: Maybe a smarter selection would be in order.
      // We should get the minimum over all outgoing interfaces.
      my_guid: i.guid,
      writer_command_receiver: i.writer_command_receiver,
      writer_command_receiver_waker: i.writer_command_receiver_waker,
      readers: BTreeMap::new(),
      matched_readers_count_total: 0,
      requested_incompatible_qos_count: 0,
      udp_sender,
      my_topic_name: i.topic_name.clone(),
      history_buffer: HistoryBuffer::new(i.topic_name),
      timed_event_timer,
      like_stateless: i.like_stateless,
      qos_policies: i.qos_policies,
      status_sender: i.status_sender,
      participant_status_sender,
      ack_waiter: None,

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

  pub fn handle_timed_event(&mut self) {
    while let Some(e) = self.timed_event_timer.poll() {
      match e {
        TimedEvent::Heartbeat => {
          self.handle_heartbeat_tick(false);
          // ^^ false = This is automatic heartbeat by timer, not manual by application
          // call.
          if let Some(period) = self.heartbeat_period {
            self
              .timed_event_timer
              .set_timeout(std::time::Duration::from(period), TimedEvent::Heartbeat);
          }
        }
        TimedEvent::CacheCleaning => {
          self.handle_cache_cleaning();
          self.timed_event_timer.set_timeout(
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
              self.timed_event_timer.set_timeout(
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
              self.timed_event_timer.set_timeout(
                self.repairfrags_continue_delay,
                TimedEvent::SendRepairFrags {
                  to_reader: reader_guid,
                },
              );
            } // if
          } // if let
        } // SendRepairFrags
      } // match
    } // while
  } // fn

  /// This is called by dp_wrapper every time cacheCleaning message is received.
  fn handle_cache_cleaning(&mut self) {
    let resource_limit = 32;
    // TODO: This limit should be obtained
    // from Topic and Writer QoS. There should be some reasonable default limit
    // in case some supplied QoS setting does not specify a larger value.
    // In any case, there has to be some limit to avoid memory leak.

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
  fn num_frags_and_frag_size(&self, payload_size: usize) -> (u32, u16) {
    let calc_frag_size = |max_size| {
      u16::try_from(max_size).unwrap_or_else(|_| {
        warn!(
          "Integer overflow when converting writer {:x?} fragmentation size ({}) to u16, using \
           u16::MAX",
          self.entity_id(),
          self.data_max_size_serialized
        );
        u16::MAX
      }) as usize
    };
    let mut fragment_size = calc_frag_size(self.data_max_size_serialized);

    // RTPS spec v2.5 Section "8.3.8.3.5 Logical Interpretation"
    let calc_num_frags = |frag_size| u32::try_from(payload_size.div_ceil(frag_size));
    let mut num_frags = calc_num_frags(fragment_size);

    while num_frags.is_err() {
      let overflow_frag_count = payload_size.div_ceil(fragment_size);
      let new_fragment_size = calc_frag_size(fragment_size * 2);
      warn!(
        "Integer overflow when converting writer {:x?} fragmentation count \
         ({overflow_frag_count}) to u32, increasing fragmentation count to {new_fragment_size}",
        self.entity_id()
      );
      num_frags = calc_num_frags(new_fragment_size);
      fragment_size = new_fragment_size;
    }
    let num_frags = num_frags.unwrap();
    debug!("Fragmenting {payload_size} to {num_frags} x {fragment_size}");
    (num_frags, fragment_size as u16)
  }

  // Receive new data samples from the DDS DataWriter
  pub fn process_writer_command(&mut self) {
    while let Ok(cc) = self.writer_command_receiver.try_recv() {
      match cc {
        WriterCommand::DDSData {
          ddsdata: dds_data,
          write_options,
          sequence_number,
        } => {
          // Signal that there is now space in the DataWriter to Writer queue
          {
            self
              .writer_command_receiver_waker
              .lock()
              .unwrap()
              .as_ref()
              .map(|w| w.wake_by_ref());
          }

          // Insert data to local HistoryBuffer
          let timestamp =
            self.insert_to_history_buffer(dds_data, write_options.clone(), sequence_number);

          // If not acting stateless-like, notify reader proxies that there is a new
          // sample
          if !self.like_stateless {
            for reader in &mut self.readers.values_mut() {
              reader.notify_new_cache_change(sequence_number);

              // If the data is meant for a single reader only, set others as pending GAP for
              // this sequence number.
              if let Some(single_reader_guid) = write_options.to_single_reader() {
                if reader.remote_reader_guid != single_reader_guid {
                  reader.insert_pending_gap(sequence_number);
                }
              }
            }
          }

          if self.push_mode {
            // Send data (DATA or DATAFRAGs) and a Heartbeat
            if let Some(cc) = self.history_buffer.get_change(timestamp) {
              let target_reader_opt = match write_options.to_single_reader() {
                Some(guid) => self.readers.get(&guid), // Sending only to this reader
                None => None,                          // Sending to all matched readers
              };

              let send_also_heartbeat = true;
              self.send_cache_change(cc, send_also_heartbeat, target_reader_opt);
            } else {
              error!("Lost the cache change that was just added?!");
            }
          } else {
            // Send Heartbeat only.
            // Readers will ask for the DATA with ACKNACK, if they are interested.
            let final_flag = false; // false = request that readers acknowledge with ACKNACK.
            let liveliness_flag = false; // This is not a manual liveliness assertion (DDS API call), but side-effect of
            let hb_message = MessageBuilder::new()
              .heartbeat_msg(
                self.entity_id(), // from Writer
                self.history_buffer.first_change_sequence_number(),
                self.history_buffer.last_change_sequence_number(),
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
            );
          }
        }

        // WriterCommand::ResetOfferedDeadlineMissedStatus { writer_guid: _, } => {
        //   self.reset_offered_deadline_missed_status();
        // }
        WriterCommand::WaitForAcknowledgments { all_acked } => {
          if self.like_stateless {
            error!(
              "Attempted to wait for acknowledgements in a stateless Writer, which currently only \
               supports BestEffort QoS. Ignoring. topic={:?}",
              self.my_topic_name
            );
            let _ = all_acked.try_send(()); // Let the poor waiter continue.
            return;
          }

          let wait_until = self.history_buffer.last_change_sequence_number();
          let readers_pending: BTreeSet<_> = self
            .readers
            .iter()
            .filter_map(|(guid, rp)| {
              if rp.qos().is_reliable() && rp.all_acked_before <= wait_until {
                Some(*guid)
              } else {
                None
              }
            })
            .collect();
          self.ack_waiter = if readers_pending.is_empty() {
            // all acked already: try to signal app waiting at DataWriter
            let _ = all_acked.try_send(());
            // but we ignore any failure to signal, if no-one is listening
            // since that is normal. They may have timed out and stopped waiting.
            None
          } else {
            // Someone still needs to ack. Wait for them.
            Some(AckWaiter {
              wait_until,
              complete_channel: all_acked,
              readers_pending,
            })
          };
        }
      }
    }
  }

  // Returns a boolean telling if the data had to be fragmented
  fn send_cache_change(
    &self,
    cc: &CacheChange,
    send_also_heartbeat: bool,
    target_reader_opt: Option<&RtpsReaderProxy>, /* if present, we are asked to send the cache
                                                  * change only to the target reader */
  ) -> bool {
    // First make sure that if the data is meant for a single reader only, we do not
    // accidentally send it to everyone
    if let Some(single_reader_guid) = cc.write_options.to_single_reader() {
      match target_reader_opt {
        None => {
          error!(
            "Data is meant for the single reader {single_reader_guid:?} but a proxy for this \
             reader was not provided. Not sending anything."
          );
          return false;
        }
        Some(target_reader) => {
          // Make the data is meant for the target reader
          if single_reader_guid != target_reader.remote_reader_guid {
            error!(
              "We were asked to send data meant for the reader {single_reader_guid:?} to a \
               different reader {:?}. Not gonna happen.",
              target_reader.remote_reader_guid
            );
            return false;
          }
        }
      }
    }

    let messages_to_send = FragmentationIter::new(self, cc, target_reader_opt, send_also_heartbeat);
    let fragmentation_needed = messages_to_send.fragmentation_needed();

    // Send the messages, either to all readers or just one
    for msg in messages_to_send {
      match target_reader_opt {
        None => {
          // To all
          self.send_message_to_readers(DeliveryMode::Multicast, msg, &mut self.readers.values());
        }
        Some(reader_proxy) => {
          // To one
          self.send_message_to_readers(
            DeliveryMode::Unicast,
            msg,
            &mut std::iter::once(reader_proxy),
          );
        }
      }
    }
    // The return value tells if the data had to be fragmented
    fragmentation_needed
  }

  fn insert_to_history_buffer(
    &mut self,
    data: DDSData,
    write_options: WriteOptions,
    new_sequence_number: SequenceNumber,
  ) -> Timestamp {
    assert!(new_sequence_number > SequenceNumber::zero());

    // Create a new CacheChange from DDSData & insert to history buffer
    // The timestamp taken here is used as a unique(!) key in the cache.
    let new_cache_change = CacheChange::new(self.guid(), new_sequence_number, write_options, data);
    let timestamp = Timestamp::now();

    self.history_buffer.add_change(timestamp, new_cache_change);

    // Do not add to Global DDS cache, as we do not necessarily have a local Reader.
    // If we do, then we will receive the ATA packet from network.

    timestamp
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------
  // --------------------------------------------------------------

  /// This is called periodically.
  pub fn handle_heartbeat_tick(&mut self, is_manual_assertion: bool) {
    if self.like_stateless {
      info!(
        "Ignoring handling heartbeat tick in a stateless-like Writer, since it currently supports \
         only BestEffort QoS. topic={:?}",
        self.my_topic_name
      );
      return;
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

    let first_change = self.history_buffer.first_change_sequence_number();
    let last_change = self.history_buffer.last_change_sequence_number();

    if self
      .readers
      .values()
      .all(|rp| last_change < rp.all_acked_before)
    {
      trace!("heartbeat tick: all readers have all available data.");
    } else {
      // the interface to .heartbeat_msg is silly: we give ref to ourself
      // and that function then queries us.
      let hb_message = MessageBuilder::new()
        .ts_msg(self.endianness, Some(Timestamp::now()))
        .heartbeat_msg(
          self.entity_id(), // from Writer
          self.history_buffer.first_change_sequence_number(),
          self.history_buffer.last_change_sequence_number(),
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
            self.send_message_to_readers(
              DeliveryMode::Unicast,
              hb_message.clone(),
              &mut std::iter::once(rp),
            );
          }
        }
      } else {
        // Normal case
        self.send_message_to_readers(
          DeliveryMode::Multicast,
          hb_message,
          &mut self.readers.values(),
        );
      }
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
        let last_seq = self.history_buffer.last_change_sequence_number(); // to avoid borrow problems

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
          info!(
            "ACKNACK SequenceNumberSet minimum must be >= 1, got {:?} from {:?} topic {:?}",
            an.reader_sn_state.base(),
            reader_guid,
            self.topic_name()
          );
        }

        let my_topic = self.my_topic_name.clone(); // for debugging
        self.update_ack_waiters(reader_guid, Some(an.reader_sn_state.base()));

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
                                             // set repair timer to fire
            self.timed_event_timer.set_timeout(
              self.nack_response_delay,
              TimedEvent::SendRepairData {
                to_reader: reader_guid,
              },
            );
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
            self.send_message_to_readers(
              DeliveryMode::Unicast,
              gap_message,
              &mut std::iter::once(reader_proxy),
            );
          }
        }
      } // AckNack
      AckSubmessage::NackFrag(ref nackfrag) => {
        // NackFrag is negative acknowledgement only, i.e. requesting missing fragments.

        let reader_guid = GUID::new(reader_guid_prefix, nackfrag.reader_id);
        if let Some(reader_proxy) = self.lookup_reader_proxy_mut(reader_guid) {
          reader_proxy.mark_frags_requested(nackfrag.writer_sn, &nackfrag.fragment_number_state);
        }
        self.timed_event_timer.set_timeout(
          self.nackfrag_response_delay,
          TimedEvent::SendRepairFrags {
            to_reader: reader_guid,
          },
        );
      }
    }
  }

  // Application may be waiting that remote Readers ACK what we are sending.
  // Notify application that the event they have been waiting for is here.
  fn update_ack_waiters(&mut self, guid: GUID, acked_before: Option<SequenceNumber>) {
    let completed = self
      .ack_waiter
      .as_mut()
      .is_some_and(|aw| aw.reader_acked_or_lost(guid, acked_before));
    if completed {
      self
        .ack_waiter
        .as_ref()
        .map(AckWaiter::notify_wait_complete);
      self.ack_waiter = None;
    }
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
      let first_available = self.history_buffer.first_change_sequence_number();
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
        // Reader not pending gap on unsent_sn. Get the cache change from topic cache
        if let Some(cc) = self.history_buffer.get_by_sn(unsent_sn) {
          // // DEBUG
          // if self.my_guid.entity_id == EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER
          //   && reader_proxy.remote_reader_guid.prefix != self.my_guid.prefix
          // {
          //   info!("Publications Writer sends repair DATA {:?}", unsent_sn);
          // }
          // // DEBUG

          // The cache change was found. Send it to the reader
          let data_was_fragmented = self.send_cache_change(cc, false, Some(reader_proxy));

          if data_was_fragmented {
            // Mark the reader as having requested all frags
            let (num_frags, _frag_size) =
              self.num_frags_and_frag_size(cc.data_value.payload_size());
            reader_proxy.mark_all_frags_requested(unsent_sn, num_frags);

            // Set a timer to send repair frags if needed
            self.timed_event_timer.set_timeout(
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
              &reader_proxy, unsent_sn, first_available, &self.my_topic_name
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

        self.send_message_to_readers(
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

      if let Some(cache_change) = self.history_buffer.get_by_sn(seq_num) {
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

        let fragment_size: u32 = self.data_max_size_serialized as u32; // TODO: overflow check
        let data_size: u32 = cache_change.data_value.payload_size() as u32; // TODO: overflow check

        message_builder = message_builder.data_frag_msg(
          cache_change,
          reader_guid.entity_id, // reader
          self.my_guid,          // writer
          frag_num,
          fragment_size as u16, // TODO: overflow check
          data_size,
          self.endianness,
          self.security_plugins.as_ref(),
        );

        // TODO: some sort of queuing is needed
        self.send_message_to_readers(
          DeliveryMode::Unicast,
          message_builder.add_header_and_build(self.my_guid.prefix),
          &mut std::iter::once(&*reader_proxy),
        );
      } else {
        error!(
          "handle_repair_frags_send_worker: {:?} missing from history_buffer. topic={:?}",
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
      if let Some(depth) = depth {
        max(
          acked_by_all_readers - SequenceNumber::from(depth),
          self.history_buffer.first_change_sequence_number(),
        )
      } else {
        // try to keep all
        self.history_buffer.first_change_sequence_number()
      }
    } else {
      // Stateless-like writer currently supports only BestEffort behavior, so here we
      // make it explicit that it does not care about acked sequence numbers
      let depth = depth.unwrap_or(0);
      max(
        self.history_buffer.last_change_sequence_number() - SequenceNumber::from(depth),
        self.history_buffer.first_change_sequence_number(),
      )
    };
    let first_keeper = max(
      max(
        first_keeper,
        self.history_buffer.last_change_sequence_number() - SequenceNumber::from(resource_limit),
      ),
      SequenceNumber::zero(),
    );
    debug!(
      "HistoryBuffer: cleaning before {first_keeper:?} topic={:?}",
      self.topic_name()
    );
    // actual cleaning
    self.history_buffer.remove_changes_before(first_keeper);
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

  fn send_message_to_readers(
    &self,
    preferred_mode: DeliveryMode,
    message: Message,
    readers: &mut dyn Iterator<Item = &RtpsReaderProxy>,
  ) {
    // TODO: This is a stupid transmit algorithm. We should compute a preferred
    // unicast and multicast locators for each reader only on every reader update,
    // and not find it dynamically on every message.

    // TODO: In addition to Locators found in Readers, we should observe
    // the Locators given in MEssageReceiverState, i.e. if there was an
    // applicable InfoReply submessage, and we are sending a reply.

    let readers = readers.collect::<Vec<_>>(); // clone iterator

    #[cfg(feature = "security")]
    let encoded = self.security_encode(message, &readers);
    #[cfg(not(feature = "security"))]
    let encoded: Result<Message, ()> = Ok(message);

    match encoded {
      Ok(message) => {
        let buffer = message.write_to_vec_with_ctx(self.endianness).unwrap();
        let mut already_sent_to = BTreeSet::new();

        macro_rules! send_unless_sent_and_mark {
          ($locs:expr) => {
            for loc in $locs.iter() {
              if already_sent_to.contains(loc) {
                trace!("Already sent to {:?}", loc);
              } else {
                self.udp_sender.send_to_locator(&buffer, loc);
                already_sent_to.insert(loc.clone());
              }
            }
          };
        }

        for reader in readers {
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
            (DeliveryMode::Multicast, _, Some(_mc_locator)) => {
              send_unless_sent_and_mark!(reader.multicast_locator_list);
            }
            (DeliveryMode::Unicast, Some(_uc_locator), _) => {
              send_unless_sent_and_mark!(reader.unicast_locator_list)
            }
            (_delivery_mode, _, Some(_mc_locator)) => {
              send_unless_sent_and_mark!(reader.multicast_locator_list);
            }
            (_delivery_mode, Some(_uc_locator), _) => {
              send_unless_sent_and_mark!(reader.unicast_locator_list)
            }
            (_delivery_mode, None, None) => {
              warn!("send_message_to_readers: No locators for {reader:?}");
            }
          } // match
        }
      }
      Err(e) => error!("Failed to send message to readers. Encoding failed: {e:?}"),
    }
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

  pub fn update_reader_proxy(
    &mut self,
    reader_proxy: &RtpsReaderProxy,
    requested_qos: &QosPolicies,
  ) {
    debug!("update_reader_proxy topic={:?}", self.my_topic_name);
    match self.qos_policies.compliance_failure_wrt(requested_qos) {
      // matched QoS
      None => {
        let change = self.matched_reader_update(reader_proxy);
        if change > 0 {
          self.matched_readers_count_total += change;
          self.send_status(DataWriterStatus::PublicationMatched {
            total: CountWithChange::new(self.matched_readers_count_total, change),
            current: CountWithChange::new(self.readers.len() as i32, change),
            reader: reader_proxy.remote_reader_guid,
          });
          self.send_participant_status(DomainParticipantStatusEvent::RemoteReaderMatched {
            local_writer: self.my_guid,
            remote_reader: reader_proxy.remote_reader_guid,
          });
          // If we're reliable, should we send out a heartbeat so that new reader can
          // catch up?
          info!(
            "Matched new remote reader on topic={:?} reader={:?}",
            self.topic_name(),
            &reader_proxy.remote_reader_guid
          );
          debug!("Reader details: {:?}", &reader_proxy);
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
  // return 0 if the reader already existed
  // return 1 if it was new ( = count of added reader proxies)
  fn matched_reader_update(&mut self, updated_reader_proxy: &RtpsReaderProxy) -> i32 {
    let mut new = 0;
    let is_volatile = self.qos().is_volatile(); // Get this in advance to work with the borrow checker
    self
      .readers
      .entry(updated_reader_proxy.remote_reader_guid)
      .and_modify(|rp| rp.update(updated_reader_proxy))
      .or_insert_with(|| {
        new = 1;
        let mut new_proxy = updated_reader_proxy.clone();
        if is_volatile {
          // With Durabilty::Volatile QoS we won't send the sequence numbers which existed
          // before matching with this reader. Therefore we set the reader as pending GAP
          // for all existing sequence numbers
          new_proxy.set_pending_gap_up_to(self.history_buffer.last_change_sequence_number());
        }
        new_proxy
      });
    new
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
        &guid
      );
      self.matched_reader_remove(guid);
      // self.matched_readers_count_total -= 1; // this never decreases
      self.send_status(DataWriterStatus::PublicationMatched {
        total: CountWithChange::new(self.matched_readers_count_total, 0),
        current: CountWithChange::new(self.readers.len() as i32, -1),
        reader: guid,
      });
    }
    // also remember to remove reader from ack_waiter
    self.update_ack_waiters(guid, None);
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
  fn new(
    writer: &'a Writer,
    cache_change: &'a CacheChange,
    target_reader_opt: Option<&'a RtpsReaderProxy>,
    send_heartbeat: bool,
  ) -> Self {
    // The EntityId of the destination
    let reader_entity_id =
      target_reader_opt.map_or(EntityId::UNKNOWN, |p| p.remote_reader_guid.entity_id);

    let data_size = cache_change.data_value.payload_size();
    let fragmentation_needed = data_size > writer.data_max_size_serialized;

    let state = if fragmentation_needed {
      FragmentationIterState::Fragmented(FragmentedState::TargetReader, data_size)
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
  Fragments(FragmentNumberRange, u16),
  Heartbeat,
}

impl<'a> Iterator for FragmentationIter<'a> {
  type Item = Message;
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
            *state = FragmentedState::Fragments(
              FragmentNumber::range_inclusive(
                FragmentNumber::new(1),
                FragmentNumber::new(num_frags),
              ),
              fragment_size,
            );

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
                return Some(gap_msg);
              }
            }
            self.next()
          }
          FragmentedState::Fragments(fragments, fragment_size) => {
            if let Some(frag_num) = fragments.next() {
              let mut message_builder = MessageBuilder::new(); // fresh builder

              if let Some(src_ts) = cc.write_options.source_timestamp() {
                // Add timestamp
                message_builder = message_builder.ts_msg(writer.endianness, Some(src_ts));
              }

              if let Some(reader) = target_reader_opt {
                // Add info_destination
                message_builder = message_builder
                  .dst_submessage(writer.endianness, reader.remote_reader_guid.prefix);
              }

              message_builder = message_builder.data_frag_msg(
                cc,
                reader_entity_id, // reader
                writer.my_guid,
                frag_num,
                *fragment_size,
                (*data_size).try_into().unwrap(),
                writer.endianness,
                writer.security_plugins.as_ref(),
              );

              let datafrag_msg = message_builder.add_header_and_build(writer.my_guid.prefix);
              return Some(datafrag_msg);
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
                  writer.history_buffer.first_change_sequence_number(),
                  writer.history_buffer.last_change_sequence_number(),
                  writer.next_heartbeat_count(),
                  writer.endianness,
                  reader_entity_id, // to Reader
                  final_flag,
                  liveliness_flag,
                )
                .add_header_and_build(writer.my_guid.prefix);
              return Some(hb_msg);
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
            writer.history_buffer.first_change_sequence_number(),
            writer.history_buffer.last_change_sequence_number(),
            writer.next_heartbeat_count(),
            writer.endianness,
            reader_entity_id, // to Reader
            final_flag,
            liveliness_flag,
          );
        }

        let data_message = message_builder.add_header_and_build(writer.my_guid.prefix);
        self.finished = true;
        Some(data_message)
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
