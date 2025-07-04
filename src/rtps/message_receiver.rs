use std::collections::{btree_map::Entry, BTreeMap};

use enumflags2::BitFlags;
use mio_extras::{channel as mio_channel, channel::TrySendError};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use bytes::Bytes;

use crate::{
  messages::{protocol_version::ProtocolVersion, submessages::submessages::*, vendor_id::VendorId},
  rtps::{reader::Reader, Message, Submessage, SubmessageBody},
  structure::{
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    locator::Locator,
    time::Timestamp,
  },
};
#[cfg(feature = "security")]
use crate::security::{
  cryptographic::{DecodeOutcome, DecodedSubmessage},
  security_plugins::SecurityPluginsHandle,
};
#[cfg(feature = "security")]
use crate::messages::submessages::{secure_postfix::SecurePostfix, secure_prefix::SecurePrefix};
#[cfg(not(feature = "security"))]
use crate::no_security::SecurityPluginsHandle;
#[cfg(test)]
use crate::dds::ddsdata::DDSData;
#[cfg(test)]
use crate::structure::sequence_number::SequenceNumber;

const RTPS_MESSAGE_HEADER_SIZE: usize = 20;

// Secure submessage receiving state machine:
//
// [None] ---SecurePrefix--> [Prefix] ---some Submessage--> [SecureSubmessage]
// ---SecurePostfix--> [None]
//
// [None] ---other submessage--> [None]
//
// If the submessage sequence does not follow either of these, we fail and reset
// to [None].
//
#[cfg(not(feature = "security"))]
#[derive(Clone, Eq, PartialEq, Debug)]
struct SecureReceiverState {}

#[cfg(feature = "security")]
#[derive(Clone, Eq, PartialEq, Debug)]
enum SecureReceiverState {
  // SecurePrefix received
  Prefix(SecurePrefix),
  // Submessage after SecurePrefix
  SecureSubmessage(SecurePrefix, Submessage),
}

// This type identifies what kind of security wrapper was unwrapped from a
// resulting submessage. There may be several layers, such as entire RTPS
// message was secured, or individual submessage was secured, or both. The
// purpose is to communicate the wrapping to Readers and Writers.
#[cfg(feature = "security")]
#[derive(Clone, Debug)]
pub struct SecureWrapping {
  // TODO
}

// This is partial receiver state to be sent to a Reader or a Writer with a
// Submessage
#[derive(Debug)]
pub struct MessageReceiverState<'a> {
  pub source_guid_prefix: GuidPrefix,
  pub unicast_reply_locator_list: &'a [Locator],

  #[allow(dead_code)]
  // TODO: We do not use the multicast Locators from InfoReply for anything for now.
  pub multicast_reply_locator_list: &'a [Locator],

  pub source_timestamp: Option<Timestamp>,

  #[allow(dead_code)] // TODO: Remove this when/if SecureWrapping actually does something.
  #[cfg(feature = "security")]
  pub secure_rtps_wrapped: Option<SecureWrapping>,
}

impl Default for MessageReceiverState<'_> {
  fn default() -> Self {
    Self {
      source_guid_prefix: GuidPrefix::default(),
      unicast_reply_locator_list: &[],
      multicast_reply_locator_list: &[],
      source_timestamp: Some(Timestamp::INVALID),
      #[cfg(feature = "security")]
      secure_rtps_wrapped: None,
    }
  }
}

/// [`MessageReceiver`] is the submessage sequence interpreter described in
/// RTPS spec v2.3 Section 8.3.4 "The RTPS Message Receiver".
/// It calls the message/submessage deserializers to parse the sequence of
/// submessages. Then it processes the instructions in the Interpreter
/// SUbmessages and forwards data in Entity Submessages to the appropriate
/// Entities. (See RTPS spec Section 8.3.7)
pub(crate) struct MessageReceiver {
  pub available_readers: BTreeMap<EntityId, Reader>,
  // GuidPrefix sent in this channel needs to be RTPSMessage source_guid_prefix. Writer needs this
  // to locate RTPSReaderProxy if negative acknack.
  acknack_sender: mio_channel::SyncSender<(GuidPrefix, AckSubmessage)>,
  // We send notification of remote DomainParticipant liveness to Discovery to
  // bypass Reader, DDSCache, DatasampleCache, and DataReader, because these will drop
  // repeated messages with duplicate SequenceNumbers, but Discovery needs to see them.
  spdp_liveness_sender: mio_channel::SyncSender<GuidPrefix>,
  security_plugins: Option<SecurityPluginsHandle>,

  own_guid_prefix: GuidPrefix,
  pub source_version: ProtocolVersion,
  pub source_vendor_id: VendorId,
  pub source_guid_prefix: GuidPrefix,
  pub dest_guid_prefix: GuidPrefix,
  pub unicast_reply_locator_list: Vec<Locator>,
  pub multicast_reply_locator_list: Vec<Locator>,
  pub source_timestamp: Option<Timestamp>,

  submessage_count: usize, // Used in tests and error messages only?
  secure_receiver_state: Option<SecureReceiverState>,
  #[cfg(feature = "security")]
  secure_rtps_wrapped: Option<SecureWrapping>,
  #[cfg(feature = "security")]
  // For certain topics we have to allow unprotected rtps messages even if the domain is
  // rtps-protected
  must_be_rtps_protection_special_case: bool,
}

impl MessageReceiver {
  pub fn new(
    participant_guid_prefix: GuidPrefix,
    acknack_sender: mio_channel::SyncSender<(GuidPrefix, AckSubmessage)>,
    spdp_liveness_sender: mio_channel::SyncSender<GuidPrefix>,
    security_plugins: Option<SecurityPluginsHandle>,
  ) -> Self {
    Self {
      available_readers: BTreeMap::new(),
      acknack_sender,
      spdp_liveness_sender,
      security_plugins,
      own_guid_prefix: participant_guid_prefix,

      source_version: ProtocolVersion::THIS_IMPLEMENTATION,
      source_vendor_id: VendorId::VENDOR_UNKNOWN,
      source_guid_prefix: GuidPrefix::UNKNOWN,
      dest_guid_prefix: GuidPrefix::UNKNOWN,
      unicast_reply_locator_list: vec![Locator::Invalid],
      multicast_reply_locator_list: vec![Locator::Invalid],
      source_timestamp: None,

      submessage_count: 0,
      secure_receiver_state: None,
      #[cfg(feature = "security")]
      secure_rtps_wrapped: None,
      #[cfg(feature = "security")]
      // Protection on by default
      must_be_rtps_protection_special_case: true,
    }
  }

  pub fn reset(&mut self) {
    self.source_version = ProtocolVersion::THIS_IMPLEMENTATION;
    self.source_vendor_id = VendorId::VENDOR_UNKNOWN;
    self.source_guid_prefix = GuidPrefix::UNKNOWN;
    self.dest_guid_prefix = GuidPrefix::UNKNOWN;
    self.unicast_reply_locator_list.clear();
    self.multicast_reply_locator_list.clear();
    self.source_timestamp = None;

    self.submessage_count = 0;

    self.secure_receiver_state = None; // This exists regardless of security

    #[cfg(feature = "security")]
    {
      self.secure_rtps_wrapped = None;
    }
  }

  fn partial_message_receiver_state(
    &mut self,
    target_reader_entity_id: &EntityId,
  ) -> (
    MessageReceiverState<'_>,
    Option<&mut Reader>,
    Option<&SecurityPluginsHandle>,
  ) {
    let reader = self.available_readers.get_mut(target_reader_entity_id);
    let state = MessageReceiverState {
      source_guid_prefix: self.source_guid_prefix,
      unicast_reply_locator_list: &self.unicast_reply_locator_list,
      multicast_reply_locator_list: &self.multicast_reply_locator_list,
      source_timestamp: self.source_timestamp,
      #[cfg(feature = "security")]
      secure_rtps_wrapped: self.secure_rtps_wrapped.clone(),
    };
    (state, reader, self.security_plugins.as_ref())
  }

  pub fn add_reader(&mut self, new_reader: Reader) {
    let eid = new_reader.guid().entity_id;
    match self.available_readers.entry(eid) {
      Entry::Occupied(_) => warn!("Already have Reader {eid:?} - not adding."),
      Entry::Vacant(e) => {
        e.insert(new_reader);
      }
    }
  }

  pub fn remove_reader(&mut self, old_reader_guid: GUID) -> Option<Reader> {
    self.available_readers.remove(&old_reader_guid.entity_id)
  }

  pub fn reader_mut(&mut self, reader_id: EntityId) -> Option<&mut Reader> {
    self.available_readers.get_mut(&reader_id)
  }

  pub fn handle_received_packet(&mut self, msg_bytes: &Bytes) {
    // Check for RTPS ping message. At least RTI implementation sends these.
    // What should we do with them? The spec does not say.
    if msg_bytes.len() < RTPS_MESSAGE_HEADER_SIZE {
      if msg_bytes.len() >= 16
        && msg_bytes[0..4] == b"RTPS"[..]
        && msg_bytes[9..16] == b"DDSPING"[..]
      {
        // TODO: Add some sensible ping message handling here.
        info!("Received RTPS PING. Do not know how to respond.");
        debug!("Data was {:?}", &msg_bytes);
      } else {
        warn!("Message is shorter than RTPS header. Cannot deserialize.");
        debug!("Data was {:?}", &msg_bytes);
      }
      return;
    }

    // Check that the 4-byte magic string matches "RTPS"
    if msg_bytes.len() >= 4 {
      let magic = &msg_bytes[0..4];
      if *magic == b"RTPS"[..] {
        // go ahead and try to decode message
      } else if *magic == b"RTPX"[..] {
        // RTI Connext sends also packets with magic RTPX in the header.
        // We do not know are these really same as RTPS or different, so let's
        // ignore those.
        info!("Received message with RTPX header. Ignoring.");
        return;
      } else {
        warn!("Received message with unknown start of header {magic:x?}. Ignoring.");
        return;
      }
    }

    // call Speedy reader
    // Bytes .clone() is cheap, so no worries
    let rtps_message = match Message::read_from_buffer(msg_bytes) {
      Ok(m) => m,
      Err(speedy_err) => {
        warn!("RTPS deserialize error {speedy_err:?}");
        debug!("Data was {msg_bytes:?}");
        return;
      }
    };

    // And process message
    self.handle_parsed_message(rtps_message);
  }

  // This is also called directly from dp_event_loop in case of loopback messages.
  pub fn handle_parsed_message(&mut self, rtps_message: Message) {
    self.reset();
    self.dest_guid_prefix = self.own_guid_prefix;
    self.source_guid_prefix = rtps_message.header.guid_prefix;
    self.source_version = rtps_message.header.protocol_version;
    self.source_vendor_id = rtps_message.header.vendor_id;

    #[cfg(not(feature = "security"))]
    let decoded_message = rtps_message;

    #[cfg(feature = "security")]
    let decoded_message = match &self.security_plugins {
      None => {
        self.must_be_rtps_protection_special_case = false; // No plugins, no protection
        rtps_message
      }

      Some(security_plugins_handle) => {
        let security_plugins = security_plugins_handle.get_plugins();

        // If the first submessage is SecureRTPSPrefix, the message has to be decoded
        // using the cryptographic plugin
        if let Some(Submessage {
          body: SubmessageBody::Security(SecuritySubmessage::SecureRTPSPrefix(..)),
          ..
        }) = rtps_message.submessages.first()
        {
          match security_plugins.decode_rtps_message(rtps_message, &self.source_guid_prefix) {
            Ok(DecodeOutcome::Success(message)) => {
              self.must_be_rtps_protection_special_case = false; // Message was protected
              message
            }
            Ok(DecodeOutcome::KeysNotFound(header_key_id)) => {
              return trace!(
                "No matching message decode keys found for the key id {:?} for the remote \
                 participant {:?}",
                header_key_id,
                self.source_guid_prefix
              )
            }
            Ok(DecodeOutcome::ValidatingReceiverSpecificMACFailed) => {
              return trace!("Failed to validate the receiver-specif MAC for the rtps message.");
            }
            Ok(DecodeOutcome::ParticipantCryptoHandleNotFound(guid_prefix)) => {
              return trace!(
                "No participant crypto handle found for the participant {guid_prefix:?} for rtps \
                 message decoding."
              )
            }
            Err(e) => return error!("{e:?}"),
          }
        } else {
          if security_plugins.rtps_not_protected(&self.dest_guid_prefix) {
            // The domain is not rtps-protected, the additional check does not apply
            self.must_be_rtps_protection_special_case = false;
          } else {
            // The messages in a rtps-protected domain are expected to start
            // with SecureRTPSPrefix. The only exception is if the
            // message contains only submessages for the following
            // builtin topics: DCPSParticipants,
            // DCPSParticipantStatelessMessage,
            // DCPSParticipantVolatileMessageSecure
            // (8.4.2.4, table 27).
            self.must_be_rtps_protection_special_case = true;
          }
          rtps_message
        }
      }
    };

    // Process the submessages
    for submessage in decoded_message.submessages {
      self.handle_submessage(submessage);
      self.submessage_count += 1;
    }
  }

  fn handle_submessage(&mut self, submessage: Submessage) {
    match self.secure_receiver_state.take() {
      // Note that .take() always resets the state to "None", so we must
      // set it in every branch where it should remain in some other value.
      None => {
        // Just normal, non-security processing
        match submessage.body {
          SubmessageBody::Interpreter(m) => self.handle_interpreter_submessage(m),
          SubmessageBody::Writer(submessage) => {
            let security_plugins_clone = self.security_plugins.clone();
            let receiver_entity_id = submessage.receiver_entity_id();

            // For writer submessages, if the receiver entity ID is unknown, we have to try
            // to give it to all matched readers. When security is enabled, we do this for
            // topics that have no submessage protection
            if receiver_entity_id == EntityId::UNKNOWN {
              let sending_writer_entity_id = submessage.sender_entity_id();

              let available_target_entity_ids: Vec<EntityId> = self
                .available_readers
                .values()
                .filter(|target_reader| {
                  // Reader must contain the writer
                  target_reader.contains_writer(sending_writer_entity_id)
                    // But there are two exceptions:
                    // 1. SPDP reader must read from unknown SPDP writers
                    //  TODO: This logic here is uglyish. Can we just inject a
                    //  presupposed writer (proxy) to the built-in reader as it is created?
                    || (sending_writer_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
                      && target_reader.entity_id() == EntityId::SPDP_BUILTIN_PARTICIPANT_READER)
                    // 2. ParticipantStatelessReader does not contain any writers, since it is stateless
                    || (sending_writer_entity_id == EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER
                      && target_reader.entity_id() == EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER)
                })
                .map(Reader::entity_id).collect();

              match security_plugins_clone {
                None => {
                  for target_entity_id in available_target_entity_ids {
                    self.handle_writer_submessage(target_entity_id, submessage.clone());
                  }
                }

                #[cfg(not(feature = "security"))]
                Some(_) => {}

                #[cfg(feature = "security")]
                Some(plugins_handle) => {
                  for target_entity_id in available_target_entity_ids {
                    let destination_guid = GUID {
                      prefix: self.dest_guid_prefix,
                      entity_id: target_entity_id,
                    };
                    if plugins_handle
                      .get_plugins()
                      .submessage_not_protected(&destination_guid)
                    {
                      self.handle_writer_submessage(target_entity_id, submessage.clone());
                    }
                  }
                }
              }
            } else {
              match security_plugins_clone {
                None => self.handle_writer_submessage(receiver_entity_id, submessage),

                #[cfg(not(feature = "security"))]
                Some(_) => {}

                #[cfg(feature = "security")]
                Some(plugins_handle) => {
                  let destination_guid = GUID {
                    prefix: self.dest_guid_prefix,
                    entity_id: receiver_entity_id,
                  };
                  if plugins_handle
                    .get_plugins()
                    .submessage_not_protected(&destination_guid)
                  {
                    self.handle_writer_submessage(receiver_entity_id, submessage);
                  } else {
                    error!(
                      "No reader with unprotected submessages found for the GUID \
                       {destination_guid:?}"
                    );
                  }
                }
              }
            }
          }

          SubmessageBody::Reader(submessage) => {
            #[cfg(not(feature = "security"))]
            {
              self.handle_reader_submessage(submessage);
            }
            #[cfg(feature = "security")]
            match self
              .security_plugins
              .as_ref()
              .map(SecurityPluginsHandle::get_plugins)
            {
              None => self.handle_reader_submessage(submessage),
              Some(plugins) => {
                let destination_guid = GUID {
                  prefix: self.dest_guid_prefix,
                  entity_id: submessage.receiver_entity_id(),
                };
                #[cfg(feature = "security")]
                // This match branch can only be taken with security feature
                if plugins.submessage_not_protected(&destination_guid) {
                  self.handle_reader_submessage(submessage);
                } else {
                  error!(
                    "No writer with unprotected submessages found for the GUID \
                     {destination_guid:?}"
                  );
                }
              }
            }
          }

          #[cfg(feature = "security")]
          SubmessageBody::Security(m) => {
            if self.dest_guid_prefix != self.own_guid_prefix
              && self.dest_guid_prefix != GuidPrefix::UNKNOWN
            {
              trace!(
                "Message is not for this participant. Dropping. dest_guid_prefix={:?} participant \
                 guid={:?}",
                self.dest_guid_prefix,
                self.own_guid_prefix
              );
            } else {
              match m {
                SecuritySubmessage::SecureBody(_sec_body, _sec_body_flags) => {
                  warn!("SecureBody submessage without SecurePrefix. Discarding.");
                }
                SecuritySubmessage::SecurePrefix(sec_prefix, _) => {
                  // just store secure prefix
                  self.secure_receiver_state = Some(SecureReceiverState::Prefix(sec_prefix));
                }
                SecuritySubmessage::SecurePostfix(_sec_postfix, _sec_postfix_flags) => {
                  warn!("SecurePostfix submessage out of sequence. Discarding.");
                }
                SecuritySubmessage::SecureRTPSPrefix(..) => {
                  // DDS Security spec Section "7.3.6.6.3 Validity" requires that this is the
                  // first submessage in a message, in which case it has been taken care of by
                  // decode_rtps_message
                  warn!(
                    "SecureRTPSPrefix is only allowed at the start of the message, now received \
                     at count={}.",
                    self.submessage_count
                  );
                }
                SecuritySubmessage::SecureRTPSPostfix(
                  _sec_rtps_postfix,
                  _sec_rtps_postfix_flags,
                ) => {
                  warn!("SecureRTPSPostfix submessage out of sequence. Discarding.");
                }
              } // match
            } // if
          }
        } // match submessage kind
      } // state None

      #[cfg(not(feature = "security"))]
      Some(_) => {}
      // No security feature => secure_receiver_state is always None.
      #[cfg(feature = "security")]
      Some(SecureReceiverState::Prefix(sec_prefix)) => {
        self.secure_receiver_state = Some(SecureReceiverState::SecureSubmessage(
          sec_prefix, submessage,
        ));
      } // state Prefix

      #[cfg(feature = "security")]
      Some(SecureReceiverState::SecureSubmessage(sec_prefix, sec_submessage)) => {
        // Secure prefix and a single other submessage received.
        // Now expecting postfix, and only that.
        match submessage.body {
          SubmessageBody::Security(SecuritySubmessage::SecurePostfix(sec_postfix, _)) => {
            self.handle_secure_submessage(&sec_prefix, &sec_submessage, &sec_postfix);
          }
          other => {
            warn!(
              "Expected SecurePostfix submessage after SecurePrefix and payload submessage. \
               Discarding."
            );
            debug!("Unexpected submessage instead: {other:?}");
          }
        }
      } // state SecureSubmessage
    } // match secure_submessage_state
  } // fn

  fn handle_writer_submessage(
    &mut self,
    target_reader_entity_id: EntityId,
    submessage: WriterSubmessage,
  ) {
    if self.dest_guid_prefix != self.own_guid_prefix && self.dest_guid_prefix != GuidPrefix::UNKNOWN
    {
      debug!(
        "Message is not for this participant. Dropping. dest_guid_prefix={:?} participant \
         guid={:?}",
        self.dest_guid_prefix, self.own_guid_prefix
      );
      return;
    }

    #[cfg(feature = "security")]
    if self.must_be_rtps_protection_special_case {
      match target_reader_entity_id {
        // These submessages are the special case
        EntityId::SPDP_BUILTIN_PARTICIPANT_READER
        | EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER
        | EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_READER => (),
        // Otherwise we have to reject
        other => {
          return error!(
            "Received an unprotected message containing a writer submessage for the reader \
             {other:?} in an rtps-protected domain."
          )
        }
      }
    }

    let (mr_state, target_reader, security_plugins) =
      self.partial_message_receiver_state(&target_reader_entity_id);
    let writer_entity_id = submessage.sender_entity_id();
    let source_guid_prefix = mr_state.source_guid_prefix;
    let source_guid = &GUID {
      prefix: source_guid_prefix,
      entity_id: writer_entity_id,
    };

    let target_reader = if let Some(target_reader) = target_reader {
      target_reader
    } else {
      return error!("No reader matching the CryptoHandle found");
    };

    match submessage {
      WriterSubmessage::Data(data, data_flags) => {
        Self::decode_and_handle_data(
          security_plugins,
          source_guid,
          data,
          data_flags,
          target_reader,
          &mr_state,
        );

        // Notify discovery that the remote PArticipant seems to be alive
        if writer_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
          && target_reader_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_READER
        {
          self
            .spdp_liveness_sender
            .try_send(source_guid_prefix)
            .unwrap_or_else(|e| {
              debug!("spdp_liveness_sender.try_send(): {e:?}. Is Discovery alive?");
            });
        }
      }

      WriterSubmessage::Heartbeat(heartbeat, flags) => {
        target_reader.handle_heartbeat_msg(
          &heartbeat,
          flags.contains(HEARTBEAT_Flags::Final),
          &mr_state,
        );
      }

      WriterSubmessage::Gap(gap, _flags) => {
        target_reader.handle_gap_msg(&gap, &mr_state);
      }

      WriterSubmessage::DataFrag(datafrag, flags) => {
        Self::decode_and_handle_datafrag(
          security_plugins,
          source_guid,
          datafrag.clone(),
          flags,
          target_reader,
          &mr_state,
        );
      }

      WriterSubmessage::HeartbeatFrag(heartbeatfrag, _flags) => {
        target_reader.handle_heartbeatfrag_msg(&heartbeatfrag, &mr_state);
      }
    }
  }

  // see security version of the same function below
  #[cfg(not(feature = "security"))]
  fn decode_and_handle_data(
    _security_plugins: Option<&SecurityPluginsHandle>,
    _source_guid: &GUID,
    data: Data,
    data_flags: BitFlags<DATA_Flags, u8>,
    reader: &mut Reader,
    mr_state: &MessageReceiverState<'_>,
  ) {
    reader.handle_data_msg(data, data_flags, mr_state);
  }

  #[cfg(feature = "security")]
  fn decode_and_handle_data(
    security_plugins: Option<&SecurityPluginsHandle>,
    source_guid: &GUID,
    data: Data,
    data_flags: BitFlags<DATA_Flags, u8>,
    reader: &mut Reader,
    mr_state: &MessageReceiverState<'_>,
  ) {
    let Data {
      inline_qos,
      serialized_payload,
      ..
    } = data.clone();

    serialized_payload
      // If there is an encoded_payload, decode it
      .map(
        |encoded_payload| match security_plugins.map(SecurityPluginsHandle::get_plugins) {
          Some(security_plugins) => security_plugins
            .decode_serialized_payload(
              encoded_payload,
              inline_qos.unwrap_or_default(),
              source_guid,
              &reader.guid(),
            )
            .map_err(|e| error!("{e:?}")),
          None => Ok(encoded_payload),
        },
      )
      .transpose()
      // If there were no errors, give to the reader
      .map(|decoded_payload| {
        reader.handle_data_msg(
          Data {
            serialized_payload: decoded_payload,
            ..data
          },
          data_flags,
          mr_state,
        );
      })
      // Errors have already been printed
      .ok();
  }

  #[cfg(not(feature = "security"))]
  // see security version below
  fn decode_and_handle_datafrag(
    _security_plugins: Option<&SecurityPluginsHandle>,
    _source_guid: &GUID,
    datafrag: DataFrag,
    datafrag_flags: BitFlags<DATAFRAG_Flags, u8>,
    reader: &mut Reader,
    mr_state: &MessageReceiverState<'_>,
  ) {
    let payload_buffer_length = datafrag.serialized_payload.len();
    if payload_buffer_length
      > (datafrag.fragments_in_submessage as usize) * (datafrag.fragment_size as usize)
    {
      error!(
        "{:?}",
        std::io::Error::new(
          std::io::ErrorKind::Other,
          format!(
            "Invalid DataFrag. serializedData length={} should be less than or equal to \
             (fragments_in_submessage={}) x (fragment_size={})",
            payload_buffer_length, datafrag.fragments_in_submessage, datafrag.fragment_size
          ),
        )
      );
      // and we're done
    } else {
      reader.handle_datafrag_msg(&datafrag, datafrag_flags, mr_state);
    }
    // Consume to keep the same method signature as in the security case
    drop(datafrag);
  }

  #[cfg(feature = "security")]
  fn decode_and_handle_datafrag(
    security_plugins: Option<&SecurityPluginsHandle>,
    source_guid: &GUID,
    datafrag: DataFrag,
    datafrag_flags: BitFlags<DATAFRAG_Flags, u8>,
    reader: &mut Reader,
    mr_state: &MessageReceiverState<'_>,
  ) {
    let DataFrag {
      inline_qos,
      serialized_payload: encoded_payload,
      ..
    } = datafrag.clone();

    match security_plugins.map(SecurityPluginsHandle::get_plugins) {
      Some(security_plugins) => {
        // Decode
        security_plugins
          .decode_serialized_payload(
            encoded_payload,
            inline_qos.unwrap_or_default(),
            source_guid,
            &reader.guid(),
          )
          .map_err(|e| error!("{e:?}"))
      }
      None => Ok(encoded_payload),
    }
    .ok()
    // Deserialize
    .and_then(|serialized_payload| {
      // The check that used to be in DataFrag deserialization
      if serialized_payload.len()
        > (datafrag.fragments_in_submessage as usize) * (datafrag.fragment_size as usize)
      {
        error!(
          "{:?}",
          std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
              "Invalid DataFrag. serializedData length={} should be less than or equal to \
               (fragments_in_submessage={}) x (fragment_size={})",
              serialized_payload.len(),
              datafrag.fragments_in_submessage,
              datafrag.fragment_size
            ),
          )
        );
        None
      } else {
        Some(serialized_payload)
      }
    })
    // If there were no errors, give DecodedDataFrag to the reader
    .map(|decoded_payload| {
      reader.handle_datafrag_msg(
        &DataFrag {
          serialized_payload: decoded_payload,
          ..datafrag
        },
        datafrag_flags,
        mr_state,
      );
    });
  }

  fn handle_reader_submessage(&self, submessage: ReaderSubmessage) {
    if self.dest_guid_prefix != self.own_guid_prefix && self.dest_guid_prefix != GuidPrefix::UNKNOWN
    {
      debug!(
        "Message is not for this participant. Dropping. dest_guid_prefix={:?} participant \
         guid={:?}",
        self.dest_guid_prefix, self.own_guid_prefix
      );
      return;
    }

    #[cfg(feature = "security")]
    if self.must_be_rtps_protection_special_case {
      match submessage.receiver_entity_id() {
        // These submessages are the special case
        EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
        | EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER
        | EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER => (),
        // Otherwise we have to reject
        other => {
          return error!(
            "Received an unprotected message containing a reader submessage for the writer \
             {other:?} in an rtps-protected domain."
          )
        }
      }
    }

    match submessage {
      ReaderSubmessage::AckNack(acknack, _) => {
        // Note: This must not block, because the receiving end is the same thread,
        // i.e. blocking here is an instant deadlock.
        match self
          .acknack_sender
          .try_send((self.source_guid_prefix, AckSubmessage::AckNack(acknack)))
        {
          Ok(_) => (),
          Err(TrySendError::Full(_)) => {
            info!("AckNack pipe full. Looks like I am very busy. Discarding submessage.");
          }
          Err(e) => warn!("AckNack pipe fail: {e:?}"),
        }
      }

      ReaderSubmessage::NackFrag(_, _) => {
        // TODO: Implement NackFrag handling
      }
    }
  }

  #[cfg(feature = "security")]
  fn handle_secure_submessage(
    &mut self,
    sec_prefix: &SecurePrefix,
    encoded_submessage: &Submessage,
    sec_postfix: &SecurePostfix,
  ) {
    let security_plugins = self.security_plugins.clone();
    match security_plugins {
      None => {
        warn!("Cannot handle secure submessage: No security plugins configured.");
      }
      Some(ref security_plugins_handle) => {
        // Call 8.5.1.9.6 Operation: preprocess_secure_submsg to determine what
        // the submessage contains and then proceed to decode and process accordingly.

        let decode_result = security_plugins_handle.get_plugins().decode_submessage(
          (
            sec_prefix.clone(),
            encoded_submessage.clone(),
            sec_postfix.clone(),
          ),
          &self.source_guid_prefix,
        );
        match decode_result {
          Err(e) => {
            error!("Submessage decoding failed: {e:?}");
          }
          Ok(DecodeOutcome::Success(DecodedSubmessage::Writer(
            decoded_writer_submessage,
            approved_receiving_datareader_crypto_handles,
          ))) => {
            let receiver_entity_id = decoded_writer_submessage.receiver_entity_id();

            // If the receiver entity ID is unknown, we try to find the correct id based on
            // whether it matches the crypto handle
            if receiver_entity_id == EntityId::UNKNOWN {
              let sending_writer_entity_id = decoded_writer_submessage.sender_entity_id();

              if let Some(target_reader)=self.available_readers.values().find(|target_reader| {
                (
                // Reader must contain the writer
                target_reader.contains_writer(sending_writer_entity_id)
                    // But there are two exceptions:
                    // 1. SPDP reader must read from unknown SPDP writers
                    //  TODO: This logic here is uglyish. Can we just inject a
                    //  presupposed writer (proxy) to the built-in reader as it is created?
                    || (sending_writer_entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
                      && target_reader.entity_id() == EntityId::SPDP_BUILTIN_PARTICIPANT_READER)
                    // 2. ParticipantStatelessReader does not contain any writers, since it is stateless
                    || (sending_writer_entity_id == EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER
                      && target_reader.entity_id() == EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER)
                    )
                    &&
                    security_plugins_handle.get_plugins()
                    .confirm_local_endpoint_guid(&approved_receiving_datareader_crypto_handles,
                      &GUID { prefix: self.dest_guid_prefix,entity_id: target_reader.entity_id() })
              }){
                self.handle_writer_submessage(target_reader.entity_id(), decoded_writer_submessage);
              }else{
                error!("No reader matching the CryptoHandle found");
              }
            } else {
              let receiver_guid = GUID {
                prefix: self.dest_guid_prefix,
                entity_id: receiver_entity_id,
              };
              if security_plugins_handle
                .get_plugins()
                .confirm_local_endpoint_guid(
                  &approved_receiving_datareader_crypto_handles,
                  &receiver_guid,
                )
              {
                self.handle_writer_submessage(receiver_entity_id, decoded_writer_submessage);
              } else {
                error!("Destination GUID did not match the handle used for decoding.");
              }
            }
          }
          Ok(DecodeOutcome::Success(DecodedSubmessage::Reader(
            decoded_reader_submessage,
            approved_receiving_datawriter_crypto_handles,
          ))) => {
            let receiver_entity_id = decoded_reader_submessage.receiver_entity_id();
            let receiver_guid = GUID {
              prefix: self.dest_guid_prefix,
              entity_id: receiver_entity_id,
            };
            if security_plugins_handle
              .get_plugins()
              .confirm_local_endpoint_guid(
                &approved_receiving_datawriter_crypto_handles,
                &receiver_guid,
              )
            {
              self.handle_reader_submessage(decoded_reader_submessage);
            } else {
              error!("Destination GUID did not match the handle used for decoding.");
            }
          }
          Ok(DecodeOutcome::Success(DecodedSubmessage::Interpreter(interpreter_submessage))) => {
            // This is not defined in the specification, but we accept for compatibility, as
            // we would also accept unprotected ones.
            self.handle_interpreter_submessage(interpreter_submessage);
          }
          Ok(DecodeOutcome::KeysNotFound(header_key_id)) => {
            trace!(
              "No matching submessage decode keys found for the key id {:?} for the remote \
               participant {:?}",
              header_key_id,
              self.source_guid_prefix
            );
          }
          Ok(DecodeOutcome::ValidatingReceiverSpecificMACFailed) => {
            trace!("No endpoints passed the receiver-specific MAC validation for the submessage.");
          }
          Ok(DecodeOutcome::ParticipantCryptoHandleNotFound(guid_prefix)) => {
            trace!(
              "No participant crypto handle found for the participant {guid_prefix:?} for \
               submessage decoding."
            );
          }
        }
      }
    };
  }

  fn handle_interpreter_submessage(&mut self, interpreter_submessage: InterpreterSubmessage)
  // no return value, just change state of self.
  {
    match interpreter_submessage {
      InterpreterSubmessage::InfoTimestamp(ts_struct, _flags) => {
        // flags value was used already when parsing timestamp into an Option
        self.source_timestamp = ts_struct.timestamp;
      }
      InterpreterSubmessage::InfoSource(info_src, _flags) => {
        self.source_guid_prefix = info_src.guid_prefix;
        self.source_version = info_src.protocol_version;
        self.source_vendor_id = info_src.vendor_id;

        // TODO: Why are the following set on InfoSource?
        self.unicast_reply_locator_list.clear(); // Or invalid?
        self.multicast_reply_locator_list.clear(); // Or invalid?
        self.source_timestamp = None; // TODO: Why does InfoSource set timestamp
                                      // to None?
      }
      InterpreterSubmessage::InfoReply(info_reply, flags) => {
        self.unicast_reply_locator_list = info_reply.unicast_locator_list;
        self.multicast_reply_locator_list = match (
          flags.contains(INFOREPLY_Flags::Multicast),
          info_reply.multicast_locator_list,
        ) {
          (true, Some(ll)) => ll, // expected case
          (true, None) => {
            warn!(
              "InfoReply submessage flag indicates multicast_reply_locator_list, but none found."
            );
            vec![]
          }
          (false, None) => vec![], // This one is normal again
          (false, Some(_)) => {
            warn!("InfoReply submessage has unexpected multicast_reply_locator_list, ignoring.");
            vec![]
          }
        };
      }
      InterpreterSubmessage::InfoDestination(info_dest, _flags) => {
        if info_dest.guid_prefix == GUID::GUID_UNKNOWN.prefix {
          self.dest_guid_prefix = self.own_guid_prefix;
        } else {
          self.dest_guid_prefix = info_dest.guid_prefix;
        }
      }
    }
  }

  pub fn notify_data_to_readers(&mut self, readers: Vec<EntityId>) {
    for eid in readers {
      self
        .available_readers
        .get_mut(&eid)
        .map(Reader::notify_cache_change);
    }
  }

  // sends 0 seqnum acknacks for those writer that haven't had any action
  pub fn send_preemptive_acknacks(&mut self) {
    for reader in self.available_readers.values_mut() {
      reader.send_preemptive_acknacks();
    }
  }

  // use for test and debugging only
  #[cfg(test)]
  fn get_reader_and_history_cache_change(
    &self,
    reader_id: EntityId,
    sequence_number: SequenceNumber,
  ) -> Option<DDSData> {
    Some(
      self
        .available_readers
        .get(&reader_id)
        .unwrap()
        .history_cache_change_data(sequence_number)
        .unwrap(),
    )
  }

  #[cfg(test)]
  fn get_reader_history_cache_start_and_end_seq_num(
    &self,
    reader_id: EntityId,
  ) -> Vec<SequenceNumber> {
    self
      .available_readers
      .get(&reader_id)
      .unwrap()
      .history_cache_sequence_start_and_end_numbers()
  }
} // impl messageReceiver

// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::{
    rc::Rc,
    sync::{Arc, Mutex, RwLock},
  };

  use speedy::{Readable, Writable};
  use log::info;
  use serde::{Deserialize, Serialize};
  use mio_extras::channel as mio_channel;
  use byteorder::LittleEndian;

  use crate::{
    dds::{
      qos::QosPolicies,
      statusevents::{sync_status_channel, DataReaderStatus},
      typedesc::TypeDesc,
      with_key::simpledatareader::ReaderCommand,
    },
    messages::header::Header,
    mio_source,
    network::udp_sender::UDPSender,
    rtps::reader::ReaderIngredients,
    serialization::from_bytes,
    structure::{dds_cache::DDSCache, guid::EntityKind},
  };
  use super::*;

  #[test]

  fn test_shapes_demo_message_deserialization() {
    // The following message bytes contain serialized INFO_DST, INFO_TS, DATA &
    // HEARTBEAT submessages. The DATA submessage contains a ShapeType value.
    // The bytes have been captured from WireShark.
    let udp_bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d,
      0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x09, 0x01, 0x08, 0x00, 0x1a, 0x15, 0xf3, 0x5e, 0x00,
      0xcc, 0xfb, 0x13, 0x15, 0x05, 0x2c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x07,
      0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x5b, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x52, 0x45, 0x44, 0x00, 0x69, 0x00, 0x00, 0x00, 0x17, 0x00,
      0x00, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x07, 0x01, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00,
      0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x5b, 0x00, 0x00, 0x00, 0x1f, 0x00, 0x00, 0x00,
    ]);

    // The message bytes contain the following guid prefix as the message target.
    let target_gui_prefix = GuidPrefix::new(&[
      0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d, 0x31, 0xa2, 0x28, 0x20, 0x02, 0x08,
    ]);

    // The message bytes contain the following guid as the message source
    let remote_writer_guid = GUID::new(
      GuidPrefix::new(&[
        0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
      ]),
      EntityId::create_custom_entity_id([0, 0, 1], EntityKind::WRITER_WITH_KEY_USER_DEFINED),
    );

    // Create a message receiver
    let (acknack_sender, _acknack_receiver) =
      mio_channel::sync_channel::<(GuidPrefix, AckSubmessage)>(10);
    let (spdp_liveness_sender, _spdp_liveness_receiver) = mio_channel::sync_channel(8);
    let mut message_receiver = MessageReceiver::new(
      target_gui_prefix,
      acknack_sender,
      spdp_liveness_sender,
      None,
    );

    // Create a reader to process the message
    let entity =
      EntityId::create_custom_entity_id([0, 0, 0], EntityKind::READER_WITH_KEY_USER_DEFINED);
    let reader_guid = GUID::new_with_prefix_and_id(target_gui_prefix, entity);

    let (notification_sender, _notification_receiver) = mio_channel::sync_channel::<()>(100);
    let (_notification_event_source, notification_event_sender) =
      mio_source::make_poll_channel().unwrap();
    let data_reader_waker = Arc::new(Mutex::new(None));

    let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    let (_reader_command_sender, reader_command_receiver) =
      mio_channel::sync_channel::<ReaderCommand>(10);

    let qos_policy = QosPolicies::qos_none();

    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));

    let topic_cache_handle = dds_cache.write().unwrap().add_new_topic(
      "test".to_string(),
      TypeDesc::new("test".to_string()),
      &qos_policy,
    );
    let reader_ing = ReaderIngredients {
      guid: reader_guid,
      notification_sender,
      status_sender,
      topic_name: "test".to_string(),
      topic_cache_handle: topic_cache_handle.clone(),
      like_stateless: false,
      qos_policy,
      data_reader_command_receiver: reader_command_receiver,
      data_reader_waker: data_reader_waker.clone(),
      poll_event_sender: notification_event_sender,
      security_plugins: None,
    };

    let mut new_reader = Reader::new(
      reader_ing,
      Rc::new(UDPSender::new_with_random_port().unwrap()),
      mio_extras::timer::Builder::default().build(),
      participant_status_sender,
    );

    // Add info of the writer to the reader
    new_reader.matched_writer_add(
      remote_writer_guid,
      EntityId::UNKNOWN,
      vec![],
      vec![],
      &QosPolicies::qos_none(),
    );

    // Add reader to message reader and process the bytes message
    message_receiver.add_reader(new_reader);

    message_receiver.handle_received_packet(&udp_bits1);

    // Verify the message reader has recorded the right amount of submessages
    assert_eq!(message_receiver.submessage_count, 4);

    // This is not correct way to read history cache values but it serves as a test
    let sequence_numbers =
      message_receiver.get_reader_history_cache_start_and_end_seq_num(reader_guid.entity_id);
    info!("history change sequence number range: {sequence_numbers:?}");

    // Get the DDSData (serialized) from the topic cache / history cache
    let a = message_receiver
      .get_reader_and_history_cache_change(
        reader_guid.entity_id,
        *sequence_numbers.first().unwrap(),
      )
      .expect("No data in topic cache");
    info!("reader history cache DATA: {:?}", a.data());

    // Deserialize the ShapesType value from the data
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct ShapeType {
      color: String,
      x: i32,
      y: i32,
      size: i32,
    }
    let (deserialized_shape_type, _) = from_bytes::<ShapeType, LittleEndian>(&a.data()).unwrap();
    info!("deserialized shapeType: {deserialized_shape_type:?}");

    // Verify the color in the deserialized value is correct
    assert_eq!(deserialized_shape_type.color, "RED");
  }

  #[test]
  fn mr_test_submsg_count() {
    // Udp packet with INFO_DST, INFO_TS, DATA, HEARTBEAT
    let udp_bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d,
      0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x09, 0x01, 0x08, 0x00, 0x18, 0x15, 0xf3, 0x5e, 0x00,
      0x5c, 0xf0, 0x34, 0x15, 0x05, 0x2c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x07,
      0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x43, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x52, 0x45, 0x44, 0x00, 0x21, 0x00, 0x00, 0x00, 0x89, 0x00,
      0x00, 0x00, 0x1e, 0x00, 0x00, 0x00, 0x07, 0x01, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00,
      0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x43, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
    ]);
    // Udp packet with INFO_DST, ACKNACK
    let udp_bits2 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d,
      0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x06, 0x03, 0x18, 0x00, 0x00, 0x00, 0x04, 0xc7, 0x00,
      0x00, 0x04, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x03, 0x00, 0x00, 0x00,
    ]);

    let guid_new = GUID::default();
    let (acknack_sender, _acknack_receiver) =
      mio_channel::sync_channel::<(GuidPrefix, AckSubmessage)>(10);
    let (spdp_liveness_sender, _spdp_liveness_receiver) = mio_channel::sync_channel(8);
    let mut message_receiver =
      MessageReceiver::new(guid_new.prefix, acknack_sender, spdp_liveness_sender, None);

    message_receiver.handle_received_packet(&udp_bits1);
    assert_eq!(message_receiver.submessage_count, 4);

    message_receiver.handle_received_packet(&udp_bits2);
    assert_eq!(message_receiver.submessage_count, 2);
  }

  #[test]
  fn mr_test_header() {
    let guid_new = GUID::default();
    let header = Header::new(guid_new.prefix);

    let bytes = header.write_to_vec().unwrap();
    let new_header = Header::read_from_buffer(&bytes).unwrap();
    assert_eq!(header, new_header);
  }
}
