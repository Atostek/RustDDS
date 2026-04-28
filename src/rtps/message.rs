use std::{cmp::min, collections::BTreeSet, io};

#[allow(unused_imports)]
use log::{debug, error, trace, warn};
use speedy::{Context, Endianness, Readable, Writable, Writer};
use enumflags2::BitFlags;
use bytes::Bytes;

use crate::{
  dds::ddsdata::DDSData,
  messages::{
    header::Header,
    protocol_id::ProtocolId,
    protocol_version::ProtocolVersion,
    submessages::{
      elements::{parameter::Parameter, parameter_list::ParameterList},
      submessages::*,
    },
    validity_trait::Validity,
    vendor_id::VendorId,
  },
  rtps::{Submessage, SubmessageBody},
  structure::{
    cache_change::CacheChange,
    guid::{EntityId, GuidPrefix, GUID},
    parameter_id::ParameterId,
    sequence_number::{FragmentNumber, SequenceNumber, SequenceNumberSet},
    time::Timestamp,
  },
};
#[cfg(feature = "security")]
use crate::{
  create_security_error_and_log,
  security::{security_plugins::SecurityPluginsHandle, SecurityError},
};
#[cfg(not(feature = "security"))]
use crate::no_security::SecurityPluginsHandle;

#[derive(Debug, Clone)]
pub struct Message {
  pub header: Header,
  pub submessages: Vec<Submessage>,
}

impl Message {
  pub fn add_submessage(&mut self, submessage: Submessage) {
    self.submessages.push(submessage);
  }

  #[cfg(test)]
  pub fn submessages(self) -> Vec<Submessage> {
    self.submessages
  }

  #[cfg(test)]
  pub fn set_header(&mut self, header: Header) {
    self.header = header;
  }

  // We implement this instead of Speedy trait Readable, because
  // we need to run-time decide which endianness we input. Speedy requires the
  // top level to fix that. And there seems to be no reasonable way to change
  // endianness. TODO: The error type should be something better
  pub fn read_from_buffer(buffer: &Bytes) -> io::Result<Self> {
    // The Header deserializes the same
    let rtps_header = Header::read_from_buffer(buffer).map_err(io::Error::other)?;
    if !rtps_header.valid() {
      return Err(io::Error::other("Invalid RTPS header"));
    }
    let mut message = Self::new(rtps_header);
    let mut submessages_left: Bytes = buffer.slice(20..); // header is 20 bytes
                                                          // submessage loop
    while !submessages_left.is_empty() {
      if let Some(submessage) = Submessage::read_from_buffer(&mut submessages_left)? {
        message.submessages.push(submessage);
      }
    } // loop

    Ok(message)
  }
}

impl Message {
  pub fn new(header: Header) -> Self {
    Self {
      header,
      submessages: vec![],
    }
  }
}

impl Default for Message {
  fn default() -> Self {
    Self {
      header: Header::new(GuidPrefix::UNKNOWN),
      submessages: vec![],
    }
  }
}

impl<C: Context> Writable<C> for Message {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    writer.write_value(&self.header)?;
    for x in &self.submessages {
      writer.write_value(&x)?;
    }
    Ok(())
  }
}

#[derive(Default, Clone)]
pub(crate) struct MessageBuilder {
  submessages: Vec<Submessage>,
}

impl MessageBuilder {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn dst_submessage(mut self, endianness: Endianness, guid_prefix: GuidPrefix) -> Self {
    let flags = BitFlags::<INFODESTINATION_Flags>::from_endianness(endianness);
    let submessage_header = SubmessageHeader {
      kind: SubmessageKind::INFO_DST,
      flags: flags.bits(),
      content_length: 12u16,
      // InfoDST length is always 12 because message contains only GuidPrefix
    };
    let dst_submessage = Submessage {
      header: submessage_header,
      body: SubmessageBody::Interpreter(InterpreterSubmessage::InfoDestination(
        InfoDestination { guid_prefix },
        flags,
      )),
      original_bytes: None,
    };

    self.submessages.push(dst_submessage);
    self
  }

  /// Argument Some(timestamp) means that a timestamp is sent.
  /// Argument None means "invalidate", i.e. the previously sent
  /// [`InfoTimestamp`] submessage no longer applies.
  pub fn ts_msg(mut self, endianness: Endianness, timestamp: Option<Timestamp>) -> Self {
    let mut flags = BitFlags::<INFOTIMESTAMP_Flags>::from_endianness(endianness);
    if timestamp.is_none() {
      flags |= INFOTIMESTAMP_Flags::Invalidate;
    }

    let content_length = match timestamp {
      Some(_) => 8, // Timestamp is serialized as 2 x 32b words
      None => 0,    // Not serialized at all
    };

    let submessage_header = SubmessageHeader {
      kind: SubmessageKind::INFO_TS,
      flags: flags.bits(),
      content_length,
    };

    let submessage = Submessage {
      header: submessage_header,
      body: SubmessageBody::Interpreter(InterpreterSubmessage::InfoTimestamp(
        InfoTimestamp { timestamp },
        flags,
      )),
      original_bytes: None,
    };

    self.submessages.push(submessage);
    self
  }

  pub fn data_msg(
    mut self,
    cache_change: &CacheChange,
    reader_entity_id: EntityId, // The entity id to be included in the submessage
    writer_guid: GUID,
    endianness: Endianness,
    security_plugins: Option<&SecurityPluginsHandle>,
  ) -> Self {
    #[cfg(not(feature = "security"))]
    // Parameter not used
    let _ = security_plugins;

    let writer_entity_id = writer_guid.entity_id;

    let mut param_list = ParameterList::new(); // inline QoS goes here

    // Check if we are disposing (by key or by key hash).
    // If yes, then Indicate Dispose by PID_STATUS_INFO in Inline QoS
    // RTPS Spec v2.5 Section "9.6.4.9 StatusInfo_t (PID_STATUS_INFO)"
    // Dispose must be indicated in Inline QoS:
    // RTPS Spec v2.5 Section "8.7.4 Changes in the Instance State"
    //
    // When disposing, we also indicate "unregistered", although we do not
    // support whole register/unregister mechanism at all. TODO: Does this
    // make sense?
    match cache_change.data_value {
      DDSData::Data { .. } => (), // data sample, not dispose

      DDSData::DisposeByKey { .. } => {
        param_list.push(Parameter::create_pid_status_info_parameter(
          /* disposed */ true, /* unregistered */ true, /* filtered */ false,
        ));
      }
      DDSData::DisposeByKeyHash { key_hash, .. } => {
        // yes, insert key hash to inline QoS
        param_list.push(Parameter {
          parameter_id: ParameterId::PID_KEY_HASH,
          value: key_hash.to_vec(),
        });
        // ... and tell what the key_hash means
        let status_info = Parameter::create_pid_status_info_parameter(
          /* disposed */ true, /* unregistered */ true, /* filtered */ false,
        );
        param_list.push(status_info);
      }
    }

    // If we are sending related sample identity, then insert that.
    if let Some(si) = cache_change.write_options.related_sample_identity() {
      let related_sample_identity_serialized = si.write_to_vec_with_ctx(endianness).unwrap();
      // Insert two parameters, because we are not sure which one is the correct
      // parameter id. Or what the receiver thinks is correct. This behaviour
      // was observed from eProsima FastDDS on 2024-11-18.
      param_list.push(Parameter {
        parameter_id: ParameterId::PID_RELATED_SAMPLE_IDENTITY,
        value: related_sample_identity_serialized.clone(),
      });
      param_list.push(Parameter {
        parameter_id: ParameterId::PID_RELATED_SAMPLE_IDENTITY_CUSTOM,
        value: related_sample_identity_serialized,
      });
    }

    let serialized_payload = match cache_change.data_value {
      DDSData::Data {
        ref serialized_payload,
      } => Some(serialized_payload.clone()), // contents is Bytes
      DDSData::DisposeByKey { ref key, .. } => Some(key.clone()),
      DDSData::DisposeByKeyHash { .. } => None,
    };

    #[cfg(not(feature = "security"))]
    let encoded_payload = serialized_payload;

    #[cfg(feature = "security")]
    let encoded_payload = match serialized_payload
      // Encode payload if it exists
      .map(|serialized_payload| {
        serialized_payload
          // Serialize
          .write_to_vec()
          .map_err(|e| create_security_error_and_log!("{e:?}"))
          .and_then(|serialized_payload| {
            match security_plugins.map(SecurityPluginsHandle::get_plugins) {
              Some(security_plugins) => {
                security_plugins
                  .encode_serialized_payload(serialized_payload, &writer_guid)
                  // Add the extra qos
                  .map(|(encoded_payload, extra_inline_qos)| {
                    param_list.concat(extra_inline_qos);
                    encoded_payload
                  })
              }
              None => Ok(serialized_payload),
            }
          })
      })
      .transpose()
    {
      Ok(encoded_payload) => encoded_payload,
      Err(e) => {
        error!("{e:?}");
        return self;
      }
    }; // end security

    let have_inline_qos = !param_list.is_empty(); // we need this later also
    let inline_qos = if have_inline_qos {
      Some(param_list)
    } else {
      None
    };

    let data_message = Data {
      reader_id: reader_entity_id,
      writer_id: writer_entity_id,
      writer_sn: cache_change.sequence_number,
      inline_qos,
      serialized_payload: encoded_payload.map(Bytes::from),
    };

    let flags: BitFlags<DATA_Flags> = BitFlags::<DATA_Flags>::from_endianness(endianness)
      | (match cache_change.data_value {
        DDSData::Data { .. } => BitFlags::<DATA_Flags>::from_flag(DATA_Flags::Data),
        DDSData::DisposeByKey { .. } => BitFlags::<DATA_Flags>::from_flag(DATA_Flags::Key),
        DDSData::DisposeByKeyHash { .. } => {
          BitFlags::<DATA_Flags>::from_flag(DATA_Flags::InlineQos)
        }
      })
      | (if have_inline_qos {
        BitFlags::<DATA_Flags>::from_flag(DATA_Flags::InlineQos)
      } else {
        BitFlags::<DATA_Flags>::empty()
      });

    self.submessages.push(Submessage {
      header: SubmessageHeader {
        kind: SubmessageKind::DATA,
        flags: flags.bits(),
        content_length: data_message.len_serialized() as u16, // TODO: Handle overflow?
      },
      body: SubmessageBody::Writer(WriterSubmessage::Data(data_message, flags)),
      original_bytes: None,
    });
    self
  }

  // This whole MessageBuilder structure should be refactored into something more
  // coherent. Now it just looks messy.
  #[allow(clippy::too_many_arguments)]
  pub fn data_frag_msg(
    mut self,
    cache_change: &CacheChange,
    reader_entity_id: EntityId,
    writer_guid: GUID,
    fragment_number: FragmentNumber, // We support only submessages with one fragment
    fragment_size: u16,
    sample_size: u32, // all fragments together
    endianness: Endianness,
    security_plugins: Option<&SecurityPluginsHandle>,
  ) -> Self {
    #[cfg(not(feature = "security"))]
    // Parameter not used
    let _ = security_plugins;

    let writer_entity_id = writer_guid.entity_id;

    let mut param_list = ParameterList::new(); // inline QoS goes here

    // Check if we are disposing by key hash
    match cache_change.data_value {
      DDSData::Data { .. } | DDSData::DisposeByKey { .. } => (), // no => ok
      DDSData::DisposeByKeyHash { .. } => {
        error!(
          "data_frag_msg: Called with DDSData::DisposeByKeyHash. This is not legit! Discarding."
        );
        // DataFrag must contain either data or key payload, disposing by key hash
        // sent in inline QoS (without key or data) is not possible like in Data
        // submessages. See e.g. RTPS spec v2.5 Table 8.42 in Section "8.3.8.3
        // DataFrag"
        return self;
      }
    }

    // If we are sending related sample identity, then insert that.
    if let Some(si) = cache_change.write_options.related_sample_identity() {
      let related_sample_identity_serialized = si.write_to_vec_with_ctx(endianness).unwrap();
      // Insert two parameters, because we are not sure which one is the correct
      // parameter id. Or what the receiver thinks is correct. This behaviour
      // was observed from eProsima FastDDS on 2024-11-18.
      param_list.push(Parameter {
        parameter_id: ParameterId::PID_RELATED_SAMPLE_IDENTITY,
        value: related_sample_identity_serialized.clone(),
      });
      param_list.push(Parameter {
        parameter_id: ParameterId::PID_RELATED_SAMPLE_IDENTITY_CUSTOM,
        value: related_sample_identity_serialized,
      });
    }

    let have_inline_qos = !param_list.is_empty(); // we need this later also

    // fragments are numbered starting from 1, not 0.
    let from_byte: usize = (usize::from(fragment_number) - 1) * usize::from(fragment_size);
    let up_to_before_byte: usize = min(
      usize::from(fragment_number) * usize::from(fragment_size),
      sample_size.try_into().unwrap(),
    );

    let serialized_payload = Vec::from(
      cache_change
        .data_value
        .bytes_slice(from_byte, up_to_before_byte),
    );

    #[cfg(not(feature = "security"))]
    let encoded_payload = serialized_payload;

    #[cfg(feature = "security")]
    let encoded_payload = {
      let encode_result = match security_plugins.map(SecurityPluginsHandle::get_plugins) {
        Some(security_plugins) => {
          security_plugins
            .encode_serialized_payload(serialized_payload, &writer_guid)
            // Add the extra qos
            .map(|(encoded_payload, extra_inline_qos)| {
              param_list.concat(extra_inline_qos);
              encoded_payload
            })
        }
        None =>
        // If there are no security plugins, use plaintext
        {
          Ok(serialized_payload)
        }
      };

      match encode_result {
        Ok(encoded_payload) => encoded_payload,
        Err(e) => {
          error!("{e:?}");
          return self;
        }
      }
    }; // end security encoding

    let data_message = DataFrag {
      reader_id: reader_entity_id,
      writer_id: writer_entity_id,
      writer_sn: cache_change.sequence_number,
      fragment_starting_num: fragment_number,
      fragments_in_submessage: 1,
      data_size: sample_size, // total, assembled data (SerializedPayload) size
      fragment_size,
      inline_qos: if have_inline_qos {
        Some(param_list)
      } else {
        None
      },
      serialized_payload: Bytes::from(encoded_payload),
    };

    let flags: BitFlags<DATAFRAG_Flags> =
      // endianness flag
      BitFlags::<DATAFRAG_Flags>::from_endianness(endianness)
      // key flag
      | (match cache_change.data_value {
        DDSData::Data { .. } => BitFlags::<DATAFRAG_Flags>::empty(),
        DDSData::DisposeByKey { .. } => BitFlags::<DATAFRAG_Flags>::from_flag(DATAFRAG_Flags::Key),
        DDSData::DisposeByKeyHash { .. } => unreachable!(),
      })
      // inline QoS flag
      | (if have_inline_qos {
        BitFlags::<DATAFRAG_Flags>::from_flag(DATAFRAG_Flags::InlineQos)
      } else {
        BitFlags::<DATAFRAG_Flags>::empty()
      });

    self.submessages.push(Submessage {
      header: SubmessageHeader {
        kind: SubmessageKind::DATA_FRAG,
        flags: flags.bits(),
        content_length: data_message.len_serialized() as u16, // TODO: Handle overflow
      },
      body: SubmessageBody::Writer(WriterSubmessage::DataFrag(data_message, flags)),
      original_bytes: None,
    });
    self
  }

  pub fn gap_msg(
    mut self,
    irrelevant_sns: &BTreeSet<SequenceNumber>,
    writer_entity_id: EntityId,
    writer_endianness: Endianness,
    reader_guid: GUID,
  ) -> Self {
    match (irrelevant_sns.first(), irrelevant_sns.last()) {
      (Some(&gap_start), Some(&_last_sn)) => {
        // Determine the contiguous range of irrelevant seqnums starting from gap_start.
        // Do this by finding the first seqnum which is larger than gap_start but is not
        // included in irrelevant_sns. That is, find the
        // exclusive endpoint of the contiguous range.
        let mut range_endpoint_excl = gap_start.plus_1();
        while irrelevant_sns.contains(&range_endpoint_excl) {
          range_endpoint_excl = range_endpoint_excl.plus_1();
        }
        // Set gapList.base as this exclusive endpoint of the range
        let list_base = range_endpoint_excl;
        let list_set = irrelevant_sns.clone().split_off(&list_base);
        let gap_list = SequenceNumberSet::from_base_and_set(list_base, &list_set);

        let gap = Gap {
          reader_id: reader_guid.entity_id,
          writer_id: writer_entity_id,
          gap_start,
          gap_list,
        };
        let gap_flags = BitFlags::<GAP_Flags>::from_endianness(writer_endianness);
        gap
          .create_submessage(gap_flags)
          .map(|s| self.submessages.push(s));
      }
      (_, _) => error!("gap_msg called with empty SN set. Skipping GAP submessage"),
    }
    self
  }

  // GAP for range 1 <= SN < irrelevant_sns_before
  pub fn gap_msg_before(
    mut self,
    irrelevant_sns_before: SequenceNumber,
    writer_entity_id: EntityId,
    writer_endianness: Endianness,
    reader_guid: GUID,
  ) -> Self {
    let gap_list = SequenceNumberSet::new_empty(irrelevant_sns_before);
    let gap = Gap {
      reader_id: reader_guid.entity_id,
      writer_id: writer_entity_id,
      gap_start: SequenceNumber::from(1),
      gap_list,
    };

    let gap_flags = BitFlags::<GAP_Flags>::from_endianness(writer_endianness);
    gap
      .create_submessage(gap_flags)
      .map(|s| self.submessages.push(s));
    self
  }

  #[allow(clippy::too_many_arguments)] // Heartbeat just is complicated.
  pub fn heartbeat_msg(
    mut self,
    writer_entity_id: EntityId,
    first: SequenceNumber,
    last: SequenceNumber,
    heartbeat_count: i32,
    endianness: Endianness,
    reader_entity_id: EntityId,
    set_final_flag: bool,
    set_liveliness_flag: bool,
  ) -> Self {
    let heartbeat = Heartbeat {
      reader_id: reader_entity_id,
      writer_id: writer_entity_id,
      first_sn: first,
      last_sn: last,
      count: heartbeat_count,
    };

    let mut flags = BitFlags::<HEARTBEAT_Flags>::from_endianness(endianness);

    if set_final_flag {
      flags.insert(HEARTBEAT_Flags::Final);
    }
    if set_liveliness_flag {
      flags.insert(HEARTBEAT_Flags::Liveliness);
    }

    let submessage = heartbeat.create_submessage(flags);
    match submessage {
      Some(sm) => self.submessages.push(sm),
      None => return self,
    }
    self
  }

  pub fn add_header_and_build(self, guid_prefix: GuidPrefix) -> Message {
    Message {
      header: Header {
        protocol_id: ProtocolId::default(),
        protocol_version: ProtocolVersion::THIS_IMPLEMENTATION,
        vendor_id: VendorId::THIS_IMPLEMENTATION,
        guid_prefix,
      },
      submessages: self.submessages,
    }
  }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
  use log::info;

  use super::*;

  #[test]

  fn rtps_message_test_shapes_demo_message_deserialization() {
    // Data message should contain Shapetype values.
    // captured with wireshark from shapes demo.
    // packet with INFO_DST, INFO_TS, DATA, HEARTBEAT
    let bits1 = Bytes::from_static(&[
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
    let rtps = Message::read_from_buffer(&bits1).unwrap();
    info!("{rtps:?}");

    let serialized = Bytes::from(
      rtps
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap(),
    );
    assert_eq!(bits1, serialized);
  }
  #[test]
  fn rtps_message_test_shapes_demo_DataP() {
    // / captured with wireshark from shapes demo.
    // packet with DATA(p)
    let bits2 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x04, 0x01, 0x03, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d, 0x31,
      0xa2, 0x28, 0x20, 0x02, 0x08, 0x15, 0x05, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x01, 0x00, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x23, 0x00, 0x00, 0x00, 0x00,
      0x03, 0x00, 0x00, 0x77, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x04, 0x00, 0x02, 0x04, 0x00, 0x00, 0x50, 0x00, 0x10,
      0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d, 0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x00, 0x00,
      0x01, 0xc1, 0x16, 0x00, 0x04, 0x00, 0x01, 0x03, 0x00, 0x00, 0x44, 0x00, 0x04, 0x00, 0x3f,
      0x0c, 0x00, 0x00, 0x58, 0x00, 0x04, 0x00, 0x3f, 0x0c, 0x00, 0x00, 0x32, 0x00, 0x18, 0x00,
      0x01, 0x00, 0x00, 0x00, 0x9f, 0xa4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x50, 0x8e, 0xc9, 0x32, 0x00, 0x18, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x9f, 0xa4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0xc0, 0xa8, 0x45, 0x14, 0x32, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00,
      0x9f, 0xa4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0xac, 0x11, 0x00, 0x01, 0x33, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xea, 0x1c,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef,
      0xff, 0x00, 0x01, 0x31, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x39, 0x30, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7f, 0x00, 0x00,
      0x01, 0x48, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0x39, 0x30, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7f, 0x00, 0x00, 0x01, 0x34,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0xb0, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00,
      0x02, 0x00, 0x08, 0x00, 0x2c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
      0x00,
    ]);

    let rtps_data = Message::read_from_buffer(&bits2).unwrap();

    let serialized_data = Bytes::from(
      rtps_data
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap(),
    );
    assert_eq!(bits2, serialized_data);
  }

  #[test]
  fn rtps_message_test_shapes_demo_info_TS_dataP() {
    // captured with wireshark from shapes demo.
    // rtps packet with info TS and Data(p)
    let bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x09, 0x01, 0x08, 0x00, 0x0e, 0x15, 0xf3, 0x5e, 0x00, 0x28,
      0x74, 0xd2, 0x15, 0x05, 0xa8, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x01, 0x00, 0xc7, 0x00,
      0x01, 0x00, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
      0x15, 0x00, 0x04, 0x00, 0x02, 0x03, 0x00, 0x00, 0x16, 0x00, 0x04, 0x00, 0x01, 0x0f, 0x00,
      0x00, 0x50, 0x00, 0x10, 0x00, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x01, 0xc1, 0x32, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xf4,
      0x1c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x0a, 0x50, 0x8e, 0x68, 0x31, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xf5, 0x1c, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x50,
      0x8e, 0x68, 0x02, 0x00, 0x08, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x58,
      0x00, 0x04, 0x00, 0x3f, 0x0c, 0x3f, 0x0c, 0x62, 0x00, 0x18, 0x00, 0x14, 0x00, 0x00, 0x00,
      0x66, 0x61, 0x73, 0x74, 0x72, 0x74, 0x70, 0x73, 0x50, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69,
      0x70, 0x61, 0x6e, 0x74, 0x00, 0x01, 0x00, 0x00, 0x00,
    ]);

    let rtps = Message::read_from_buffer(&bits1).unwrap();
    info!("{rtps:?}");

    let serialized = Bytes::from(
      rtps
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap(),
    );
    assert_eq!(bits1, serialized);
  }

  #[test]
  fn rtps_message_test_shapes_demo_info_TS_AckNack() {
    // captured with wireshark from shapes demo.
    // rtps packet with info TS three AckNacks
    let bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x04, 0x01, 0x03, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d, 0x31,
      0xa2, 0x28, 0x20, 0x02, 0x08, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34,
      0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x03, 0x18, 0x00, 0x00, 0x00, 0x03, 0xc7, 0x00,
      0x00, 0x03, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x01, 0x00, 0x00, 0x00, 0x06, 0x03, 0x18, 0x00, 0x00, 0x00, 0x04, 0xc7, 0x00, 0x00, 0x04,
      0xc2, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x06, 0x03, 0x18, 0x00, 0x00, 0x02, 0x00, 0xc7, 0x00, 0x02, 0x00, 0xc2, 0x00,
      0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    ]);

    let rtps = Message::read_from_buffer(&bits1).unwrap();
    info!("{rtps:?}");

    let serialized = Bytes::from(
      rtps
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap(),
    );
    assert_eq!(bits1, serialized);
  }

  #[test]
  fn rtps_message_info_ts_and_dataP() {
    // captured with wireshark from shapes demo.
    // rtps packet with info TS and data(p)
    let bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x09, 0x01, 0x08, 0x00, 0x0e, 0x15, 0xf3, 0x5e, 0x00, 0x28,
      0x74, 0xd2, 0x15, 0x05, 0xa8, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x01, 0x00, 0xc7, 0x00,
      0x01, 0x00, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
      0x15, 0x00, 0x04, 0x00, 0x02, 0x03, 0x00, 0x00, 0x16, 0x00, 0x04, 0x00, 0x01, 0x0f, 0x00,
      0x00, 0x50, 0x00, 0x10, 0x00, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x01, 0xc1, 0x32, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xf4,
      0x1c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x0a, 0x50, 0x8e, 0x68, 0x31, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xf5, 0x1c, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x50,
      0x8e, 0x68, 0x02, 0x00, 0x08, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x58,
      0x00, 0x04, 0x00, 0x3f, 0x0c, 0x3f, 0x0c, 0x62, 0x00, 0x18, 0x00, 0x14, 0x00, 0x00, 0x00,
      0x66, 0x61, 0x73, 0x74, 0x72, 0x74, 0x70, 0x73, 0x50, 0x61, 0x72, 0x74, 0x69, 0x63, 0x69,
      0x70, 0x61, 0x6e, 0x74, 0x00, 0x01, 0x00, 0x00, 0x00,
    ]);

    let rtps = Message::read_from_buffer(&bits1).unwrap();
    info!("{rtps:?}");

    let serialized = Bytes::from(
      rtps
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap(),
    );
    assert_eq!(bits1, serialized);
  }

  #[test]
  fn rtps_message_infoDST_infoTS_Data_w_heartbeat() {
    // captured with wireshark from shapes demo.
    // rtps packet with InfoDST InfoTS Data(w) Heartbeat
    // This datamessage serialized payload maybe contains topic name (square) and
    // its type (shapetype) look https://www.omg.org/spec/DDSI-RTPS/2.3/PDF page 185
    let bits1 = Bytes::from_static(&[
      0x52, 0x54, 0x50, 0x53, 0x02, 0x03, 0x01, 0x0f, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d,
      0x31, 0xa2, 0x28, 0x20, 0x02, 0x08, 0x09, 0x01, 0x08, 0x00, 0x12, 0x15, 0xf3, 0x5e, 0x00,
      0xc8, 0xa9, 0xfa, 0x15, 0x05, 0x0c, 0x01, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x03, 0xc7,
      0x00, 0x00, 0x03, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00,
      0x00, 0x2f, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xf5, 0x1c, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x50, 0x8e, 0x68, 0x50,
      0x00, 0x10, 0x00, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x01, 0xc1, 0x05, 0x00, 0x0c, 0x00, 0x07, 0x00, 0x00, 0x00, 0x53, 0x71, 0x75,
      0x61, 0x72, 0x65, 0x00, 0x00, 0x07, 0x00, 0x10, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x53, 0x68,
      0x61, 0x70, 0x65, 0x54, 0x79, 0x70, 0x65, 0x00, 0x00, 0x00, 0x70, 0x00, 0x10, 0x00, 0x01,
      0x0f, 0x99, 0x06, 0x78, 0x34, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
      0x5a, 0x00, 0x10, 0x00, 0x01, 0x0f, 0x99, 0x06, 0x78, 0x34, 0x00, 0x00, 0x01, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x01, 0x02, 0x60, 0x00, 0x04, 0x00, 0x5f, 0x01, 0x00, 0x00, 0x15, 0x00,
      0x04, 0x00, 0x02, 0x03, 0x00, 0x00, 0x16, 0x00, 0x04, 0x00, 0x01, 0x0f, 0x00, 0x00, 0x1d,
      0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x23, 0x00, 0x08, 0x00, 0xff, 0xff, 0xff, 0x7f,
      0xff, 0xff, 0xff, 0xff, 0x27, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x1b, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff,
      0xff, 0xff, 0x1a, 0x00, 0x0c, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x9a,
      0x99, 0x99, 0x19, 0x2b, 0x00, 0x08, 0x00, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff,
      0x1f, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x01, 0x00, 0x00, 0x00, 0x07, 0x01, 0x1c, 0x00, 0x00, 0x00, 0x03, 0xc7, 0x00, 0x00,
      0x03, 0xc2, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
      0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
    ]);

    let rtps = Message::read_from_buffer(&bits1).unwrap();
    info!("{rtps:?}");

    let data_submessage = match &rtps.submessages[2] {
      Submessage {
        header: _,
        body: SubmessageBody::Writer(WriterSubmessage::Data(d, _flags)),
        ..
      } => d,
      wtf => panic!("Unexpected message structure {wtf:?}"),
    };

    let serialized_payload = data_submessage.unwrap_serialized_payload_value().clone();
    info!("{serialized_payload:x?}");

    let serialized = Bytes::from(
      rtps
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap(),
    );
    assert_eq!(bits1, serialized);
  }

  #[test]
  fn fuzz_rtps() {
    // https://github.com/jhelovuo/RustDDS/issues/280
    use hex_literal::hex;

    let bits = Bytes::copy_from_slice(&hex!(
      "
      52 54 50 53
      02 02 ff ff 01 0f 45 d2 b3 f5 58 b9 01 00 00 00
      15 0b 18 00 00 00 00 00 00 00 02 c2 00 00 00 00
      7d 00 00 00 00 01 00 00
    "
    ));
    info!("bytes = {bits:?}");
    let rtps = Message::read_from_buffer(&bits);
    info!("read_from_buffer() --> {rtps:?}");
    // if we get here without panic, the test passes
  }
}
