use std::io;

use bytes::Bytes;
use enumflags2::BitFlags;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use speedy::{Context, Readable, Writable, Writer};

use crate::{
  messages::submessages::{
    ack_nack::AckNack,
    heartbeat::Heartbeat,
    info_destination::InfoDestination,
    info_source::InfoSource,
    info_timestamp::InfoTimestamp,
    nack_frag::NackFrag,
    submessage::{ReaderSubmessage, WriterSubmessage},
    submessage_flag::{
      endianness_flag, ACKNACK_Flags, DATAFRAG_Flags, DATA_Flags, GAP_Flags, HEARTBEATFRAG_Flags,
      HEARTBEAT_Flags, INFODESTINATION_Flags, INFOREPLY_Flags, INFOSOURCE_Flags,
      INFOTIMESTAMP_Flags, NACKFRAG_Flags,
    },
    submessage_header::SubmessageHeader,
    submessage_kind::SubmessageKind,
    submessages::{Data, DataFrag, Gap, HeartbeatFrag, InfoReply, InterpreterSubmessage},
  },
  Timestamp,
};
#[cfg(feature = "security")]
use crate::messages::submessages::{
  secure_body::SecureBody,
  secure_postfix::SecurePostfix,
  secure_prefix::SecurePrefix,
  secure_rtps_postfix::SecureRTPSPostfix,
  secure_rtps_prefix::SecureRTPSPrefix,
  submessage::SecuritySubmessage,
  submessage_flag::{
    SECUREBODY_Flags, SECUREPOSTFIX_Flags, SECUREPREFIX_Flags, SECURERTPSPOSTFIX_Flags,
    SECURERTPSPREFIX_Flags,
  },
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Submessage {
  pub header: SubmessageHeader,
  pub body: SubmessageBody,
  pub original_bytes: Option<Bytes>,
  // original_bytes contains the original bytes if Submessage was created by parsing from Bytes.
  // If message was constructed from components instead, it is None.
}

// We implement this instead of Speedy trait Readable, because
// we need to run-time decide which endianness we input. Speedy requires the
// top level to fix that. And there seems to be no reasonable way to change
// endianness. TODO: The error type should be something better
impl Submessage {
  pub fn read_from_buffer(buffer: &mut Bytes) -> io::Result<Option<Self>> {
    let sub_header = SubmessageHeader::read_from_buffer(buffer).map_err(io::Error::other)?;
    // Try to figure out how large this submessage is.
    let sub_header_length = 4; // 4 bytes
    let proposed_sub_content_length = if sub_header.content_length == 0 {
      // RTPS spec 2.3, section 9.4.5.1.3:
      //           In case octetsToNextHeader==0 and the kind of Submessage is
      // NOT PAD or INFO_TS, the Submessage is the last Submessage in the Message
      // and extends up to the end of the Message. This makes it possible to send
      // Submessages larger than 64k (the size that can be stored in the
      // octetsToNextHeader field), provided they are the last Submessage in the
      // Message. In case the octetsToNextHeader==0 and the kind of Submessage is
      // PAD or INFO_TS, the next Submessage header starts immediately after the
      // current Submessage header OR the PAD or INFO_TS is the last Submessage
      // in the Message.
      match sub_header.kind {
        SubmessageKind::PAD | SubmessageKind::INFO_TS => 0,
        _not_pad_or_info_ts => buffer.len() - sub_header_length,
      }
    } else {
      sub_header.content_length as usize
    };
    // check if the declared content length makes sense
    let sub_content_length = if sub_header_length + proposed_sub_content_length <= buffer.len() {
      proposed_sub_content_length
    } else {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
          "Submessage header declares length larger than remaining message size: \
           {sub_header_length} + {proposed_sub_content_length} <= {}",
          buffer.len()
        ),
      ));
    };

    // split first submessage to new buffer
    let mut sub_buffer = buffer.split_to(sub_header_length + sub_content_length);
    let original_submessage_bytes = sub_buffer.clone();
    // split tail part (submessage body) to new buffer
    let sub_content_buffer = sub_buffer.split_off(sub_header_length);

    let e = endianness_flag(sub_header.flags);
    let original_bytes = Some(original_submessage_bytes.clone());
    let mk_w_subm = move |s: WriterSubmessage| {
      io::Result::<Option<Self>>::Ok(Some(Submessage {
        header: sub_header,
        body: SubmessageBody::Writer(s),
        original_bytes,
      }))
    };
    let original_bytes = Some(original_submessage_bytes.clone());
    let mk_r_subm = move |s: ReaderSubmessage| {
      io::Result::<Option<Self>>::Ok(Some(Submessage {
        header: sub_header,
        body: SubmessageBody::Reader(s),
        original_bytes,
      }))
    };

    #[cfg(feature = "security")]
    let original_bytes = Some(original_submessage_bytes.clone());
    #[cfg(feature = "security")]
    let mk_s_subm = move |s: SecuritySubmessage| {
      io::Result::<Option<Self>>::Ok(Some(Submessage {
        header: sub_header,
        body: SubmessageBody::Security(s),
        original_bytes,
      }))
    };

    let original_bytes = Some(original_submessage_bytes.clone());
    let mk_i_subm = move |s: InterpreterSubmessage| {
      io::Result::<Option<Self>>::Ok(Some(Submessage {
        header: sub_header,
        body: SubmessageBody::Interpreter(s),
        original_bytes,
      }))
    };

    match sub_header.kind {
      SubmessageKind::DATA => {
        // Manually implemented deserialization for DATA. Speedy does not quite cut it.
        let f = BitFlags::<DATA_Flags>::from_bits_truncate(sub_header.flags);
        mk_w_subm(WriterSubmessage::Data(
          Data::deserialize_data(&sub_content_buffer, f)?,
          f,
        ))
      }

      SubmessageKind::DATA_FRAG => {
        // Manually implemented deserialization for DATA. Speedy does not quite cut it.
        let f = BitFlags::<DATAFRAG_Flags>::from_bits_truncate(sub_header.flags);
        mk_w_subm(WriterSubmessage::DataFrag(
          DataFrag::deserialize(&sub_content_buffer, f)?,
          f,
        ))
      }

      SubmessageKind::GAP => {
        let f = BitFlags::<GAP_Flags>::from_bits_truncate(sub_header.flags);
        mk_w_subm(WriterSubmessage::Gap(
          Gap::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }

      SubmessageKind::ACKNACK => {
        let f = BitFlags::<ACKNACK_Flags>::from_bits_truncate(sub_header.flags);
        mk_r_subm(ReaderSubmessage::AckNack(
          AckNack::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }

      SubmessageKind::NACK_FRAG => {
        let f = BitFlags::<NACKFRAG_Flags>::from_bits_truncate(sub_header.flags);
        mk_r_subm(ReaderSubmessage::NackFrag(
          NackFrag::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }

      SubmessageKind::HEARTBEAT => {
        let f = BitFlags::<HEARTBEAT_Flags>::from_bits_truncate(sub_header.flags);
        mk_w_subm(WriterSubmessage::Heartbeat(
          Heartbeat::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }

      SubmessageKind::HEARTBEAT_FRAG => {
        let f = BitFlags::<HEARTBEATFRAG_Flags>::from_bits_truncate(sub_header.flags);
        mk_w_subm(WriterSubmessage::HeartbeatFrag(
          HeartbeatFrag::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }

      // interpreter submessages
      SubmessageKind::INFO_DST => {
        let f = BitFlags::<INFODESTINATION_Flags>::from_bits_truncate(sub_header.flags);
        mk_i_subm(InterpreterSubmessage::InfoDestination(
          InfoDestination::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }
      SubmessageKind::INFO_SRC => {
        let f = BitFlags::<INFOSOURCE_Flags>::from_bits_truncate(sub_header.flags);
        mk_i_subm(InterpreterSubmessage::InfoSource(
          InfoSource::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }
      SubmessageKind::INFO_TS => {
        let f = BitFlags::<INFOTIMESTAMP_Flags>::from_bits_truncate(sub_header.flags);
        let tso = if f.contains(INFOTIMESTAMP_Flags::Invalidate) {
          None
        } else {
          Some(Timestamp::read_from_buffer_with_ctx(
            e,
            &sub_content_buffer,
          )?)
        };
        mk_i_subm(InterpreterSubmessage::InfoTimestamp(
          InfoTimestamp { timestamp: tso },
          f,
        ))
      }
      SubmessageKind::INFO_REPLY => {
        let f = BitFlags::<INFOREPLY_Flags>::from_bits_truncate(sub_header.flags);
        mk_i_subm(InterpreterSubmessage::InfoReply(
          InfoReply::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }
      SubmessageKind::PAD => {
        Ok(None) // nothing to do here
      }

      #[cfg(feature = "security")]
      SubmessageKind::SEC_BODY => {
        let f = BitFlags::<SECUREBODY_Flags>::from_bits_truncate(sub_header.flags);
        mk_s_subm(SecuritySubmessage::SecureBody(
          SecureBody::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }

      #[cfg(feature = "security")]
      SubmessageKind::SEC_PREFIX => {
        let f = BitFlags::<SECUREPREFIX_Flags>::from_bits_truncate(sub_header.flags);
        mk_s_subm(SecuritySubmessage::SecurePrefix(
          SecurePrefix::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }
      #[cfg(feature = "security")]
      SubmessageKind::SEC_POSTFIX => {
        let f = BitFlags::<SECUREPOSTFIX_Flags>::from_bits_truncate(sub_header.flags);
        mk_s_subm(SecuritySubmessage::SecurePostfix(
          SecurePostfix::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }
      #[cfg(feature = "security")]
      SubmessageKind::SRTPS_PREFIX => {
        let f = BitFlags::<SECURERTPSPREFIX_Flags>::from_bits_truncate(sub_header.flags);
        mk_s_subm(SecuritySubmessage::SecureRTPSPrefix(
          SecureRTPSPrefix::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }
      #[cfg(feature = "security")]
      SubmessageKind::SRTPS_POSTFIX => {
        let f = BitFlags::<SECURERTPSPOSTFIX_Flags>::from_bits_truncate(sub_header.flags);
        mk_s_subm(SecuritySubmessage::SecureRTPSPostfix(
          SecureRTPSPostfix::read_from_buffer_with_ctx(e, &sub_content_buffer)?,
          f,
        ))
      }
      unknown_kind => {
        let kind = u8::from(unknown_kind);
        if kind >= 0x80 {
          // Kinds 0x80 - 0xFF are vendor-specific.
          trace!("Received vendor-specific submessage kind {unknown_kind:?}");
          trace!("Submessage was {:?}", &sub_buffer);
        } else {
          // Kind is 0x00 - 0x7F, it should be in the standard.
          info!("Received unknown submessage kind {unknown_kind:?}");
          debug!("Submessage was {:?}", &sub_buffer);
        }
        Ok(None)
      }
    } // match
  }
}

/// See section 7.3.1 of the Security specification (v. 1.1)
// TODO: Submessages should implement some Length trait that returns the length
// of Submessage in bytes. This is needed for Submessage construction.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SubmessageBody {
  Writer(WriterSubmessage),
  Reader(ReaderSubmessage),

  #[cfg(feature = "security")]
  Security(SecuritySubmessage),

  Interpreter(InterpreterSubmessage),
}
impl<C: Context> Writable<C> for SubmessageBody {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    match &self {
      SubmessageBody::Writer(m) => writer.write_value(&m),
      SubmessageBody::Reader(m) => writer.write_value(&m),
      SubmessageBody::Interpreter(m) => writer.write_value(&m),
      #[cfg(feature = "security")]
      SubmessageBody::Security(m) => writer.write_value(&m),
    }
  }
}

impl<C: Context> Writable<C> for Submessage {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    let Submessage { header, body, .. } = self;
    writer.write_value(header)?;
    let body_endianness = endianness_flag(header.flags);
    writer.write_bytes(&body.write_to_vec_with_ctx(body_endianness)?)
  }
}

#[cfg(test)]
mod tests {
  use log::info;
  use hex_literal::hex;

  use crate::{messages::submessages::submessages::*, rtps::submessage::*};

  #[test]
  fn submessage_data_submessage_deserialization() {
    // this is wireshark captured shapes demo data_submessage
    let serialized_data_submessage = Bytes::from_static(&[
      0x15, 0x05, 0x2c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x01,
      0x02, 0x00, 0x00, 0x00, 0x00, 0x5b, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x52, 0x45, 0x44, 0x00, 0x69, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x1e,
      0x00, 0x00, 0x00,
    ]);

    let header = SubmessageHeader::read_from_buffer(&serialized_data_submessage[0..4])
      .expect("could not create submessage header");
    let flags = BitFlags::<DATA_Flags>::from_bits_truncate(header.flags);
    let suba = Data::deserialize_data(&serialized_data_submessage.slice(4..), flags)
      .expect("DATA deserialization failed.");
    let sub = Submessage {
      header,
      body: SubmessageBody::Writer(WriterSubmessage::Data(suba, flags)),
      original_bytes: Some(serialized_data_submessage.clone()),
    };
    info!("{sub:?}");

    let message_buffer = sub.write_to_vec().expect("DATA serialization failed");

    assert_eq!(serialized_data_submessage, message_buffer);
  }

  #[test]
  fn submessage_heartbeat_deserialization() {
    let serialized_heartbeat_message: Vec<u8> = vec![
      0x07, 0x01, 0x1c, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00,
      0x00, 0x5b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5b, 0x00, 0x00, 0x00, 0x1f, 0x00,
      0x00, 0x00,
    ];

    let header = SubmessageHeader::read_from_buffer(&serialized_heartbeat_message[0..4])
      .expect("could not create submessage header");
    let flags = BitFlags::<HEARTBEAT_Flags>::from_bits_truncate(header.flags);
    let e = endianness_flag(header.flags);
    let suba = Heartbeat::read_from_buffer_with_ctx(e, &serialized_heartbeat_message[4..])
      .expect("deserialization failed.");
    let sub = Submessage {
      header,
      body: SubmessageBody::Writer(WriterSubmessage::Heartbeat(suba, flags)),
      original_bytes: Some(serialized_heartbeat_message.clone().into()),
    };
    info!("{sub:?}");

    let message_buffer = sub.write_to_vec().expect("serialization failed");

    assert_eq!(serialized_heartbeat_message, message_buffer);
  }

  #[test]
  fn submessage_info_dst_deserialization() {
    let serialized_info_dst_message: Vec<u8> = vec![
      0x0e, 0x01, 0x0c, 0x00, 0x01, 0x03, 0x00, 0x0c, 0x29, 0x2d, 0x31, 0xa2, 0x28, 0x20, 0x02,
      0x08,
    ];

    let header = SubmessageHeader::read_from_buffer(&serialized_info_dst_message[0..4])
      .expect("could not create submessage header");
    let flags = BitFlags::<INFODESTINATION_Flags>::from_bits_truncate(header.flags);
    let e = endianness_flag(header.flags);
    let suba = InfoDestination::read_from_buffer_with_ctx(e, &serialized_info_dst_message[4..])
      .expect("deserialization failed.");
    let sub = Submessage {
      header,
      body: SubmessageBody::Interpreter(InterpreterSubmessage::InfoDestination(suba, flags)),
      original_bytes: Some(serialized_info_dst_message.clone().into()),
    };
    info!("{sub:?}");

    let message_buffer = sub.write_to_vec().expect("serialization failed");

    assert_eq!(serialized_info_dst_message, message_buffer);
  }

  #[test]

  fn submessage_acknack_fuzz_deserialization() {
    // We have received an ACKNACK submessage with too large
    // SequenceNumberSet. Please see
    // https://github.com/jhelovuo/RustDDS/issues/287
    // for details.
    let serialized_info_submessage: Vec<u8> = hex!(
      "
      06 05 00 00 ff ff ff ff ff ff ff ff ff ff ff ff
      ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff
      ff ff ff ff ff ff ff e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff
      ff ff ff ff ff e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 cf 13 ff ff ff
      ff ff ff ff ff ff ff ff ff ff 00 00 00 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      cf 13 ff ff ff ff ff ff ff ff ff ff ff ff ff 00
      00 00 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1 e1
      e1 e1 e1 e1 e1 ff ff ff ff ff ff ff ff ff f7 ff
      ff ff ff 00 00 00 1e ff ff ff ff ff ff ff ff ff
      ff ff ff ff ff ff ff ff ff ff ff"
    )
    .to_vec();

    let header = SubmessageHeader::read_from_buffer(&serialized_info_submessage[0..4])
      .expect("could not create submessage header");
    let e = endianness_flag(header.flags);

    // This is a malformed submessage. We expect the decoder to report that, but not
    // panic.
    assert!(AckNack::read_from_buffer_with_ctx(e, &serialized_info_submessage[4..]).is_err());
  }

  // #[test]
  // fn submessage_info_ts_deserialization() {
  //   let serializedInfoTSMessage: Vec<u8> = vec![
  //     0x09, 0x01, 0x08, 0x00, 0x1a, 0x15, 0xf3, 0x5e, 0x00, 0xcc, 0xfb, 0x13,
  //   ];
  //   let header =
  // SubmessageHeader::read_from_buffer(&serializedInfoTSMessage[0..4])
  //     .expect("could not create submessage header");
  //   let flags =
  // BitFlags::<INFOTIMESTAMP_Flags>::from_bits_truncate(header.flags);
  //   let e = endianness_flag(header.flags);
  //   let suba = InfoTimestamp::read_from_buffer_with_ctx(e,
  // &serializedInfoTSMessage[4..])     .expect("deserialization failed.");
  //   let sub = SubMessage {
  //     header,
  //     body: SubmessageBody::Interpreter(InterpreterSubmessage::
  // InfoTimestamp(suba, flags)),   };
  //   info!("{:?}", sub);

  //   let message_buffer = sub.write_to_vec().expect("serialization failed");

  //   assert_eq!(serializedInfoTSMessage, message_buffer);
  // }
}
