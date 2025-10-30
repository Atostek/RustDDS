use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use speedy::{Readable, Writable};
use chrono::Utc;
use bytes::Bytes;
use cdr_encoding_size::CdrEncodingSize;

use crate::{
  dds::{participant::DomainParticipant, qos::QosPolicies},
  messages::{
    protocol_version::ProtocolVersion,
    submessages::elements::{
      parameter::Parameter,
      parameter_list::{ParameterList, ParameterListable},
    },
    vendor_id::VendorId,
  },
  rtps::{constant::*, rtps_reader_proxy::RtpsReaderProxy, rtps_writer_proxy::RtpsWriterProxy},
  serialization::{pl_cdr_adapters::*, speedy_pl_cdr_helpers::*},
  structure::{
    duration::Duration,
    entity::RTPSEntity,
    guid::{EntityId, GUID},
    locator,
    locator::Locator,
    parameter_id::ParameterId,
  },
  Key, Keyed, RepresentationIdentifier,
};
use super::builtin_endpoint::{BuiltinEndpointQos, BuiltinEndpointSet};
#[cfg(feature = "security")]
use crate::{
  dds::qos,
  security::{
    access_control::PermissionsToken, authentication::IdentityToken, ParticipantSecurityInfo,
  },
};
#[cfg(feature = "security")]
use super::secure_discovery::SecureDiscovery;
#[cfg(not(feature = "security"))]
use crate::no_security::SecureDiscovery;

// This type is used by Discovery to communicate the presence and properties
// of DomainParticipants. It is sent over topic "DCPSParticipant".
// The type is called "ParticipantBuiltinTopicData" in DDS-Security Spec, e.g.
// Section 7.4.1.4.
//
// Fields marked with PartProxy are inherited from "ParticiapntProxy"
// See RTPS Spec v2.5 Section "8.5.3.2 SPDPdiscoveredParticipantData",
// class hierarchy diagram
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpdpDiscoveredParticipantData {
  pub updated_time: chrono::DateTime<Utc>, // used internally by RustDDS
  pub protocol_version: ProtocolVersion,   // PartProxy
  pub vendor_id: VendorId,                 // PartProxy
  pub expects_inline_qos: bool,            // PartProxy
  pub participant_guid: GUID,              // PartProxy (as GUIDPrefix)
  pub metatraffic_unicast_locators: Vec<Locator>, // PartProxy
  pub metatraffic_multicast_locators: Vec<Locator>, // PartProxy
  pub default_unicast_locators: Vec<Locator>, // PartProxy
  pub default_multicast_locators: Vec<Locator>, // PartProxy
  pub available_builtin_endpoints: BuiltinEndpointSet, // PartProxy
  pub lease_duration: Option<Duration>,    // from SPDPdiscoveredParticipantData
  pub manual_liveliness_count: i32,        // PartProxy
  pub builtin_endpoint_qos: Option<BuiltinEndpointQos>, // PartProxy
  pub entity_name: Option<String>,         // where does this come from??

  // security
  #[cfg(feature = "security")]
  pub identity_token: Option<IdentityToken>,

  #[cfg(feature = "security")]
  pub permissions_token: Option<PermissionsToken>,

  #[cfg(feature = "security")]
  pub property: Option<qos::policy::Property>,

  #[cfg(feature = "security")]
  pub security_info: Option<ParticipantSecurityInfo>,
}

impl SpdpDiscoveredParticipantData {
  #[cfg(feature = "security")]
  pub(crate) fn supports_security(&self) -> bool {
    // TODO: Is this logic correct? Or maybe we could come up with a more accurate
    // version?
    self.identity_token.is_some()
      && self.permissions_token.is_some()
      && self.property.is_some()
      && self.security_info.is_some()
  }

  pub(crate) fn get_builtin_reader_proxy(
    &self,
    entity_id: EntityId,
    reader_qos: &QosPolicies,
  ) -> RtpsReaderProxy {
    assert!(entity_id.kind().is_built_in());
    let remote_reader_guid = GUID::new_with_prefix_and_id(self.participant_guid.prefix, entity_id);

    let mut proxy = RtpsReaderProxy::new(
      remote_reader_guid,
      reader_qos.clone(),
      self.expects_inline_qos,
    );

    proxy
      .multicast_locator_list
      .clone_from(&self.metatraffic_multicast_locators);
    proxy
      .unicast_locator_list
      .clone_from(&self.metatraffic_unicast_locators);

    proxy
  }

  pub(crate) fn get_builtin_writer_proxy(
    &self,
    entity_id: EntityId,
    //writer_qos: &QosPolicies,
  ) -> RtpsWriterProxy {
    assert!(entity_id.kind().is_built_in());
    let remote_writer_guid = GUID::new_with_prefix_and_id(self.participant_guid.prefix, entity_id);

    RtpsWriterProxy::new(
      remote_writer_guid,
      self.metatraffic_unicast_locators.clone(),
      self.metatraffic_multicast_locators.clone(),
      EntityId::UNKNOWN, // Remote Group Entityid, which we do not implement
    )
  }

  pub(crate) fn from_local_participant(
    participant: &DomainParticipant,
    _secure_discovery_opt: &Option<SecureDiscovery>, // If present, security is enabled
    lease_duration: Duration,
  ) -> Self {
    let dp_locators = participant.self_locators();
    let metatraffic_multicast_locators = dp_locators
      .get(&DISCOVERY_MUL_LISTENER_TOKEN)
      .cloned()
      .unwrap_or_default();

    let metatraffic_unicast_locators = dp_locators
      .get(&DISCOVERY_LISTENER_TOKEN)
      .cloned()
      .unwrap_or_default();

    let default_multicast_locators = dp_locators
      .get(&USER_TRAFFIC_MUL_LISTENER_TOKEN)
      .cloned()
      .unwrap_or_default();

    let default_unicast_locators = dp_locators
      .get(&USER_TRAFFIC_LISTENER_TOKEN)
      .cloned()
      .unwrap_or_default();

    #[allow(unused_mut)] // only security feature mutates this
    let mut builtin_endpoints = BuiltinEndpointSet::PARTICIPANT_ANNOUNCER
      | BuiltinEndpointSet::PARTICIPANT_DETECTOR
      | BuiltinEndpointSet::PUBLICATIONS_ANNOUNCER
      | BuiltinEndpointSet::PUBLICATIONS_DETECTOR
      | BuiltinEndpointSet::SUBSCRIPTIONS_ANNOUNCER
      | BuiltinEndpointSet::SUBSCRIPTIONS_DETECTOR
      | BuiltinEndpointSet::PARTICIPANT_MESSAGE_DATA_WRITER
      | BuiltinEndpointSet::PARTICIPANT_MESSAGE_DATA_READER
      | BuiltinEndpointSet::TOPICS_ANNOUNCER
      | BuiltinEndpointSet::TOPICS_DETECTOR;

    // Security-related items initially None
    #[cfg(feature = "security")]
    let mut identity_token = None;
    #[cfg(feature = "security")]
    let mut permissions_token = None;
    #[cfg(feature = "security")]
    let mut property = None;
    #[cfg(feature = "security")]
    let mut security_info = None;

    #[cfg(feature = "security")]
    if let Some(secure_discovery) = _secure_discovery_opt {
      // Security enabled, add needed data
      // Builtin security endpoints
      builtin_endpoints = builtin_endpoints
        | BuiltinEndpointSet::PUBLICATIONS_SECURE_WRITER
        | BuiltinEndpointSet::PUBLICATIONS_SECURE_READER
        | BuiltinEndpointSet::SUBSCRIPTIONS_SECURE_WRITER
        | BuiltinEndpointSet::SUBSCRIPTIONS_SECURE_READER
        | BuiltinEndpointSet::PARTICIPANT_MESSAGE_SECURE_WRITER
        | BuiltinEndpointSet::PARTICIPANT_MESSAGE_SECURE_READER
        | BuiltinEndpointSet::PARTICIPANT_STATELESS_MESSAGE_WRITER
        | BuiltinEndpointSet::PARTICIPANT_STATELESS_MESSAGE_READER
        | BuiltinEndpointSet::PARTICIPANT_VOLATILE_MESSAGE_SECURE_WRITER
        | BuiltinEndpointSet::PARTICIPANT_VOLATILE_MESSAGE_SECURE_READER
        | BuiltinEndpointSet::PARTICIPANT_SECURE_WRITER
        | BuiltinEndpointSet::PARTICIPANT_SECURE_READER;

      // Tokens and the rest
      identity_token = Some(secure_discovery.local_dp_identity_token.clone());
      permissions_token = Some(secure_discovery.local_dp_permissions_token.clone());
      property = Some(secure_discovery.local_dp_property_qos.clone());
      security_info = Some(ParticipantSecurityInfo::from(
        secure_discovery.local_dp_sec_attributes.clone(),
      ));
    }

    Self {
      updated_time: Utc::now(),
      protocol_version: ProtocolVersion::PROTOCOLVERSION_2_3,
      vendor_id: VendorId::THIS_IMPLEMENTATION,
      expects_inline_qos: false,
      participant_guid: participant.guid(),
      metatraffic_unicast_locators,
      metatraffic_multicast_locators,
      default_unicast_locators,
      default_multicast_locators,
      available_builtin_endpoints: BuiltinEndpointSet::from_u32(builtin_endpoints),
      lease_duration: Some(lease_duration),
      manual_liveliness_count: 0,
      builtin_endpoint_qos: None,
      entity_name: None,

      // DDS Security
      #[cfg(feature = "security")]
      identity_token,
      #[cfg(feature = "security")]
      permissions_token,
      #[cfg(feature = "security")]
      property,
      #[cfg(feature = "security")]
      security_info,
    }
  }
}

impl PlCdrDeserialize for SpdpDiscoveredParticipantData {
  fn from_pl_cdr_bytes(
    input_bytes: &[u8],
    encoding: RepresentationIdentifier,
  ) -> Result<Self, PlCdrDeserializeError> {
    let ctx = pl_cdr_rep_id_to_speedy_d(encoding)?;
    let pl = ParameterList::read_from_buffer_with_ctx(ctx, input_bytes)?;
    let pl_map = pl.to_map();
    let protocol_version: ProtocolVersion = get_first_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_PROTOCOL_VERSION,
      "Protocol Version",
    )?;
    let vendor_id: VendorId =
      get_first_from_pl_map(&pl_map, ctx, ParameterId::PID_VENDOR_ID, "Vendor Id")?;
    let expects_inline_qos : bool = // This one has default value false
      get_option_from_pl_map(&pl_map, ctx, ParameterId::PID_EXPECTS_INLINE_QOS, "Expects inline Qos")?
      .unwrap_or(false);
    let participant_guid: GUID = get_first_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_PARTICIPANT_GUID,
      "Participant GUID",
    )?;

    let metatraffic_unicast_locators: Vec<Locator> = get_all_from_pl_map(
      &pl_map,
      &ctx,
      ParameterId::PID_METATRAFFIC_UNICAST_LOCATOR,
      "Metatraffic unicast locators",
    )?;
    let metatraffic_multicast_locators: Vec<Locator> = get_all_from_pl_map(
      &pl_map,
      &ctx,
      ParameterId::PID_METATRAFFIC_MULTICAST_LOCATOR,
      "Metatraffic multicast locators",
    )?;
    let default_unicast_locators: Vec<Locator> = get_all_from_pl_map(
      &pl_map,
      &ctx,
      ParameterId::PID_DEFAULT_UNICAST_LOCATOR,
      "Default unicast locators",
    )?;
    let default_multicast_locators: Vec<Locator> = get_all_from_pl_map(
      &pl_map,
      &ctx,
      ParameterId::PID_DEFAULT_MULTICAST_LOCATOR,
      "Default multicast locators",
    )?;

    let lease_duration: Option<Duration> = get_option_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_PARTICIPANT_LEASE_DURATION,
      "participant lease duration",
    )?;
    let manual_liveliness_count : i32 =  // Default value is 0. TODO: What is the meaning of this?
      get_option_from_pl_map(&pl_map, ctx, ParameterId::PID_PARTICIPANT_MANUAL_LIVELINESS_COUNT, "Manual liveness count")?
      .unwrap_or(0);
    let available_builtin_endpoints: BuiltinEndpointSet = get_first_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_BUILTIN_ENDPOINT_SET,
      "Available builtin endpoints",
    )?;
    let builtin_endpoint_qos: Option<BuiltinEndpointQos> = get_option_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_BUILTIN_ENDPOINT_QOS,
      "Builtin Endpoint Qos",
    )?;

    let entity_name : Option<String> = // Note the serialized type is StringWithNul
      get_option_from_pl_map::< _ , StringWithNul>(&pl_map, ctx, ParameterId::PID_ENTITY_NAME, "entity name")?
      .map( String::from );

    // DDS security
    #[cfg(feature = "security")]
    let identity_token: Option<IdentityToken> = get_option_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_IDENTITY_TOKEN,
      "identity token",
    )?;
    #[cfg(feature = "security")]
    let permissions_token: Option<PermissionsToken> = get_option_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_PERMISSIONS_TOKEN,
      "permissions token",
    )?;
    #[cfg(feature = "security")]
    let property: Option<qos::policy::Property> = get_option_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_PROPERTY_LIST,
      "property list",
    )?;

    #[cfg(feature = "security")]
    let security_info: Option<ParticipantSecurityInfo> = get_option_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_PARTICIPANT_SECURITY_INFO,
      "participant security info",
    )?;

    Ok(Self {
      updated_time: Utc::now(),
      protocol_version,
      vendor_id,
      expects_inline_qos,
      participant_guid,
      metatraffic_unicast_locators,
      metatraffic_multicast_locators,
      default_unicast_locators,
      default_multicast_locators,
      available_builtin_endpoints,
      lease_duration,
      manual_liveliness_count,
      builtin_endpoint_qos,
      entity_name,
      #[cfg(feature = "security")]
      identity_token,
      #[cfg(feature = "security")]
      permissions_token,
      #[cfg(feature = "security")]
      property,
      #[cfg(feature = "security")]
      security_info,
    })
  }
}

impl PlCdrSerialize for SpdpDiscoveredParticipantData {
  fn to_pl_cdr_bytes(
    &self,
    encoding: RepresentationIdentifier,
  ) -> Result<Bytes, PlCdrSerializeError> {
    let ctx = pl_cdr_rep_id_to_speedy(encoding)?;
    let pl = self.to_parameter_list(encoding)?;
    let bytes = pl.serialize_to_bytes(ctx)?;
    Ok(bytes)
  }
}

impl ParameterListable for SpdpDiscoveredParticipantData {
  fn to_parameter_list(
    &self,
    encoding: RepresentationIdentifier,
  ) -> Result<ParameterList, PlCdrSerializeError> {
    // This "unnecessary" binding is to trigger a warning if we forget to
    // serialize any fields.
    let Self {
      updated_time: _, // except this field. It is not serialized.
      protocol_version,
      vendor_id,
      expects_inline_qos,
      participant_guid,
      metatraffic_unicast_locators,
      metatraffic_multicast_locators,
      default_unicast_locators,
      default_multicast_locators,
      available_builtin_endpoints,
      lease_duration,
      manual_liveliness_count,
      builtin_endpoint_qos,
      entity_name,

      // DDS security
      #[cfg(feature = "security")]
      identity_token,
      #[cfg(feature = "security")]
      permissions_token,
      #[cfg(feature = "security")]
      property,
      #[cfg(feature = "security")]
      security_info,
    } = self;

    let mut pl = ParameterList::new();
    let ctx = pl_cdr_rep_id_to_speedy(encoding)?;

    macro_rules! emit {
      ($pid:ident, $member:expr, $type:ty) => {
        pl.push(Parameter::new(ParameterId::$pid, {
          let m: &$type = $member;
          m.write_to_vec_with_ctx(ctx)?
        }))
      };
    }
    macro_rules! emit_option {
      ($pid:ident, $member:expr, $type:ty) => {
        if let Some(m) = $member {
          emit!($pid, m, $type)
        }
      };
    }

    emit!(PID_PROTOCOL_VERSION, protocol_version, ProtocolVersion);
    emit!(PID_VENDOR_ID, vendor_id, VendorId);
    emit!(PID_EXPECTS_INLINE_QOS, expects_inline_qos, bool);
    emit!(PID_PARTICIPANT_GUID, participant_guid, GUID);
    for loc in metatraffic_unicast_locators {
      emit!(
        PID_METATRAFFIC_UNICAST_LOCATOR,
        &locator::repr::Locator::from(*loc),
        locator::repr::Locator
      );
    }
    for loc in metatraffic_multicast_locators {
      emit!(
        PID_METATRAFFIC_MULTICAST_LOCATOR,
        &locator::repr::Locator::from(*loc),
        locator::repr::Locator
      );
    }
    for loc in default_unicast_locators {
      emit!(
        PID_DEFAULT_UNICAST_LOCATOR,
        &locator::repr::Locator::from(*loc),
        locator::repr::Locator
      );
    }
    for loc in default_multicast_locators {
      emit!(
        PID_DEFAULT_MULTICAST_LOCATOR,
        &locator::repr::Locator::from(*loc),
        locator::repr::Locator
      );
    }
    emit!(
      PID_BUILTIN_ENDPOINT_SET,
      available_builtin_endpoints,
      BuiltinEndpointSet
    );
    emit_option!(PID_PARTICIPANT_LEASE_DURATION, lease_duration, Duration);
    emit!(
      PID_PARTICIPANT_MANUAL_LIVELINESS_COUNT,
      manual_liveliness_count,
      i32
    );
    emit_option!(
      PID_BUILTIN_ENDPOINT_QOS,
      builtin_endpoint_qos,
      BuiltinEndpointQos
    );

    // Here we need to serialize as StringWithNul, as String is Speedy built-in,
    // and does not follow CDR encoding.
    let entity_name_n: Option<StringWithNul> = entity_name.clone().map(|e| e.into());
    emit_option!(PID_ENTITY_NAME, &entity_name_n, StringWithNul);

    #[cfg(feature = "security")] // DDS security
    {
      emit_option!(PID_IDENTITY_TOKEN, identity_token, IdentityToken);
      emit_option!(PID_PERMISSIONS_TOKEN, permissions_token, PermissionsToken);
      emit_option!(PID_PROPERTY_LIST, property, qos::policy::Property);
      emit_option!(
        PID_PARTICIPANT_SECURITY_INFO,
        security_info,
        ParticipantSecurityInfo
      );
    }

    Ok(pl)
  }
}

// We need a wrapper to distinguish between Participant and Endpoint GUIDs.
#[allow(non_camel_case_types)]
#[derive(
  PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy, Serialize, Deserialize, CdrEncodingSize, Hash,
)]
pub struct Participant_GUID(pub GUID);

impl Key for Participant_GUID {}

impl Keyed for SpdpDiscoveredParticipantData {
  type K = Participant_GUID;
  fn key(&self) -> Self::K {
    Participant_GUID(self.participant_guid)
  }
}

impl PlCdrDeserialize for Participant_GUID {
  fn from_pl_cdr_bytes(
    input_bytes: &[u8],
    encoding: RepresentationIdentifier,
  ) -> Result<Self, PlCdrDeserializeError> {
    let ctx = pl_cdr_rep_id_to_speedy_d(encoding)?;
    let pl = ParameterList::read_from_buffer_with_ctx(ctx, input_bytes)?;
    let pl_map = pl.to_map();

    let guid: GUID = get_first_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_PARTICIPANT_GUID,
      "Participant GUID",
    )?;

    Ok(Participant_GUID(guid))
  }
}

impl PlCdrSerialize for Participant_GUID {
  fn to_pl_cdr_bytes(
    &self,
    encoding: RepresentationIdentifier,
  ) -> Result<Bytes, PlCdrSerializeError> {
    let mut pl = ParameterList::new();
    let ctx = pl_cdr_rep_id_to_speedy(encoding)?;
    macro_rules! emit {
      ($pid:ident, $member:expr, $type:ty) => {
        pl.push(Parameter::new(ParameterId::$pid, {
          let m: &$type = $member;
          m.write_to_vec_with_ctx(ctx)?
        }))
      };
    }
    emit!(PID_PARTICIPANT_GUID, &self.0, GUID);
    let bytes = pl.serialize_to_bytes(ctx)?;
    Ok(bytes)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    dds::adapters::no_key::DeserializerAdapter,
    messages::submessages::submessages::WriterSubmessage,
    rtps::{submessage::*, Message},
    test::test_data::*,
  };

  #[test]
  fn pdata_deserialize_serialize() {
    let data = spdp_participant_data_raw();

    let rtpsmsg = Message::read_from_buffer(&data).unwrap();
    let submsgs = rtpsmsg.submessages();

    for submsg in &submsgs {
      match &submsg.body {
        SubmessageBody::Writer(WriterSubmessage::Data(d, _)) => {
          let participant_data: SpdpDiscoveredParticipantData =
            PlCdrDeserializerAdapter::from_bytes(
              &d.unwrap_serialized_payload_value(),
              RepresentationIdentifier::PL_CDR_LE,
            )
            .unwrap();
          let sdata = participant_data
            .to_pl_cdr_bytes(RepresentationIdentifier::PL_CDR_LE)
            .unwrap();
          eprintln!("message data = {:?}", &data);
          eprintln!(
            "payload    = {:?}",
            &d.unwrap_serialized_payload_value().to_vec()
          );
          eprintln!("deserialized  = {:?}", &participant_data);
          eprintln!("serialized = {:?}", &sdata);
          // order cannot be known at this point
          // assert_eq!(
          //  sdata.len(),
          //  d.serialized_payload.as_ref().unwrap().value.len()
          //);

          let mut participant_data_2: SpdpDiscoveredParticipantData =
            PlCdrDeserializerAdapter::from_bytes(&sdata, RepresentationIdentifier::PL_CDR_LE)
              .unwrap();
          // force timestamps to be the same, as these are not serialized/deserialized,
          // but stamped during deserialization
          participant_data_2.updated_time = participant_data.updated_time;

          eprintln!("again deserialized = {:?}", &participant_data_2);
          let _sdata_2 = participant_data
            .to_pl_cdr_bytes(RepresentationIdentifier::PL_CDR_LE)
            .unwrap();
          // now the order of bytes should be the same
          assert_eq!(&participant_data_2, &participant_data);
        }
        SubmessageBody::Interpreter(_) => (),
        _ => continue,
      }
    }
  }

  #[test]
  fn deserialize_evil_spdp_fuzz() {
    use hex_literal::hex;
    let data = Bytes::copy_from_slice(&hex!(
      "
    52 54 50 53
    02 02 ff ff 01 0f 45 d2 b3 f5 58 b9 01 00 00 00
    15 07 1e 00 00 00 10 00 00 00 00 00 00 01 00 c2
    00 00 00 00 00 00 00 00 01 00 00 00 00 02 44 d5
    cf 7a
    "
    ));

    let rtpsmsg = Message::read_from_buffer(&data).unwrap();
    let submsgs = rtpsmsg.submessages();

    for submsg in &submsgs {
      match &submsg.body {
        SubmessageBody::Writer(WriterSubmessage::Data(d, _)) => {
          let participant_data: Result<SpdpDiscoveredParticipantData, PlCdrDeserializeError> =
            PlCdrDeserializerAdapter::from_bytes(
              &d.unwrap_serialized_payload_value(),
              RepresentationIdentifier::PL_CDR_LE,
            );
          eprintln!("message data = {:?}", &data);
          eprintln!(
            "payload    = {:?}",
            &d.unwrap_serialized_payload_value().to_vec()
          );
          eprintln!("deserialized  = {:?}", &participant_data);
        }
        SubmessageBody::Interpreter(_) => (),
        _ => continue,
      }
    }
  }
  #[test]
  fn deserialize_evil_spdp_fuzz_2() {
    // https://github.com/jhelovuo/RustDDS/issues/279
    use hex_literal::hex;
    let data = Bytes::copy_from_slice(&hex!(
      "
      52 54 50 53
      02 02 ff ff 01 0f 45 d2 b3 f5 58 b9 01 00 00 00
      15 05 19 00 00 00 10 00 00 00 00 00 00 01 00 c2
      00 00 00 00 02 00 00 00 00 03 90 fe c7
    "
    ));

    let rtpsmsg = Message::read_from_buffer(&data).unwrap();
    let submsgs = rtpsmsg.submessages();

    for submsg in &submsgs {
      match &submsg.body {
        SubmessageBody::Writer(WriterSubmessage::Data(d, _)) => {
          let participant_data: Result<SpdpDiscoveredParticipantData, PlCdrDeserializeError> =
            PlCdrDeserializerAdapter::from_bytes(
              &d.unwrap_serialized_payload_value(),
              RepresentationIdentifier::PL_CDR_LE,
            );
          eprintln!("message data = {:?}", &data);
          eprintln!(
            "payload    = {:?}",
            &d.unwrap_serialized_payload_value().to_vec()
          );
          eprintln!("deserialized  = {:?}", &participant_data);
        }

        SubmessageBody::Interpreter(_) => (),
        _ => continue,
      }
    }
  }

  #[test]
  fn deserialize_evil_spdp_fuzz_3() {
    // https://github.com/jhelovuo/RustDDS/issues/281
    use hex_literal::hex;
    let data = Bytes::copy_from_slice(&hex!(
      "
      52 54 50 53
      02 02 ff ff 01 0f 45 d2 b3 f5 58 b9 01 00 00 00
      15 05 00 00 00 00 32 00 00 00 00 00 00 01 00 c2
      00 00 00 00 02 00 00 00 00 03 00 00 77 00 04 00
      00 00 00 00
    "
    ));

    let rtpsmsg = match Message::read_from_buffer(&data) {
      Ok(m) => m,
      Err(e) => {
        eprintln!("{e}");
        return;
      }
    };
    let submsgs = rtpsmsg.submessages();

    for submsg in &submsgs {
      match &submsg.body {
        SubmessageBody::Writer(WriterSubmessage::Data(d, _)) => {
          let participant_data: Result<SpdpDiscoveredParticipantData, PlCdrDeserializeError> =
            PlCdrDeserializerAdapter::from_bytes(
              &d.unwrap_serialized_payload_value(),
              RepresentationIdentifier::PL_CDR_LE,
            );
          eprintln!("message data = {:?}", &data);
          eprintln!(
            "payload    = {:?}",
            &d.unwrap_serialized_payload_value().to_vec()
          );
          eprintln!("deserialized  = {:?}", &participant_data);
        }
        SubmessageBody::Interpreter(_) => (),
        _ => continue,
      }
    }
  }
}
