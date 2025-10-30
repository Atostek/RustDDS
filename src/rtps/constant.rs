use std::time::Duration;

use mio_06::Token;
use mio_extras::channel as mio_channel;

use crate::{
  discovery::{
    builtin_endpoint::BuiltinEndpointSet,
    discovery::Discovery,
    sedp_messages::{DiscoveredReaderData, DiscoveredWriterData},
  },
  structure::guid::{EntityId, EntityKind, GuidPrefix, GUID},
  QosPolicies,
};

pub const PREEMPTIVE_ACKNACK_PERIOD: Duration = Duration::from_secs(5);

pub const CACHE_CLEAN_PERIOD: Duration = Duration::from_secs(4);

// RTPS spec Section 8.4.7.1.1  "Default Timing-Related Values"
pub const NACK_RESPONSE_DELAY: Duration = Duration::from_millis(200);
pub const NACK_SUPPRESSION_DURATION: Duration = Duration::from_millis(0);

// Helper list for initializing remote standard (non-secure) built-in readers
// Structure is (builtin_writer_entity_id, builtin_reader_entity_id,
// reader_as_BuiltinEndpointSet)
pub const STANDARD_BUILTIN_READERS_INIT_LIST: &[(EntityId, EntityId, u32, QosPolicies)] = &[
  (
    EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER, // SPDP
    EntityId::SPDP_BUILTIN_PARTICIPANT_READER,
    BuiltinEndpointSet::PARTICIPANT_DETECTOR,
    Discovery::create_spdp_participant_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER, // SEDP ...
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER,
    BuiltinEndpointSet::SUBSCRIPTIONS_DETECTOR,
    Discovery::subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
    EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
    BuiltinEndpointSet::PUBLICATIONS_DETECTOR,
    Discovery::subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_TOPIC_WRITER,
    EntityId::SEDP_BUILTIN_TOPIC_READER,
    BuiltinEndpointSet::TOPICS_DETECTOR,
    Discovery::subscriber_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER,
    BuiltinEndpointSet::PARTICIPANT_MESSAGE_DATA_READER,
    Discovery::PARTICIPANT_MESSAGE_QOS,
  ),
];

// Helper list for initializing remote standard (non-secure) built-in writers
pub const STANDARD_BUILTIN_WRITERS_INIT_LIST: &[(EntityId, EntityId, u32, QosPolicies)] = &[
  (
    EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER, // SPDP
    EntityId::SPDP_BUILTIN_PARTICIPANT_READER,
    BuiltinEndpointSet::PARTICIPANT_ANNOUNCER,
    Discovery::create_spdp_participant_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER, // SEDP ...
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER,
    BuiltinEndpointSet::PUBLICATIONS_ANNOUNCER,
    Discovery::publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
    EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
    BuiltinEndpointSet::PUBLICATIONS_ANNOUNCER,
    Discovery::publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_TOPIC_WRITER,
    EntityId::SEDP_BUILTIN_TOPIC_READER,
    BuiltinEndpointSet::TOPICS_ANNOUNCER,
    Discovery::publisher_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER,
    BuiltinEndpointSet::PARTICIPANT_MESSAGE_DATA_WRITER,
    Discovery::PARTICIPANT_MESSAGE_QOS,
  ),
];

// Helper list for initializing the authentication topic built-in reader
#[cfg(feature = "security")]
pub const AUTHENTICATION_BUILTIN_READERS_INIT_LIST: &[(EntityId, EntityId, u32, QosPolicies)] =
  &[(
    EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER,
    BuiltinEndpointSet::PARTICIPANT_STATELESS_MESSAGE_READER,
    Discovery::create_participant_stateless_message_qos(),
  )];

// Helper list for initializing the authentication topic built-in writer
#[cfg(feature = "security")]
pub const AUTHENTICATION_BUILTIN_WRITERS_INIT_LIST: &[(EntityId, EntityId, u32, QosPolicies)] =
  &[(
    EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER,
    BuiltinEndpointSet::PARTICIPANT_STATELESS_MESSAGE_WRITER,
    Discovery::create_participant_stateless_message_qos(),
  )];

// Helper list for initializing remote secure built-in readers
#[cfg(feature = "security")]
pub const SECURE_BUILTIN_READERS_INIT_LIST: &[(EntityId, EntityId, u32, QosPolicies)] = &[
  (
    EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_WRITER, // SPDP
    EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_SECURE_READER,
    Discovery::subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_WRITER, // SEDP ...
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_READER,
    BuiltinEndpointSet::PUBLICATIONS_SECURE_READER,
    Discovery::subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_WRITER,
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_READER,
    BuiltinEndpointSet::SUBSCRIPTIONS_SECURE_READER,
    Discovery::subscriber_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_MESSAGE_SECURE_READER,
    Discovery::subscriber_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_VOLATILE_MESSAGE_SECURE_READER,
    Discovery::create_participant_volatile_message_secure_qos(),
  ),
];

// Helper list for initializing remote secure built-in writers
#[cfg(feature = "security")]
pub const SECURE_BUILTIN_WRITERS_INIT_LIST: &[(EntityId, EntityId, u32, QosPolicies)] = &[
  (
    EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_WRITER, // SPDP
    EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_SECURE_WRITER,
    Discovery::publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_WRITER, // SEDP ...
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_READER,
    BuiltinEndpointSet::PUBLICATIONS_SECURE_WRITER,
    Discovery::publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_WRITER,
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_READER,
    BuiltinEndpointSet::SUBSCRIPTIONS_SECURE_WRITER,
    Discovery::publisher_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_MESSAGE_SECURE_WRITER,
    Discovery::publisher_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_VOLATILE_MESSAGE_SECURE_WRITER,
    Discovery::create_participant_volatile_message_secure_qos(),
  ),
];

// EntityIds for built-in readers with secured communication
// See the definition of “Builtin Secure Endpoints” in the Security spec
// This list is used for detecting if a built-in reader needs to be secure.
// TODO: STANDARD_BUILTIN_READERS_INIT_LIST already contains these
// EntityIds. Could we use that list directly and get rid of this one?
#[cfg(feature = "security")]
pub const SECURE_BUILTIN_READER_ENTITY_IDS: &[EntityId] = &[
  EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_READER,
  EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_READER,
  EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_READER,
  EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_READER,
  EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_READER,
];

// EntityIds for built-in writers with secured communication
// This list is used for detecting if a built-in writer needs to be secure.
// TODO: STANDARD_BUILTIN_WRITERS_INIT_LIST already contains these
// EntityIds. Could we use that list directly and get rid of this one?
#[cfg(feature = "security")]
pub const SECURE_BUILTIN_WRITER_ENTITY_IDS: &[EntityId] = &[
  EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_WRITER,
  EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_WRITER,
  EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_WRITER,
  EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_WRITER,
  EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER,
];

// Poll token constants list.

// The numbering of these constants must not exceed the range allowed in token
// decoding in the definition of EntityId.
// The current maximum is PTB+79 . Do not define higher numbers here without
// modifying EntityId and EntityKind.
//
// The poll tokens defined above are just arbitrary names used to correlate
// poll registrations and invocations. Their order is not relevant.

const PTB: usize = EntityKind::POLL_TOKEN_BASE;

pub const STOP_POLL_TOKEN: Token = Token(PTB);

// pub const DISCOVERY_SENDER_TOKEN: Token = Token(1 + PTB);
// pub const USER_TRAFFIC_SENDER_TOKEN: Token = Token(2 + PTB);

// pub const DATA_SEND_TOKEN: Token = Token(5 + PTB);

pub const DISCOVERY_LISTENER_TOKEN: Token = Token(6 + PTB);
pub const DISCOVERY_MUL_LISTENER_TOKEN: Token = Token(7 + PTB);
pub const USER_TRAFFIC_LISTENER_TOKEN: Token = Token(8 + PTB);
pub const USER_TRAFFIC_MUL_LISTENER_TOKEN: Token = Token(9 + PTB);

pub const ADD_READER_TOKEN: Token = Token(10 + PTB);
pub const REMOVE_READER_TOKEN: Token = Token(11 + PTB);

// pub const READER_CHANGE_TOKEN: Token = Token(12 + PTB);
// pub const DATAREADER_CHANGE_TOKEN: Token = Token(13 + PTB);

// pub const ADD_DATAREADER_TOKEN: Token = Token(14 + PTB);
// pub const REMOVE_DATAREADER_TOKEN: Token = Token(15 + PTB);

pub const ADD_WRITER_TOKEN: Token = Token(16 + PTB);
pub const REMOVE_WRITER_TOKEN: Token = Token(17 + PTB);

// pub const ADD_DATAWRITER_TOKEN: Token = Token(18 + PTB);
// pub const REMOVE_DATAWRITER_TOKEN: Token = Token(19 + PTB);

pub const ACKNACK_MESSAGE_TO_LOCAL_WRITER_TOKEN: Token = Token(20 + PTB);

pub const DISCOVERY_UPDATE_NOTIFICATION_TOKEN: Token = Token(21 + PTB);
pub const DISCOVERY_COMMAND_TOKEN: Token = Token(22 + PTB);
pub const SPDP_LIVENESS_TOKEN: Token = Token(23 + PTB);

pub const DISCOVERY_PARTICIPANT_DATA_TOKEN: Token = Token(30 + PTB);
pub const DISCOVERY_PARTICIPANT_CLEANUP_TOKEN: Token = Token(31 + PTB);
pub const DISCOVERY_SEND_PARTICIPANT_INFO_TOKEN: Token = Token(32 + PTB);
pub const DISCOVERY_READER_DATA_TOKEN: Token = Token(33 + PTB);
pub const DISCOVERY_WRITER_DATA_TOKEN: Token = Token(35 + PTB);
pub const DISCOVERY_TOPIC_DATA_TOKEN: Token = Token(37 + PTB);
pub const DISCOVERY_TOPIC_CLEANUP_TOKEN: Token = Token(38 + PTB);
pub const DISCOVERY_PARTICIPANT_MESSAGE_TOKEN: Token = Token(40 + PTB);
pub const DISCOVERY_PARTICIPANT_MESSAGE_TIMER_TOKEN: Token = Token(41 + PTB);

pub const DPEV_ACKNACK_TIMER_TOKEN: Token = Token(45 + PTB);
pub const DPEV_CACHE_CLEAN_TIMER_TOKEN: Token = Token(46 + PTB);

pub const SECURE_DISCOVERY_PARTICIPANT_DATA_TOKEN: Token = Token(50 + PTB);
// pub const DISCOVERY_PARTICIPANT_CLEANUP_TOKEN: Token = Token(51 + PTB);
pub const SECURE_DISCOVERY_READER_DATA_TOKEN: Token = Token(53 + PTB);
pub const SECURE_DISCOVERY_WRITER_DATA_TOKEN: Token = Token(55 + PTB);
pub const P2P_SECURE_DISCOVERY_PARTICIPANT_MESSAGE_TOKEN: Token = Token(60 + PTB);

pub const P2P_PARTICIPANT_STATELESS_MESSAGE_TOKEN: Token = Token(62 + PTB);
pub const CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_TIMER_TOKEN: Token = Token(63 + PTB);
pub const P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_TOKEN: Token = Token(64 + PTB);

// See note about maximum allowed number above.

pub struct TokenReceiverPair<T> {
  pub token: Token,
  pub receiver: mio_channel::Receiver<T>,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum DiscoveryNotificationType {
  ReaderUpdated {
    discovered_reader_data: DiscoveredReaderData,
  },
  ReaderLost {
    reader_guid: GUID,
  },
  WriterUpdated {
    discovered_writer_data: DiscoveredWriterData,
  },
  WriterLost {
    writer_guid: GUID,
  },
  ParticipantUpdated {
    guid_prefix: GuidPrefix,
  },
  ParticipantLost {
    guid_prefix: GuidPrefix,
  },
  AssertTopicLiveliness {
    writer_guid: GUID,
    manual_assertion: bool,
  },
  #[cfg(feature = "security")]
  ParticipantAuthenticationStatusChanged {
    guid_prefix: GuidPrefix,
  },
}

pub mod builtin_topic_names {
  // DDS-RTPS 2.5: 8.5.2
  pub const DCPS_PARTICIPANT: &str = "DCPSParticipant";
  pub const DCPS_PUBLICATION: &str = "DCPSPublication";
  pub const DCPS_SUBSCRIPTION: &str = "DCPSSubscription";
  pub const DCPS_TOPIC: &str = "DCPSTopic";
  // DDS-RTPS 2.5: 8.4.13.4
  pub const DCPS_PARTICIPANT_MESSAGE: &str = "DCPSParticipantMessage";

  // DDS-SECURITY 1.1: 7.4
  pub const DCPS_PARTICIPANT_SECURE: &str = "DCPSParticipantSecure";
  pub const DCPS_PUBLICATIONS_SECURE: &str = "DCPSPublicationsSecure";
  pub const DCPS_SUBSCRIPTIONS_SECURE: &str = "DCPSSubscriptionsSecure";
  pub const DCPS_PARTICIPANT_MESSAGE_SECURE: &str = "DCPSParticipantMessageSecure";
  pub const DCPS_PARTICIPANT_STATELESS_MESSAGE: &str = "DCPSParticipantStatelessMessage";
  pub const DCPS_PARTICIPANT_VOLATILE_MESSAGE_SECURE: &str = "DCPSParticipantVolatileMessageSecure";
}

// topic type name over RTPS
pub mod builtin_topic_type_names {
  pub const DCPS_PARTICIPANT: &str = "SPDPDiscoveredParticipantData";
  pub const DCPS_PUBLICATION: &str = "DiscoveredWriterData";
  pub const DCPS_SUBSCRIPTION: &str = "DiscoveredReaderData";
  pub const DCPS_TOPIC: &str = "DiscoveredTopicData";

  pub const DCPS_PARTICIPANT_MESSAGE: &str = "ParticipantMessageData";

  pub const DCPS_PARTICIPANT_SECURE: &str = "ParticipantBuiltinTopicDataSecure";
  pub const DCPS_PUBLICATIONS_SECURE: &str = "PublicationBuiltinTopicDataSecure";
  pub const DCPS_SUBSCRIPTIONS_SECURE: &str = "SubscriptionBuiltinTopicDataSecure";
  pub const DCPS_PARTICIPANT_MESSAGE_SECURE: &str = "ParticipantMessageData";
  pub const DCPS_PARTICIPANT_STATELESS_MESSAGE: &str = "ParticipantStatelessMessage";
  pub const DCPS_PARTICIPANT_VOLATILE_MESSAGE_SECURE: &str = "ParticipantVolatileMessageSecure";
}
