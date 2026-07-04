use std::time::Duration;

use mio_06::Token;
use mio_extras::channel as mio_channel;

use crate::{
  discovery::{
    builtin_endpoint::BuiltinEndpointSet,
    discovery::Discovery,
    sedp_messages::{DiscoveredReaderData, DiscoveredWriterData},
  },
  rtps::outbound::SocketId,
  structure::guid::{EntityId, EntityKind, GuidPrefix, GUID},
  QosPolicies,
};

pub const PREEMPTIVE_ACKNACK_PERIOD: Duration = Duration::from_secs(5);

pub const CACHE_CLEAN_PERIOD: Duration = Duration::from_secs(4);

// RTPS spec Section 8.4.7.1.1  "Default Timing-Related Values"
pub const NACK_RESPONSE_DELAY: Duration = Duration::from_millis(200);
pub const NACK_SUPPRESSION_DURATION: Duration = Duration::from_millis(0);

// Periodic HEARTBEAT period for reliable Writers. The period is adaptive: while
// some matched reader is behind (has unacknowledged samples), heartbeats are
// sent at the FAST period to prompt prompt repair (some peers rely on a
// standalone HEARTBEAT, not the one piggybacked on DATA). Once all readers have
// acknowledged everything, the writer backs off to the SLOW period to keep
// idle-traffic low.
// Note: these use the RTPS `Duration` type (not `std::time::Duration`) to match
// the Writer's `heartbeat_period` field.
pub const HEARTBEAT_PERIOD_SLOW: crate::structure::duration::Duration =
  crate::structure::duration::Duration::from_secs(1);
pub const HEARTBEAT_PERIOD_FAST: crate::structure::duration::Duration =
  crate::structure::duration::Duration::from_millis(100);

// Fallback upper bound on the number of CacheChanges a Writer retains in its
// history when the Writer QoS does not specify ResourceLimits.max_samples. This
// is only a memory-safety backstop: for reliable Writers, samples that matched
// reliable readers have not yet acknowledged are retained up to this bound (so
// they remain available for repair) rather than being evicted eagerly.
pub const DEFAULT_WRITER_MAX_SAMPLES: usize = 8192;

// Memory-safety backstop for the per-matched-reader set of sequence numbers a
// Writer still intends to send (`RtpsReaderProxy::unsent_changes`). In normal
// operation this set is pruned as samples are pushed or acknowledged, but a
// best-effort flood (no ACKNACKs) or a pathological peer could otherwise let it
// grow without bound. When the set exceeds this cap, the oldest entries are
// dropped (they are the least useful to retransmit).
pub const MAX_UNSENT_CHANGES_PER_READER: usize = DEFAULT_WRITER_MAX_SAMPLES;

// Memory-safety backstop for the per-matched-writer map of received/irrelevant
// sequence numbers a Reader tracks (`RtpsWriterProxy::changes`). Entries below
// `ack_base` are pruned eagerly, but under best-effort loss `ack_base` can stall
// at a permanently missing sample while received sequence numbers pile up above
// it. When the map exceeds this cap, `ack_base` is forced forward past the
// oldest gap (those old samples are given up as lost) so the map stays bounded.
pub const MAX_TRACKED_CHANGES_PER_WRITER: usize = DEFAULT_WRITER_MAX_SAMPLES;

// Upper bound on the serialized size (RTPS header + submessages) of an
// aggregated datagram built by the writer's DATA-coalescing path. Kept below a
// typical Ethernet MTU (1500) minus IPv4 (20) + UDP (8) headers = 1472, with a
// small margin, so aggregated datagrams do not trigger IP fragmentation. Only
// unfragmented samples are coalesced; a single sample larger than this still
// goes out on its own (equivalent to the non-aggregated path).
pub const MAX_AGGREGATED_DATAGRAM_SIZE: usize = 1452;

// Serialized size of a trailing HEARTBEAT submessage (4-byte submessage header +
// readerId 4 + writerId 4 + firstSN 8 + lastSN 8 + count 4). The coalescing loop
// reserves this much of the datagram budget for reliable writers so the single
// trailing HEARTBEAT always fits after the last DATA.
pub const HEARTBEAT_SUBMESSAGE_SERIALIZED_SIZE: usize = 32;

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
    Discovery::builtin_subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
    EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
    BuiltinEndpointSet::PUBLICATIONS_DETECTOR,
    Discovery::builtin_subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_TOPIC_WRITER,
    EntityId::SEDP_BUILTIN_TOPIC_READER,
    BuiltinEndpointSet::TOPICS_DETECTOR,
    Discovery::builtin_subscriber_qos(),
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
    Discovery::builtin_publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
    EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
    BuiltinEndpointSet::PUBLICATIONS_ANNOUNCER,
    Discovery::builtin_publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_TOPIC_WRITER,
    EntityId::SEDP_BUILTIN_TOPIC_READER,
    BuiltinEndpointSet::TOPICS_ANNOUNCER,
    Discovery::builtin_publisher_qos(),
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
    Discovery::builtin_subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_WRITER, // SEDP ...
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_READER,
    BuiltinEndpointSet::PUBLICATIONS_SECURE_READER,
    Discovery::builtin_subscriber_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_WRITER,
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_READER,
    BuiltinEndpointSet::SUBSCRIPTIONS_SECURE_READER,
    Discovery::builtin_subscriber_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_MESSAGE_SECURE_READER,
    Discovery::builtin_subscriber_qos(),
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
    Discovery::builtin_publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_WRITER, // SEDP ...
    EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_READER,
    BuiltinEndpointSet::PUBLICATIONS_SECURE_WRITER,
    Discovery::builtin_publisher_qos(),
  ),
  (
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_WRITER,
    EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_READER,
    BuiltinEndpointSet::SUBSCRIPTIONS_SECURE_WRITER,
    Discovery::builtin_publisher_qos(),
  ),
  (
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_WRITER,
    EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_READER,
    BuiltinEndpointSet::PARTICIPANT_MESSAGE_SECURE_WRITER,
    Discovery::builtin_publisher_qos(),
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

// Single shared timer for the whole discovery event loop. All periodic
// discovery tasks (SPDP publish, participant/topic cleanup, participant message
// publish, secure message resend) fire through this one timer (one background
// thread), replacing the former per-topic and per-task timers.
pub const DISCOVERY_TIMER_TOKEN: Token = Token(42 + PTB);

pub const DPEV_ACKNACK_TIMER_TOKEN: Token = Token(45 + PTB);
pub const DPEV_CACHE_CLEAN_TIMER_TOKEN: Token = Token(46 + PTB);

// Single shared timer for the whole dp_event_loop. All Reader/Writer timeouts
// plus the periodic preemptive-ACKNACK and DDSCache GC timeouts fire through
// this one timer (and thus one background thread), replacing the former
// per-endpoint timers and the separate acknack/cache-clean timers.
pub const DPEV_TIMER_TOKEN: Token = Token(47 + PTB);

pub const SECURE_DISCOVERY_PARTICIPANT_DATA_TOKEN: Token = Token(50 + PTB);
// pub const DISCOVERY_PARTICIPANT_CLEANUP_TOKEN: Token = Token(51 + PTB);
pub const SECURE_DISCOVERY_READER_DATA_TOKEN: Token = Token(53 + PTB);
pub const SECURE_DISCOVERY_WRITER_DATA_TOKEN: Token = Token(55 + PTB);
pub const P2P_SECURE_DISCOVERY_PARTICIPANT_MESSAGE_TOKEN: Token = Token(60 + PTB);

pub const P2P_PARTICIPANT_STATELESS_MESSAGE_TOKEN: Token = Token(62 + PTB);
pub const CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_TIMER_TOKEN: Token = Token(63 + PTB);
pub const P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_TOKEN: Token = Token(64 + PTB);

// nonblocking-transmit: write-readiness tokens for the sender sockets. The
// unicast socket uses SENDER_WRITABLE_BASE; each multicast interface socket i
// uses SENDER_WRITABLE_BASE + 1 + i. These are fixed poll tokens and must stay
// within the PTB+79 maximum, so at most 13 multicast sender sockets are
// supported (in practice one per multicast-capable local interface).
// (see src/rtps/nonblocking_transmit_design.md)
pub const SENDER_WRITABLE_BASE: usize = 65 + PTB;
pub const SENDER_WRITABLE_MAX: usize = 79 + PTB;

/// The fixed poll token used to watch a sender socket for write readiness.
pub fn sender_writable_token(id: SocketId) -> Token {
  match id {
    SocketId::Unicast => Token(SENDER_WRITABLE_BASE),
    SocketId::Multicast(i) => Token(SENDER_WRITABLE_BASE + 1 + i),
  }
}

/// Decode a fixed poll token back into the sender socket it watches, if any.
pub fn sender_writable_socket_id(token: Token) -> Option<SocketId> {
  if token.0 < SENDER_WRITABLE_BASE || token.0 > SENDER_WRITABLE_MAX {
    return None;
  }
  if token.0 == SENDER_WRITABLE_BASE {
    Some(SocketId::Unicast)
  } else {
    Some(SocketId::Multicast(token.0 - SENDER_WRITABLE_BASE - 1))
  }
}

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
