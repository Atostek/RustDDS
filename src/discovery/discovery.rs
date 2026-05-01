use std::{
  sync::{Arc, RwLock},
  time::Duration as StdDuration,
};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use mio_06::{Events, Poll, PollOpt, Ready};
use mio_extras::{channel as mio_channel, timer::Timer};
use paste::paste; // token pasting macro

use crate::{
  dds::{
    participant::DomainParticipantWeak,
    qos::{
      policy::{
        Deadline, DestinationOrder, Durability, History, Liveliness, Ownership, Presentation,
        PresentationAccessScope, Reliability, TimeBasedFilter,
      },
      QosPolicies, QosPolicyBuilder,
    },
    readcondition::ReadCondition,
    result::{CreateError, CreateResult},
    statusevents::{DomainParticipantStatusEvent, LostReason, StatusChannelSender},
  },
  discovery::{
    discovery_db::{discovery_db_read, discovery_db_write, DiscoveredVia, DiscoveryDB},
    sedp_messages::{
      DiscoveredReaderData, DiscoveredTopicData, DiscoveredWriterData, Endpoint_GUID,
      ParticipantMessageData, ParticipantMessageDataKind,
    },
    spdp_participant_data::{Participant_GUID, SpdpDiscoveredParticipantData},
  },
  polling::{new_simple_timer, TimerPolicy},
  rtps::constant::*,
  serialization::{pl_cdr_adapters::*, CDRDeserializerAdapter, CDRSerializerAdapter},
  structure::{
    duration::Duration,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    time::Timestamp,
  },
  with_key::{DataReader, DataWriter, Sample},
  DomainParticipant,
};
// This module implements the control logic of the Discovery process.
//
// The built-in discovery consists of
// Simple Participant Discovery Protocol (SPDP), (RTPS spec v2.5
// Section "8.5.3 The Simple Participant Discovery Protocol") and
// Simple Endpoint Discovery Protocol (SEDP) (RTPS spec v2.5
// Section "8.5.4 The Simple Endpoint Discovery Protocol")
//
// Notes:
//
// SPDP is periodically broadcast from each DomainParticipant to announce presence to others.
// It is essentially stateless and best-effort. It also announces built-in endpoints.
// See module spdp_participant_data for details.
//
// SEDP is Reliable transfer of user-defined endpoints (Readers, Writers and Topics).
// Many DDS implementations do not communicate Topics at all, and seem to work just fine.
//
// "Participant Message" protocol is NOT the same as SPDP. See RTPS spec v2.5 Section
// "8.4.13 Writer Liveliness Protocol". It can be used as liveliness (hearbeat)
// signalling of individual Writers.
//
// "Participant Stateless Message" and "Participant Volatile Message" only appear
// in Secure RTPS. Please see the RTPS Security specification for explanation.
#[cfg(feature = "security")]
use crate::{
  discovery::secure_discovery::SecureDiscovery,
  security::{security_plugins::SecurityPluginsHandle, types::*},
};
#[cfg(not(feature = "security"))]
use crate::no_security::*;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DiscoveryCommand {
  StopDiscovery,
  AddLocalWriter {
    guid: GUID,
  },
  AddLocalReader {
    guid: GUID,
  },
  AddTopic {
    topic_name: String,
  },
  RemoveLocalWriter {
    guid: GUID,
  },
  RemoveLocalReader {
    guid: GUID,
  },
  ManualAssertLiveliness,
  AssertTopicLiveliness {
    writer_guid: GUID,
    manual_assertion: bool,
  },

  #[cfg(feature = "security")]
  StartKeyExchangeWithRemoteParticipant {
    participant_guid_prefix: GuidPrefix,
  },

  #[cfg(feature = "security")]
  StartKeyExchangeWithRemoteEndpoint {
    local_endpoint_guid: GUID,
    remote_endpoint_guid: GUID,
  },
}

pub struct LivelinessState {
  last_auto_update: Timestamp,
  manual_participant_liveness_refresh_requested: bool,
}

impl LivelinessState {
  pub fn new() -> Self {
    Self {
      last_auto_update: Timestamp::now(),
      manual_participant_liveness_refresh_requested: false,
    }
  }
}

// TODO: Refactor this. Maybe the repeating groups of "topic", "reader",
// "writer", "timer" below could be abstracted to a common struct:

pub(super) type DataReaderPlCdr<D> = DataReader<D, PlCdrDeserializerAdapter<D>>;
pub(super) type DataWriterPlCdr<D> = DataWriter<D, PlCdrSerializerAdapter<D>>;

mod with_key {
  use serde::{de::DeserializeOwned, Serialize};
  use mio_extras::timer::Timer;

  use super::{DataReaderPlCdr, DataWriterPlCdr};
  use crate::{
    polling::TimerPolicy, serialization::pl_cdr_adapters::*, Key, Keyed, Topic, TopicKind,
  };

  pub const TOPIC_KIND: TopicKind = TopicKind::WithKey;

  pub(super) struct DiscoveryTopicPlCdr<D>
  where
    D: Keyed + PlCdrSerialize + PlCdrDeserialize,
    <D as Keyed>::K: Key + PlCdrSerialize + PlCdrDeserialize,
  {
    #[allow(dead_code)] // The topic may not be accessed after initialization
    pub topic: Topic,
    pub reader: DataReaderPlCdr<D>,
    pub writer: DataWriterPlCdr<D>,
    pub timer: Timer<TimerPolicy>,
  }

  pub(super) struct DiscoveryTopicCDR<D>
  where
    D: Keyed + Serialize + DeserializeOwned,
    <D as Keyed>::K: Key + Serialize + DeserializeOwned,
  {
    #[allow(dead_code)] // The topic may not be accessed after initialization
    pub topic: Topic,
    pub reader: crate::with_key::DataReaderCdr<D>,
    pub writer: crate::with_key::DataWriterCdr<D>,
    pub timer: Timer<TimerPolicy>,
  }
}

#[cfg(feature = "security")] // only used with security feature for now, this is to avoid warning
mod no_key {
  use serde::{de::DeserializeOwned, Serialize};
  use mio_extras::timer::Timer;

  use crate::{polling::TimerPolicy, Topic, TopicKind};

  pub const TOPIC_KIND: TopicKind = TopicKind::NoKey;

  pub(super) struct DiscoveryTopicCDR<D>
  where
    D: Serialize + DeserializeOwned,
  {
    #[allow(dead_code)] // The topic may not be accessed after initialization
    pub topic: Topic,
    pub reader: crate::no_key::DataReader<D, crate::CDRDeserializerAdapter<D>>,
    pub writer: crate::no_key::DataWriter<D, crate::CDRSerializerAdapter<D>>,
    #[allow(dead_code)] // Timers currently not used for no_key discovery topics
    pub timer: Timer<TimerPolicy>,
  }
}

// Enum indicating if secure discovery allows normal discovery to process
// something
#[derive(PartialEq)]
pub(crate) enum NormalDiscoveryPermission {
  Allow,
  #[cfg(feature = "security")] // Deny variant is never constructed if security feature is not on
  Deny,
}

pub(crate) struct Discovery {
  poll: Poll,
  domain_participant: DomainParticipantWeak,
  discovery_db: Arc<RwLock<DiscoveryDB>>,

  // Discovery started sender confirms to application thread that we are running
  discovery_started_sender: std::sync::mpsc::Sender<CreateResult<()>>,
  // notification sender goes to dp_event_loop thread
  discovery_updated_sender: mio_channel::SyncSender<DiscoveryNotificationType>,
  // Discovery gets commands from dp_event_loop from this channel
  discovery_command_receiver: mio_channel::Receiver<DiscoveryCommand>,
  spdp_liveness_receiver: mio_channel::Receiver<GuidPrefix>,

  liveliness_state: LivelinessState,

  participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,

  // DDS Subscriber and Publisher for Discovery
  // ...but these are not actually used after initialization
  // discovery_subscriber: Subscriber,
  // discovery_publisher: Publisher,

  // Handling of "DCPSParticipant" topic. This is the mother of all topics
  // where participants announce their presence and built-in readers and writers.
  // and
  // timer to periodically announce our presence
  dcps_participant: with_key::DiscoveryTopicPlCdr<SpdpDiscoveredParticipantData>,
  participant_cleanup_timer: Timer<()>, // garbage collection timer for dead remote participants

  // Topic "DCPSSubscription" - announcing and detecting Readers
  dcps_subscription: with_key::DiscoveryTopicPlCdr<DiscoveredReaderData>,

  // Topic "DCPSPublication" - announcing and detecting Writers
  dcps_publication: with_key::DiscoveryTopicPlCdr<DiscoveredWriterData>,

  // Topic "DCPSTopic" - announcing and detecting topics
  dcps_topic: with_key::DiscoveryTopicPlCdr<DiscoveredTopicData>,
  topic_cleanup_timer: Timer<()>,

  // DCPSParticipantMessage - used by participants to communicate liveness
  dcps_participant_message: with_key::DiscoveryTopicCDR<ParticipantMessageData>,

  // If security is enabled, this field contains a SecureDiscovery struct, an appendix
  // which is used for Secure functionality
  security_opt: Option<SecureDiscovery>,

  // Following topics from DDS Security spec v1.1

  // DCPSParticipantSecure - 7.4.1.6 New DCPSParticipantSecure Builtin Topic
  #[cfg(feature = "security")]
  dcps_participant_secure: with_key::DiscoveryTopicPlCdr<ParticipantBuiltinTopicDataSecure>,

  // DCPSPublicationsSecure - 7.4.1.7 New DCPSPublicationsSecure Builtin Topic
  #[cfg(feature = "security")]
  dcps_publications_secure: with_key::DiscoveryTopicPlCdr<PublicationBuiltinTopicDataSecure>,

  // DCPSSubscriptionsSecure - 7.4.1.8 New DCPSSubscriptionsSecure Builtin Topic
  #[cfg(feature = "security")]
  dcps_subscriptions_secure: with_key::DiscoveryTopicPlCdr<SubscriptionBuiltinTopicDataSecure>,

  // DCPSParticipantMessageSecure - used by participants to communicate secure liveness
  // 7.4.2 New DCPSParticipantMessageSecure builtin Topic
  #[cfg(feature = "security")]
  dcps_participant_message_secure: with_key::DiscoveryTopicCDR<ParticipantMessageData>, /* CDR, not PL_CDR */

  // DCPSParticipantStatelessMessageSecure
  // 7.4.3 New DCPSParticipantStatelessMessage builtin Topic
  #[cfg(feature = "security")]
  dcps_participant_stateless_message: no_key::DiscoveryTopicCDR<ParticipantStatelessMessage>,

  // DCPSParticipantVolatileMessageSecure
  // 7.4.4 New DCPSParticipantVolatileMessageSecure builtin Topic
  #[cfg(feature = "security")]
  dcps_participant_volatile_message_secure:
    no_key::DiscoveryTopicCDR<ParticipantVolatileMessageSecure>, // CDR?

  #[cfg(feature = "security")]
  cached_secure_discovery_messages_resend_timer: Timer<()>,
}

impl Discovery {
  const PARTICIPANT_CLEANUP_PERIOD: StdDuration = StdDuration::from_secs(2);
  const TOPIC_CLEANUP_PERIOD: StdDuration = StdDuration::from_secs(60); // timer for cleaning up inactive topics
  const SPDP_PUBLISH_PERIOD: StdDuration = StdDuration::from_secs(10);
  const CHECK_PARTICIPANT_MESSAGES: StdDuration = StdDuration::from_secs(1);
  #[cfg(feature = "security")]
  const CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_PERIOD: StdDuration = StdDuration::from_secs(1);

  #[allow(clippy::too_many_arguments)]
  pub fn new(
    domain_participant: DomainParticipantWeak,
    discovery_db: Arc<RwLock<DiscoveryDB>>,
    discovery_started_sender: std::sync::mpsc::Sender<CreateResult<()>>,
    discovery_updated_sender: mio_channel::SyncSender<DiscoveryNotificationType>,
    discovery_command_receiver: mio_channel::Receiver<DiscoveryCommand>,
    spdp_liveness_receiver: mio_channel::Receiver<GuidPrefix>,
    participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
    security_plugins_opt: Option<SecurityPluginsHandle>,
  ) -> CreateResult<Self> {
    // helper macro to handle initialization failures.
    macro_rules! try_construct {
      ($constructor:expr, $msg:literal) => {
        match $constructor {
          Ok(r) => r,
          Err(e) => {
            error!("{} {:?}", $msg, e);
            discovery_started_sender
              .send(Err(CreateError::OutOfResources {
                reason: $msg.to_string(),
              }))
              .unwrap_or(()); // We are trying to quit. If send fails, just ignore it.
            return Err(CreateError::OutOfResources {
              reason: $msg.to_string(),
            });
          }
        }
      };
    }

    let poll = try_construct!(mio_06::Poll::new(), "Failed to allocate discovery poll.");
    let discovery_subscriber_qos = Self::builtin_subscriber_qos();
    let discovery_publisher_qos = Self::builtin_publisher_qos();

    // Create DDS Publisher and Subscriber for Discovery.
    // These are needed to create DataWriter and DataReader objects
    let discovery_subscriber = try_construct!(
      domain_participant.create_subscriber(&discovery_subscriber_qos),
      "Unable to create Discovery Subscriber."
    );
    let discovery_publisher = try_construct!(
      domain_participant.create_publisher(&discovery_publisher_qos),
      "Unable to create Discovery Publisher."
    );

    macro_rules! construct_topic_and_poll {
      ( $repr:ident, $has_key:ident,
        $topic_name:expr, $topic_type_name:expr, $message_type:ty,
        $endpoint_qos_opt:expr,
        $stateless_RTPS:expr,
        $reader_entity_id:expr, $reader_token:expr,
        $writer_entity_id:expr,
        $timeout_and_timer_token_opt:expr, ) => {{
        let endpoint_qos_opt_bind = $endpoint_qos_opt;
        let topic_qos_ref = if let Some(qos) = endpoint_qos_opt_bind.as_ref() {
          qos
        } else {
          &discovery_subscriber_qos
        };

        let publisher_qos: QosPolicies = if let Some(qos) = endpoint_qos_opt_bind.as_ref() {
          qos.clone()
        } else {
          discovery_publisher_qos.clone()
        };

        let topic = domain_participant
          .create_topic(
            $topic_name.to_string(),
            $topic_type_name.to_string(),
            topic_qos_ref,
            $has_key::TOPIC_KIND,
          )
          .expect("Unable to create topic. ");
        paste! {
          let reader =
            discovery_subscriber
            . [< create_datareader_with_entity_id_ $has_key >]
              ::<$message_type, [<$repr DeserializerAdapter>] <$message_type>>(
              &topic,
              $reader_entity_id,
              $endpoint_qos_opt,
              $stateless_RTPS,
            ).expect("Unable to create DataReader. ");

          let writer =
              discovery_publisher.[< create_datawriter_with_entity_id_ $has_key >]
                ::<$message_type, [<$repr SerializerAdapter>] <$message_type>>(
                $writer_entity_id,
                &topic,
                Some(publisher_qos),
                $stateless_RTPS,
              ).expect("Unable to create DataWriter .");
        }
        poll
          .register(&reader, $reader_token, Ready::readable(), PollOpt::edge())
          .expect("Failed to register a discovery reader to poll.");

        let mut timer: Timer<TimerPolicy> = new_simple_timer();
        let timeout_and_timer_token_opt: Option<mio_06::Token> = $timeout_and_timer_token_opt;
        if let Some(timer_token) = timeout_and_timer_token_opt {
          timer.set_timeout(StdDuration::from_millis(100), TimerPolicy::Repeat);
          poll
            .register(&timer, timer_token, Ready::readable(), PollOpt::edge())
            .expect("Unable to register timer token. ");
        }
        paste! { $has_key ::[<DiscoveryTopic $repr>] { topic, reader, writer, timer } }
      }}; // macro
    }

    try_construct!(
      poll.register(
        &discovery_command_receiver,
        DISCOVERY_COMMAND_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      ),
      "Failed to register Discovery poll."
    );

    try_construct!(
      poll.register(
        &spdp_liveness_receiver,
        SPDP_LIVENESS_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      ),
      "Failed to register Discovery poll."
    );

    // Participant
    let dcps_participant = construct_topic_and_poll!(
      PlCdr,
      with_key,
      builtin_topic_names::DCPS_PARTICIPANT,
      builtin_topic_type_names::DCPS_PARTICIPANT,
      SpdpDiscoveredParticipantData,
      Some(Self::create_spdp_participant_qos()),
      false, // Regular stateful RTPS Reader & Writer
      EntityId::SPDP_BUILTIN_PARTICIPANT_READER,
      DISCOVERY_PARTICIPANT_DATA_TOKEN,
      EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER,
      Some(DISCOVERY_SEND_PARTICIPANT_INFO_TOKEN),
    );

    // create lease duration check timer
    let mut participant_cleanup_timer: Timer<()> = new_simple_timer();
    participant_cleanup_timer.set_timeout(Self::PARTICIPANT_CLEANUP_PERIOD, ());
    try_construct!(
      poll.register(
        &participant_cleanup_timer,
        DISCOVERY_PARTICIPANT_CLEANUP_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      ),
      "Unable to create participant cleanup timer."
    );

    // Subscriptions: What are the Readers on the network and what are they
    // subscribing to?
    let dcps_subscription = construct_topic_and_poll!(
      PlCdr,
      with_key,
      builtin_topic_names::DCPS_SUBSCRIPTION,
      builtin_topic_type_names::DCPS_SUBSCRIPTION,
      DiscoveredReaderData,
      None,  // QoS
      false, // Regular stateful RTPS Reader & Writer
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_READER,
      DISCOVERY_READER_DATA_TOKEN,
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_WRITER,
      None, // No timer
    );

    // Publication : Who are the Writers here and elsewhere
    let dcps_publication = construct_topic_and_poll!(
      PlCdr,
      with_key,
      builtin_topic_names::DCPS_PUBLICATION,
      builtin_topic_type_names::DCPS_PUBLICATION,
      DiscoveredWriterData,
      None,  // QoS,
      false, // Regular stateful RTPS Reader & Writer
      EntityId::SEDP_BUILTIN_PUBLICATIONS_READER,
      DISCOVERY_WRITER_DATA_TOKEN,
      EntityId::SEDP_BUILTIN_PUBLICATIONS_WRITER,
      None, // No timer
    );

    // Topic topic (not a typo)
    let dcps_topic = construct_topic_and_poll!(
      PlCdr,
      with_key,
      builtin_topic_names::DCPS_TOPIC,
      builtin_topic_type_names::DCPS_TOPIC,
      DiscoveredTopicData,
      None,  // QoS,
      false, // Regular stateful RTPS Reader & Writer
      EntityId::SEDP_BUILTIN_TOPIC_READER,
      DISCOVERY_TOPIC_DATA_TOKEN,
      EntityId::SEDP_BUILTIN_TOPIC_WRITER,
      None, // No timer
    );

    // create lease duration check timer
    let mut topic_cleanup_timer: Timer<()> = new_simple_timer();
    topic_cleanup_timer.set_timeout(Self::TOPIC_CLEANUP_PERIOD, ());
    try_construct!(
      poll.register(
        &topic_cleanup_timer,
        DISCOVERY_TOPIC_CLEANUP_TOKEN,
        Ready::readable(),
        PollOpt::edge(),
      ),
      "Unable to register topic cleanup timer."
    );

    // Participant Message Data 8.4.13
    let dcps_participant_message = construct_topic_and_poll!(
      CDR,
      with_key,
      builtin_topic_names::DCPS_PARTICIPANT_MESSAGE,
      builtin_topic_type_names::DCPS_PARTICIPANT_MESSAGE,
      ParticipantMessageData,
      Some(Self::PARTICIPANT_MESSAGE_QOS),
      false, // Regular stateful RTPS Reader & Writer
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER,
      DISCOVERY_PARTICIPANT_MESSAGE_TOKEN,
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_WRITER,
      Some(DISCOVERY_PARTICIPANT_MESSAGE_TIMER_TOKEN),
    );

    // DDS Security

    // Participant
    #[cfg(feature = "security")]
    let dcps_participant_secure = construct_topic_and_poll!(
      PlCdr,
      with_key,
      builtin_topic_names::DCPS_PARTICIPANT_SECURE,
      builtin_topic_type_names::DCPS_PARTICIPANT_SECURE,
      ParticipantBuiltinTopicDataSecure,
      None,  // QoS
      false, // Regular stateful RTPS Reader & Writer
      EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_READER,
      SECURE_DISCOVERY_PARTICIPANT_DATA_TOKEN,
      EntityId::SPDP_RELIABLE_BUILTIN_PARTICIPANT_SECURE_WRITER,
      None, // No timer. Periodic sending is done simultaneously with the non-secure topic
    );

    // Subscriptions: What are the Readers on the network and what are they
    // subscribing to?
    #[cfg(feature = "security")]
    let dcps_subscriptions_secure = construct_topic_and_poll!(
      PlCdr,
      with_key,
      builtin_topic_names::DCPS_SUBSCRIPTIONS_SECURE,
      builtin_topic_type_names::DCPS_SUBSCRIPTIONS_SECURE,
      SubscriptionBuiltinTopicDataSecure,
      None,  // QoS
      false, // Regular stateful RTPS Reader & Writer
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_READER,
      SECURE_DISCOVERY_READER_DATA_TOKEN,
      EntityId::SEDP_BUILTIN_SUBSCRIPTIONS_SECURE_WRITER,
      None, // No timer
    );

    // Publication : Who are the Writers here and elsewhere
    #[cfg(feature = "security")]
    let dcps_publications_secure = construct_topic_and_poll!(
      PlCdr,
      with_key,
      builtin_topic_names::DCPS_PUBLICATIONS_SECURE,
      builtin_topic_type_names::DCPS_PUBLICATIONS_SECURE,
      PublicationBuiltinTopicDataSecure,
      None,  // QoS
      false, // Regular stateful RTPS Reader & Writer
      EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_READER,
      SECURE_DISCOVERY_WRITER_DATA_TOKEN,
      EntityId::SEDP_BUILTIN_PUBLICATIONS_SECURE_WRITER,
      None, // No timer
    );

    // p2p Participant message secure
    #[cfg(feature = "security")]
    let dcps_participant_message_secure = construct_topic_and_poll!(
      CDR,
      with_key,
      builtin_topic_names::DCPS_PARTICIPANT_MESSAGE_SECURE,
      builtin_topic_type_names::DCPS_PARTICIPANT_MESSAGE_SECURE,
      ParticipantMessageData, // actually reuse the non-secure data type
      None,                   // QoS
      false,                  // Regular stateful RTPS Reader & Writer
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_READER,
      P2P_SECURE_DISCOVERY_PARTICIPANT_MESSAGE_TOKEN,
      EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_SECURE_WRITER,
      None, // No timer. Periodic sending is done simultaneously with the non-secure topic
    );
    // p2p Participant stateless message, used for authentication and Diffie-Hellman
    // key exchange
    #[cfg(feature = "security")]
    let dcps_participant_stateless_message = construct_topic_and_poll!(
      CDR,
      no_key,
      builtin_topic_names::DCPS_PARTICIPANT_STATELESS_MESSAGE,
      builtin_topic_type_names::DCPS_PARTICIPANT_STATELESS_MESSAGE,
      ParticipantStatelessMessage,
      Some(Self::create_participant_stateless_message_qos()),
      true, // Important: STATELESS RTPS Reader & Writer (see Security spec. section 7.4.3)
      EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_READER,
      P2P_PARTICIPANT_STATELESS_MESSAGE_TOKEN,
      EntityId::P2P_BUILTIN_PARTICIPANT_STATELESS_WRITER,
      None, // No timer
    );

    // p2p Participant volatile message secure, used for key exchange
    // Used for distributing symmetric (AES) crypto keys
    #[cfg(feature = "security")]
    let dcps_participant_volatile_message_secure = construct_topic_and_poll!(
      CDR,
      no_key,
      builtin_topic_names::DCPS_PARTICIPANT_VOLATILE_MESSAGE_SECURE,
      builtin_topic_type_names::DCPS_PARTICIPANT_VOLATILE_MESSAGE_SECURE,
      ParticipantVolatileMessageSecure,
      Some(Self::create_participant_volatile_message_secure_qos()),
      false, // Regular stateful RTPS Reader & Writer
      EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_READER,
      P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_TOKEN,
      EntityId::P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_WRITER,
      None, // No timer.
    );

    // Create a timer to periodically check whether to resend any cached security
    // (authentication, key exchange) messages
    #[cfg(feature = "security")]
    let secure_message_resend_timer = {
      let mut secure_message_resend_timer: Timer<()> = new_simple_timer();
      secure_message_resend_timer
        .set_timeout(Self::CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_PERIOD, ());
      try_construct!(
        poll.register(
          &secure_message_resend_timer,
          CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_TIMER_TOKEN,
          Ready::readable(),
          PollOpt::edge(),
        ),
        "Unable to create secure message resend timer."
      );
      secure_message_resend_timer
    };

    #[cfg(not(feature = "security"))]
    let security_opt = security_plugins_opt.and(None); // = None, but avoid warning.

    #[cfg(feature = "security")]
    let security_opt = if let Some(plugins_handle) = security_plugins_opt {
      // Plugins is Some so security is enabled. Initialize SecureDiscovery
      let security = try_construct!(
        SecureDiscovery::new(&domain_participant, &discovery_db, plugins_handle),
        "Could not initialize Secure Discovery."
      );
      Some(security)
    } else {
      None // no security configured
    };

    Ok(Self {
      poll,
      domain_participant,
      discovery_db,
      discovery_started_sender,
      discovery_updated_sender,
      discovery_command_receiver,
      spdp_liveness_receiver,
      participant_status_sender,

      liveliness_state: LivelinessState::new(),

      // discovery_subscriber,
      // discovery_publisher,
      dcps_participant,
      participant_cleanup_timer, // SPDP
      dcps_subscription,
      dcps_publication, // SEDP
      dcps_topic,
      topic_cleanup_timer,      // SEDP
      dcps_participant_message, // liveliness messages

      security_opt,
      #[cfg(feature = "security")]
      dcps_participant_secure,
      #[cfg(feature = "security")]
      dcps_publications_secure,
      #[cfg(feature = "security")]
      dcps_subscriptions_secure,
      #[cfg(feature = "security")]
      dcps_participant_message_secure,
      #[cfg(feature = "security")]
      dcps_participant_stateless_message,
      #[cfg(feature = "security")]
      dcps_participant_volatile_message_secure,
      #[cfg(feature = "security")]
      cached_secure_discovery_messages_resend_timer: secure_message_resend_timer,
    })
  }

  pub fn discovery_event_loop(&mut self) {
    self.initialize_participant();

    // Send out info about user (=non-built-in) Writers and Readers that we have.
    // We are just initializing local participant, so likely there are none yet,
    // but better make sure.
    // TODO: Check if there can acually be any. If not, then this code is
    // unnecessary.
    let db = discovery_db_read(&self.discovery_db);
    for reader in db.get_all_local_topic_readers() {
      self.sedp_publish_single_user_reader(reader);
    }
    for writer in db.get_all_local_topic_writers() {
      self.sedp_publish_single_user_writer(writer);
    }
    drop(db);

    self
      .discovery_started_sender
      .send(Ok(()))
      .expect("Discovery start notification send failure!");
    // If this triggers, then discovery_started channel has been
    // closed, which likely means the receiver (app thread) has paniced also.
    // They will wait for up to 10 sec for us to send this handshake.

    let mut events = Events::with_capacity(32);
    loop {
      match self
        .poll
        .poll(&mut events, Some(std::time::Duration::from_millis(5000)))
      {
        Ok(_) => (),
        Err(e) => {
          error!("Failed in waiting of poll in discovery. {e:?}");
          return;
        }
      }
      if events.is_empty() {
        debug!("Discovery event loop idling.");
      }

      for event in events.iter() {
        match event.token() {
          DISCOVERY_COMMAND_TOKEN => {
            while let Ok(command) = self.discovery_command_receiver.try_recv() {
              match command {
                DiscoveryCommand::StopDiscovery => {
                  info!("Stopping Discovery");
                  self.on_participant_shutting_down();
                  info!("Stopped Discovery");
                  return; // terminate event loop
                }
                DiscoveryCommand::AddLocalWriter { guid } => {
                  self.add_local_writer(guid);
                }
                DiscoveryCommand::AddLocalReader { guid } => {
                  self.add_local_reader(guid);
                }
                DiscoveryCommand::AddTopic { topic_name } => {
                  self.sedp_publish_topic(&topic_name);
                }
                DiscoveryCommand::RemoveLocalWriter { guid } => {
                  if guid == self.dcps_publication.writer.guid() {
                    continue;
                  }
                  self.send_endpoint_dispose_message(guid);
                  discovery_db_write(&self.discovery_db).remove_local_topic_writer(guid);
                }
                DiscoveryCommand::RemoveLocalReader { guid } => {
                  if guid == self.dcps_subscription.writer.guid() {
                    continue;
                  }
                  self.send_endpoint_dispose_message(guid);
                  discovery_db_write(&self.discovery_db).remove_local_topic_reader(guid);
                }
                DiscoveryCommand::ManualAssertLiveliness => {
                  self
                    .liveliness_state
                    .manual_participant_liveness_refresh_requested = true;
                }
                DiscoveryCommand::AssertTopicLiveliness {
                  writer_guid,
                  manual_assertion,
                } => {
                  self.send_discovery_notification(
                    DiscoveryNotificationType::AssertTopicLiveliness {
                      writer_guid,
                      manual_assertion,
                    },
                  );
                }
                #[cfg(feature = "security")]
                DiscoveryCommand::StartKeyExchangeWithRemoteParticipant {
                  participant_guid_prefix,
                } => {
                  if let Some(security) = self.security_opt.as_mut() {
                    security.start_key_exchange_with_remote_participant(
                      participant_guid_prefix,
                      &self.dcps_participant_volatile_message_secure.writer,
                      &self.discovery_db,
                    );
                  }
                }
                #[cfg(feature = "security")]
                DiscoveryCommand::StartKeyExchangeWithRemoteEndpoint {
                  local_endpoint_guid,
                  remote_endpoint_guid,
                } => {
                  if let Some(security) = self.security_opt.as_mut() {
                    security.start_key_exchange_with_remote_endpoint(
                      local_endpoint_guid,
                      remote_endpoint_guid,
                      &self.dcps_participant_volatile_message_secure.writer,
                      &self.discovery_db,
                    );
                  }
                }
              };
            }
          }

          DISCOVERY_PARTICIPANT_DATA_TOKEN => {
            debug!("triggered participant reader");
            self.spdp_receive();
          }

          DISCOVERY_PARTICIPANT_CLEANUP_TOKEN => {
            self.participant_cleanup();
            // setting next cleanup timeout
            self
              .participant_cleanup_timer
              .set_timeout(Self::PARTICIPANT_CLEANUP_PERIOD, ());
          }

          DISCOVERY_SEND_PARTICIPANT_INFO_TOKEN => {
            if let Some(dp) = self.domain_participant.clone().upgrade() {
              self.spdp_publish(&dp);
            } else {
              error!("DomainParticipant doesn't exist anymore, exiting Discovery.");
              return;
            };
            // reschedule timer
            while let Some(policy) = self.dcps_participant.timer.poll() {
              match policy {
                TimerPolicy::Repeat => {
                  self
                    .dcps_participant
                    .timer
                    .set_timeout(Self::SPDP_PUBLISH_PERIOD, TimerPolicy::Repeat);
                }
                TimerPolicy::OneShot => {
                  // Do not set again, since it was one-shot.
                }
              }
            }
          }
          DISCOVERY_READER_DATA_TOKEN => {
            self.sedp_receive_subscription(None);
          }
          DISCOVERY_WRITER_DATA_TOKEN => {
            self.sedp_receive_publication(None);
          }
          DISCOVERY_TOPIC_DATA_TOKEN => {
            self.sedp_receive_topic_data(None);
          }
          DISCOVERY_TOPIC_CLEANUP_TOKEN => {
            self.topic_cleanup();

            self
              .topic_cleanup_timer
              .set_timeout(Self::TOPIC_CLEANUP_PERIOD, ());
          }
          DISCOVERY_PARTICIPANT_MESSAGE_TOKEN | P2P_SECURE_DISCOVERY_PARTICIPANT_MESSAGE_TOKEN => {
            self.receive_participant_message();
          }
          DISCOVERY_PARTICIPANT_MESSAGE_TIMER_TOKEN => {
            self.publish_participant_message();
            self
              .dcps_participant_message
              .timer
              .set_timeout(Self::CHECK_PARTICIPANT_MESSAGES, TimerPolicy::Repeat);
          }
          SPDP_LIVENESS_TOKEN => {
            while let Ok(guid_prefix) = self.spdp_liveness_receiver.try_recv() {
              discovery_db_write(&self.discovery_db).participant_is_alive(guid_prefix);
            }
          }
          P2P_PARTICIPANT_STATELESS_MESSAGE_TOKEN => {
            #[cfg(feature = "security")]
            self.receive_participant_stateless_message();
          }
          CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_TIMER_TOKEN => {
            #[cfg(feature = "security")]
            self.on_secure_discovery_message_resend_triggered();
          }
          P2P_BUILTIN_PARTICIPANT_VOLATILE_SECURE_TOKEN => {
            #[cfg(feature = "security")]
            self.receive_participant_volatile_message();
          }
          SECURE_DISCOVERY_PARTICIPANT_DATA_TOKEN => {
            #[cfg(feature = "security")]
            self.secure_spdp_receive();
          }
          SECURE_DISCOVERY_READER_DATA_TOKEN => {
            #[cfg(feature = "security")]
            self.secure_sedp_receive_subscription(None);
          }
          SECURE_DISCOVERY_WRITER_DATA_TOKEN => {
            #[cfg(feature = "security")]
            self.secure_sedp_receive_publication(None);
          }

          other_token => {
            error!("discovery event loop got token: {other_token:?}");
          }
        } // match
      } // for
    } // loop
  } // fn

  // Initialize our own participant data into the Discovery DB.
  // That causes ReaderProxies and WriterProxies to be constructed and
  // and we also get our own local readers and writers connected, both
  // built-in and user-defined.
  // If we did not do this, the Readers and Writers in this participant could not
  // find each other.
  fn initialize_participant(&self) {
    let dp = if let Some(dp) = self.domain_participant.clone().upgrade() {
      dp
    } else {
      error!("Cannot get actual DomainParticipant in initialize_participant! Giving up.");
      return;
    };

    let participant_data = SpdpDiscoveredParticipantData::from_local_participant(
      &dp,
      &self.security_opt,
      Duration::INFINITE,
    );

    // Initialize our own participant data into the Discovery DB, so we can talk to
    // ourself.
    discovery_db_write(&self.discovery_db).update_participant(&participant_data);

    // This will read the participant from Discovery DB and construct
    // ReaderProxy and WriterProxy objects for built-in Readers and Writers
    self.send_discovery_notification(DiscoveryNotificationType::ParticipantUpdated {
      guid_prefix: dp.guid().prefix,
    });
  }

  pub fn spdp_receive(&mut self) {
    loop {
      let s = self.dcps_participant.reader.take_next_sample();
      debug!("spdp_receive read {:?}", &s);
      match s {
        Ok(Some(ds)) => {
          #[cfg(not(feature = "security"))]
          let permission = NormalDiscoveryPermission::Allow;

          #[cfg(feature = "security")]
          let permission = if let Some(security) = self.security_opt.as_mut() {
            // Security is enabled. Do a secure read, potentially starting the
            // authentication protocol. The return value tells if normal Discovery is
            // allowed to process the message.
            security.participant_read(
              &ds,
              &self.discovery_db,
              &self.discovery_updated_sender,
              &self.dcps_participant_stateless_message.writer,
            )
          } else {
            // No security configured, always allowed
            NormalDiscoveryPermission::Allow
          };

          if permission == NormalDiscoveryPermission::Allow {
            match ds.value {
              Sample::Value(participant_data) => {
                debug!("spdp_receive discovered {:?}", &participant_data);
                self.process_discovered_participant_data(&participant_data);
              }
              // Sample::Dispose means that DomainParticipant was disposed
              Sample::Dispose(participant_guid) => {
                self.process_participant_dispose(participant_guid.0.prefix);
              }
            }
          }
        }
        Ok(None) => {
          trace!("spdp_receive: no more data");
          return;
        } // no more data
        Err(e) => {
          error!(" !!! spdp_receive: {e:?}");
          return;
        }
      }
    } // loop
  }

  fn process_discovered_participant_data(
    &mut self,
    participant_data: &SpdpDiscoveredParticipantData,
  ) {
    let was_new = discovery_db_write(&self.discovery_db).update_participant(participant_data);
    let guid_prefix = participant_data.participant_guid.prefix;

    // Send notification to dp_event_loop. It will update local reader/writer
    // proxies as the SpdpDiscoveredParticipantData contains a bitmap of the
    // available built-in endpoints.
    self.send_discovery_notification(DiscoveryNotificationType::ParticipantUpdated { guid_prefix });

    if was_new {
      // Inform DDS Applications
      self.send_participant_status(DomainParticipantStatusEvent::ParticipantDiscovered {
        dpd: participant_data.into(),
      });

      // Send a quick response to make discovery faster.
      //
      // RTPS spec v2.5 Section "8.5.3.1 General Approach" [to SPDP] says
      // "Implementations can minimize any start-up delays by sending an additional
      // SPDPdiscoveredParticipantData in response to receiving this data-object from
      // a previously unknown Participant, but this behavior is optional."
      //
      // But not to reply to self, because we know that we exist.
      // TODO: Maybe add some rate-limiting to this to avoid packet storms.
      if guid_prefix != self.domain_participant.guid().prefix {
        self
          .dcps_participant
          .timer
          .set_timeout(StdDuration::from_millis(10), TimerPolicy::OneShot);
      }

      // This may be a rediscovery of a previously seen participant that
      // was temporarily lost due to network outage. Check if we already know
      // what it has (readers, writers, topics).
      debug!("Participant rediscovery start");
      self.sedp_receive_topic_data(Some(guid_prefix));
      self.sedp_receive_subscription(Some(guid_prefix));
      self.sedp_receive_publication(Some(guid_prefix));
      debug!("Participant rediscovery finished");
    }
  }

  fn process_participant_dispose(&mut self, participant_guidp: GuidPrefix) {
    discovery_db_write(&self.discovery_db).remove_participant(participant_guidp, true); // true = actively removed
    self.send_discovery_notification(DiscoveryNotificationType::ParticipantLost {
      guid_prefix: participant_guidp,
    });
    self.send_participant_status(DomainParticipantStatusEvent::ParticipantLost {
      id: participant_guidp,
      reason: LostReason::Disposed,
    });
  }

  fn send_endpoint_dispose_message(&self, endpoint_guid: GUID) {
    let is_writer = endpoint_guid.entity_id.entity_kind.is_writer();
    if is_writer {
      self
        .dcps_publication
        .writer
        .dispose(&Endpoint_GUID(endpoint_guid), None)
        .unwrap_or_else(|e| error!("Disposing local Writer: {e:?}"));
      #[cfg(feature = "security")]
      self
        .dcps_publications_secure
        .writer
        .dispose(&Endpoint_GUID(endpoint_guid), None)
        .unwrap_or_else(|e| error!("Disposing local Writer: {e:?}"));
    } else {
      // is reader
      self
        .dcps_subscription
        .writer
        .dispose(&Endpoint_GUID(endpoint_guid), None)
        .unwrap_or_else(|e| error!("Disposing local Reader: {e:?}"));
      #[cfg(feature = "security")]
      self
        .dcps_subscriptions_secure
        .writer
        .dispose(&Endpoint_GUID(endpoint_guid), None)
        .unwrap_or_else(|e| error!("Disposing local Reader: {e:?}"));
    }
  }

  fn on_participant_shutting_down(&mut self) {
    let db = discovery_db_read(&self.discovery_db);

    for reader in db.get_all_local_topic_readers() {
      self.send_endpoint_dispose_message(reader.reader_proxy.remote_reader_guid);
    }

    for writer in db.get_all_local_topic_writers() {
      self.send_endpoint_dispose_message(writer.writer_proxy.remote_writer_guid);
    }

    self
      .dcps_participant
      .writer
      .dispose(&Participant_GUID(self.domain_participant.guid()), None)
      .unwrap_or(());
    #[cfg(feature = "security")]
    self
      .dcps_participant_secure
      .writer
      .dispose(&Participant_GUID(self.domain_participant.guid()), None)
      .unwrap_or(());
  }

  // Check if there are messages about new Readers
  pub fn sedp_receive_subscription(&mut self, read_history: Option<GuidPrefix>) {
    let drds: Vec<Sample<DiscoveredReaderData, GUID>> =
      match self.dcps_subscription.reader.into_iterator() {
        Ok(ds) => ds
          .map(|d| d.map_dispose(|g| g.0)) // map_dispose removes Endpoint_GUID wrapper around GUID
          .filter(|d|
              // If a participant was specified, we must match its GUID prefix.
              match (read_history, d) {
                (None, _) => true, // Not asked to filter by participant
                (Some(participant_to_update), Sample::Value(drd)) =>
                  drd.reader_proxy.remote_reader_guid.prefix == participant_to_update,
                (Some(participant_to_update), Sample::Dispose(guid)) =>
                  guid.prefix == participant_to_update,
              })
          .collect(),
        Err(e) => {
          error!("sedp_receive_subscription: {e:?}");
          return;
        }
      };

    for d in drds {
      #[cfg(not(feature = "security"))]
      let permission = NormalDiscoveryPermission::Allow;

      #[cfg(feature = "security")]
      let permission = if let Some(security) = self.security_opt.as_mut() {
        // Security is enabled. Do a secure read
        security.check_nonsecure_subscription_read(&d, &self.discovery_db)
      } else {
        // No security configured, always allowed
        NormalDiscoveryPermission::Allow
      };

      if permission == NormalDiscoveryPermission::Allow {
        match d {
          Sample::Value(d) => {
            trace!("sedp_receive_subscription - {d:?}");
            let drd = discovery_db_write(&self.discovery_db).update_subscription(&d);
            debug!(
              "sedp_receive_subscription - send_discovery_notification ReaderUpdated  {:?}",
              &drd
            );
            self.send_discovery_notification(DiscoveryNotificationType::ReaderUpdated {
              discovered_reader_data: drd,
            });
            if read_history.is_some() {
              info!(
                "Rediscovered reader {:?} topic={:?}",
                d.reader_proxy.remote_reader_guid,
                d.subscription_topic_data.topic_name()
              );
            }
          }
          Sample::Dispose(reader_key) => {
            info!("Dispose Reader {reader_key:?}");
            discovery_db_write(&self.discovery_db).remove_topic_reader(reader_key);
            self.send_discovery_notification(DiscoveryNotificationType::ReaderLost {
              reader_guid: reader_key,
            });
            self.send_participant_status(DomainParticipantStatusEvent::ReaderLost {
              guid: reader_key,
              reason: LostReason::Disposed,
            });
          }
        }
      }
    } // loop
  }

  pub fn sedp_receive_publication(&mut self, read_history: Option<GuidPrefix>) {
    let dwds: Vec<Sample<DiscoveredWriterData, GUID>> =
      match self.dcps_publication.reader.into_iterator() {
        // a lot of cloning here, but we must copy the data out of the
        // reader before we can use self again, as .read() returns references to within
        // a reader and thus self
        Ok(ds) => ds
          .map(|d| d.map_dispose(|g| g.0)) // map_dispose removes Endpoint_GUID wrapper around GUID
          // If a participant was specified, we must match its GUID prefix.
          .filter(|d| match (read_history, d) {
            (None, _) => true, // Not asked to filter by participant
            (Some(participant_to_update), Sample::Value(dwd)) => {
              dwd.writer_proxy.remote_writer_guid.prefix == participant_to_update
            }
            (Some(participant_to_update), Sample::Dispose(guid)) => {
              guid.prefix == participant_to_update
            }
          })
          .collect(),
        Err(e) => {
          error!("sedp_receive_publication: {e:?}");
          return;
        }
      };

    for d in dwds {
      #[cfg(not(feature = "security"))]
      let permission = NormalDiscoveryPermission::Allow;

      #[cfg(feature = "security")]
      let permission = if let Some(security) = self.security_opt.as_mut() {
        // Security is enabled. Do a secure read
        security.check_nonsecure_publication_read(&d, &self.discovery_db)
      } else {
        // No security configured, always allowed
        NormalDiscoveryPermission::Allow
      };

      if permission == NormalDiscoveryPermission::Allow {
        match d {
          Sample::Value(dwd) => {
            trace!("sedp_receive_publication discovered {:?}", &dwd);
            let discovered_writer_data =
              discovery_db_write(&self.discovery_db).update_publication(&dwd);
            self.send_discovery_notification(DiscoveryNotificationType::WriterUpdated {
              discovered_writer_data,
            });
            debug!("Discovered Writer {:?}", &dwd);
          }
          Sample::Dispose(writer_key) => {
            discovery_db_write(&self.discovery_db).remove_topic_writer(writer_key);
            self.send_discovery_notification(DiscoveryNotificationType::WriterLost {
              writer_guid: writer_key,
            });
            self.send_participant_status(DomainParticipantStatusEvent::WriterLost {
              guid: writer_key,
              reason: LostReason::Disposed,
            });

            debug!("Disposed Writer {writer_key:?}");
          }
        }
      }
    } // loop
  }

  // TODO: Try to remember why the read_history parameter below was introduced
  // in the first place. Git history should help here.
  // Likely it is something to do with an unreliable network and
  // DomainParticipants timing out and then coming back. The read_history was
  // supposed to help in recovering from that.
  pub fn sedp_receive_topic_data(&mut self, _read_history: Option<GuidPrefix>) {
    let ts: Vec<Sample<(DiscoveredTopicData, GUID), GUID>> = match self
      .dcps_topic
      .reader
      .take(usize::MAX, ReadCondition::any())
    {
      Ok(ds) => ds
        .iter()
        .map(|d| {
          d.value
            .clone()
            .map_value(|o| (o, d.sample_info.writer_guid()))
            .map_dispose(|g| g.0)
        })
        .collect(),
      Err(e) => {
        error!("sedp_receive_topic_data: {e:?}");
        return;
      }
    };

    for t in ts {
      #[cfg(not(feature = "security"))]
      let permission = NormalDiscoveryPermission::Allow;

      #[cfg(feature = "security")]
      let permission = if let Some(security) = self.security_opt.as_mut() {
        // Security is enabled. Do a secure read
        security.check_topic_read(&t, &self.discovery_db)
      } else {
        // No security configured, always allowed
        NormalDiscoveryPermission::Allow
      };

      if permission == NormalDiscoveryPermission::Allow {
        match t {
          Sample::Value((topic_data, writer)) => {
            debug!("sedp_receive_topic_data discovered {:?}", &topic_data);
            discovery_db_write(&self.discovery_db).update_topic_data(
              &topic_data,
              writer,
              DiscoveredVia::Topic,
            );
            // Now check if we know any readers of writers to this topic. The topic QoS
            // could cause these to became viable matches against local
            // writers/readers. This is because at least RTI Connext sends QoS
            // policies on a Topic, and then (apparently) assumes that its
            // readers/writers inherit those policies unless specified otherwise.

            // Note that additional security checks are not needed here, since if a
            // reader/writer is in our DiscoveryDB, it has already passed the security
            // checks.
            let writers = discovery_db_read(&self.discovery_db)
              .writers_on_topic_and_participant(topic_data.topic_name(), writer.prefix);
            debug!("writers {:?}", &writers);
            for discovered_writer_data in writers {
              self.send_discovery_notification(DiscoveryNotificationType::WriterUpdated {
                discovered_writer_data,
              });
            }

            let readers = discovery_db_read(&self.discovery_db)
              .readers_on_topic_and_participant(topic_data.topic_name(), writer.prefix);
            for discovered_reader_data in readers {
              self.send_discovery_notification(DiscoveryNotificationType::ReaderUpdated {
                discovered_reader_data,
              });
            }
          }
          // Sample::Dispose means disposed
          Sample::Dispose(key) => {
            warn!("not implemented - Topic was disposed: {:?}", &key);
          }
        }
      }
    } // loop
  }

  // These messages are for updating participant liveliness
  // The protocol distinguishes between automatic (by DDS library)
  // and manual (by by application, via DDS API call) liveness
  pub fn receive_participant_message(&mut self) {
    // First read from nonsecure reader
    let mut samples = match self
      .dcps_participant_message
      .reader
      .take(usize::MAX, ReadCondition::any())
    {
      Ok(nonsecure_samples) => nonsecure_samples,
      Err(e) => {
        error!("receive_participant_message: {e:?}");
        return;
      }
    };

    // Then from secure reader if needed
    #[cfg(not(feature = "security"))]
    let mut secure_samples = vec![];
    #[cfg(feature = "security")]
    let mut secure_samples = match self
      .dcps_participant_message_secure
      .reader
      .take(usize::MAX, ReadCondition::any())
    {
      Ok(secure_samples) => secure_samples,
      Err(e) => {
        error!("Secure receive_participant_message: {e:?}");
        return;
      }
    };

    samples.append(&mut secure_samples);

    let msgs = samples
      .into_iter()
      .filter_map(|p| p.value().clone().value());

    let mut db = discovery_db_write(&self.discovery_db);
    for msg in msgs {
      db.update_lease_duration(&msg);
    }
  }

  fn spdp_publish(&self, local_dp: &DomainParticipant) {
    // setting 5 times the duration so lease doesn't break if update fails once or
    // twice
    let data = SpdpDiscoveredParticipantData::from_local_participant(
      local_dp,
      &self.security_opt,
      5.0 * Duration::from(Self::SPDP_PUBLISH_PERIOD),
    );

    #[cfg(feature = "security")]
    if let Some(security) = self.security_opt.as_ref() {
      security.secure_spdp_publish(&self.dcps_participant_secure.writer, data.clone());
    }

    self
      .dcps_participant
      .writer
      .write(data, None)
      .unwrap_or_else(|e| {
        error!("Discovery: Publishing to DCPS participant topic failed: {e:?}");
      });
  }

  pub fn publish_participant_message(&mut self) {
    // Inspect if we need to send liveness messages
    // See 8.4.13.5 "Implementing Writer Liveliness Protocol .." in the RPTS spec

    // Dig out the smallest lease duration for writers with Automatic liveliness QoS
    let writer_livelinesses: Vec<Liveliness> = discovery_db_read(&self.discovery_db)
      .get_all_local_topic_writers()
      .filter_map(|p| p.publication_topic_data.liveliness)
      .collect();

    let min_automatic_lease_duration_opt = writer_livelinesses
      .iter()
      .filter_map(|liveliness| match liveliness {
        Liveliness::Automatic { lease_duration } => Some(*lease_duration),
        _other => None,
      })
      .min();

    let timenow = Timestamp::now();

    let mut messages_to_be_sent: Vec<ParticipantMessageData> = vec![];

    // Send Automatic liveness update if needed
    if let Some(min_auto_duration) = min_automatic_lease_duration_opt {
      let time_since_last_auto_update =
        timenow.duration_since(self.liveliness_state.last_auto_update);
      trace!(
        "time_since_last_auto_update: {time_since_last_auto_update:?}, min_auto_duration \
         {min_auto_duration:?}"
      );

      // We choose to send a new liveliness message if longer than half of the min
      // auto duration has elapsed since last message
      if time_since_last_auto_update > min_auto_duration / 2 {
        let msg = ParticipantMessageData {
          guid: self.domain_participant.guid_prefix(),
          kind: ParticipantMessageDataKind::AUTOMATIC_LIVELINESS_UPDATE,
          data: Vec::new(),
        };
        messages_to_be_sent.push(msg);
      }
    }

    // Send ManualByParticipant liveliness update if someone has requested us to do
    // so.
    // Note: According to the RTPS spec (8.7.2.2.3 LIVELINESS) the interval at which
    // we check if we need to send a manual liveness update should depend on the
    // lease durations of writers with ManualByParticipant liveness QoS.
    // Now we just check this at the same interval as with Automatic liveness.
    // So TODO if needed: comply with the spec.
    if self
      .liveliness_state
      .manual_participant_liveness_refresh_requested
    {
      let msg = ParticipantMessageData {
        guid: self.domain_participant.guid_prefix(),
        kind: ParticipantMessageDataKind::MANUAL_LIVELINESS_UPDATE,
        data: Vec::new(),
      };
      messages_to_be_sent.push(msg);
    }

    for msg in messages_to_be_sent {
      let msg_kind = msg.kind;

      #[cfg(not(feature = "security"))]
      let write_result = self.dcps_participant_message.writer.write(msg, None);

      #[cfg(feature = "security")]
      let write_result = if let Some(security) = self.security_opt.as_ref() {
        security.write_liveness_message(
          &self.dcps_participant_message_secure.writer,
          &self.dcps_participant_message.writer,
          msg,
        )
      } else {
        // No security enabled
        self.dcps_participant_message.writer.write(msg, None)
      };

      match write_result {
        Ok(_) => {
          match msg_kind {
            ParticipantMessageDataKind::AUTOMATIC_LIVELINESS_UPDATE => {
              self.liveliness_state.last_auto_update = timenow;
            }
            ParticipantMessageDataKind::MANUAL_LIVELINESS_UPDATE => {
              // We delivered what was requested
              self
                .liveliness_state
                .manual_participant_liveness_refresh_requested = false;
            }
            _ => (),
          }
        }
        Err(e) => {
          error!("Failed to writer ParticipantMessageData. {e:?}");
        }
      }
    }
  }

  #[cfg(feature = "security")]
  fn receive_participant_stateless_message(&mut self) {
    if let Some(security) = self.security_opt.as_mut() {
      // Security enabled. Get messages from the stateless data reader & feed to
      // Secure Discovery.
      match self
        .dcps_participant_stateless_message
        .reader
        .into_iterator()
      {
        Ok(dr_iter) => {
          for msg in dr_iter {
            security.participant_stateless_message_read(
              &msg,
              &self.discovery_db,
              &self.discovery_updated_sender,
              &self.dcps_participant_stateless_message.writer,
            );
          }
        }
        Err(e) => {
          error!("receive_participant_stateless_message: {e:?}");
        }
      };
    }
  }

  #[cfg(feature = "security")]
  fn receive_participant_volatile_message(&mut self) {
    if let Some(security) = self.security_opt.as_mut() {
      // Security enabled. Get messages from the volatile message reader & feed to
      // Secure Discovery.
      match self
        .dcps_participant_volatile_message_secure
        .reader
        .into_iterator()
      {
        Ok(dr_iter) => {
          for msg in dr_iter {
            security.volatile_message_secure_read(&msg);
          }
        }
        Err(e) => {
          error!("receive_participant_volatile_message: {e:?}");
        }
      };
    }
  }

  #[cfg(feature = "security")]
  pub fn secure_spdp_receive(&mut self) {
    let sample_iter = match self.dcps_participant_secure.reader.into_iterator() {
      Ok(iter) => iter,
      Err(e) => {
        error!("secure_spdp_receive: {e:?}");
        return;
      }
    };

    for sample in sample_iter {
      let permission = if let Some(security) = self.security_opt.as_mut() {
        security.secure_participant_read(
          &sample,
          &self.discovery_db,
          &self.discovery_updated_sender,
        )
      } else {
        debug!("In secure_spdp_receive even though security not enabled?");
        return;
      };

      if permission == NormalDiscoveryPermission::Allow {
        match sample {
          Sample::Value(sec_data) => {
            let participant_data = sec_data.participant_data;
            self.process_discovered_participant_data(&participant_data);
          }
          // Sample::Dispose means that DomainParticipant was disposed
          Sample::Dispose(participant_guid) => {
            self.process_participant_dispose(participant_guid.0.prefix);
          }
        }
      }
    }
  }

  #[cfg(feature = "security")]
  pub fn secure_sedp_receive_subscription(&mut self, read_history: Option<GuidPrefix>) {
    let sec_subs: Vec<Sample<SubscriptionBuiltinTopicDataSecure, GUID>> =
      match self.dcps_subscriptions_secure.reader.into_iterator() {
        Ok(ds) => ds
          .map(|d| d.map_dispose(|g| g.0)) // map_dispose removes Endpoint_GUID wrapper around GUID
          .filter(|d|
              // If a participant was specified, we must match its GUID prefix.
              match (read_history, d) {
                (None, _) => true, // Not asked to filter by participant
                (Some(participant_to_update), Sample::Value(sec_sub)) =>
                sec_sub.discovered_reader_data.reader_proxy.remote_reader_guid.prefix == participant_to_update,
                (Some(participant_to_update), Sample::Dispose(guid)) =>
                  guid.prefix == participant_to_update,
              })
          .collect(),
        Err(e) => {
          error!("secure_sedp_receive_subscription: {e:?}");
          return;
        }
      };

    for sec_sub_sample in sec_subs {
      let permission = if let Some(security) = self.security_opt.as_mut() {
        security.check_secure_subscription_read(&sec_sub_sample, &self.discovery_db)
      } else {
        debug!("In secure_sedp_receive_subscription even though security not enabled?");
        return;
      };

      if permission == NormalDiscoveryPermission::Allow {
        match sec_sub_sample {
          Sample::Value(sec_sub) => {
            // Currently we use only the DiscoveredReaderData field, no DataTag
            let drd_from_topic = sec_sub.discovered_reader_data;
            let drd = discovery_db_write(&self.discovery_db).update_subscription(&drd_from_topic);
            self.send_discovery_notification(DiscoveryNotificationType::ReaderUpdated {
              discovered_reader_data: drd,
            });
          }
          Sample::Dispose(reader_guid) => {
            info!("Secure Dispose Reader {reader_guid:?}");
            discovery_db_write(&self.discovery_db).remove_topic_reader(reader_guid);
            self.send_discovery_notification(DiscoveryNotificationType::ReaderLost { reader_guid });
            self.send_participant_status(DomainParticipantStatusEvent::ReaderLost {
              guid: reader_guid,
              reason: LostReason::Disposed,
            });
          }
        }
      }
    }
  }

  #[cfg(feature = "security")]
  pub fn secure_sedp_receive_publication(&mut self, read_history: Option<GuidPrefix>) {
    let sec_pubs: Vec<Sample<PublicationBuiltinTopicDataSecure, GUID>> =
      match self.dcps_publications_secure.reader.into_iterator() {
        Ok(ds) => ds
          .map(|d| d.map_dispose(|g| g.0)) // map_dispose removes Endpoint_GUID wrapper around GUID
          // If a participant was specified, we must match its GUID prefix.
          .filter(|d| match (read_history, d) {
            (None, _) => true, // Not asked to filter by participant
            (Some(participant_to_update), Sample::Value(sec_pub)) => {
              sec_pub
                .discovered_writer_data
                .writer_proxy
                .remote_writer_guid
                .prefix
                == participant_to_update
            }
            (Some(participant_to_update), Sample::Dispose(guid)) => {
              guid.prefix == participant_to_update
            }
          })
          .collect(),
        Err(e) => {
          error!("secure_sedp_receive_publication: {e:?}");
          return;
        }
      };

    for sec_pub_sample in sec_pubs {
      let permission = if let Some(security) = self.security_opt.as_mut() {
        security.check_secure_publication_read(&sec_pub_sample, &self.discovery_db)
      } else {
        debug!("In secure_sedp_receive_publication even though security not enabled?");
        return;
      };

      if permission == NormalDiscoveryPermission::Allow {
        match sec_pub_sample {
          Sample::Value(se_pub) => {
            // Currently we use only the DiscoveredWriterData field, no DataTag
            let dwd_from_topic = se_pub.discovered_writer_data;
            let dwd = discovery_db_write(&self.discovery_db).update_publication(&dwd_from_topic);
            self.send_discovery_notification(DiscoveryNotificationType::WriterUpdated {
              discovered_writer_data: dwd,
            });
          }
          Sample::Dispose(writer_guid) => {
            info!("Secure Dispose Writer {writer_guid:?}");
            discovery_db_write(&self.discovery_db).remove_topic_writer(writer_guid);
            self.send_discovery_notification(DiscoveryNotificationType::WriterLost { writer_guid });
            self.send_participant_status(DomainParticipantStatusEvent::WriterLost {
              guid: writer_guid,
              reason: LostReason::Disposed,
            });
          }
        }
      }
    }
  }

  #[cfg(feature = "security")]
  fn on_secure_discovery_message_resend_triggered(&mut self) {
    if let Some(security) = self.security_opt.as_mut() {
      // Security is enabled
      security.resend_cached_secure_discovery_messages(
        &self.dcps_participant_stateless_message.writer,
        &self.dcps_participant_volatile_message_secure.writer,
      );

      // Reset timer for resending security messages
      self
        .cached_secure_discovery_messages_resend_timer
        .set_timeout(Self::CACHED_SECURE_DISCOVERY_MESSAGE_RESEND_PERIOD, ());
    }
  }

  pub fn participant_cleanup(&self) {
    let removed = discovery_db_write(&self.discovery_db).participant_cleanup();
    for (guid_prefix, reason) in removed {
      debug!("participant cleanup - timeout for {guid_prefix:?}");
      self.send_discovery_notification(DiscoveryNotificationType::ParticipantLost { guid_prefix });
      self.send_participant_status(DomainParticipantStatusEvent::ParticipantLost {
        id: guid_prefix,
        reason,
      });
    }
  }

  pub fn topic_cleanup(&self) {
    discovery_db_write(&self.discovery_db).topic_cleanup();
  }

  fn sedp_publish_single_user_reader(&self, reader_data: &DiscoveredReaderData) {
    if !reader_data
      .reader_proxy
      .remote_reader_guid
      .entity_id
      .kind()
      .is_user_defined()
    {
      // Only readers of user-defined topics are published to discovery
      return;
    }

    #[cfg(not(feature = "security"))]
    let do_nonsecure_write = true;

    #[cfg(feature = "security")]
    let do_nonsecure_write = if let Some(security) = self.security_opt.as_ref() {
      security.sedp_publish_single_reader(
        &self.dcps_subscription.writer,
        &self.dcps_subscriptions_secure.writer,
        reader_data,
      );
      false
    } else {
      true // No security configured
    };

    if do_nonsecure_write {
      match self
        .dcps_subscription
        .writer
        .write(reader_data.clone(), None)
      {
        Ok(()) => {
          debug!(
            "Published DCPSSubscription data on topic {}, reader guid {:?}, data {:?}",
            reader_data.subscription_topic_data.topic_name(),
            reader_data.reader_proxy.remote_reader_guid,
            reader_data,
          );
        }
        Err(e) => {
          error!(
            "Failed to publish DCPSSubscription data on topic {}, reader guid {:?}. Error: {e}",
            reader_data.subscription_topic_data.topic_name(),
            reader_data.reader_proxy.remote_reader_guid
          );
          // TODO: try again later?
        }
      }
    }
  }

  fn add_local_writer(&self, guid: GUID) {
    // Get writer data from db
    let db = discovery_db_read(&self.discovery_db);
    let writer_data = match db.get_local_topic_writer(guid) {
      Some(d) => d,
      None => {
        warn!("Did not find a local writer {guid:?}");
        return;
      }
    };

    // Publish it to SEDP (if needed)
    self.sedp_publish_single_user_writer(writer_data);

    // Send the ReaderUpdated notification on any existing readers on the writer's
    // topic This will result in matching the endpoints if possible
    let existing_readers = db.readers_on_topic(writer_data.publication_topic_data.topic_name());
    for reader in existing_readers {
      self.send_discovery_notification(DiscoveryNotificationType::ReaderUpdated {
        discovered_reader_data: reader.clone(),
      });
    }
  }

  fn add_local_reader(&self, guid: GUID) {
    // Get reader data from db
    let db = discovery_db_read(&self.discovery_db);
    let reader_data = match db.get_local_topic_reader(guid) {
      Some(d) => d,
      None => {
        warn!("Did not find a local reader {guid:?}");
        return;
      }
    };

    // Publish it to SEDP (if needed)
    self.sedp_publish_single_user_reader(reader_data);

    // Send the WriterUpdated notification on any existing writers on the readers's
    // topic This will result in matching the endpoints if possible
    let existing_writers = db.writers_on_topic(reader_data.subscription_topic_data.topic_name());
    for writer in existing_writers {
      self.send_discovery_notification(DiscoveryNotificationType::WriterUpdated {
        discovered_writer_data: writer.clone(),
      });
    }
  }

  fn sedp_publish_single_user_writer(&self, writer_data: &DiscoveredWriterData) {
    if !writer_data
      .writer_proxy
      .remote_writer_guid
      .entity_id
      .kind()
      .is_user_defined()
    {
      // Only writers of user-defined topics are published to discovery
      return;
    }

    #[cfg(not(feature = "security"))]
    let do_nonsecure_write = true;

    #[cfg(feature = "security")]
    let do_nonsecure_write = if let Some(security) = self.security_opt.as_ref() {
      security.sedp_publish_single_writer(
        &self.dcps_publication.writer,
        &self.dcps_publications_secure.writer,
        writer_data,
      );
      false
    } else {
      true // No security configured
    };

    if do_nonsecure_write {
      match self
        .dcps_publication
        .writer
        .write(writer_data.clone(), None)
      {
        Ok(()) => {
          debug!(
            "Published DCPSPublication data on topic {}, writer guid {:?}",
            writer_data.publication_topic_data.topic_name(),
            writer_data.writer_proxy.remote_writer_guid
          );
        }
        Err(e) => {
          error!(
            "Failed to publish DCPSPublication data on topic {}, writer guid {:?}. Error: {e}",
            writer_data.publication_topic_data.topic_name(),
            writer_data.writer_proxy.remote_writer_guid
          );
          // TODO: try again later?
        }
      }
    }
  }

  pub fn sedp_publish_topic(&self, topic_name: &str) {
    let db = discovery_db_read(&self.discovery_db);
    // We might have multiple topics with the same name (but different Qos etc..),
    // and the following call gets just one of them. Should we publish all of
    // them or is this enough?
    let topic_data = match db.get_topic(topic_name) {
      Some(data) => data,
      None => {
        warn!("Did not find topic data with topic name {topic_name}");
        return;
      }
    };

    // Only user-defined topics are published to discovery
    let is_user_defined = !topic_data.topic_name().starts_with("DCPS");
    if !is_user_defined {
      return;
    }

    match self.dcps_topic.writer.write(topic_data.clone(), None) {
      Ok(()) => {
        debug!("Published topic {topic_name} to DCPSTopic");
      }
      Err(e) => {
        error!("Failed to publish topic {topic_name} to DCPSTopic: {e}");
        // TODO: try again later?
      }
    }
  }

  pub const fn builtin_subscriber_qos_builder() -> QosPolicyBuilder {
    // The Subscriber QoS is specified in DDS Spec v1.4 Section
    // "2.2.5 Built-in Topics"
    QosPolicyBuilder::new()
      .durability(Durability::TransientLocal)
      .presentation(Presentation {
        access_scope: PresentationAccessScope::Topic,
        coherent_access: false,
        ordered_access: false,
      })
      .deadline(Deadline(Duration::INFINITE))
      .ownership(Ownership::Shared)
      .liveliness(Liveliness::Automatic {
        lease_duration: Duration::INFINITE,
      })
      .time_based_filter(TimeBasedFilter {
        minimum_separation: Duration::ZERO,
      })
      .reliability(Reliability::Reliable {
        max_blocking_time: Duration::from_std(StdDuration::from_millis(100)),
      })
      .destination_order(DestinationOrder::ByReceptionTimestamp)
      // Spec gives History KeepLast depth = 1, but we
      // use somewhat higher to avoid losing data at the receiver in case
      // it comes in bursts and there is some delay in Discovery processing.
      .history(History::KeepLast { depth: 4 })
    // TODO:
    // Spec says all resource limits should be "LENGTH_UNLIMITED",
    // but that may lead to memory exhaustion.
    //
    // .resource_limits(ResourceLimits { // TODO: Maybe lower limits would
    // suffice?   max_instances: std::i32::MAX,
    //   max_samples: std::i32::MAX,
    //   max_samples_per_instance: std::i32::MAX,
    // })
  }

  pub const fn builtin_subscriber_qos() -> QosPolicies {
    Self::builtin_subscriber_qos_builder().build()
  }

  pub const fn builtin_publisher_qos() -> QosPolicies {
    // TODO: Check if this definition is correct (spec?)
    // Problem: DDS spec v1.4 Section 2.2.5 gives Subscriber QoS policies,
    // but does not mention publisher QoS for built-in topics.
    //
    //
    QosPolicyBuilder::new()
      .durability(Durability::TransientLocal)
      .presentation(Presentation {
        access_scope: PresentationAccessScope::Topic,
        coherent_access: false,
        ordered_access: false,
      })
      .deadline(Deadline(Duration::INFINITE))
      .ownership(Ownership::Shared)
      .liveliness(Liveliness::Automatic {
        lease_duration: Duration::INFINITE,
      })
      .time_based_filter(TimeBasedFilter {
        minimum_separation: Duration::ZERO,
      })
      .reliability(Reliability::Reliable {
        max_blocking_time: Duration::from_std(StdDuration::from_millis(100)),
      })
      .destination_order(DestinationOrder::ByReceptionTimestamp)
      // History must be different from Subscriber side, because otherwise
      // only the latest created Reder/Writer/Topic will be buffered and the older
      // ones forgotten.
      .history(History::KeepAll)
      // .resource_limits(ResourceLimits { // TODO: Maybe lower limits would suffice?
      //   max_instances: std::i32::MAX,
      //   max_samples: std::i32::MAX,
      //   max_samples_per_instance: std::i32::MAX,
      // })
      .build()
  }

  // This is (partially) gven in DDS Spec v1.4 Section 8.5.3.3.1
  // SPDPbuiltinParticipantWriter Table 8.79 - Attributes of the RTPS
  // StatelessWriter used by the SPDP at least that is is BestEffort.
  pub const fn create_spdp_participant_qos() -> QosPolicies {
    Self::builtin_subscriber_qos_builder()
      .reliability(Reliability::BestEffort)
      // Use depth=8 to avoid losing data when we receive notifications
      // faster than we can process.
      .history(History::KeepLast { depth: 8 })
      .build()
  }

  // Note: Topics "participant" and "participant message" are differet things.
  pub(crate) const PARTICIPANT_MESSAGE_QOS: QosPolicies = Self::builtin_subscriber_qos_builder()
    .reliability(Reliability::Reliable {
      max_blocking_time: Duration::from_millis(10),
    })
    .history(History::KeepLast { depth: 1 })
    .build();

  // Note RTPS 2.3 / 2.5 spec Section
  // "8.4.13.3 BuiltinParticipantMessageWriter and
  // BuiltinParticipantMessageReader QoS":
  // "BuiltinParticipantMessageWriter shall use reliability.kind =
  // RELIABLE_RELIABILITY_QOS."
  //
  // But the reader side can use either Realiability setting.
  // SPDPdiscoveredParticipantData may indicate over the wire that
  // their reader for these messages is BestEffort, via builtinEndpointQos,
  // which writers must notice. This allows them to not expect ACKNACKs from
  // BestEffort Reader and avoid stalling.

  // TODO: Check if the following built-in QoS policies should also be based on
  // builtin_subscriber_qos_builder rather than empty QoS policy. The change
  // requires interop testing with differet DDSs, including security.

  #[cfg(feature = "security")]
  pub const fn create_participant_stateless_message_qos() -> QosPolicies {
    // See section 7.4.3 "New DCPSParticipantStatelessMessage builtin Topic" of the
    // Security spec
    QosPolicyBuilder::new()
      .reliability(Reliability::BestEffort) // Important!
      .history(History::KeepLast { depth: 1 })
      .build()
  }

  #[cfg(feature = "security")]
  pub const fn create_participant_volatile_message_secure_qos() -> QosPolicies {
    // See Table 18 – Non-default Qos policies for
    // BuiltinParticipantVolatileMessageSecureWriter of the Security spec
    QosPolicyBuilder::new()
      .reliability(Reliability::Reliable {
        max_blocking_time: Duration::from_std(StdDuration::from_millis(100)),
      })
      .history(History::KeepAll)
      .durability(Durability::Volatile)
      .build()
  }

  fn send_discovery_notification(&self, dntype: DiscoveryNotificationType) {
    match self.discovery_updated_sender.send(dntype) {
      Ok(_) => (),
      Err(e) => error!("Failed to send DiscoveryNotification {e:?}"),
    }
  }

  fn send_participant_status(&self, event: DomainParticipantStatusEvent) {
    self
      .participant_status_sender
      .try_send(event)
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));
  }
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use chrono::Utc;
  use speedy::{Endianness, Writable};
  use mio_06::Token;

  use super::*;
  use crate::{
    dds::adapters::no_key::DeserializerAdapter,
    discovery::sedp_messages::TopicBuiltinTopicData,
    messages::submessages::submessages::{InterpreterSubmessage, WriterSubmessage},
    network::{constant::*, udp_listener::UDPListener, udp_sender::UDPSender},
    rtps::submessage::*,
    test::{
      shape_type::ShapeType,
      test_data::{
        create_cdr_pl_rtps_data_message, spdp_participant_msg_mod, spdp_publication_msg,
        spdp_subscription_msg,
      },
    },
    RepresentationIdentifier,
  };

  #[test]
  fn discovery_participant_data_test() {
    let poll = Poll::new().unwrap();
    const LISTENER_PORT: u16 = spdp_well_known_unicast_port(12, 0);

    let mut udp_listener =
      UDPListener::new_unicast("127.0.0.1", LISTENER_PORT).expect("udp listener creation");
    poll
      .register(
        udp_listener.mio_socket(),
        Token(0),
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();

    // sending participant data to discovery
    let udp_sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses = vec![SocketAddr::new("127.0.0.1".parse().unwrap(), LISTENER_PORT)];

    let tdata = spdp_participant_msg_mod(11000);
    let msg_data = tdata
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write msg data");

    udp_sender.send_to_all(&msg_data, &addresses);

    let mut events = Events::with_capacity(10);
    poll
      .poll(&mut events, Some(StdDuration::from_secs(1)))
      .unwrap();

    let _data2 = udp_listener.get_message();
    // TODO: we should have received our own participants info decoding the
    // actual message might be good idea
  }

  #[test]
  fn discovery_reader_data_test() {
    use crate::{
      serialization::pl_cdr_adapters::PlCdrSerialize, structure::locator::Locator, TopicKind,
    };

    let participant = DomainParticipant::new(0).expect("participant creation");

    let topic = participant
      .create_topic(
        "Square".to_string(),
        "ShapeType".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();

    let publisher = participant
      .create_publisher(&QosPolicies::qos_none())
      .unwrap();
    let _writer = publisher
      .create_datawriter_cdr::<ShapeType>(&topic, None)
      .unwrap();

    let subscriber = participant
      .create_subscriber(&QosPolicies::qos_none())
      .unwrap();
    let _reader =
      subscriber.create_datareader::<ShapeType, CDRDeserializerAdapter<ShapeType>>(&topic, None);

    let poll: Poll = Poll::new().unwrap();
    const LISTENER_PORT: u16 = spdp_well_known_unicast_port(14, 0);

    let mut udp_listener = UDPListener::new_unicast("127.0.0.1", LISTENER_PORT).unwrap();
    poll
      .register(
        udp_listener.mio_socket(),
        Token(0),
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();

    let udp_sender: UDPSender =
      UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses: Vec<SocketAddr> =
      vec![SocketAddr::new("127.0.0.1".parse().unwrap(), LISTENER_PORT)];

    let mut tdata: crate::rtps::Message = spdp_subscription_msg();
    let mut data: bytes::Bytes;
    for submsg in &mut tdata.submessages {
      match &mut submsg.body {
        SubmessageBody::Writer(WriterSubmessage::Data(d, _)) => {
          let mut drd: DiscoveredReaderData = PlCdrDeserializerAdapter::from_bytes(
            &d.unwrap_serialized_payload_value(),
            RepresentationIdentifier::PL_CDR_LE,
          )
          .unwrap();
          drd.reader_proxy.unicast_locator_list.clear();
          drd
            .reader_proxy
            .unicast_locator_list
            .push(Locator::from(SocketAddr::new(
              "127.0.0.1".parse().unwrap(),
              11001,
            )));
          drd.reader_proxy.multicast_locator_list.clear();

          data = drd
            .to_pl_cdr_bytes(RepresentationIdentifier::PL_CDR_LE)
            .unwrap();
          d.update_serialized_payload_value(data.clone());
        }
        SubmessageBody::Interpreter(_) => (),
        _ => continue,
      }
    }

    let msg_data: Vec<u8> = tdata
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write msg data");

    udp_sender.send_to_all(&msg_data, &addresses);

    let mut events: Events = Events::with_capacity(10);
    poll
      .poll(&mut events, Some(StdDuration::from_secs(1)))
      .unwrap();

    let _data2: Vec<u8> = udp_listener.get_message();
  }

  #[test]
  fn discovery_writer_data_test() {
    use crate::TopicKind;
    let participant = DomainParticipant::new(0).expect("Failed to create participant");

    let topic = participant
      .create_topic(
        "Square".to_string(),
        "ShapeType".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();

    let publisher = participant
      .create_publisher(&QosPolicies::qos_none())
      .unwrap();
    let _writer = publisher
      .create_datawriter_cdr::<ShapeType>(&topic, None)
      .unwrap();

    let subscriber = participant
      .create_subscriber(&QosPolicies::qos_none())
      .unwrap();
    let _reader =
      subscriber.create_datareader::<ShapeType, CDRDeserializerAdapter<ShapeType>>(&topic, None);

    let poll = Poll::new().unwrap();
    let mut udp_listener = UDPListener::new_unicast("127.0.0.1", 0).unwrap();
    poll
      .register(
        udp_listener.mio_socket(),
        Token(0),
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();

    let udp_sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses = vec![SocketAddr::new(
      "127.0.0.1".parse().unwrap(),
      spdp_well_known_unicast_port(15, 0),
    )];

    let mut tdata = spdp_publication_msg();
    for submsg in &mut tdata.submessages {
      match &mut submsg.body {
        SubmessageBody::Interpreter(v) => match v {
          InterpreterSubmessage::InfoDestination(dst, _flags) => {
            dst.guid_prefix = participant.guid_prefix();
          }
          _ => continue,
        },
        SubmessageBody::Writer(_) => (),
        SubmessageBody::Reader(_) => (),
        #[cfg(feature = "security")]
        SubmessageBody::Security(_) => (),
      }
    }

    let par_msg_data = spdp_participant_msg_mod(udp_listener.port())
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write participant data.");

    let msg_data = tdata
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .expect("Failed to write msg data");

    udp_sender.send_to_all(&par_msg_data, &addresses);
    udp_sender.send_to_all(&msg_data, &addresses);

    let mut events = Events::with_capacity(10);
    poll
      .poll(&mut events, Some(StdDuration::from_secs(1)))
      .unwrap();

    for _ in udp_listener.messages() {
      info!("Message received");
    }
  }

  #[test]
  fn discovery_topic_data_test() {
    let _participant = DomainParticipant::new(0);

    let topic_data = DiscoveredTopicData::new(
      Utc::now(),
      TopicBuiltinTopicData {
        key: None,
        name: String::from("Square"),
        type_name: String::from("ShapeType"),
        durability: None,
        deadline: None,
        latency_budget: None,
        liveliness: None,
        reliability: None,
        lifespan: None,
        destination_order: None,
        presentation: None,
        history: None,
        resource_limits: None,
        ownership: None,
      },
    );

    let rtps_message = create_cdr_pl_rtps_data_message(
      &topic_data,
      EntityId::SEDP_BUILTIN_TOPIC_READER,
      EntityId::SEDP_BUILTIN_TOPIC_WRITER,
    );

    let udp_sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");
    let addresses = vec![SocketAddr::new(
      "127.0.0.1".parse().unwrap(),
      spdp_well_known_unicast_port(16, 0),
    )];

    let rr = rtps_message
      .write_to_vec_with_ctx(Endianness::LittleEndian)
      .unwrap();

    udp_sender.send_to_all(&rr, &addresses);
  }
}
