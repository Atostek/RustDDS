use std::{
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
  time::{Duration, Instant},
};

use futures::Future;
use mio_06::{Ready, SetReadiness};
use mio_extras::channel::{self as mio_channel, SendError};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    adapters::with_key::SerializerAdapter,
    ddsdata::DDSData,
    pubsub::Publisher,
    qos::{
      policy::{Liveliness, Reliability},
      HasQoSPolicy, QosPolicies,
    },
    result::{CreateResult, WriteError, WriteResult},
    statusevents::*,
    topic::Topic,
  },
  discovery::{discovery::DiscoveryCommand, sedp_messages::SubscriptionBuiltinTopicData},
  messages::submessages::elements::serialized_payload::SerializedPayload,
  rtps::writer_send_buffer::{Admission, WriterSendBuffer},
  serialization::CDRSerializerAdapter,
  structure::{
    cache_change::ChangeKind, entity::RTPSEntity, guid::GUID, rpc::SampleIdentity,
    sequence_number::SequenceNumber, time::Timestamp,
  },
  Keyed, TopicDescription,
};

// TODO: Move the write options and the builder type to some lower-level module
// to avoid circular dependencies.
#[derive(Debug, Default)]
pub struct WriteOptionsBuilder {
  related_sample_identity: Option<SampleIdentity>,
  source_timestamp: Option<Timestamp>,
  to_single_reader: Option<GUID>,
}

impl WriteOptionsBuilder {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn build(self) -> WriteOptions {
    WriteOptions {
      related_sample_identity: self.related_sample_identity,
      source_timestamp: self.source_timestamp,
      to_single_reader: self.to_single_reader,
    }
  }

  #[must_use]
  pub fn related_sample_identity(mut self, related_sample_identity: SampleIdentity) -> Self {
    self.related_sample_identity = Some(related_sample_identity);
    self
  }

  #[must_use]
  pub fn related_sample_identity_opt(
    mut self,
    related_sample_identity_opt: Option<SampleIdentity>,
  ) -> Self {
    self.related_sample_identity = related_sample_identity_opt;
    self
  }

  #[must_use]
  pub fn source_timestamp(mut self, source_timestamp: Timestamp) -> Self {
    self.source_timestamp = Some(source_timestamp);
    self
  }

  #[must_use]
  pub fn to_single_reader(mut self, reader: GUID) -> Self {
    self.to_single_reader = Some(reader);
    self
  }
}

/// Type to be used with write_with_options.
/// Use WriteOptionsBuilder to construct this.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Default)]
pub struct WriteOptions {
  related_sample_identity: Option<SampleIdentity>, // for DDS-RPC
  source_timestamp: Option<Timestamp>,             // from DDS spec
  to_single_reader: Option<GUID>,                  /* try to send to one Reader only
                                                    * future extension room fo other fields. */
}

impl WriteOptions {
  pub fn related_sample_identity(&self) -> Option<SampleIdentity> {
    self.related_sample_identity
  }

  pub fn source_timestamp(&self) -> Option<Timestamp> {
    self.source_timestamp
  }

  pub fn to_single_reader(&self) -> Option<GUID> {
    self.to_single_reader
  }
}

impl From<Option<Timestamp>> for WriteOptions {
  fn from(source_timestamp: Option<Timestamp>) -> Self {
    Self {
      related_sample_identity: None,
      source_timestamp,
      to_single_reader: None,
    }
  }
}

/// Simplified type for CDR encoding
pub type DataWriterCdr<D> = DataWriter<D, CDRSerializerAdapter<D>>;

/// DDS DataWriter for keyed topics
///
/// # Examples
///
/// ```
/// use serde::{Serialize, Deserialize};
/// use rustdds::*;
/// use rustdds::with_key::DataWriter;
/// use rustdds::serialization::CDRSerializerAdapter;
///
/// let domain_participant = DomainParticipant::new(0).unwrap();
/// let qos = QosPolicyBuilder::new().build();
/// let publisher = domain_participant.create_publisher(&qos).unwrap();
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct SomeType { a: i32 }
/// impl Keyed for SomeType {
///   type K = i32;
///
///   fn key(&self) -> Self::K {
///     self.a
///   }
/// }
///
/// // WithKey is important
/// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
/// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None);
/// ```
pub struct DataWriter<D: Keyed, SA: SerializerAdapter<D> = CDRSerializerAdapter<D>> {
  data_phantom: PhantomData<D>,
  ser_phantom: PhantomData<SA>,
  my_publisher: Publisher,
  my_topic: Topic,
  qos_policy: QosPolicies,
  my_guid: GUID,
  /// Shared, flow-controlled send buffer. Admission allocates the sequence
  /// number and stores the sample only when the reliable window has room.
  send_buffer: WriterSendBuffer,
  /// mio readiness "doorbell" rung after a successful admission so the event
  /// loop wakes and transmits the sample.
  doorbell: SetReadiness,
  discovery_command: mio_channel::SyncSender<DiscoveryCommand>,
  status_receiver: StatusChannelReceiver<DataWriterStatus>,
}

impl<D, SA> Drop for DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  fn drop(&mut self) {
    // Tell Publisher to drop the corresponding RTPS Writer
    self.my_publisher.remove_writer(self.my_guid);

    // Notify Discovery that we are no longer
    match self
      .discovery_command
      .send(DiscoveryCommand::RemoveLocalWriter { guid: self.guid() })
    {
      Ok(_) => {}

      // This is fairly normal at shutdown, as the other end is down already.
      Err(SendError::Disconnected(_cmd)) => {
        debug!("Failed to send REMOVE_LOCAL_WRITER DiscoveryCommand: Disconnected.");
      }
      // other errors must be taken more seriously
      Err(e) => error!("Failed to send REMOVE_LOCAL_WRITER DiscoveryCommand. {e:?}"),
    }
  }
}

impl<D, SA> DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    publisher: Publisher,
    topic: Topic,
    qos: QosPolicies,
    guid: GUID,
    send_buffer: WriterSendBuffer,
    doorbell: SetReadiness,
    discovery_command: mio_channel::SyncSender<DiscoveryCommand>,
    status_receiver: StatusChannelReceiver<DataWriterStatus>,
  ) -> CreateResult<Self> {
    if let Some(lv) = qos.liveliness {
      match lv {
        Liveliness::Automatic { .. } | Liveliness::ManualByTopic { .. } => (),
        Liveliness::ManualByParticipant { .. } => {
          if let Err(e) = discovery_command.send(DiscoveryCommand::ManualAssertLiveliness) {
            error!("Failed to send DiscoveryCommand - Refresh. {e:?}");
          }
        }
      }
    };
    Ok(Self {
      data_phantom: PhantomData,
      ser_phantom: PhantomData,
      my_publisher: publisher,
      my_topic: topic,
      qos_policy: qos,
      my_guid: guid,
      send_buffer,
      doorbell,
      discovery_command,
      status_receiver,
    })
  }

  // Wake the event loop to transmit a freshly admitted sample.
  fn ring_doorbell(&self) {
    if let Err(e) = self.doorbell.set_readiness(Ready::readable()) {
      warn!(
        "Failed to ring writer doorbell: topic={:?} {e}",
        self.my_topic.name()
      );
    }
  }

  /// Manually refreshes liveliness
  ///
  /// Corresponds to DDS Spec 1.4 Section 2.2.2.4.2.22 assert_liveliness.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// data_writer.refresh_manual_liveliness();
  /// ```
  pub fn refresh_manual_liveliness(&self) {
    if let Some(lv) = self.qos().liveliness {
      match lv {
        Liveliness::Automatic { .. } | Liveliness::ManualByTopic { .. } => (),
        Liveliness::ManualByParticipant { .. } => {
          if let Err(e) = self
            .discovery_command
            .send(DiscoveryCommand::ManualAssertLiveliness)
          {
            error!("Failed to send DiscoveryCommand - Refresh. {e:?}");
          }
        }
      }
    };
  }

  /// Writes single data instance to a topic.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// let some_data = SomeType { a: 1 };
  /// data_writer.write(some_data, None).unwrap();
  /// ```
  pub fn write(&self, data: D, source_timestamp: Option<Timestamp>) -> WriteResult<(), D> {
    self.write_with_options(data, WriteOptions::from(source_timestamp))?;
    Ok(())
  }

  pub fn write_with_options(
    &self,
    data: D,
    write_options: WriteOptions,
  ) -> WriteResult<SampleIdentity, D> {
    // serialize
    let send_buffer = match SA::to_bytes(&data) {
      Ok(b) => b,
      Err(e) => {
        return Err(WriteError::Serialization {
          reason: format!("{e}"),
          data,
        })
      }
    };

    let ddsdata = DDSData::new(SerializedPayload::new_from_bytes(
      SA::output_encoding(),
      send_buffer,
    ));

    // Admission allocates the sequence number and stores the sample only if the
    // reliable send window has room; otherwise it blocks up to
    // `reliable_max_blocking_time` and then returns WouldBlock (back-pressure).
    let timeout = self.qos().reliable_max_blocking_time().map(|d| d.to_std());
    match self
      .send_buffer
      .admit_blocking(write_options, ddsdata, timeout)
    {
      Admission::Admitted(sequence_number) => {
        self.ring_doorbell();
        self.refresh_manual_liveliness();
        Ok(SampleIdentity {
          writer_guid: self.my_guid,
          sequence_number,
        })
      }
      Admission::WouldBlock => {
        warn!(
          "Write timed out (reliable send window full): topic={:?}  timeout={:?}",
          self.my_topic.name(),
          timeout,
        );
        Err(WriteError::WouldBlock { data })
      }
    }
  }

  /// This operation blocks the calling thread until either all data written by
  /// the reliable DataWriter entities is acknowledged by all
  /// matched reliable DataReader entities, or else the duration specified by
  /// the `max_wait` parameter elapses, whichever happens first.
  ///
  /// See DDS Spec 1.4 Section 2.2.2.4.1.12 wait_for_acknowledgments.
  ///
  /// If this DataWriter is not set to Reliable, or there are no matched
  /// DataReaders with Reliable QoS, the call succeeds immediately.
  ///
  /// Return values
  /// * `Ok(true)` - all acknowledged
  /// * `Ok(false)`- timed out waiting for acknowledgments
  /// * `Err(_)` - something went wrong
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// let some_data = SomeType { a: 1 };
  /// data_writer.write(some_data, None).unwrap();
  /// data_writer.wait_for_acknowledgments(std::time::Duration::from_millis(100));
  /// ```
  pub fn wait_for_acknowledgments(&self, max_wait: Duration) -> WriteResult<bool, ()> {
    match &self.qos_policy.reliability {
      None | Some(Reliability::BestEffort) => Ok(true),
      Some(Reliability::Reliable { .. }) => {
        // Wait until every matched reliable reader has acknowledged everything we
        // have written so far (the current last sequence number), or we time out.
        let target = self.send_buffer.last_change_sequence_number();
        Ok(self.send_buffer.wait_for_acked_through(target, max_wait))
      }
    } // match
  }

  /*

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when functional
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Liveliness lost status has changed
  ///
  /// if let Ok(lls) = data_writer.get_liveliness_lost_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_liveliness_lost_status(&self) -> Result<LivelinessLostStatus> {
    todo!()
  }

  /// Should get latest offered deadline missed status. <b>Do not use yet</b> use `get_status_lister` instead for the moment.
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Deadline missed status has changed
  ///
  /// if let Ok(odms) = data_writer.get_offered_deadline_missed_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_offered_deadline_missed_status(&self) -> Result<OfferedDeadlineMissedStatus> {
    let mut fstatus = OfferedDeadlineMissedStatus::new();
    while let Ok(status) = self.status_receiver.try_recv() {
      match status {
        StatusChange::OfferedDeadlineMissedStatus(status) => fstatus = status,
  // TODO: possibly save old statuses
        _ => (),
      }
    }

    match self
      .cc_upload
      .try_send(WriterCommand::ResetOfferedDeadlineMissedStatus {
        writer_guid: self.guid(),
      }) {
      Ok(_) => (),
      Err(e) => error!("Unable to send ResetOfferedDeadlineMissedStatus. {e:?}"),
    };

    Ok(fstatus)
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when functional
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Liveliness lost status has changed
  ///
  /// if let Ok(oiqs) = data_writer.get_offered_incompatible_qos_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_offered_incompatible_qos_status(&self) -> Result<OfferedIncompatibleQosStatus> {
    todo!()
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when functional
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(topic, None).unwrap();
  ///
  /// // Liveliness lost status has changed
  ///
  /// if let Ok(pms) = data_writer.get_publication_matched_status() {
  ///   // do something
  /// }
  /// ```
  pub fn get_publication_matched_status(&self) -> Result<PublicationMatchedStatus> {
    todo!()
  }

  */

  /// Topic assigned to this DataWriter
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// assert_eq!(data_writer.topic(), &topic);
  /// ```
  pub fn topic(&self) -> &Topic {
    &self.my_topic
  }

  /// Publisher assigned to this DataWriter
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// assert_eq!(data_writer.publisher(), &publisher);
  pub fn publisher(&self) -> &Publisher {
    &self.my_publisher
  }

  /// Manually asserts liveliness (use this instead of refresh) according to QoS
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// data_writer.assert_liveliness().unwrap();
  /// ```
  ///
  /// An `Err` result means that livelines assertion message could not be sent,
  /// likely because Discovery has too much work to do.
  pub fn assert_liveliness(&self) -> WriteResult<(), ()> {
    self.refresh_manual_liveliness();

    match self.qos().liveliness {
      Some(Liveliness::ManualByTopic { lease_duration: _ }) => {
        self
          .discovery_command
          .send(DiscoveryCommand::AssertTopicLiveliness {
            writer_guid: self.guid(),
            manual_assertion: true, // by definition of this function
          })
          .map_err(|e| {
            error!("assert_liveness - Failed to send DiscoveryCommand. {e:?}");
            WriteError::WouldBlock { data: () }
          })
      }
      _other => Ok(()),
    }
  }

  /// Unimplemented. <b>Do not use</b>.
  ///
  /// # Examples
  ///
  /// ```no_run
  // TODO: enable when available
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32 }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(),
  /// "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType,
  /// CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// for sub in data_writer.get_matched_subscriptions().iter() {
  ///   // do something
  /// }
  pub fn get_matched_subscriptions(&self) -> Vec<SubscriptionBuiltinTopicData> {
    todo!()
  }

  /// Disposes data instance with specified key
  ///
  /// # Arguments
  ///
  /// * `key` - Key of the instance
  /// * `source_timestamp` - DDS source timestamp (None uses now as time as
  ///   specified in DDS spec)
  ///
  /// # Examples
  ///
  /// ```
  /// # use serde::{Serialize, Deserialize};
  /// # use rustdds::*;
  /// # use rustdds::with_key::DataWriter;
  /// # use rustdds::serialization::CDRSerializerAdapter;
  /// #
  /// let domain_participant = DomainParticipant::new(0).unwrap();
  /// let qos = QosPolicyBuilder::new().build();
  /// let publisher = domain_participant.create_publisher(&qos).unwrap();
  ///
  /// #[derive(Serialize, Deserialize, Debug)]
  /// struct SomeType { a: i32, val: usize }
  /// impl Keyed for SomeType {
  ///   type K = i32;
  ///
  ///   fn key(&self) -> Self::K {
  ///     self.a
  ///   }
  /// }
  ///
  /// // WithKey is important
  /// let topic = domain_participant.create_topic("some_topic".to_string(), "SomeType".to_string(), &qos, TopicKind::WithKey).unwrap();
  /// let data_writer = publisher.create_datawriter::<SomeType, CDRSerializerAdapter<_>>(&topic, None).unwrap();
  ///
  /// let some_data_1_1 = SomeType { a: 1, val: 3};
  /// let some_data_1_2 = SomeType { a: 1, val: 4};
  /// // different key
  /// let some_data_2_1 = SomeType { a: 2, val: 5};
  /// let some_data_2_2 = SomeType { a: 2, val: 6};
  ///
  /// data_writer.write(some_data_1_1, None).unwrap();
  /// data_writer.write(some_data_1_2, None).unwrap();
  /// data_writer.write(some_data_2_1, None).unwrap();
  /// data_writer.write(some_data_2_2, None).unwrap();
  ///
  /// // disposes both some_data_1_1 and some_data_1_2. They are no longer offered by this writer to this topic.
  /// data_writer.dispose(&1, None).unwrap();
  /// ```
  pub fn dispose(
    &self,
    key: &<D as Keyed>::K,
    source_timestamp: Option<Timestamp>,
  ) -> WriteResult<(), ()> {
    let send_buffer = SA::key_to_bytes(key).map_err(|e| WriteError::Serialization {
      reason: format!("{e}"),
      data: (),
    })?; // serialize key

    let ddsdata = DDSData::new_disposed_by_key(
      ChangeKind::NotAliveDisposed,
      SerializedPayload::new_from_bytes(SA::output_encoding(), send_buffer),
    );
    let timeout = self.qos().reliable_max_blocking_time().map(|d| d.to_std());
    match self
      .send_buffer
      .admit_blocking(WriteOptions::from(source_timestamp), ddsdata, timeout)
    {
      Admission::Admitted(_seq) => {
        self.ring_doorbell();
        self.refresh_manual_liveliness();
        Ok(())
      }
      Admission::WouldBlock => Err(WriteError::WouldBlock { data: () }),
    }
  }
}

impl<'a, D, SA> StatusEvented<'a, DataWriterStatus, StatusReceiverStream<'a, DataWriterStatus>>
  for DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  fn as_status_evented(&mut self) -> &dyn mio_06::Evented {
    self.status_receiver.as_status_evented()
  }

  fn as_status_source(&mut self) -> &mut dyn mio_08::event::Source {
    self.status_receiver.as_status_source()
  }

  fn as_async_status_stream(&'a self) -> StatusReceiverStream<'a, DataWriterStatus> {
    self.status_receiver.as_async_status_stream()
  }

  fn try_recv_status(&self) -> Option<DataWriterStatus> {
    self.status_receiver.try_recv_status()
  }
}

impl<D, SA> RTPSEntity for DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  fn guid(&self) -> GUID {
    self.my_guid
  }
}

impl<D, SA> HasQoSPolicy for DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  fn qos(&self) -> QosPolicies {
    self.qos_policy.clone()
  }
}

//-------------------------------------------------------------------------------
// async writing implementation
//

// A future for an asynchronous write operation
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct AsyncWrite<'a, D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  writer: &'a DataWriter<D, SA>,
  // The (write options, serialized sample) awaiting admission into the send
  // buffer. Taken out once the write succeeds or fails.
  pending: Option<(WriteOptions, DDSData)>,
  timeout_instant: Instant,
  // The original sample, returned to the caller on WouldBlock.
  sample: Option<D>,
}

// This is required, because AsyncWrite contains "D".
// TODO: Is it ok to promise Unpin here?
impl<D, SA> Unpin for AsyncWrite<'_, D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
}

impl<D, SA> Future for AsyncWrite<'_, D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  type Output = WriteResult<SampleIdentity, D>;

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let Some((write_options, ddsdata)) = self.pending.take() else {
      // Polled after completion. This should not happen.
      return Poll::Ready(Err(WriteError::Internal {
        reason: "AsyncWrite polled after completion".to_owned(),
      }));
    };

    // Non-blocking admission. On a full reliable window the waker is registered
    // inside `try_admit`, so we are re-polled when room becomes available.
    match self
      .writer
      .send_buffer
      .try_admit(write_options, ddsdata, cx.waker())
    {
      Ok(sequence_number) => {
        self.writer.ring_doorbell();
        self.writer.refresh_manual_liveliness();
        Poll::Ready(Ok(SampleIdentity {
          writer_guid: self.writer.my_guid,
          sequence_number,
        }))
      }
      Err((write_options, ddsdata)) => {
        if Instant::now() < self.timeout_instant {
          self.pending = Some((write_options, ddsdata));
          Poll::Pending
        } else {
          Poll::Ready(Err(WriteError::WouldBlock {
            data: self.sample.take().unwrap(),
          }))
        }
      }
    }
  }
}

// A future for an asynchronous operation of waiting for acknowledgements.
// Resolves once every matched reliable reader has acknowledged everything up to
// `target`. There is no timeout here; use async combinators to add one.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct AsyncWaitForAcknowledgments<'a, D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  writer: &'a DataWriter<D, SA>,
  target: SequenceNumber,
}

impl<D, SA> Future for AsyncWaitForAcknowledgments<'_, D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  type Output = WriteResult<bool, ()>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.writer.send_buffer.is_acked_through(self.target) {
      return Poll::Ready(Ok(true));
    }
    // Register to be woken when the acknowledgement frontier advances, then
    // re-check to avoid a lost-wakeup race.
    self.writer.send_buffer.register_ack_waker(cx.waker());
    if self.writer.send_buffer.is_acked_through(self.target) {
      Poll::Ready(Ok(true))
    } else {
      Poll::Pending
    }
  }
}

impl<D, SA> DataWriter<D, SA>
where
  D: Keyed,
  SA: SerializerAdapter<D>,
{
  pub async fn async_write(
    &self,
    data: D,
    source_timestamp: Option<Timestamp>,
  ) -> WriteResult<(), D> {
    match self
      .async_write_with_options(data, WriteOptions::from(source_timestamp))
      .await
    {
      Ok(_sample_identity) => Ok(()),
      Err(e) => Err(e),
    }
  }

  pub async fn async_write_with_options(
    &self,
    data: D,
    write_options: WriteOptions,
  ) -> WriteResult<SampleIdentity, D> {
    // Construct a future for an async write operation and await for its completion

    let send_buffer = match SA::to_bytes(&data) {
      Ok(s) => s,
      Err(e) => {
        return Err(WriteError::Serialization {
          reason: format!("{e}"),
          data,
        })
      }
    };

    let dds_data = DDSData::new(SerializedPayload::new_from_bytes(
      SA::output_encoding(),
      send_buffer,
    ));

    let timeout = self.qos().reliable_max_blocking_time();

    let write_future = AsyncWrite {
      writer: self,
      pending: Some((write_options, dds_data)),
      timeout_instant: std::time::Instant::now()
        + timeout
          .map(|t| t.to_std())
          .unwrap_or(crate::dds::helpers::TIMEOUT_FALLBACK.to_std()),
      sample: Some(data),
    };
    write_future.await
  }

  /// Like the synchronous version.
  /// But there is no timeout. Use asyncs to bring your own timeout.
  pub async fn async_wait_for_acknowledgments(&self) -> WriteResult<bool, ()> {
    match &self.qos_policy.reliability {
      None | Some(Reliability::BestEffort) => Ok(true),
      Some(Reliability::Reliable { .. }) => {
        let target = self.send_buffer.last_change_sequence_number();
        AsyncWaitForAcknowledgments {
          writer: self,
          target,
        }
        .await
      }
    }
  }
} // impl

#[cfg(test)]
mod tests {
  use std::thread;

  use byteorder::LittleEndian;
  use log::info;

  use super::*;
  use crate::{
    dds::{key::Key, participant::DomainParticipant},
    structure::topic_kind::TopicKind,
    test::random_data::*,
  };

  #[test]
  fn dw_write_test() {
    let domain_participant = DomainParticipant::new(0).expect("Publisher creation failed!");
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

    let mut data = RandomData {
      a: 4,
      b: "Fobar".to_string(),
    };

    data_writer
      .write(data.clone(), None)
      .expect("Unable to write data");

    data.a = 5;
    let timestamp = Timestamp::now();
    data_writer
      .write(data, Some(timestamp))
      .expect("Unable to write data with timestamp");

    // TODO: verify that data is sent/written correctly
    // TODO: write also with timestamp
  }

  #[test]
  fn dw_dispose_test() {
    let domain_participant = DomainParticipant::new(0).expect("Publisher creation failed!");
    let qos = QosPolicies::qos_none();
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

    let key = &data.key().hash_key(false);
    info!("key: {key:?}");

    data_writer
      .write(data.clone(), None)
      .expect("Unable to write data");

    thread::sleep(Duration::from_millis(100));
    data_writer
      .dispose(&data.key(), None)
      .expect("Unable to dispose data");

    // TODO: verify that dispose is sent correctly
  }

  #[test]
  fn dw_wait_for_ack_test() {
    let domain_participant = DomainParticipant::new(0).expect("Participant creation failed!");
    let qos = QosPolicies::qos_none();
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

    data_writer.write(data, None).expect("Unable to write data");

    let res = data_writer
      .wait_for_acknowledgments(Duration::from_secs(2))
      .unwrap();
    assert!(res); // we should get "true" immediately, because we have
                  // no Reliable QoS
  }
}
