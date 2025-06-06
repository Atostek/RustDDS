use std::{
  cmp::max,
  collections::BTreeMap,
  io,
  marker::PhantomData,
  pin::Pin,
  sync::{Arc, Mutex, MutexGuard},
  task::{Context, Poll, Waker},
};

use futures::stream::{FusedStream, Stream};
use serde::de::DeserializeOwned;
use mio_extras::channel as mio_channel;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    adapters::with_key::{Decode, DefaultDecoder, DeserializerAdapter},
    ddsdata::*,
    key::*,
    pubsub::Subscriber,
    qos::*,
    result::*,
    statusevents::*,
    topic::{Topic, TopicDescription},
    with_key::datasample::{DeserializedCacheChange, Sample},
  },
  discovery::discovery::DiscoveryCommand,
  mio_source::PollEventSource,
  serialization::CDRDeserializerAdapter,
  structure::{
    cache_change::CacheChange,
    dds_cache::TopicCache,
    entity::RTPSEntity,
    guid::{EntityId, GUID},
    sequence_number::SequenceNumber,
    time::Timestamp,
  },
};

#[derive(Clone, Debug)]
pub(crate) enum ReaderCommand {
  #[allow(dead_code)] // TODO: Implement this (resetting) feature
  ResetRequestedDeadlineStatus,
}

// This is helper struct.
// All mutable state needed for reading should go here.
pub(crate) struct ReadState<K: Key> {
  latest_instant: Timestamp, /* This is used as a read pointer from dds_cache for BEST_EFFORT
                              * reading */
  last_read_sn: BTreeMap<GUID, SequenceNumber>, // collection of read pointers for RELIABLE reading
  /// hash_to_key_map is used for decoding received key hashes back to original
  /// key values. This is needed when we receive a dispose message via hash
  /// only.
  hash_to_key_map: BTreeMap<KeyHash, K>, // TODO: garbage collect this somehow
}

impl<K: Key> ReadState<K> {
  fn new() -> Self {
    ReadState {
      latest_instant: Timestamp::ZERO,
      last_read_sn: BTreeMap::new(),
      hash_to_key_map: BTreeMap::<KeyHash, K>::new(),
    }
  }

  // This is a helper function so that borrow checker understands
  // that we are splitting one mutable borrow into two _disjoint_ mutable
  // borrows.
  fn get_sn_map_and_hash_map(
    &mut self,
  ) -> (
    &mut BTreeMap<GUID, SequenceNumber>,
    &mut BTreeMap<KeyHash, K>,
  ) {
    let ReadState {
      last_read_sn,
      hash_to_key_map,
      ..
    } = self;
    (last_read_sn, hash_to_key_map)
  }
}

/// SimpleDataReaders can only do "take" semantics and does not have
/// any deduplication or other DataSampleCache functionality.
pub struct SimpleDataReader<D: Keyed, DA: DeserializerAdapter<D> = CDRDeserializerAdapter<D>> {
  my_subscriber: Subscriber,

  my_topic: Topic,
  qos_policy: QosPolicies,
  my_guid: GUID,

  // mio_channel::Receiver is not thread-safe, so Mutex protects it.
  pub(crate) notification_receiver: Mutex<mio_channel::Receiver<()>>,

  // SimpleDataReader stores a pointer to a mutex on the topic cache
  topic_cache: Arc<Mutex<TopicCache>>,

  read_state: Mutex<ReadState<<D as Keyed>::K>>,

  deserializer_type: PhantomData<DA>, // This is to provide use for DA

  discovery_command: mio_channel::SyncSender<DiscoveryCommand>,
  status_receiver: StatusChannelReceiver<DataReaderStatus>,

  #[allow(dead_code)] // TODO: This is currently unused, because we do not implement
  // resetting deadline missed status. Remove attribute when it is supported.
  reader_command: mio_channel::SyncSender<ReaderCommand>,
  data_reader_waker: Arc<Mutex<Option<Waker>>>,

  event_source: PollEventSource,
}

impl<D, DA> Drop for SimpleDataReader<D, DA>
where
  D: Keyed,
  DA: DeserializerAdapter<D>,
{
  fn drop(&mut self) {
    // Tell dp_event_loop
    self.my_subscriber.remove_reader(self.my_guid);

    // Tell discovery
    match self
      .discovery_command
      .send(DiscoveryCommand::RemoveLocalReader { guid: self.my_guid })
    {
      Ok(_) => {}
      Err(mio_channel::SendError::Disconnected(_)) => {
        debug!("Failed to send DiscoveryCommand::RemoveLocalReader . Maybe shutting down?");
      }
      Err(e) => error!("Failed to send DiscoveryCommand::RemoveLocalReader. {e:?}"),
    }
  }
}

impl<D: 'static, DA> SimpleDataReader<D, DA>
where
  D: Keyed,
  DA: DeserializerAdapter<D>,
{
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    subscriber: Subscriber,
    my_id: EntityId,
    topic: Topic,
    qos_policy: QosPolicies,
    // Each notification sent to this channel must be try_recv'd
    notification_receiver: mio_channel::Receiver<()>,
    topic_cache: Arc<Mutex<TopicCache>>,
    discovery_command: mio_channel::SyncSender<DiscoveryCommand>,
    status_receiver: StatusChannelReceiver<DataReaderStatus>,
    reader_command: mio_channel::SyncSender<ReaderCommand>,
    data_reader_waker: Arc<Mutex<Option<Waker>>>,
    event_source: PollEventSource,
  ) -> CreateResult<Self> {
    let dp = match subscriber.participant() {
      Some(dp) => dp,
      None => {
        return Err(CreateError::ResourceDropped {
          reason: "Cannot create new DataReader, DomainParticipant doesn't exist.".to_string(),
        })
      }
    };

    let my_guid = GUID::new_with_prefix_and_id(dp.guid_prefix(), my_id);

    // Verify that the topic cache corresponds to the topic of the Reader
    let topic_cache_name = topic_cache.lock().unwrap().topic_name();
    if topic.name() != topic_cache_name {
      return Err(CreateError::Internal {
        reason: format!(
          "Topic name = {} and topic cache name = {} not equal when creating a SimpleDataReader",
          topic.name(),
          topic_cache_name
        ),
      });
    }

    Ok(Self {
      my_subscriber: subscriber,
      qos_policy,
      my_guid,
      notification_receiver: Mutex::new(notification_receiver),
      topic_cache,
      read_state: Mutex::new(ReadState::new()),
      my_topic: topic,
      deserializer_type: PhantomData,
      discovery_command,
      status_receiver,
      reader_command,
      data_reader_waker,
      event_source,
    })
  }
  pub(crate) fn set_waker(&self, w: Option<Waker>) {
    *self.data_reader_waker.lock().unwrap() = w;
  }

  pub(crate) fn drain_read_notifications(&self) {
    let rec = self.notification_receiver.lock().unwrap();
    while rec.try_recv().is_ok() {}
    self.event_source.drain();
  }

  fn try_take_undecoded<'a, 'b: 'a>(
    is_reliable: bool,
    topic_cache: &'a TopicCache,
    latest_instant: Timestamp,
    last_read_sn: &'b BTreeMap<GUID, SequenceNumber>,
  ) -> impl Iterator<Item = (Timestamp, &'a CacheChange)> {
    topic_cache.get_changes_in_range(is_reliable, latest_instant, last_read_sn)
  }

  fn update_hash_to_key_map(
    hash_to_key_map: &mut BTreeMap<KeyHash, D::K>,
    deserialized: &Sample<D, D::K>,
  ) {
    let instance_key = match deserialized {
      Sample::Value(d) => d.key(),
      Sample::Dispose(k) => k.clone(),
    };
    hash_to_key_map.insert(instance_key.hash_key(false), instance_key);
  }

  fn deserialize_with<S>(
    &self,
    timestamp: Timestamp,
    cc: &CacheChange,
    hash_to_key_map: &mut BTreeMap<KeyHash, D::K>,
    decoder: S,
  ) -> ReadResult<DeserializedCacheChange<D>>
  where
    S: Decode<DA::Decoded, DA::DecodedKey>,
  {
    match cc.data_value {
      DDSData::Data {
        ref serialized_payload,
      } => {
        // what is our data serialization format (representation identifier) ?
        if let Some(recognized_rep_id) = DA::supported_encodings()
          .iter()
          .find(|r| **r == serialized_payload.representation_identifier)
        {
          match DA::from_bytes_with(&serialized_payload.value, *recognized_rep_id, decoder) {
            // Data update, decoded ok
            Ok(payload) => {
              let p = Sample::Value(payload);
              Self::update_hash_to_key_map(hash_to_key_map, &p);
              Ok(DeserializedCacheChange::new(timestamp, cc, p))
            }
            Err(e) => Err(ReadError::Deserialization {
              reason: format!(
                "Failed to deserialize sample bytes: {}, , Topic = {}, Type = {:?}",
                e,
                self.my_topic.name(),
                self.my_topic.get_type()
              ),
            }),
          }
        } else {
          info!(
            "Unknown representation id: {:?} , Topic = {}, Type = {:?} data = {:02x?}",
            serialized_payload.representation_identifier,
            self.my_topic.name(),
            self.my_topic.get_type(),
            serialized_payload.value,
          );
          Err(ReadError::Deserialization {
            reason: format!(
              "Unknown representation id {:?} , Topic = {}, Type = {:?}",
              serialized_payload.representation_identifier,
              self.my_topic.name(),
              self.my_topic.get_type()
            ),
          })
        }
      }

      DDSData::DisposeByKey {
        key: ref serialized_key,
        ..
      } => {
        match DA::key_from_bytes_with(
          &serialized_key.value,
          serialized_key.representation_identifier,
          decoder,
        ) {
          Ok(key) => {
            let k = Sample::Dispose(key);
            Self::update_hash_to_key_map(hash_to_key_map, &k);
            Ok(DeserializedCacheChange::new(timestamp, cc, k))
          }
          Err(e) => Err(ReadError::Deserialization {
            reason: format!(
              "Failed to deserialize key {}, Topic = {}, Type = {:?}",
              e,
              self.my_topic.name(),
              self.my_topic.get_type()
            ),
          }),
        }
      }

      DDSData::DisposeByKeyHash { key_hash, .. } => {
        // The cache should know hash -> key mapping even if the sample
        // has been disposed or .take()n
        if let Some(key) = hash_to_key_map.get(&key_hash) {
          Ok(DeserializedCacheChange::new(
            timestamp,
            cc,
            Sample::Dispose(key.clone()),
          ))
        } else {
          Err(ReadError::UnknownKey {
            details: format!(
              "Received dispose with unknown key hash: {:x?}, Topic = {}, Type = {:?}",
              key_hash,
              self.my_topic.name(),
              self.my_topic.get_type()
            ),
          })
        }
      }
    } // match
  }

  /// Note: Always remember to call .drain_read_notifications() just before
  /// calling this one. Otherwise, new notifications may not appear.
  pub fn try_take_one(&self) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    DA: DeserializerAdapter<D> + DefaultDecoder<D>,
  {
    Self::try_take_one_with(self, DA::DECODER)
  }

  /// Note: Always remember to call .drain_read_notifications() just before
  /// calling this one. Otherwise, new notifications may not appear.
  #[allow(clippy::needless_pass_by_value)]
  pub fn try_take_one_with<S>(&self, decoder: S) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    S: Decode<DA::Decoded, DA::DecodedKey> + Clone,
  {
    let is_reliable = matches!(
      self.qos_policy.reliability(),
      Some(policy::Reliability::Reliable { .. })
    );

    let topic_cache = self.acquire_the_topic_cache_guard();

    let mut read_state_ref = self.read_state.lock().unwrap();
    let latest_instant = read_state_ref.latest_instant;
    let (last_read_sn, hash_to_key_map) = read_state_ref.get_sn_map_and_hash_map();

    let mut changes = Self::try_take_undecoded(is_reliable, &topic_cache, latest_instant, last_read_sn);

    // loop in case we get a sample that should be ignored, so we try next.
    loop {
      let (timestamp, cc) =
        match changes
          .next()
        {
          None => return Ok(None), // no more data available right now
          Some((ts, cc)) => (ts, cc),
        };

      let result = self.deserialize_with(timestamp, cc, hash_to_key_map, decoder.clone());

      if let Err(ReadError::UnknownKey { .. }) = result {
        // ignore unknown key hash, continue looping
      } else {
        // explicitly drop the cache changes to update the reader state
        drop(changes);

        // return with this result
        // make copies of guid and SN to calm down borrow checker.
        let writer_guid = cc.writer_guid;
        let sequence_number = cc.sequence_number;
        // Advance read pointer, error or not, because otherwise
        // the SimpleDatareader is stuck.
        read_state_ref.latest_instant = max(latest_instant, timestamp);
        read_state_ref
          .last_read_sn
          .insert(writer_guid, sequence_number);

        // // Debug sanity check:
        // use crate::Duration;
        // if Timestamp::now().duration_since(timestamp) > Duration::from_secs(1) {
        //   error!("Sample delayed by {:?} , Topic = {} {:?}",
        //     Timestamp::now().duration_since(timestamp), self.topic().name(),
        //     sequence_number,
        //      );
        // }

        return result.map(Some);
      }
    }
  }

  pub fn qos(&self) -> &QosPolicies {
    &self.qos_policy
  }

  pub fn guid(&self) -> GUID {
    self.my_guid
  }

  pub fn topic(&self) -> &Topic {
    &self.my_topic
  }

  pub fn as_async_stream<S>(&self) -> SimpleDataReaderStream<'_, D, S, DA>
  where
    DA: DefaultDecoder<D, Decoder = S>,
    DA::Decoder: Clone,
    S: Decode<DA::Decoded, DA::DecodedKey>,
  {
    Self::as_async_stream_with(self, DA::DECODER)
  }

  pub fn as_async_stream_with<S>(&self, decoder: S) -> SimpleDataReaderStream<'_, D, S, DA>
  where
    S: Decode<DA::Decoded, DA::DecodedKey> + Clone,
  {
    SimpleDataReaderStream {
      simple_datareader: self,
      decoder,
    }
  }

  fn acquire_the_topic_cache_guard(&self) -> MutexGuard<'_, TopicCache> {
    self.topic_cache.lock().unwrap_or_else(|e| {
      panic!(
        "The topic cache of topic {} is poisoned. Error: {}",
        &self.my_topic.name(),
        e
      )
    })
  }
}

// This is  not part of DDS spec. We implement mio mio_06::Evented so that the
// application can asynchronously poll DataReader(s).
impl<D, DA> mio_06::Evented for SimpleDataReader<D, DA>
where
  D: Keyed,
  DA: DeserializerAdapter<D>,
{
  // We just delegate all the operations to notification_receiver, since it
  // already implements mio_06::Evented
  fn register(
    &self,
    poll: &mio_06::Poll,
    token: mio_06::Token,
    interest: mio_06::Ready,
    opts: mio_06::PollOpt,
  ) -> io::Result<()> {
    self
      .notification_receiver
      .lock()
      .unwrap()
      .register(poll, token, interest, opts)
  }

  fn reregister(
    &self,
    poll: &mio_06::Poll,
    token: mio_06::Token,
    interest: mio_06::Ready,
    opts: mio_06::PollOpt,
  ) -> io::Result<()> {
    self
      .notification_receiver
      .lock()
      .unwrap()
      .reregister(poll, token, interest, opts)
  }

  fn deregister(&self, poll: &mio_06::Poll) -> io::Result<()> {
    self.notification_receiver.lock().unwrap().deregister(poll)
  }
}

impl<D, DA> mio_08::event::Source for SimpleDataReader<D, DA>
where
  D: Keyed,
  DA: DeserializerAdapter<D>,
{
  fn register(
    &mut self,
    registry: &mio_08::Registry,
    token: mio_08::Token,
    interests: mio_08::Interest,
  ) -> io::Result<()> {
    self.event_source.register(registry, token, interests)
  }

  fn reregister(
    &mut self,
    registry: &mio_08::Registry,
    token: mio_08::Token,
    interests: mio_08::Interest,
  ) -> io::Result<()> {
    self.event_source.reregister(registry, token, interests)
  }

  fn deregister(&mut self, registry: &mio_08::Registry) -> io::Result<()> {
    self.event_source.deregister(registry)
  }
}

impl<'a, D, DA> StatusEvented<'a, DataReaderStatus, SimpleDataReaderEventStream<'a, D, DA>>
  for SimpleDataReader<D, DA>
where
  D: Keyed,
  DA: DeserializerAdapter<D>,
{
  fn as_status_evented(&mut self) -> &dyn mio_06::Evented {
    self.status_receiver.as_status_evented()
  }

  fn as_status_source(&mut self) -> &mut dyn mio_08::event::Source {
    self.status_receiver.as_status_source()
  }

  fn as_async_status_stream(&'a self) -> SimpleDataReaderEventStream<'a, D, DA> {
    SimpleDataReaderEventStream {
      simple_datareader: self,
    }
  }

  fn try_recv_status(&self) -> Option<DataReaderStatus> {
    self.status_receiver.try_recv_status()
  }
}

impl<D, DA> RTPSEntity for SimpleDataReader<D, DA>
where
  D: Keyed + DeserializeOwned,
  DA: DeserializerAdapter<D>,
{
  fn guid(&self) -> GUID {
    self.my_guid
  }
}

// ----------------------------------------------
// ----------------------------------------------

// Async interface to the SimpleDataReader

pub struct SimpleDataReaderStream<
  'a,
  D: Keyed + 'static,
  S: Decode<DA::Decoded, DA::DecodedKey>,
  DA: DeserializerAdapter<D> + 'static = CDRDeserializerAdapter<D>,
> {
  simple_datareader: &'a SimpleDataReader<D, DA>,
  decoder: S,
}

// ----------------------------------------------
// ----------------------------------------------

// https://users.rust-lang.org/t/take-in-impl-future-cannot-borrow-data-in-a-dereference-of-pin/52042
impl<D, S, DA> Unpin for SimpleDataReaderStream<'_, D, S, DA>
where
  D: Keyed + 'static,
  DA: DeserializerAdapter<D>,
  S: Decode<DA::Decoded, DA::DecodedKey> + Unpin,
{
}

impl<D, S, DA> Stream for SimpleDataReaderStream<'_, D, S, DA>
where
  D: Keyed + 'static,
  DA: DeserializerAdapter<D>,
  S: Decode<DA::Decoded, DA::DecodedKey> + Clone,
{
  type Item = ReadResult<DeserializedCacheChange<D>>;

  // The full return type is now
  // Poll<Option<Result<DeserializedCacheChange<D>>>
  // Poll -> Ready or Pending
  // Option -> Some = stream produces a value, None = stream has ended (does not
  // occur) Result -> Ok = No DDS error, Err = DDS processing error
  // (inner Option -> Some = there is new value/key, None = no new data yet)

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    debug!("poll_next");
    match self
      .simple_datareader
      .try_take_one_with(self.decoder.clone())
    {
      Err(e) =>
      // DDS fails
      {
        Poll::Ready(Some(Err(e)))
      }

      // ok, got something
      Ok(Some(d)) => Poll::Ready(Some(Ok(d))),

      // No new data (yet)
      Ok(None) => {
        // Did not get any data.
        // --> Store waker.
        // 1. synchronously store waker to background thread (must rendezvous)
        // 2. try take_bare again, in case something arrived just now
        // 3. if nothing still, return pending.

        // // DEBUG
        // if self.simple_datareader.guid().entity_id.entity_kind.is_user_defined() {
        //   error!("Setting waker for {:?}", self.simple_datareader.topic().name());
        // }
        // // DEBUG
        self.simple_datareader.set_waker(Some(cx.waker().clone()));
        match self
          .simple_datareader
          .try_take_one_with(self.decoder.clone())
        {
          Err(e) => Poll::Ready(Some(Err(e))),
          Ok(Some(d)) => Poll::Ready(Some(Ok(d))),
          Ok(None) => Poll::Pending,
        }
      }
    } // match
  } // fn
} // impl

impl<D, S, DA> FusedStream for SimpleDataReaderStream<'_, D, S, DA>
where
  D: Keyed + 'static,
  DA: DeserializerAdapter<D>,
  S: Decode<DA::Decoded, DA::DecodedKey> + Clone,
{
  fn is_terminated(&self) -> bool {
    false // Never terminate. This means it is always valid to call poll_next().
  }
}

// ----------------------------------------------
// ----------------------------------------------

pub struct SimpleDataReaderEventStream<
  'a,
  D: Keyed + 'static,
  DA: DeserializerAdapter<D> + 'static = CDRDeserializerAdapter<D>,
> {
  simple_datareader: &'a SimpleDataReader<D, DA>,
}

impl<D, DA> Stream for SimpleDataReaderEventStream<'_, D, DA>
where
  D: Keyed + 'static,
  DA: DeserializerAdapter<D>,
{
  type Item = DataReaderStatus;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(
      &mut self
        .simple_datareader
        .status_receiver
        .as_async_status_stream(),
    )
    .poll_next(cx)
  } // fn
} // impl

impl<D, DA> FusedStream for SimpleDataReaderEventStream<'_, D, DA>
where
  D: Keyed + 'static,
  DA: DeserializerAdapter<D>,
{
  fn is_terminated(&self) -> bool {
    self
      .simple_datareader
      .status_receiver
      .as_async_status_stream()
      .is_terminated()
  }
}
