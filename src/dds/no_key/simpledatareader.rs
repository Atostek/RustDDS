use std::{
  io,
  pin::Pin,
  task::{Context, Poll, Waker},
};

use futures::stream::{FusedStream, Stream, StreamExt};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    adapters::no_key::*,
    no_key::{datasample::DeserializedCacheChange, wrappers::DecodeWrapper},
    qos::*,
    result::ReadResult,
    statusevents::*,
    with_key,
  },
  serialization::CDRDeserializerAdapter,
  structure::entity::RTPSEntity,
  GUID,
};
use super::wrappers::{DAWrapper, NoKeyWrapper};

/// SimpleDataReaders can only do "take" semantics and does not have
/// any deduplication or other DataSampleCache functionality.
pub struct SimpleDataReader<D, DA: DeserializerAdapter<D> = CDRDeserializerAdapter<D>> {
  keyed_simpledatareader: with_key::SimpleDataReader<NoKeyWrapper<D>, DAWrapper<DA>>,
}

/// Simplified type for CDR encoding
pub type SimpleDataReaderCdr<D> = SimpleDataReader<D, CDRDeserializerAdapter<D>>;

impl<D: 'static, DA> SimpleDataReader<D, DA>
where
  DA: DeserializerAdapter<D> + 'static,
{
  // TODO: Make it possible to construct SimpleDataReader (particularly, no_key
  // version) from the public API. That is, From a Subscriber object like a
  // normal Datareader. This is to be then used from the ros2-client package.
  pub(crate) fn from_keyed(
    keyed_simpledatareader: with_key::SimpleDataReader<NoKeyWrapper<D>, DAWrapper<DA>>,
  ) -> Self {
    Self {
      keyed_simpledatareader,
    }
  }

  pub fn set_waker(&self, w: Option<Waker>) {
    self.keyed_simpledatareader.set_waker(w);
  }

  pub fn drain_read_notifications(&self) {
    self.keyed_simpledatareader.drain_read_notifications();
  }

  pub fn try_take_one(&self) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    DA: DefaultDecoder<D>,
  {
    Self::try_take_one_with(self, DA::DECODER)
  }

  pub fn try_take_one_with<S>(&self, decoder: S) -> ReadResult<Option<DeserializedCacheChange<D>>>
  where
    S: for<'de> Decode<'de, DA::Decoded, Error = DA::Error> + Clone,
  {
    match self
      .keyed_simpledatareader
      .try_take_one_with(DecodeWrapper::new(decoder))
    {
      Err(e) => Err(e),
      Ok(None) => Ok(None),
      Ok(Some(kdcc)) => match DeserializedCacheChange::<D>::from_keyed(kdcc) {
        Some(dcc) => Ok(Some(dcc)),
        None => Ok(None),
      },
    }
  }

  pub fn qos(&self) -> &QosPolicies {
    self.keyed_simpledatareader.qos()
  }

  pub fn guid(&self) -> GUID {
    self.keyed_simpledatareader.guid()
  }

  pub fn as_async_stream(
    &self,
  ) -> impl FusedStream<Item = ReadResult<DeserializedCacheChange<D>>> + '_
  where
    DA: DefaultDecoder<D>,
  {
    Self::as_async_stream_with(self, DA::DECODER)
  }

  pub fn as_async_stream_with<'a, S>(
    &'a self,
    decoder: S,
  ) -> impl FusedStream<Item = ReadResult<DeserializedCacheChange<D>>> + 'a
  where
    S: for<'de> Decode<'de, DA::Decoded, Error = DA::Error> + Clone + 'a,
  {
    self
      .keyed_simpledatareader
      .as_async_stream_with(DecodeWrapper::new(decoder))
      .filter_map(move |r| async {
        // This is Stream::filter_map, so returning None means just skipping Item.
        match r {
          Err(e) => Some(Err(e)),
          Ok(kdcc) => match DeserializedCacheChange::<D>::from_keyed(kdcc) {
            None => {
              info!("Got dispose from no_key topic.");
              // This means there is some disgreement over the kind of this Topic between
              // us and some Writer. They must think it is WITH_KEY, since they sent a
              // Dispose.
              None
            }
            Some(dcc) => Some(Ok(dcc)),
          },
        }
      })
  }

  // pub fn as_simple_data_reader_event_stream(
  //   &self,
  // ) -> impl Stream<Item = ReadResult<DataReaderStatus>> + '_ { self
  //   .keyed_simpledatareader .as_simple_data_reader_event_stream()
  // }
}

// This is  not part of DDS spec. We implement mio Eventd so that the
// application can asynchronously poll DataReader(s).
impl<D, DA> mio_06::Evented for SimpleDataReader<D, DA>
where
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
      .keyed_simpledatareader
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
      .keyed_simpledatareader
      .reregister(poll, token, interest, opts)
  }

  fn deregister(&self, poll: &mio_06::Poll) -> io::Result<()> {
    self.keyed_simpledatareader.deregister(poll)
  }
}

impl<D, DA> mio_08::event::Source for SimpleDataReader<D, DA>
where
  DA: DeserializerAdapter<D>,
{
  fn register(
    &mut self,
    registry: &mio_08::Registry,
    token: mio_08::Token,
    interests: mio_08::Interest,
  ) -> io::Result<()> {
    mio_08::event::Source::register(&mut self.keyed_simpledatareader, registry, token, interests)
    // self.keyed_simpledatareader.register(registry, token, interests)
  }

  fn reregister(
    &mut self,
    registry: &mio_08::Registry,
    token: mio_08::Token,
    interests: mio_08::Interest,
  ) -> io::Result<()> {
    mio_08::event::Source::reregister(&mut self.keyed_simpledatareader, registry, token, interests)
    // self.keyed_simpledatareader.reregister(registry, token, interests)
  }

  fn deregister(&mut self, registry: &mio_08::Registry) -> io::Result<()> {
    mio_08::event::Source::deregister(&mut self.keyed_simpledatareader, registry)
    // self.keyed_simpledatareader.deregister(registry)
  }
}

//use crate::with_key::SimpleDataReaderEventStream;

impl<'a, D, DA> StatusEvented<'a, DataReaderStatus, SimpleDataReaderEventStream<'a, D, DA>>
  for SimpleDataReader<D, DA>
where
  D: 'static,
  DA: DeserializerAdapter<D>,
{
  fn as_status_evented(&mut self) -> &dyn mio_06::Evented {
    self.keyed_simpledatareader.as_status_evented()
  }

  fn as_status_source(&mut self) -> &mut dyn mio_08::event::Source {
    self.keyed_simpledatareader.as_status_source()
  }

  fn as_async_status_stream(&'a self) -> SimpleDataReaderEventStream<'a, D, DA> {
    SimpleDataReaderEventStream {
      keyed_event_stream: self.keyed_simpledatareader.as_async_status_stream(),
    }
  }

  fn try_recv_status(&self) -> Option<DataReaderStatus> {
    self.keyed_simpledatareader.try_recv_status()
  }
}

impl<D, DA> RTPSEntity for SimpleDataReader<D, DA>
where
  D: 'static,
  DA: DeserializerAdapter<D>,
{
  fn guid(&self) -> GUID {
    self.keyed_simpledatareader.guid()
  }
}

// ----------------------------------------------
// ----------------------------------------------

pub struct SimpleDataReaderEventStream<
  'a,
  D: 'static,
  DA: DeserializerAdapter<D> + 'static = CDRDeserializerAdapter<D>,
> {
  keyed_event_stream:
    crate::with_key::SimpleDataReaderEventStream<'a, NoKeyWrapper<D>, DAWrapper<DA>>,
}

impl<'a, D, DA> SimpleDataReaderEventStream<'a, D, DA>
where
  D: 'static,
  DA: DeserializerAdapter<D>,
{
  pub(crate) fn from_keyed(
    keyed_event_stream: with_key::SimpleDataReaderEventStream<'a, NoKeyWrapper<D>, DAWrapper<DA>>,
  ) -> Self {
    Self { keyed_event_stream }
  }
}

impl<D, DA> Stream for SimpleDataReaderEventStream<'_, D, DA>
where
  D: 'static,
  DA: DeserializerAdapter<D>,
{
  type Item = DataReaderStatus;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.keyed_event_stream).poll_next(cx)
  }
} // impl

impl<D, DA> FusedStream for SimpleDataReaderEventStream<'_, D, DA>
where
  D: 'static,
  DA: DeserializerAdapter<D>,
{
  fn is_terminated(&self) -> bool {
    self.keyed_event_stream.is_terminated()
  }
}
