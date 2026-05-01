use std::{any::TypeId, io, marker::PhantomData};

use serde::{
  de::{Deserialize, DeserializeOwned, DeserializeSeed},
  Serialize,
};
use bytes::Bytes;
use byteorder::{BigEndian, ByteOrder, LittleEndian};

use crate::{
  dds::{
    adapters::{no_key, with_key},
    key::Keyed,
  },
  RepresentationIdentifier,
};
pub use super::*;

/// This type adapts [`CdrSerializer`] (which implements
/// [`serde::Serializer`]) to work as a [`no_key::SerializerAdapter`] and
/// [`with_key::SerializerAdapter`].
///
/// [`CdrSerializer`] cannot directly implement the trait itself, because
/// [`CdrSerializer`] has the type parameter BO open, and the adapter needs to
/// be bi-endian.
pub struct CDRSerializerAdapter<D, BO = LittleEndian>
where
  BO: ByteOrder,
{
  phantom: PhantomData<D>,
  ghost: PhantomData<BO>,
}

impl<D, BO> no_key::SerializerAdapter<D> for CDRSerializerAdapter<D, BO>
where
  D: Serialize,
  BO: ByteOrder + 'static,
{
  type Error = Error;

  fn output_encoding() -> RepresentationIdentifier {
    if TypeId::of::<BO>() == TypeId::of::<LittleEndian>() {
      RepresentationIdentifier::CDR_LE
    } else if TypeId::of::<BO>() == TypeId::of::<BigEndian>() {
      RepresentationIdentifier::CDR_BE
    } else {
      // The trait ByteOrder is sealed, so there are no implementations
      // outside the byteorder package, which defines only LittleEndian
      // and BigEndian impls.
      // If you end up here, please explain how did you find a third implementation.
      unreachable!()
    }
  }

  fn to_bytes(value: &D) -> Result<Bytes> {
    let size_estimate = std::mem::size_of_val(value) * 2; // TODO: crude estimate
    let mut buffer: Vec<u8> = Vec::with_capacity(size_estimate);
    to_writer::<D, BO, &mut Vec<u8>>(&mut buffer, value)?;
    Ok(Bytes::from(buffer))
  }
}

impl<D, BO> with_key::SerializerAdapter<D> for CDRSerializerAdapter<D, BO>
where
  D: Keyed + Serialize,
  <D as Keyed>::K: Serialize,
  BO: ByteOrder + 'static,
{
  fn key_to_bytes(value: &D::K) -> Result<Bytes> {
    let size_estimate = std::mem::size_of_val(value) * 2; // TODO: crude estimate
    let mut buffer: Vec<u8> = Vec::with_capacity(size_estimate);
    to_writer::<D::K, BO, &mut Vec<u8>>(&mut buffer, value)?;
    Ok(Bytes::from(buffer))
  }
}

/// Serialize
pub fn to_writer_with_rep_id<T, W>(
  writer: W,
  value: &T,
  encoding: RepresentationIdentifier,
) -> Result<()>
where
  T: Serialize,
  W: io::Write,
{
  match encoding {
    RepresentationIdentifier::CDR_LE | RepresentationIdentifier::PL_CDR_LE => {
      to_writer::<T, LittleEndian, W>(writer, value)
    }
    _ => to_writer::<T, BigEndian, W>(writer, value),
  }
}

/// This type adapts CdrDeserializer (which implements serde::Deserializer) to
/// work as a [`with_key::DeserializerAdapter`] and
/// [`no_key::DeserializerAdapter`].
///
/// CdrDeserializer cannot directly implement
/// the trait itself, because CdrDeserializer has the type parameter BO open,
/// and the adapter needs to be bi-endian.
pub struct CDRDeserializerAdapter<D> {
  phantom: PhantomData<D>,
}

const REPR_IDS: [RepresentationIdentifier; 3] = [
  RepresentationIdentifier::CDR_BE,
  RepresentationIdentifier::CDR_LE,
  RepresentationIdentifier::PL_CDR_LE,
];

impl<D> no_key::DeserializerAdapter<D> for CDRDeserializerAdapter<D> {
  type Error = Error;
  type Decoded = D;

  fn supported_encodings() -> &'static [RepresentationIdentifier] {
    &REPR_IDS
  }

  // no transform, just the identity function
  fn transform_decoded(decoded: Self::Decoded) -> D {
    decoded
  }
}

impl<D> with_key::DeserializerAdapter<D> for CDRDeserializerAdapter<D>
where
  D: Keyed + DeserializeOwned,
  <D as Keyed>::K: DeserializeOwned, // Key should do this already?
{
  type DecodedKey = D::K;

  fn transform_decoded_key(decoded_key: Self::DecodedKey) -> D::K {
    decoded_key
  }
}

/// A default decoder is available for all owned types that implement
/// `serde::Deserialize`. Borrowing types use `from_bytes_with` with an
/// explicit decoder instead.
impl<D> no_key::DefaultDecoder<D> for CDRDeserializerAdapter<D>
where
  D: DeserializeOwned,
{
  type Decoder = CdrDeserializeDecoder<D>;
  const DECODER: Self::Decoder = CdrDeserializeDecoder(PhantomData);
}

impl<D> with_key::DefaultDecoder<D> for CDRDeserializerAdapter<D>
where
  D: Keyed + DeserializeOwned,
  D::K: DeserializeOwned,
{
  type Decoder = CdrDeserializeDecoder<D>;
  const DECODER: Self::Decoder = CdrDeserializeDecoder(PhantomData);
}

/// Decode type based on a `serde::Deserialize` implementation.
pub struct CdrDeserializeDecoder<D>(PhantomData<D>);

impl<'de, D> no_key::Decode<'de, D> for CdrDeserializeDecoder<D>
where
  D: serde::Deserialize<'de>,
{
  type Error = Error;

  fn decode_bytes(self, input_bytes: &'de [u8], encoding: RepresentationIdentifier) -> Result<D> {
    deserialize_from_cdr_with_decoder_and_rep_id(input_bytes, encoding, PhantomData).map(|r| r.0)
  }
}

impl<'de, Dec, DecKey> with_key::Decode<'de, Dec, DecKey> for CdrDeserializeDecoder<Dec>
where
  Dec: serde::Deserialize<'de>,
  DecKey: serde::Deserialize<'de>,
{
  fn decode_key_bytes(
    self,
    input_key_bytes: &'de [u8],
    encoding: RepresentationIdentifier,
  ) -> Result<DecKey> {
    deserialize_from_cdr_with_decoder_and_rep_id(input_key_bytes, encoding, PhantomData)
      .map(|r| r.0)
  }
}

impl<D> Clone for CdrDeserializeDecoder<D> {
  fn clone(&self) -> Self {
    Self(self.0)
  }
}

/// Decode type based on a `serde::de::DeserializeSeed` implementation.
#[derive(Clone)]
pub struct CdrDeserializeSeedDecoder<S, SK> {
  value_seed: S,
  key_seed: SK,
}

impl<'de, S, SK> CdrDeserializeSeedDecoder<S, SK>
where
  S: serde::de::DeserializeSeed<'de>,
  SK: serde::de::DeserializeSeed<'de>,
{
  pub fn new(value_seed: S, key_seed: SK) -> Self {
    Self {
      value_seed,
      key_seed,
    }
  }
}

/// Decode type based on a [`serde::de::DeserializeSeed`]-based decoder.
impl<'de, D, S, SK> no_key::Decode<'de, D> for CdrDeserializeSeedDecoder<S, SK>
where
  S: serde::de::DeserializeSeed<'de, Value = D>,
{
  type Error = Error;

  fn decode_bytes(self, input_bytes: &'de [u8], encoding: RepresentationIdentifier) -> Result<D> {
    deserialize_from_cdr_with_decoder_and_rep_id(input_bytes, encoding, self.value_seed)
      .map(|r| r.0)
  }
}

impl<'de, Dec, DecKey, S, SK> with_key::Decode<'de, Dec, DecKey>
  for CdrDeserializeSeedDecoder<S, SK>
where
  S: serde::de::DeserializeSeed<'de, Value = Dec>,
  SK: serde::de::DeserializeSeed<'de, Value = DecKey>,
{
  fn decode_key_bytes(
    self,
    input_key_bytes: &'de [u8],
    encoding: RepresentationIdentifier,
  ) -> Result<DecKey> {
    deserialize_from_cdr_with_decoder_and_rep_id(input_key_bytes, encoding, self.key_seed)
      .map(|r| r.0)
  }
}

/// Decode type using the given [`DeserializeSeed`]-based decoder.
///
/// Returns deserialized object. Byte count is discarded.
pub fn deserialize_from_cdr_with_rep_id<'de, T>(
  input_bytes: &'de [u8],
  encoding: RepresentationIdentifier,
) -> Result<(T, usize)>
where
  T: Deserialize<'de>,
{
  deserialize_from_cdr_with_decoder_and_rep_id::<PhantomData<T>>(input_bytes, encoding, PhantomData)
}

/// Decode type using the given [`DeserializeSeed`]-based decoder.
///
/// Returns deserialized object and byte count of stream consumed.
pub fn deserialize_from_cdr_with_decoder_and_rep_id<'de, S>(
  input_bytes: &'de [u8],
  encoding: RepresentationIdentifier,
  decoder: S,
) -> Result<(S::Value, usize)>
where
  S: DeserializeSeed<'de>,
{
  match encoding {
    RepresentationIdentifier::CDR_LE | RepresentationIdentifier::PL_CDR_LE => {
      let mut deserializer = CdrDeserializer::<LittleEndian>::new(input_bytes);
      Ok((
        decoder.deserialize(&mut deserializer)?,
        deserializer.bytes_consumed(),
      ))
    }

    RepresentationIdentifier::CDR_BE | RepresentationIdentifier::PL_CDR_BE => {
      let mut deserializer = CdrDeserializer::<BigEndian>::new(input_bytes);
      Ok((
        decoder.deserialize(&mut deserializer)?,
        deserializer.bytes_consumed(),
      ))
    }

    repr_id => Err(Error::Message(format!(
      "Unknown serialization format. requested={repr_id:?}."
    ))),
  }
}
