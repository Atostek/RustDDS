use std::marker::PhantomData;

use bytes::Bytes;
use byteorder::{ByteOrder, LittleEndian};

use crate::{
  dds::adapters::{no_key, with_key},
  structure::parameter_id::ParameterId,
  Keyed, RepresentationIdentifier,
};

// This is to be implemented by all Discovery message types.
// .. likely it is not useful for others.
pub trait PlCdrSerialize {
  // encoding must be either PL_CDR_LE or PL_CDR_BE
  fn to_pl_cdr_bytes(
    &self,
    encoding: RepresentationIdentifier,
  ) -> Result<Bytes, PlCdrSerializeError>;
}

#[derive(Debug, thiserror::Error)]
pub enum PlCdrSerializeError {
  #[error("Serializer does not support this operation: {0}")]
  NotSupported(String),

  #[error("Speedy serializer error: {0}")]
  Speedy(#[from] speedy::Error),
}

pub struct PlCdrSerializerAdapter<D, BO = LittleEndian>
where
  BO: ByteOrder,
{
  phantom: PhantomData<D>,
  ghost: PhantomData<BO>,
}

impl<D, BO> no_key::SerializerAdapter<D> for PlCdrSerializerAdapter<D, BO>
where
  D: PlCdrSerialize,
  BO: ByteOrder,
{
  type Error = PlCdrSerializeError;

  fn output_encoding() -> RepresentationIdentifier {
    // TODO: This works only for BO=LittleEndian
    RepresentationIdentifier::PL_CDR_LE
  }

  fn to_bytes(value: &D) -> Result<Bytes, Self::Error> {
    // TODO: This works only for BO=LittleEndian
    value.to_pl_cdr_bytes(RepresentationIdentifier::PL_CDR_LE)
  }
}

impl<D, BO> with_key::SerializerAdapter<D> for PlCdrSerializerAdapter<D, BO>
where
  D: Keyed + PlCdrSerialize,
  <D as Keyed>::K: PlCdrSerialize,
  BO: ByteOrder,
{
  fn key_to_bytes(value: &D::K) -> Result<Bytes, Self::Error> {
    // TODO: This works only for BO=LittleEndian
    value.to_pl_cdr_bytes(RepresentationIdentifier::PL_CDR_LE)
  }
}

// ----------------------------------
// ----------------------------------
// ----------------------------------

// This is to be implemented by all Discovery message types.
// .. likely it is not useful for others.
pub trait PlCdrDeserialize: Sized {
  // encoding must be either PL_CDR_LE or PL_CDR_BE
  fn from_pl_cdr_bytes(
    input_bytes: &[u8],
    encoding: RepresentationIdentifier,
  ) -> Result<Self, PlCdrDeserializeError>;
}

#[derive(Debug, thiserror::Error)]
pub enum PlCdrDeserializeError {
  #[error("Deserializer does not support this operation: {0}")]
  NotSupported(String),

  #[error("Speedy deserializer error: {0}")]
  Speedy(#[from] speedy::Error),

  #[error("Parameter List missing {0:?} , expected for field {1}")]
  MissingField(ParameterId, String),
}

pub struct PlCdrDeserializerAdapter<D> {
  phantom: PhantomData<D>,
}

const REPR_IDS: [RepresentationIdentifier; 2] = [
  // PL_CDR_* are expected
  RepresentationIdentifier::PL_CDR_BE,
  RepresentationIdentifier::PL_CDR_LE,
];

impl<D> no_key::DeserializerAdapter<D> for PlCdrDeserializerAdapter<D>
where
  D: PlCdrDeserialize,
{
  type Error = PlCdrDeserializeError;
  type Decoded = D;

  fn supported_encodings() -> &'static [RepresentationIdentifier] {
    &REPR_IDS
  }

  fn transform_decoded(deserialized: Self::Decoded) -> D {
    deserialized
  }
}

impl<D> with_key::DeserializerAdapter<D> for PlCdrDeserializerAdapter<D>
where
  D: Keyed + PlCdrDeserialize,
  <D as Keyed>::K: PlCdrDeserialize,
{
  type DecodedKey = D::K;

  fn transform_decoded_key(decoded_key: Self::DecodedKey) -> D::K {
    decoded_key
  }

  // fn key_from_bytes(
  //   input_bytes: &[u8],
  //   encoding: RepresentationIdentifier,
  // ) -> Result<D::K, Self::Error> {
  //   match encoding {
  //     RepresentationIdentifier::PL_CDR_LE | RepresentationIdentifier::PL_CDR_BE
  // => {       <D::K>::from_pl_cdr_bytes(input_bytes, encoding)
  //     }
  //     repr_id => Err(PlCdrDeserializeError::NotSupported(format!(
  //       "Unknown representation identifier {:?}",
  //       repr_id
  //     ))),
  //   }
  // }
}

/// A default decoder is available if the target type implements
/// [`PlCdrDeserialize`].
impl<D> no_key::DefaultDecoder<D> for PlCdrDeserializerAdapter<D>
where
  D: PlCdrDeserialize,
{
  type Decoder = PlCdrDeserializer<D>;
  const DECODER: Self::Decoder = PlCdrDeserializer(PhantomData);
}

impl<D> with_key::DefaultDecoder<D> for PlCdrDeserializerAdapter<D>
where
  D: Keyed + PlCdrDeserialize,
  D::K: PlCdrDeserialize,
{
  type Decoder = PlCdrDeserializer<D>;
  const DECODER: Self::Decoder = PlCdrDeserializer(PhantomData);
}

/// Decode type based on [`PlCdrDeserialize`] implementation.
pub struct PlCdrDeserializer<D>(PhantomData<D>);

impl<'de, D> no_key::Decode<'de, D> for PlCdrDeserializer<D>
where
  D: PlCdrDeserialize,
{
  type Error = PlCdrDeserializeError;

  fn decode_bytes(
    self,
    input_bytes: &'de [u8],
    encoding: RepresentationIdentifier,
  ) -> Result<D, Self::Error> {
    match encoding {
      RepresentationIdentifier::PL_CDR_LE | RepresentationIdentifier::PL_CDR_BE => {
        D::from_pl_cdr_bytes(input_bytes, encoding)
      }
      repr_id => Err(PlCdrDeserializeError::NotSupported(format!(
        "Unknown representation identifier {repr_id:?}"
      ))),
    }
  }
}

impl<'de, Dec, DecKey> with_key::Decode<'de, Dec, DecKey> for PlCdrDeserializer<Dec>
where
  Dec: PlCdrDeserialize,
  DecKey: PlCdrDeserialize,
{
  fn decode_key_bytes(
    self,
    input_bytes: &'de [u8],
    encoding: RepresentationIdentifier,
  ) -> Result<DecKey, Self::Error> {
    match encoding {
      RepresentationIdentifier::PL_CDR_LE | RepresentationIdentifier::PL_CDR_BE => {
        DecKey::from_pl_cdr_bytes(input_bytes, encoding)
      }
      repr_id => Err(PlCdrDeserializeError::NotSupported(format!(
        "Unknown (key) representation identifier {repr_id:?}"
      ))),
    }
  }
}

impl<D> Clone for PlCdrDeserializer<D> {
  fn clone(&self) -> Self {
    Self(self.0)
  }
}
