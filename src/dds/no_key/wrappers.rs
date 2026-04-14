use std::{marker::PhantomData, ops::Deref};

use bytes::Bytes;

use crate::{
  dds::adapters::*, messages::submessages::submessages::RepresentationIdentifier, Keyed,
};

// This wrapper is used to convert NO_KEY types to WITH_KEY
// * inside the wrapper there is a NO_KEY type
// * the wrapper is good for WITH_KEY
// The wrapper introduces a dummy key of type (), which of course has an always
// known value ()
pub(crate) struct NoKeyWrapper<D> {
  pub(crate) d: D,
}

impl<D> From<D> for NoKeyWrapper<D> {
  fn from(d: D) -> Self {
    Self { d }
  }
}

// implement Deref so that &NoKeyWrapper<D> is coercible to &D
impl<D> Deref for NoKeyWrapper<D> {
  type Target = D;
  fn deref(&self) -> &Self::Target {
    &self.d
  }
}

impl<D> Keyed for NoKeyWrapper<D> {
  type K = ();
  fn key(&self) {}
}

// wrapper for SerializerAdapter
// * inside is NO_KEY
// * outside of wrapper is WITH_KEY
pub struct SAWrapper<SA> {
  no_key: PhantomData<SA>,
}

// have to implement base trait first, just trivial passthrough
impl<D, SA> no_key::SerializerAdapter<NoKeyWrapper<D>> for SAWrapper<SA>
where
  SA: no_key::SerializerAdapter<D>,
{
  type Error = SA::Error;

  fn output_encoding() -> RepresentationIdentifier {
    SA::output_encoding()
  }

  fn to_bytes(value: &NoKeyWrapper<D>) -> Result<Bytes, SA::Error> {
    SA::to_bytes(&value.d)
  }
}

// This is the point of wrapping. Implement dummy key serialization
// Of course, this is never supposed to be actually called.
impl<D, SA> with_key::SerializerAdapter<NoKeyWrapper<D>> for SAWrapper<SA>
where
  SA: no_key::SerializerAdapter<D>,
{
  fn key_to_bytes(_value: &()) -> Result<Bytes, SA::Error> {
    Ok(Bytes::new())
  }
}

// wrapper for DeserializerAdapter
// * inside is NO_KEY
// * outside of wrapper is WITH_KEY
pub struct DAWrapper<DA> {
  no_key: PhantomData<DA>,
}

// first, implement no_key DA
impl<D, DA> no_key::DeserializerAdapter<NoKeyWrapper<D>> for DAWrapper<DA>
where
  DA: no_key::DeserializerAdapter<D>,
{
  type Error = DA::Error;
  type Decoded = DA::Decoded;

  fn supported_encodings() -> &'static [RepresentationIdentifier] {
    DA::supported_encodings()
  }

  /// Wraps the deserialized value into a [`NoKeyWrapper`].
  fn transform_decoded(deserialized: Self::Decoded) -> NoKeyWrapper<D> {
    NoKeyWrapper::<D> {
      d: DA::transform_decoded(deserialized),
    }
  }
}

// then, implement with_key DA
impl<D, DA> with_key::DeserializerAdapter<NoKeyWrapper<D>> for DAWrapper<DA>
where
  DA: no_key::DeserializerAdapter<D>,
{
  type DecodedKey = ();

  // transform_decoded_key is supposed to return
  // a value, but in this instance it is of type unit.
  #[allow(clippy::unused_unit, clippy::semicolon_if_nothing_returned)]
  fn transform_decoded_key(_decoded_key: Self::DecodedKey) -> () {
    ()
  }
}

// If DeserializerAdpter DA additionally implements no_key::DefaultDecoder, then
// the wrapped version implements both no_key::DefaultDecoder and
// with_key::DefaultDecoder

impl<D, DA> no_key::DefaultDecoder<NoKeyWrapper<D>> for DAWrapper<DA>
where
  DA: no_key::DefaultDecoder<D>,
{
  type Decoder = DA::Decoder;
  const DECODER: Self::Decoder = DA::DECODER;
}

impl<D, DA> with_key::DefaultDecoder<NoKeyWrapper<D>> for DAWrapper<DA>
where
  DA: no_key::DefaultDecoder<D>,
{
  type Decoder = DecodeWrapper<DA::Decoder>;
  const DECODER: Self::Decoder = DecodeWrapper::new(DA::DECODER);
}

/// Wrapper to turn any no_key::Decode into a with_key::Decode with a unit key.
#[derive(Clone)]
pub struct DecodeWrapper<NoKeyDecode> {
  no_key: NoKeyDecode,
}

impl<NoKeyDecode> DecodeWrapper<NoKeyDecode> {
  pub const fn new(no_key: NoKeyDecode) -> Self {
    DecodeWrapper { no_key }
  }
}

// re-implement no_key::Decode<Decoded> for the wrapper also. Wrapped type
// already does it for us.
impl<Decoded, NoKeyDecode> no_key::Decode<Decoded> for DecodeWrapper<NoKeyDecode>
where
  NoKeyDecode: no_key::Decode<Decoded>,
{
  type Error = NoKeyDecode::Error;

  fn decode_bytes(
    self,
    input_bytes: &[u8],
    encoding: RepresentationIdentifier,
  ) -> Result<Decoded, Self::Error> {
    self.no_key.decode_bytes(input_bytes, encoding)
  }
}

// implement with_key::Decode<Decoded> for the wrapper.
// The key has type `()`, so the decoded value is always `()` regardless of the
// input bytes.
impl<Decoded, NoKeyDecode> with_key::Decode<Decoded, ()> for DecodeWrapper<NoKeyDecode>
where
  NoKeyDecode: no_key::Decode<Decoded>,
{
  fn decode_key_bytes(
    self,
    _input_key_bytes: &[u8],
    _encoding: RepresentationIdentifier,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}
