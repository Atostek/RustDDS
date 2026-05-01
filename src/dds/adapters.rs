//! DeserializerAdapter is used to fit serde Deserializer implementations and
//! DataReader together.
//!
//! DataReader/DataWriter cannot assume a specific serialization
//! format, so it needs to be given as a parameter when instantiating.
//! For WITH_KEY topics, we need to be able to (de)serialize the key in addition
//! to data values.
//!
//! Type parameter `D` is the type resulting from (successful) deserialization.
//!
//! Deserialization consists of two steps: decode and transform. Decode inputs a
//! byte slice and outputs an intermediate type `Decoded`. Transform step maps
//! `Decoded`into `D`.
//!
//! It is a common case that `Decoded` and `D` are the same and the
//! transform step is the identity function.
//!
//! # How to implement `DeserializerAdapter<D>`
//!
//! We call the imaginary example serialization format `MyDataFormat`.
//!
//! 0. Define a type `MyDataFormatAdapter<D>`, that is used as a link between
//!    RustDDS and MyDataFormat decoder routines. The decoder routines may
//!    reside in
//! a pre-existing Rust crate. The adapter type is
//! necessary to bridge  together RustDDS and MyDataFormat, because Rust's
//! orphan implementation rule prevents  an application crate from implementing
//! RustDDS-defined traits on types in MyDataFormat crate.
//!
//! 1. Implement `no_key::DeserializerAdapter<D> for MyDataFormatAdapter<D>`
//!   * Define `Error` type to indicate deserialization failures
//!   * Define the `Decoded` type.
//!   * Implement `supported_encodings` function. This function defines which
//!     RTPS data encodings are decodable by this adapter.
//!   * Implement the `transform_decoded` function.
//!   * Do not implement the provided functions in this trait.
//!
//! At this point, you should be all set for deserializing no_key data with
//! run-time seed, i.e. you are required to provide a decoder function at run
//! time, every time.
//!
//! If you do not need/want provide the decoder object at every deserialization
//! call, and can define a deserializer that only depends on type `D`, and the
//! incoming byte slice, then extend your `MyDataFormatAdapter<D>` as
//! follows
//!
//! 2. Implement `no_key::DefaultDecoder<D> for MyDataFormatAdapter<D>`
//!   * Define a type, e.g. `MyDataFormatDecoder<D>`, that implements trait
//!     `no_key::Decode<Decoded>`
//!     * Implement the `decode_bytes` function to do the decoding from byte
//!       slice to `Decoded`
//!     * The decoder may minimally be a struct with no actual fields, possibly
//!       only a `PhantomData<D>`, which makes it zero size and no cost to
//!       clone. But this depends on the workings of MyDataFormat.
//!   * Implement `no_key::DefaultDecoder<D> for MyDataFormatAdapter<D>`
//!     * Define the associated type `Decoder = MyDataFormatDecoder<D>`
//!     * Define a const `DECODER` as an instance of `MyDataFormatDecoder`
//!   * Implement or derive `Clone` for `MyDataFormatDecoder`. The cloning
//!     should be cheap, as a new instance is needed for each deserialization
//!     operation.
//!
//! Now you should be able to deserialize no_key data with a default decoder
//! function.
//!
//! If you need to handle also with_key deserialization, then we need more
//! implementations to also handle the instance keys.
//!
//! 3. Implement `with_key::DeserializerAdapter<D> for MyDataFormatAdapter<D>`
//!   * Define the `DecodedKey` type.
//!   * Implement the `transform_decoded_key` function.
//!
//! To use a default decoder for with_key deserialization also:
//!
//! 4. Implement `with_key::DefaultDecoder<D> for MyDataFormatAdapter<D>`
//!   * Implement also `with_key::Decode<DecodedValue, DecodedKey>` for
//!     `MyDataFormatDecoder`.
//!   * One member function `decode_key_bytes` is needed. It defines how to
//!     decode a key.

/// Deserializer/Serializer adapters for `no_key` data types.
pub mod no_key {
  use bytes::Bytes;

  use crate::RepresentationIdentifier;

  /// trait for connecting a Deserializer implementation and DataReader
  /// together - no_key version.
  ///
  /// Deserialization is done in two steps: decode and transform.
  /// The basic pipeline is:
  ///
  /// byte slice → decode → value of type `Self::Decoded` → call
  /// [`Self::transform_decoded`] → value of type `D`
  ///
  /// The transform step may be an identity function, which means that
  /// `Self::Decoded` and `D` are the same.
  ///
  /// The decoding step can be parameterized at run time, if the decoder is
  /// defined suitaby. If there is no need for such parameterization (i.e.
  /// decoder object), use subtrait `DefaultDecoder`, which defines decoding
  /// that needs no decoder "seed" object at run time. In that case decoding is
  /// dependent only on the type of the decoder and incoming byte stream.
  pub trait DeserializerAdapter<D> {
    /// The error type returned when decoding fails.
    type Error: std::error::Error;

    /// Type after decoding.
    ///
    /// The adapter might apply additional operations or wrapper types to the
    /// decoded value, so this type might be different from `D`.
    type Decoded;

    /// Which data representations can the DeserializerAdapter read?
    /// See RTPS specification Section 10 and Table 10.3
    fn supported_encodings() -> &'static [RepresentationIdentifier];

    /// Transform the `Self::Decoded` type returned by the decoder into a
    /// value of type `D`.
    ///
    /// If [`Self::Decoded`] is set to `D`, this method can be the identity
    /// function.
    fn transform_decoded(decoded: Self::Decoded) -> D;

    /// Deserialize data from bytes to an object using the given decoder.
    ///
    /// `encoding` must be something given by `supported_encodings()`, or
    /// implementation may fail with Err or `panic!()`.
    ///
    /// The input slice's lifetime is tied to the deserialization lifetime
    /// `'de`, so adapters that produce borrowed data (zero-copy) are
    /// supported.
    fn from_bytes_with<'de, S>(
      input_bytes: &'de [u8],
      encoding: RepresentationIdentifier,
      decoder: S,
    ) -> Result<D, S::Error>
    where
      S: Decode<'de, Self::Decoded>,
    {
      decoder
        .decode_bytes(input_bytes, encoding)
        .map(Self::transform_decoded)
    }

    /// Deserialize data from bytes to an object.
    /// `encoding` must be something given by `supported_encodings()`, or
    /// implementation may fail with Err or `panic!()`.
    ///
    /// Only usable if the adapter has a default decoder, i.e. implements
    /// `DefaultDecoder`.
    fn from_bytes<'de>(
      input_bytes: &'de [u8],
      encoding: RepresentationIdentifier,
    ) -> Result<D, Self::Error>
    where
      Self: DefaultDecoder<D>,
    {
      Self::from_bytes_with(input_bytes, encoding, Self::DECODER)
    }
  }

  /// The `DeserializerAdapter` can be used without a decoder object as there is
  /// a default one.
  pub trait DefaultDecoder<D>: DeserializerAdapter<D> {
    /// Type of the default decoder.
    ///
    /// The default decoder needs to be clonable to be usable for async stream
    /// creation (as it's needed multiple times). It must work for any input
    /// lifetime, hence the higher-ranked `for<'de>` bound.
    type Decoder: for<'de> Decode<'de, Self::Decoded, Error = Self::Error> + Clone;

    /// The default decoder value.
    ///
    /// This default decoder is typically implemented by forwarding to a
    /// different trait, e.g. `serde::Deserialize`.
    const DECODER: Self::Decoder;
  }

  /// The trait `Decode` defines a decoder object that produces a value of type
  /// `Decoded` from a slice of bytes and a [`RepresentationIdentifier`].
  ///
  /// Note that `Decoded` maps to associated type `Decoded` in
  /// `DeserializerAdapter`, not `D`.
  ///
  /// The lifetime `'de` is the deserialization lifetime: the returned
  /// `Decoded` value may borrow from `input_bytes` for `'de`.
  pub trait Decode<'de, Decoded> {
    /// The decoding error type returned by [`Self::decode_bytes`].
    type Error: std::error::Error;

    /// Tries to decode the given byte slice to a value of type `Decoded`
    /// using the given encoding.
    fn decode_bytes(
      self,
      input_bytes: &'de [u8],
      encoding: RepresentationIdentifier,
    ) -> Result<Decoded, Self::Error>;
  }

  /// trait for connecting a Serializer implementation and DataWriter
  /// together - no_key version.
  ///
  /// This is much simpler that the Deserializer above, because any the starting
  /// point is an application-defined type, than can be generated by any
  /// means.
  pub trait SerializerAdapter<D> {
    type Error: std::error::Error; // Error type

    // what encoding do we produce?
    fn output_encoding() -> RepresentationIdentifier;

    fn to_bytes(value: &D) -> Result<Bytes, Self::Error>;
  }
}

/// Deserializer/Serializer adapters for `with_key` data types.
pub mod with_key {
  use bytes::Bytes;

  use crate::{Keyed, RepresentationIdentifier};
  use super::no_key;

  /// trait for connecting a Deserializer implementation and DataReader
  /// together - with_key version.
  ///
  /// The keyed versions inherit the no_key versions,
  /// but here the payload type `D` implements `Keyed`, which means that thre is
  /// an associated key type `D::K`. The `with_key` versions extend the
  /// deserialization functionality to also handle that key type.
  ///
  /// `with_key` functionality is needed in DDS topics that have a key, e.g. DDS
  /// Discovery topics. ROS 2 (as of Iron Irwini) has no concept of with_key
  /// topics.
  pub trait DeserializerAdapter<D>: no_key::DeserializerAdapter<D>
  where
    D: Keyed,
  {
    /// Key type after decoding and before transformation.
    ///
    /// The adapter might apply additional operations or wrapper types to the
    /// decoded value, so this type might be different from `D`.
    type DecodedKey;

    /// Transform the `Self::DecodedKey` type returned by the decoder into a
    /// value of type `D`.
    ///
    /// If [`Self::DecodedKey`] is set to `D`, this method can be the identity
    /// function.
    fn transform_decoded_key(decoded_key: Self::DecodedKey) -> D::K;

    /// Deserialize data from bytes to an object using the given decoder.
    ///
    /// `encoding` must be something given by `supported_encodings()`, or
    /// implementation may fail with Err or `panic!()`.
    fn key_from_bytes_with<'de, S>(
      input_bytes: &'de [u8],
      encoding: RepresentationIdentifier,
      decoder: S,
    ) -> Result<D::K, S::Error>
    where
      S: Decode<'de, Self::Decoded, Self::DecodedKey>,
    {
      decoder
        .decode_key_bytes(input_bytes, encoding)
        .map(Self::transform_decoded_key)
    }

    /// Deserialize data from bytes to an object.
    /// `encoding` must be something given by `supported_encodings()`, or
    /// implementation may fail with Err or `panic!()`.
    ///
    /// Only usable if the adapter has a default decoder.
    fn key_from_bytes<'de>(
      input_bytes: &'de [u8],
      encoding: RepresentationIdentifier,
    ) -> Result<D::K, Self::Error>
    where
      Self: DefaultDecoder<D>,
    {
      Self::key_from_bytes_with(input_bytes, encoding, Self::DECODER)
    }
  }

  /// The `DeserializerAdapter` can be used without a decoder object as there is
  /// a default one. This trait defines the default.
  pub trait DefaultDecoder<D>: DeserializerAdapter<D>
  where
    D: Keyed,
  {
    /// Type of the default decoder.
    ///
    /// The default decoder needs to be clonable to be usable for async stream
    /// creation (as it's needed multiple times). It must work for any input
    /// lifetime, hence the higher-ranked `for<'de>` bound.
    type Decoder: for<'de> Decode<'de, Self::Decoded, Self::DecodedKey, Error = Self::Error> + Clone;

    /// The default decoder value.
    ///
    /// This default decoder is typically implemented by forwarding to a
    /// different trait, e.g. `serde::Deserialize`.
    const DECODER: Self::Decoder;
  }

  /// Decodes a value of type `Dec` (or its key `DecKey`) from a slice of
  /// bytes and a [`RepresentationIdentifier`]. Note that `Dec` maps to
  /// associated type `Decoded` in `DeserializerAdapter`, not `D`.
  ///
  /// The lifetime `'de` is the deserialization lifetime: the returned key
  /// may borrow from `input_key_bytes` for `'de`.
  pub trait Decode<'de, Dec, DecKey>: no_key::Decode<'de, Dec> {
    /// Tries to decode the given byte slice to a value of type `DecKey`
    /// using the given encoding.
    fn decode_key_bytes(
      self,
      input_key_bytes: &'de [u8],
      encoding: RepresentationIdentifier,
    ) -> Result<DecKey, Self::Error>;
  }

  /// trait for connecting a Serializer implementation and DataWriter
  /// together - with_key version.
  pub trait SerializerAdapter<D>: no_key::SerializerAdapter<D>
  where
    D: Keyed,
  {
    /// serialize a key `D::K` to Bytes.
    fn key_to_bytes(value: &D::K) -> Result<Bytes, Self::Error>;
  }
}
