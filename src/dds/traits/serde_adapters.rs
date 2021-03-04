
/// DeserializerAdapter is used to fit serde Deserializer implementations and DataReader together.
/// DataReader cannot assume a specific serialization format, so it needs to be given as a parameter.
///
/// for WITH_KEY topics, we need to be able to (de)serailize the key in addition to data.
pub mod no_key {
	use serde::de::DeserializeOwned;
	use serde::ser::Serialize;
	use bytes::Bytes;

	use crate::serialization::error::Result;
	use crate::messages::submessages::submessage_elements::serialized_payload::RepresentationIdentifier;

	pub trait DeserializerAdapter<D>
	where
	  D: DeserializeOwned,
	{
		// Which data encodings can this deserializer read?
	  fn supported_encodings() -> &'static [RepresentationIdentifier]; 

	  fn from_bytes<'de>(input_bytes: &'de [u8], encoding: RepresentationIdentifier) -> Result<D>;
	}

	pub trait SerializerAdapter<D>
	where
	  D: Serialize,
	{
		// what encoding do we produce?
	  fn output_encoding() -> RepresentationIdentifier;

	  fn to_Bytes(value: &D) -> Result<Bytes>;
	}

}

pub mod with_key {
	use serde::Serialize;
	use serde::de::DeserializeOwned;
	
	use bytes::Bytes;

	use crate::serialization::error::Result;
	use crate::dds::traits::key::*;
	use crate::messages::submessages::submessage_elements::serialized_payload::RepresentationIdentifier;

	use super::no_key;

	pub trait DeserializerAdapter<D> : no_key::DeserializerAdapter<D>
	where
	  D: Keyed + DeserializeOwned,
	{
	  fn key_from_bytes<'de>(input_bytes: &'de [u8], encoding: RepresentationIdentifier) -> Result<D::K>;
	}

	pub trait SerializerAdapter<D> : no_key::SerializerAdapter<D>
	where
	  D: Keyed + Serialize,
	{
	  fn key_to_Bytes(value: &D::K) -> Result<Bytes>;
	}

}
