use std::collections::HashMap;

use bytes::Bytes;
use enumflags2::{bitflags, BitFlags};
use speedy::{Context, Readable, Reader, Writable, Writer};
use serde::{Deserialize, Serialize};

use crate::{
  dds::qos,
  discovery,
  messages::submessages::elements::{
    parameter::Parameter,
    parameter_list::{ParameterList, ParameterListable},
  },
  security,
  security::config::ConfigError,
  serialization::{
    padding_needed_for_alignment_4,
    pl_cdr_adapters::{
      PlCdrDeserialize, PlCdrDeserializeError, PlCdrSerialize, PlCdrSerializeError,
    },
    speedy_pl_cdr_helpers::*,
  },
  structure::{guid::GuidPrefix, parameter_id::ParameterId},
  Keyed, QosPolicies, RepresentationIdentifier, GUID,
};

// Result type with generic OK type. Error type is SecurityError.
pub type SecurityResult<T> = std::result::Result<T, SecurityError>;

// Something like the SecurityException of the specification
#[derive(Debug, thiserror::Error)]
#[error("Security exception: {msg}")]
pub struct SecurityError {
  pub(crate) msg: String,
}

pub fn security_error(msg: &str) -> SecurityError {
  SecurityError {
    msg: msg.to_string(),
  }
}

impl From<ring::error::Unspecified> for SecurityError {
  fn from(_e: ring::error::Unspecified) -> Self {
    SecurityError {
      msg: "The ring crypto library gives 'Unspecified' error. That's all we are authorized to \
            know. Sorry."
        .to_string(),
    }
  }
}

impl From<speedy::Error> for SecurityError {
  fn from(e: speedy::Error) -> Self {
    SecurityError {
      msg: format!("Serialization/deserialization error: {e:?}"),
    }
  }
}

impl From<&str> for SecurityError {
  fn from(e: &str) -> Self {
    SecurityError {
      msg: format!("SecurityError {e}"),
    }
  }
}

impl From<String> for SecurityError {
  fn from(msg: String) -> Self {
    SecurityError { msg }
  }
}

impl From<ConfigError> for SecurityError {
  fn from(e: ConfigError) -> Self {
    SecurityError {
      msg: format!("ConfigError {e:?}"),
    }
  }
}

use x509_certificate::X509CertificateError;

impl From<X509CertificateError> for SecurityError {
  fn from(e: X509CertificateError) -> Self {
    SecurityError {
      msg: format!("X509CertificateError {e:?}"),
    }
  }
}

impl From<openssl::error::ErrorStack> for SecurityError {
  fn from(e: openssl::error::ErrorStack) -> Self {
    SecurityError {
      msg: format!("openssl Error: {e:?}"),
    }
  }
}

impl From<cryptoki::error::Error> for SecurityError {
  fn from(e: cryptoki::error::Error) -> Self {
    SecurityError {
      msg: format!("cryptoki (PKCS#11) Error: {e:?}"),
    }
  }
}

#[doc(hidden)]
#[macro_export]
macro_rules! create_security_error_and_log {
  ($($arg:tt)*) => (
      { log::error!($($arg)*);  // Note: this needs to be security-specific logging
        SecurityError{ msg: format!($($arg)*) }
      }
    )
}

// Property_t type from section 7.2.1 of the Security specification (v. 1.1)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)] // for CDR in Discovery
pub struct Property {
  pub(crate) name: String,
  pub(crate) value: String,
  pub(crate) propagate: bool, // NOT SERIALIZED
}

impl<'a, C: Context> Readable<'a, C> for Property {
  fn minimum_bytes_needed() -> usize {
    4
  }

  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let name: StringWithNul = reader.read_value()?;

    read_pad(reader, name.len(), 4)?; // pad according to previous read
    let value: StringWithNul = reader.read_value()?;

    Ok(Property {
      name: name.into(),
      value: value.into(),
      propagate: true, // since we read this from thw wire, it was propagated
    })
  }
}

// Writing several strings is a bit complicated, because
// we have to keep track of alignment.
// Again, alignment comes BEFORE string length, or vector item count, not after
// string.
impl<C: Context> Writable<C> for Property {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    if self.propagate {
      let name = StringWithNul::from(self.name.clone());
      // nothing yet to pad
      writer.write_value(&name)?;

      write_pad(writer, name.len(), 4)?;
      let value = StringWithNul::from(self.value.clone());
      writer.write_value(&value)?;
    }
    Ok(())
  }
}

impl Property {
  pub fn serialized_len(&self) -> usize {
    let first = 4 + self.name.len() + 1;
    let second = 4 + self.value.len() + 1;
    first + padding_needed_for_alignment_4(first) + second
  }

  pub fn value(&self) -> String {
    self.value.clone()
  }
}

fn get_optional_property(properties: &[Property], property_name: &str) -> Option<String> {
  properties
    .iter()
    .find(|Property { name, .. }| name.eq(property_name))
    .map(|Property { value, .. }| value.clone())
}
fn get_property(properties: &[Property], property_name: &str) -> SecurityResult<String> {
  get_optional_property(properties, property_name).ok_or_else(|| {
    create_security_error_and_log!("Could not find a property of the name {}.", property_name)
  })
}

impl QosPolicies {
  pub(super) fn get_optional_property(&self, property_name: &str) -> Option<String> {
    self
      .property
      .as_ref()
      .and_then(|properties_or_binary_properties| {
        get_optional_property(&properties_or_binary_properties.value, property_name)
      })
  }
  pub(super) fn get_property(&self, property_name: &str) -> SecurityResult<String> {
    self
      .property
      .as_ref()
      .ok_or_else(|| create_security_error_and_log!("The QosPolicies did not have any properties."))
      .and_then(|properties_or_binary_properties| {
        get_property(&properties_or_binary_properties.value, property_name)
      })
  }
}

// BinaryProperty_t type from section 7.2.2 of the Security specification (v.
// 1.1)
// // Serialize, Deserialize for CDR in Discovery
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(into = "repr::BinaryProperty", from = "repr::BinaryProperty")]
pub struct BinaryProperty {
  pub(crate) name: String,    // public because of serialization
  pub(crate) value: Bytes,    // Serde cannot derive for Bytes, therefore use repr::
  pub(crate) propagate: bool, // propagate field is not serialized
}

impl BinaryProperty {
  pub fn with_propagate(name: &str, value: Bytes) -> Self {
    BinaryProperty {
      name: name.to_string(),
      value,
      propagate: true,
    }
  }

  pub fn value(&self) -> Bytes {
    self.value.clone()
  }
}

fn get_optional_binary_property(
  binary_properties: &[BinaryProperty],
  binary_property_name: &str,
) -> Option<Bytes> {
  binary_properties
    .iter()
    .find(|BinaryProperty { name, .. }| name.eq(binary_property_name))
    .map(|BinaryProperty { value, .. }| value.clone())
}
fn get_binary_property(
  binary_properties: &[BinaryProperty],
  binary_property_name: &str,
) -> SecurityResult<Bytes> {
  get_optional_binary_property(binary_properties, binary_property_name).ok_or_else(|| {
    create_security_error_and_log!(
      "Could not find a binary property of the name {}.",
      binary_property_name
    )
  })
}

mod repr {
  use serde::{Deserialize, Serialize};

  #[derive(Serialize, Deserialize)]
  pub struct BinaryProperty {
    pub(crate) name: String,
    pub(crate) value: Vec<u8>,
    //pub(crate) propagate: bool,
    // The "propagate" flag is never serialized, because it is always true, if we are serializing.
  }

  impl From<BinaryProperty> for super::BinaryProperty {
    fn from(bp: BinaryProperty) -> super::BinaryProperty {
      super::BinaryProperty {
        name: bp.name,
        value: bp.value.into(),
        propagate: true,
      }
    }
  }

  impl From<super::BinaryProperty> for BinaryProperty {
    fn from(bp: super::BinaryProperty) -> BinaryProperty {
      BinaryProperty {
        name: bp.name,
        value: bp.value.into(),
      }
    }
  }
}

impl BinaryProperty {
  pub fn serialized_len(&self) -> usize {
    let first = 4 + self.name.len() + 1; // +1 for NUL byte
    let second = 4 + self.value.len(); // no nul terminator byte here
    first + padding_needed_for_alignment_4(first) + second
  }
}

impl<'a, C: Context> Readable<'a, C> for BinaryProperty {
  fn minimum_bytes_needed() -> usize {
    4
  }

  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let name: StringWithNul = reader.read_value()?;

    read_pad(reader, name.len(), 4)?; // pad according to previous read
    let value: Vec<u8> = reader.read_value()?;

    Ok(BinaryProperty {
      name: name.into(),
      value: value.into(),
      propagate: true, // since we read this from thw wire, it was propagated
    })
  }
}

// Writing several strings is a bit complicated, because
// we have to keep track of alignment.
// Again, alignment comes BEFORE string length, or vector item count, not after
// string.
impl<C: Context> Writable<C> for BinaryProperty {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    if self.propagate {
      let name = StringWithNul::from(self.name.clone());
      writer.write_value(&name)?;

      write_pad(writer, name.len(), 4)?;
      writer.write_value(&<Vec<u8>>::from(self.value.clone()))?;
    }
    Ok(())
  }
}

// Tag type from section 7.2.5 of the DDS Security specification (v. 1.1)
// The silly thing is almost the same as "Property"
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
  pub(crate) name: String,
  pub(crate) value: String,
}

impl<'a, C: Context> Readable<'a, C> for Tag {
  fn minimum_bytes_needed() -> usize {
    4
  }

  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let name: StringWithNul = reader.read_value()?;

    read_pad(reader, name.len(), 4)?; // pad according to previous read
    let value: StringWithNul = reader.read_value()?;

    Ok(Tag {
      name: name.into(),
      value: value.into(),
    })
  }
}

// See alignment comment in "Property"
impl<C: Context> Writable<C> for Tag {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    let name = StringWithNul::from(self.name.clone());
    writer.write_value(&name)?;

    write_pad(writer, name.len(), 4)?;
    writer.write_value(&StringWithNul::from(self.value.clone()))?;

    Ok(())
  }
}

impl Tag {
  pub fn serialized_len(&self) -> usize {
    let first = 4 + self.name.len() + 1;
    let misalign = first % 4;
    let align = if misalign > 0 { 4 - misalign } else { 0 };
    let second = 4 + self.value.len() + 1;
    first + align + second
  }
}

/// Utility for building [DataHolder]
#[derive(Default)]
pub struct DataHolderBuilder {
  class_id: String,
  properties: Vec<Property>,
  binary_properties: Vec<BinaryProperty>,
}

impl DataHolderBuilder {
  pub fn with_class_id(class_id: String) -> Self {
    Self {
      class_id,
      properties: vec![],
      binary_properties: vec![],
    }
  }

  pub fn add_property_opt(mut self, name: &str, value: Option<String>, propagate: bool) -> Self {
    if let Some(value) = value {
      let property = Property {
        name: name.to_string(),
        value,
        propagate,
      };
      self.properties.push(property);
    }
    self
  }

  pub fn add_binary_property(mut self, name: &str, value: Bytes, propagate: bool) -> Self {
    let bin_property = BinaryProperty {
      name: name.to_string(),
      value,
      propagate,
    };
    self.binary_properties.push(bin_property);
    self
  }

  pub fn add_binary_property_opt(
    mut self,
    name: &str,
    value: Option<Bytes>,
    propagate: bool,
  ) -> Self {
    if let Some(value) = value {
      let bin_property = BinaryProperty {
        name: name.to_string(),
        value,
        propagate,
      };
      self.binary_properties.push(bin_property);
    }
    self
  }

  pub fn build(self) -> DataHolder {
    DataHolder {
      class_id: self.class_id,
      properties: self.properties,
      binary_properties: self.binary_properties,
    }
  }
}

// DataHolder type from section 7.2.3 of the Security specification (v. 1.1)
// fields need to be public to make (de)serializable
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)] // for CDR in Discovery
pub struct DataHolder {
  pub(crate) class_id: String,
  pub(crate) properties: Vec<Property>,
  pub(crate) binary_properties: Vec<BinaryProperty>,
}

impl DataHolder {
  pub fn dummy() -> Self {
    Self {
      class_id: "dummy".to_string(),
      properties: vec![],
      binary_properties: vec![],
    }
  }

  pub(super) fn get_binary_property(&self, binary_property_name: &str) -> SecurityResult<Bytes> {
    get_binary_property(&self.binary_properties, binary_property_name)
  }

  pub fn properties_as_map(&self) -> HashMap<String, &Property> {
    // Return a HashMap where keys are property names and values are
    // references to properties
    let mut map = HashMap::new();
    for prop in &self.properties {
      map.insert(prop.name.clone(), prop);
    }
    map
  }

  pub fn binary_properties_as_map(&self) -> HashMap<String, &BinaryProperty> {
    // Return a HashMap where keys are binary property names and values are
    // references to binary properties
    let mut map = HashMap::new();
    for bin_prop in &self.binary_properties {
      map.insert(bin_prop.name.clone(), bin_prop);
    }
    map
  }
}

impl<'a, C: Context> Readable<'a, C> for DataHolder {
  fn minimum_bytes_needed() -> usize {
    4
  }

  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let class_id: StringWithNul = reader.read_value()?;

    read_pad(reader, class_id.len(), 4)?; // pad according to previous read
                                          // We can use this Qos reader, because it has identical structure.
    let qos::policy::Property {
      value,
      binary_value,
    } = reader.read_value()?;

    Ok(DataHolder {
      class_id: class_id.into(),
      properties: value,
      binary_properties: binary_value,
    })
  }
}

// See alignment comment in "Property"
impl<C: Context> Writable<C> for DataHolder {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    let class_id = StringWithNul::from(self.class_id.clone());
    writer.write_value(&class_id)?;

    write_pad(writer, class_id.len(), 4)?;
    // Use same structure equality as in Readable impl
    let q = qos::policy::Property {
      value: self.properties.clone(),
      binary_value: self.binary_properties.clone(),
    };
    writer.write_value(&q)?;

    Ok(())
  }
}

// In some cases the class_id of DataHolder is interpreted as consisting of a
// PluginClassName, a MajorVersion and a MinorVersion. For example in 9.3.2.1:
// "The value of the class_id shall be interpreted as composed of three parts: a
// PluginClassName, a MajorVersion and a MinorVersion according to the following
// format: <PluginClassName>:<MajorVersion>.<MinorVersion>. The PluginClassName
// is separated from the MajorVersion by the last ':' character in the class_id.
// The MajorVersion and MinorVersion are separated by a '.' character.
// Accordingly this version of the specification has PluginClassName equal to
// "DDS:Auth:PKI-DH", MajorVersion set to 1, and MinorVersion set to 0"
pub(super) struct PluginClassId {
  plugin_class_name: String,
  major_version: String, // Can be changed to number if needed
  #[allow(dead_code)]
  minor_version: String, // Can be changed to number if needed
}

// TODO: This struct should be used in
// auhtentication/authentication_builtin/types.rs instead of fixed strings.

impl PluginClassId {
  pub fn matches_up_to_major_version(
    &self,
    Self {
      plugin_class_name: other_plugin_class_name,
      major_version: other_major_version,
      ..
    }: &Self,
  ) -> SecurityResult<()> {
    self
      .plugin_class_name
      .eq(other_plugin_class_name)
      .then_some(())
      .ok_or_else(|| {
        create_security_error_and_log!(
          "Mismatched plugin class names: {} and {}",
          self.plugin_class_name,
          other_plugin_class_name
        )
      })
      .and(
        self
          .major_version
          .eq(other_major_version)
          .then_some(())
          .ok_or_else(|| {
            create_security_error_and_log!(
              "Mismatched plugin major versions: {} and {}",
              self.major_version,
              other_major_version
            )
          }),
      )
  }
}
impl TryFrom<String> for PluginClassId {
  type Error = SecurityError;
  fn try_from(value: String) -> Result<Self, Self::Error> {
    value
      .rsplit_once(':')
      .ok_or_else(|| {
        create_security_error_and_log!(
          "Failed to parse the class_id {}. Expected PluginClassName:VersionNumber",
          value
        )
      })
      .and_then(|(plugin_class_name, version_number)| {
        version_number
          .split_once('.')
          .ok_or_else(|| {
            create_security_error_and_log!(
              "Failed to parse the version number {} of class_id {}. Expected \
               MajorVersion.MinorVersion",
              version_number,
              value
            )
          })
          .map(|(major_version, minor_version)| Self {
            plugin_class_name: plugin_class_name.into(),
            major_version: major_version.into(),
            minor_version: minor_version.into(),
          })
      })
  }
}

// Token type from section 7.2.4 of the Security specification (v. 1.1)
//pub type Token = DataHolder; // never actually used in code.

// DDS Security spec v1.1 Section 7.2.7 ParticipantSecurityInfo
// This is communicated over Discovery

#[derive(Debug, Clone, PartialEq, Eq, Readable, Writable)]
pub struct ParticipantSecurityInfo {
  pub(crate) participant_security_attributes: ParticipantSecurityAttributesMask,
  pub(crate) plugin_participant_security_attributes: PluginParticipantSecurityAttributesMask,
}

impl From<ParticipantSecurityAttributes> for ParticipantSecurityInfo {
  fn from(sec_attributes: ParticipantSecurityAttributes) -> Self {
    Self {
      participant_security_attributes: ParticipantSecurityAttributesMask::from(
        sec_attributes.clone(),
      ),
      plugin_participant_security_attributes: sec_attributes.plugin_participant_attributes,
    }
  }
}

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Clone, Copy, Readable, Writable)]
#[bitflags]
#[repr(u32)]
#[allow(clippy::enum_variant_names)]
// Clippy complains, because all variant names have the same prefix "Is",
// but we blame the DDS Security spec for naming.
pub enum ParticipantSecurityAttributesMaskFlags {
  IsValid = 0x8000_0000, // (0x1 << 31)

  // DDS Security specification v1.1
  // Section 8.4.2.5 Definition of the ParticipantSecurityAttributesMask
  // Table 28
  IsRTPSProtected = 0b000_0001,
  IsDiscoveryProtected = 0b000_0010,
  IsLivelinessProtected = 0b000_0100,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ParticipantSecurityAttributesMask(pub BitFlags<ParticipantSecurityAttributesMaskFlags>);

impl ParticipantSecurityAttributesMask {
  pub fn is_valid(&self) -> bool {
    let Self(value) = self;
    value.contains(ParticipantSecurityAttributesMaskFlags::IsValid)
  }
}

impl<'a, C: Context> Readable<'a, C> for ParticipantSecurityAttributesMask {
  fn minimum_bytes_needed() -> usize {
    4
  }

  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let underlying_value: u32 = reader.read_value()?;
    BitFlags::<ParticipantSecurityAttributesMaskFlags>::try_from(underlying_value)
      .map(Self)
      // Convert the error to the correct type
      .map_err(|e| speedy::Error::custom(e).into())
  }
}
impl<C: Context> Writable<C> for ParticipantSecurityAttributesMask {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    let Self(value) = self;
    writer.write_value(&value.bits())
  }
}
impl PartialOrd for ParticipantSecurityAttributesMask {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}
impl Ord for ParticipantSecurityAttributesMask {
  fn cmp(&self, Self(value2): &Self) -> std::cmp::Ordering {
    let Self(value1) = self;
    value1.bits().cmp(&value2.bits())
  }
}

// 7.2.7 other than the validity bit, the mask is only understood inside the
// plugin implementations
pub type PluginParticipantSecurityAttributesMask = PluginSecurityAttributesMask;

// DDS Security spec v1.1 Section 7.2.8 EndpointSecurityInfo
// This is communicated over Discovery

#[derive(Debug, Clone, PartialEq, Eq, Readable, Writable)]
pub struct EndpointSecurityInfo {
  pub endpoint_security_attributes: EndpointSecurityAttributesMask,
  pub plugin_endpoint_security_attributes: PluginEndpointSecurityAttributesMask,
}

impl From<EndpointSecurityAttributes> for EndpointSecurityInfo {
  fn from(ep_attributes: EndpointSecurityAttributes) -> Self {
    Self {
      endpoint_security_attributes: EndpointSecurityAttributesMask::from(ep_attributes.clone()),
      plugin_endpoint_security_attributes: ep_attributes.plugin_endpoint_attributes,
    }
  }
}

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Clone, Copy, Readable, Writable)]
#[bitflags]
#[repr(u32)]
#[allow(clippy::enum_variant_names)]
// Clippy complains, because all variant names have the same prefix "Is",
// but we blame the DDS Security spec for naming.
pub enum EndpointSecurityAttributesMaskFlags {
  IsValid = 0x8000_0000, // (0x1 << 31)

  // DDS Security specification v1.1
  // Section 8.4.2.8 Definition of the EndpointSecurityAttributesMask
  // Table 31
  IsReadProtected = 0b0000_0001,
  IsWriteProtected = 0b0000_0010,
  IsDiscoveryProtected = 0b0000_0100,
  IsSubmessageProtected = 0b0000_1000,
  IsPayloadProtected = 0b0001_0000,
  IsKeyProtected = 0b0010_0000,
  IsLivelinessProtected = 0b0100_0000,
}
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EndpointSecurityAttributesMask(pub BitFlags<EndpointSecurityAttributesMaskFlags>);

impl EndpointSecurityAttributesMask {
  pub fn is_valid(&self) -> bool {
    let Self(value) = self;
    value.contains(EndpointSecurityAttributesMaskFlags::IsValid)
  }
}

impl<'a, C: Context> Readable<'a, C> for EndpointSecurityAttributesMask {
  fn minimum_bytes_needed() -> usize {
    4
  }

  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let underlying_value: u32 = reader.read_value()?;
    BitFlags::<EndpointSecurityAttributesMaskFlags>::try_from(underlying_value)
      .map(Self)
      // Convert the error to the correct type
      .map_err(|e| speedy::Error::custom(e).into())
  }
}
impl<C: Context> Writable<C> for EndpointSecurityAttributesMask {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    let Self(value) = self;
    writer.write_value(&value.bits())
  }
}
impl PartialOrd for EndpointSecurityAttributesMask {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}
impl Ord for EndpointSecurityAttributesMask {
  fn cmp(&self, Self(value2): &Self) -> std::cmp::Ordering {
    let Self(value1) = self;
    value1.bits().cmp(&value2.bits())
  }
}

// 7.2.8 other than the validity bit, the mask is only understood inside the
// plugin implementations
pub type PluginEndpointSecurityAttributesMask = PluginSecurityAttributesMask;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Clone, Copy, Readable, Writable)]
// Other than the validity bit, these masks are only understood inside the
// plugin implementations
pub struct PluginSecurityAttributesMask(pub u32);

impl PluginSecurityAttributesMask {
  pub fn is_valid(self) -> bool {
    let Self(value) = self;
    value >= 0x8000_0000 // Check whether the most significant bit is set
  }

  pub fn empty() -> Self {
    Self(0x8000_0000) // Only valid bit is set
  }
}

// ParticipantBuiltinTopicDataSecure from section 7.4.1.6 of the Security
// specification
#[derive(Debug)]
pub struct ParticipantBuiltinTopicDataSecure {
  pub participant_data: discovery::spdp_participant_data::SpdpDiscoveredParticipantData,
  pub identity_status_token_opt: Option<security::authentication::IdentityStatusToken>,
}
impl Keyed for ParticipantBuiltinTopicDataSecure {
  type K = Participant_GUID;
  fn key(&self) -> Self::K {
    self.participant_data.key()
  }
}
impl PlCdrDeserialize for ParticipantBuiltinTopicDataSecure {
  fn from_pl_cdr_bytes(
    input_bytes: &[u8],
    encoding: RepresentationIdentifier,
  ) -> Result<Self, PlCdrDeserializeError> {
    let ctx = pl_cdr_rep_id_to_speedy_d(encoding)?;
    let pl = ParameterList::read_from_buffer_with_ctx(ctx, input_bytes)?;
    let pl_map = pl.to_map();

    let identity_status_token_opt = get_option_from_pl_map(
      &pl_map,
      ctx,
      ParameterId::PID_IDENTITY_STATUS_TOKEN,
      "Identity status token",
    )?;

    let participant_data =
      discovery::spdp_participant_data::SpdpDiscoveredParticipantData::from_pl_cdr_bytes(
        input_bytes,
        encoding,
      )?;

    Ok(Self {
      participant_data,
      identity_status_token_opt,
    })
  }
}

impl PlCdrSerialize for ParticipantBuiltinTopicDataSecure {
  fn to_pl_cdr_bytes(
    &self,
    encoding: RepresentationIdentifier,
  ) -> Result<Bytes, PlCdrSerializeError> {
    let mut token_pl = ParameterList::new();
    let ctx = pl_cdr_rep_id_to_speedy(encoding)?;
    macro_rules! emit {
      ($pid:ident, $member:expr, $type:ty) => {
        token_pl.push(Parameter::new(ParameterId::$pid, {
          let m: &$type = $member;
          m.write_to_vec_with_ctx(ctx)?
        }))
      };
    }
    macro_rules! emit_option {
      ($pid:ident, $member:expr, $type:ty) => {
        if let Some(m) = $member {
          emit!($pid, m, $type)
        }
      };
    }
    emit_option!(
      PID_IDENTITY_STATUS_TOKEN,
      &self.identity_status_token_opt,
      security::authentication::IdentityStatusToken
    );

    let mut pl = self.participant_data.to_parameter_list(encoding)?;
    pl.concat(token_pl);

    let bytes = pl.serialize_to_bytes(ctx)?;
    Ok(bytes)
  }
}

// PublicationBuiltinTopicDataSecure from section 7.4.1.7 of the Security
// specification
pub struct PublicationBuiltinTopicDataSecure {
  pub discovered_writer_data: discovery::sedp_messages::DiscoveredWriterData,
  // data_tags in optional, since some DDS implementations do not send it
  pub data_tags: Option<qos::policy::DataTag>,
}

impl Keyed for PublicationBuiltinTopicDataSecure {
  type K = Endpoint_GUID;
  fn key(&self) -> Self::K {
    self.discovered_writer_data.key()
  }
}

impl From<discovery::sedp_messages::DiscoveredWriterData> for PublicationBuiltinTopicDataSecure {
  fn from(dwd: discovery::sedp_messages::DiscoveredWriterData) -> Self {
    Self {
      discovered_writer_data: dwd,
      data_tags: Some(qos::policy::DataTag::default()),
    }
  }
}

impl PlCdrDeserialize for PublicationBuiltinTopicDataSecure {
  fn from_pl_cdr_bytes(
    input_bytes: &[u8],
    encoding: RepresentationIdentifier,
  ) -> Result<Self, PlCdrDeserializeError> {
    let ctx = pl_cdr_rep_id_to_speedy_d(encoding)?;
    let pl = ParameterList::read_from_buffer_with_ctx(ctx, input_bytes)?;
    let pl_map = pl.to_map();

    let data_tags = get_option_from_pl_map(&pl_map, ctx, ParameterId::PID_DATA_TAGS, "Data tags")?;

    let discovered_writer_data =
      discovery::sedp_messages::DiscoveredWriterData::from_pl_cdr_bytes(input_bytes, encoding)?;

    Ok(Self {
      discovered_writer_data,
      data_tags,
    })
  }
}

impl PlCdrSerialize for PublicationBuiltinTopicDataSecure {
  fn to_pl_cdr_bytes(
    &self,
    encoding: RepresentationIdentifier,
  ) -> Result<Bytes, PlCdrSerializeError> {
    let mut data_tag_pl = ParameterList::new();
    let ctx = pl_cdr_rep_id_to_speedy(encoding)?;
    macro_rules! emit {
      ($pid:ident, $member:expr, $type:ty) => {
        data_tag_pl.push(Parameter::new(ParameterId::$pid, {
          let m: &$type = $member;
          m.write_to_vec_with_ctx(ctx)?
        }))
      };
    }
    if let Some(data_tags) = &self.data_tags {
      emit!(PID_DATA_TAGS, data_tags, qos::policy::DataTag);
    }

    let mut pl = self.discovered_writer_data.to_parameter_list(encoding)?;
    pl.concat(data_tag_pl);

    let bytes = pl.serialize_to_bytes(ctx)?;
    Ok(bytes)
  }
}

// SubscriptionBuiltinTopicDataSecure from section 7.4.1.8 of the Security
// specification
pub struct SubscriptionBuiltinTopicDataSecure {
  pub discovered_reader_data: discovery::sedp_messages::DiscoveredReaderData,
  // data_tags in optional, since some DDS implementations do not send it
  pub data_tags: Option<qos::policy::DataTag>,
}
impl Keyed for SubscriptionBuiltinTopicDataSecure {
  type K = Endpoint_GUID;
  fn key(&self) -> Self::K {
    self.discovered_reader_data.key()
  }
}
impl From<discovery::sedp_messages::DiscoveredReaderData> for SubscriptionBuiltinTopicDataSecure {
  fn from(drd: discovery::sedp_messages::DiscoveredReaderData) -> Self {
    Self {
      discovered_reader_data: drd,
      data_tags: Some(qos::policy::DataTag::default()),
    }
  }
}

impl PlCdrDeserialize for SubscriptionBuiltinTopicDataSecure {
  fn from_pl_cdr_bytes(
    input_bytes: &[u8],
    encoding: RepresentationIdentifier,
  ) -> Result<Self, PlCdrDeserializeError> {
    let ctx = pl_cdr_rep_id_to_speedy_d(encoding)?;
    let pl = ParameterList::read_from_buffer_with_ctx(ctx, input_bytes)?;
    let pl_map = pl.to_map();

    let data_tags = get_option_from_pl_map(&pl_map, ctx, ParameterId::PID_DATA_TAGS, "Data tags")?;

    let discovered_reader_data =
      discovery::sedp_messages::DiscoveredReaderData::from_pl_cdr_bytes(input_bytes, encoding)?;

    Ok(Self {
      discovered_reader_data,
      data_tags,
    })
  }
}

impl PlCdrSerialize for SubscriptionBuiltinTopicDataSecure {
  fn to_pl_cdr_bytes(
    &self,
    encoding: RepresentationIdentifier,
  ) -> Result<Bytes, PlCdrSerializeError> {
    let mut data_tag_pl = ParameterList::new();
    let ctx = pl_cdr_rep_id_to_speedy(encoding)?;
    macro_rules! emit {
      ($pid:ident, $member:expr, $type:ty) => {
        data_tag_pl.push(Parameter::new(ParameterId::$pid, {
          let m: &$type = $member;
          m.write_to_vec_with_ctx(ctx)?
        }))
      };
    }
    if let Some(data_tags) = &self.data_tags {
      emit!(PID_DATA_TAGS, data_tags, qos::policy::DataTag);
    }

    let mut pl = self.discovered_reader_data.to_parameter_list(encoding)?;
    pl.concat(data_tag_pl);

    let bytes = pl.serialize_to_bytes(ctx)?;
    Ok(bytes)
  }
}

// ParticipantStatelessMessage from section 7.4.3.3 of the Security
// specification
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ParticipantStatelessMessage {
  pub generic: ParticipantGenericMessage,
}
// The specification defines and uses the following specific values for the
// GenericMessageClassId:
// #define GMCLASSID_SECURITY_AUTH_REQUEST “dds.sec.auth_request”
// #define GMCLASSID_SECURITY_AUTH_HANDSHAKE “dds.sec.auth”

impl Keyed for ParticipantStatelessMessage {
  type K = GUID;

  fn key(&self) -> Self::K {
    self.generic.key()
  }
}

impl From<ParticipantGenericMessage> for ParticipantStatelessMessage {
  fn from(msg: ParticipantGenericMessage) -> Self {
    Self { generic: msg }
  }
}

pub const VOLATILE_ENDPOINT_RECOGNITION_PROPERTY_NAME: &str = "dds.sec.builtin_endpoint_name";
pub const VOLATILE_WRITER_RECOGNITION_PROPERTY_VALUE: &str =
  "BuiltinParticipantVolatileMessageSecureWriter";
pub const VOLATILE_READER_RECOGNITION_PROPERTY_VALUE: &str =
  "BuiltinParticipantVolatileMessageSecureReader";

// Property from which crypto plugin detects a volatile writer
// See 8.8.8.1 of the Security spec
pub fn volatile_writer_recognition_property() -> Property {
  Property {
    name: VOLATILE_ENDPOINT_RECOGNITION_PROPERTY_NAME.to_string(),
    value: VOLATILE_WRITER_RECOGNITION_PROPERTY_VALUE.to_string(),
    propagate: false,
  }
}

// Property from which crypto plugin detects a volatile reader
// See 8.8.8.1 of the Security spec
pub fn volatile_reader_recognition_property() -> Property {
  Property {
    name: VOLATILE_ENDPOINT_RECOGNITION_PROPERTY_NAME.to_string(),
    value: VOLATILE_READER_RECOGNITION_PROPERTY_VALUE.to_string(),
    propagate: false,
  }
}

// ParticipantVolatileMessageSecure from section 7.4.4.3 of the Security
// specification
//
// spec: typedef ParticipantVolatileMessageSecure ParticipantGenericMessage;
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ParticipantVolatileMessageSecure {
  pub generic: ParticipantGenericMessage,
}

impl From<ParticipantGenericMessage> for ParticipantVolatileMessageSecure {
  fn from(msg: ParticipantGenericMessage) -> Self {
    Self { generic: msg }
  }
}

impl Keyed for ParticipantVolatileMessageSecure {
  type K = GUID;

  fn key(&self) -> Self::K {
    self.generic.key()
  }
}

use crate::{
  discovery::{sedp_messages::Endpoint_GUID, spdp_participant_data::Participant_GUID},
  structure::rpc,
};
use super::access_control::{EndpointSecurityAttributes, ParticipantSecurityAttributes};

// This is the transport (message) type for specialized versions above.
// DDS Security Spec v1.1
// Section 7.2.6 ParticipantGenericMessage
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ParticipantGenericMessage {
  pub message_identity: rpc::SampleIdentity,
  pub related_message_identity: rpc::SampleIdentity,
  // GUIDs here need not be typed Endpoint_GUID or Participant_GUID,
  // because CDR serialization does not need the distinction, unlike PL_CDR.
  pub destination_participant_guid: GUID, //target for the request. Can be GUID_UNKNOWN
  pub destination_endpoint_guid: GUID,
  pub source_endpoint_guid: GUID,
  pub message_class_id: String,
  pub message_data: Vec<DataHolder>,
}

impl Keyed for ParticipantGenericMessage {
  type K = GUID;

  fn key(&self) -> Self::K {
    self.source_endpoint_guid
  }
}

impl ParticipantGenericMessage {
  pub fn source_guid_prefix(&self) -> GuidPrefix {
    self.message_identity.writer_guid.prefix
  }
}
