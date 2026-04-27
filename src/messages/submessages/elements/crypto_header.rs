use speedy::{Context, Readable, Reader, Writable, Writer};

use crate::security::cryptographic::{
  cryptographic_builtin::types::{BuiltinCryptoHeaderExtra, BuiltinCryptoTransformationKind},
  types::CryptoTransformIdentifier,
};

/// CryptoHeader: section 7.3.6.2.3 of the Security specification (v.
/// 1.1)
/// See section 7.3.7.3

// Note: The DDS Security spec does not specify any interpretation for the
// header extra bytes outside the plugin. There is no indication of the length either, as that
// may be plugin-specific. So we implement custom Readbale and Writable, which
// try to deduce how much to read/write.

#[derive(Debug, PartialEq, Eq, Clone, Writable)]
pub struct CryptoHeader {
  pub transformation_id: CryptoTransformIdentifier,
  pub plugin_crypto_header_extra: PluginCryptoHeaderExtra,
}

impl<'a, C: Context> Readable<'a, C> for CryptoHeader {
  fn minimum_bytes_needed() -> usize {
    std::mem::size_of::<CryptoTransformIdentifier>() + BuiltinCryptoHeaderExtra::serialized_len()
  }

  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let transformation_id = CryptoTransformIdentifier::read_from(reader)?;

    // now let's see if we recognize what this is
    if BuiltinCryptoTransformationKind::try_from(transformation_id.transformation_kind).is_ok() {
      let header_extra_data = reader.read_vec(BuiltinCryptoHeaderExtra::serialized_len())?;
      Ok(CryptoHeader {
        transformation_id,
        plugin_crypto_header_extra: header_extra_data.into(),
      })
    } else {
      Err(
        speedy::Error::custom(format!(
          "Unknown CryptoTransformationKind {:?} in CryptoHeader.",
          transformation_id.transformation_kind
        ))
        .into(),
      )
    }
  }
}

/// Should be interpreted by the plugin based on `transformation_id`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PluginCryptoHeaderExtra {
  pub data: Vec<u8>,
}
impl From<Vec<u8>> for PluginCryptoHeaderExtra {
  fn from(data: Vec<u8>) -> Self {
    Self { data }
  }
}

impl<C: Context> Writable<C> for PluginCryptoHeaderExtra {
  // Writing is simple enough. Just write everything without
  // length marker first.
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    writer.write_bytes(&self.data)
  }
}
