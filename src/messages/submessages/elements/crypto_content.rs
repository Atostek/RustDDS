use std::convert::identity;

use speedy::{Context, Readable, Writable};

/// CryptoContent: section 7.3.6.2.2 of the Security specification (v.
/// 1.1)
/// Should be interpreted by the plugin
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CryptoContent {
  pub data: Vec<u8>,
}
impl From<Vec<u8>> for CryptoContent {
  fn from(data: Vec<u8>) -> Self {
    Self { data }
  }
}
impl From<CryptoContent> for Vec<u8> {
  fn from(CryptoContent { data }: CryptoContent) -> Self {
    data
  }
}

// According to 9.5.2.4 and Wireshark, CryptoContent is always serialized as
// big-endian.
//
// TODO: Figure out if these could be done more elegantly with serde
impl<'a, C: Context> Readable<'a, C> for CryptoContent {
  fn minimum_bytes_needed() -> usize {
    4
  }

  fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
    reader
      // CDR: sequence starts with an unsigned long
      .read_u32()
      .map(if let speedy::Endianness::BigEndian = reader.endianness() {
        // If the context is big-endian, the read u32 is the sequence length
        identity
      } else {
        // If the context is little-endian, we have to swap the byte order
        u32::swap_bytes
      })
      // Convert to the platform-specific usize
      .and_then(|length| {
        usize::try_from(length)
          .map_err(|_| speedy::Error::custom("Could not convert u32 to usize.").into())
      })
      .and_then(|length| reader.read_vec(length))
      .map(CryptoContent::from)
  }
}

impl<C: Context> Writable<C> for CryptoContent {
  fn write_to<T: ?Sized + speedy::Writer<C>>(
    &self,
    writer: &mut T,
  ) -> Result<(), <C as Context>::Error> {
    u32::try_from(self.data.len())
      .map_err(|_| speedy::Error::custom("Could not convert usize to.u32").into())
      .map(if let speedy::Endianness::BigEndian = writer.endianness() {
        identity
      } else {
        // If the context is little-endian, we have to swap the byte order
        u32::swap_bytes
      })
      .and_then(|length| writer.write_u32(length))
      .and_then(|_| writer.write_bytes(&self.data))
  }
}
