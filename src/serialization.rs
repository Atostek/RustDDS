mod cdr_adapters;

pub(crate) mod pl_cdr_adapters;
pub(crate) mod speedy_pl_cdr_helpers;

mod representation_identifier;

// Most of the CDR encoding/decoding comes from this external crate
pub use cdr_encoding::{
  from_bytes, to_vec, to_writer, CdrDeserializer, CdrSerializer, Error, Result,
};
// Export some parts of inner modules
pub use cdr_adapters::{
  deserialize_from_cdr_with_decoder_and_rep_id, deserialize_from_cdr_with_rep_id,
  to_writer_with_rep_id, CDRDeserializerAdapter, CDRSerializerAdapter, CdrDeserializeSeedDecoder,
};
pub use representation_identifier::RepresentationIdentifier;

// Compute how much padding bytes are needed to
// get the next multiple of 4
pub fn padding_needed_for_alignment_4(unaligned_length: usize) -> usize {
  if unaligned_length % 4 != 0 {
    4 - (unaligned_length % 4)
  } else {
    0
  }
}

pub fn round_up_to_4(unaligned_length: usize) -> usize {
  unaligned_length + padding_needed_for_alignment_4(unaligned_length)
}
