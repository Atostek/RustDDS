// Minimal ASN.1 (DER) definitions for the subset of the Cryptographic Message
// Syntax (CMS / PKCS#7 SignedData, RFC 5652) that RustDDS needs in order to
// verify signed Domain Governance and Domain Participant Permissions documents.
//
// This vendors just the types we use so that we can depend on the stable
// `der` 0.8 ecosystem. The upstream `cms` crate only has a pre-release build
// on top of `der` 0.8, so we avoid depending on it here. The definitions
// mirror the corresponding `cms` types (same field layout and tagging).

use der::{
  asn1::{Any, ObjectIdentifier, OctetString, SetOfVec},
  Sequence, ValueOrd,
};

// RFC 5652 ContentInfo / EncapsulatedContentInfo:
//   SEQUENCE { contentType OID, content [0] EXPLICIT ANY OPTIONAL }
#[derive(Debug, Sequence)]
pub struct EncapsulatedContentInfo {
  pub econtent_type: ObjectIdentifier,
  #[asn1(context_specific = "0", tag_mode = "EXPLICIT", optional = "true")]
  pub econtent: Option<Any>,
}

// RFC 5280 AlgorithmIdentifier: SEQUENCE { algorithm OID, parameters ANY OPTIONAL }
#[derive(Debug, Sequence, ValueOrd)]
pub struct AlgorithmIdentifier {
  pub oid: ObjectIdentifier,
  pub parameters: Option<Any>,
}

// RFC 5652 Attribute: SEQUENCE { attrType OID, attrValues SET OF ANY }
#[derive(Debug, Sequence, ValueOrd)]
pub struct Attribute {
  pub oid: ObjectIdentifier,
  pub values: SetOfVec<Any>,
}

// RFC 5652 SignedAttributes ::= SET SIZE (1..MAX) OF Attribute
pub type SignedAttributes = SetOfVec<Attribute>;

// RFC 5652 MessageDigest ::= OCTET STRING
pub type MessageDigest = OctetString;

// RFC 5652 SignerInfo. `sid` (a CHOICE) is parsed as an opaque `Any` since we
// do not inspect it; it only needs to be consumed by the decoder. The ignored
// `unsigned_attrs` (a SET OF Attribute) is parsed as `SetOfVec<Any>` because
// IMPLICIT context tagging requires a type with a fixed (constructed) tag,
// which `Any` alone does not provide.
#[derive(Debug, Sequence, ValueOrd)]
pub struct SignerInfo {
  pub version: u8,
  pub sid: Any,
  pub digest_algorithm: AlgorithmIdentifier,
  #[asn1(context_specific = "0", tag_mode = "IMPLICIT", optional = "true")]
  pub signed_attrs: Option<SignedAttributes>,
  pub signature_algorithm: AlgorithmIdentifier,
  pub signature: OctetString,
  #[asn1(context_specific = "1", tag_mode = "IMPLICIT", optional = "true")]
  pub unsigned_attrs: Option<SetOfVec<Any>>,
}

// RFC 5652 SignedData. The ignored `certificates` (CertificateSet) and `crls`
// (RevocationInfoChoices) are both SET OF productions parsed as `SetOfVec<Any>`
// so that the decoder consumes them (see the SignerInfo note about IMPLICIT
// tagging requiring a fixed-tag type).
#[derive(Debug, Sequence)]
pub struct SignedData {
  pub version: u8,
  pub digest_algorithms: SetOfVec<AlgorithmIdentifier>,
  pub encap_content_info: EncapsulatedContentInfo,
  #[asn1(context_specific = "0", tag_mode = "IMPLICIT", optional = "true")]
  pub certificates: Option<SetOfVec<Any>>,
  #[asn1(context_specific = "1", tag_mode = "IMPLICIT", optional = "true")]
  pub crls: Option<SetOfVec<Any>>,
  pub signer_infos: SetOfVec<SignerInfo>,
}
