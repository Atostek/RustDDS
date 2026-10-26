use bytes::Bytes;
use log::debug;
use x509_certificate::KeyAlgorithm;

use crate::{
  create_security_error_and_log,
  security::{
    authentication::types::*, security_error, DataHolderBuilder, SecurityError, SecurityResult,
  },
};

pub const IDENTITY_TOKEN_CLASS_ID: &str = "DDS:Auth:PKI-DH:1.0";

// Section "9.3.1 Configuration" , Table 44
pub(in crate::security) const QOS_IDENTITY_CA_PROPERTY_NAME: &str = "dds.sec.auth.identity_ca";
pub(in crate::security) const QOS_IDENTITY_CERTIFICATE_PROPERTY_NAME: &str =
  "dds.sec.auth.identity_certificate";
pub(in crate::security) const QOS_PRIVATE_KEY_PROPERTY_NAME: &str = "dds.sec.auth.private_key";
pub(in crate::security) const QOS_PASSWORD_PROPERTY_NAME: &str = "dds.sec.auth.password";

// Expected property names in IdentityToken
pub(in crate::security) const CERT_SN_PROPERTY_NAME: &str = "dds.cert.sn";
const CERT_ALGO_PROPERTY_NAME: &str = "dds.cert.algo";
const CA_SN_PROPERTY_NAME: &str = "dds.ca.sn";
const CA_ALGO_PROPERTY_NAME: &str = "dds.ca.algo";

// Algorithm identifiers used in IdentityToken and PermissionsToken
const RSA_2048_ALGO_NAME: &str = "RSA-2048";
const EC_PRIME_ALGO_NAME: &str = "EC-prime256v1";

pub(in crate::security) const RSA_2048_KEY_LENGTH: usize = 256;

#[derive(Debug, Clone, Copy)]
pub(in crate::security) enum CertificateAlgorithm {
  RSA2048,
  ECPrime256v1,
}
impl From<CertificateAlgorithm> for &str {
  fn from(value: CertificateAlgorithm) -> Self {
    match value {
      CertificateAlgorithm::RSA2048 => RSA_2048_ALGO_NAME,
      CertificateAlgorithm::ECPrime256v1 => EC_PRIME_ALGO_NAME,
    }
  }
}
impl From<CertificateAlgorithm> for String {
  fn from(value: CertificateAlgorithm) -> Self {
    String::from(<&str>::from(value))
  }
}
impl std::str::FromStr for CertificateAlgorithm {
  type Err = SecurityError;
  fn from_str(value: &str) -> Result<Self, Self::Err> {
    match value {
      RSA_2048_ALGO_NAME => Ok(CertificateAlgorithm::RSA2048),
      EC_PRIME_ALGO_NAME => Ok(CertificateAlgorithm::ECPrime256v1),
      _ => Err(create_security_error_and_log!(
        "Invalid certificate algorithm value: {}",
        value
      )),
    }
  }
}
impl TryFrom<String> for CertificateAlgorithm {
  type Error = SecurityError;
  fn try_from(value: String) -> Result<Self, Self::Error> {
    value.parse()
  }
}

// Convert public-key algortihm identifiers from x509_certficate crate to
// our representation.
impl TryFrom<x509_certificate::KeyAlgorithm> for CertificateAlgorithm {
  type Error = SecurityError;
  fn try_from(value: KeyAlgorithm) -> Result<Self, Self::Error> {
    match value {
      KeyAlgorithm::Rsa => Ok(CertificateAlgorithm::RSA2048),
      KeyAlgorithm::Ecdsa(x509_certificate::algorithm::EcdsaCurve::Secp256r1) => {
        Ok(CertificateAlgorithm::ECPrime256v1)
      }
      x => Err(create_security_error_and_log!(
        "Unsuppored certificate algorithm: {:?}",
        x
      )),
    }
  }
}

/// DDS:Auth:PKI-DH IdentityToken type from section 9.3.2.1 of the
/// Security specification (v. 1.1)
///
/// According to the spec, the presence of each of properties is optional
#[derive(Debug, Clone)]
pub(in crate::security) struct BuiltinIdentityToken {
  pub certificate_subject: Option<String>,
  pub certificate_algorithm: Option<CertificateAlgorithm>,
  pub ca_subject: Option<String>,
  pub ca_algorithm: Option<CertificateAlgorithm>,
}

impl TryFrom<IdentityToken> for BuiltinIdentityToken {
  type Error = SecurityError;

  fn try_from(token: IdentityToken) -> Result<Self, Self::Error> {
    let dh = token.data_holder;
    // Verify class id
    if dh.class_id != IDENTITY_TOKEN_CLASS_ID {
      return Err(create_security_error_and_log!(
        "Invalid class ID. Got {}, expected {}",
        dh.class_id,
        IDENTITY_TOKEN_CLASS_ID
      ));
    }

    // Extract properties
    let properties_map = dh.properties_as_map();

    let certificate_subject = if let Some(prop) = properties_map.get(CERT_SN_PROPERTY_NAME) {
      Some(prop.value())
    } else {
      debug!("IdentityToken did not contain the certificate subject name property");
      None
    };

    let certificate_algorithm = if let Some(prop) = properties_map.get(CERT_ALGO_PROPERTY_NAME) {
      Some(CertificateAlgorithm::try_from(prop.value())?)
    } else {
      debug!("IdentityToken did not contain the certificate algorithm property");
      None
    };

    let ca_subject = if let Some(prop) = properties_map.get(CA_SN_PROPERTY_NAME) {
      Some(prop.value())
    } else {
      debug!("IdentityToken did not contain the CA subject name property");
      None
    };

    let ca_algorithm = if let Some(prop) = properties_map.get(CA_ALGO_PROPERTY_NAME) {
      Some(CertificateAlgorithm::try_from(prop.value())?)
    } else {
      debug!("IdentityToken did not contain the CA algorithm property");
      None
    };

    let builtin_token = Self {
      certificate_subject,
      certificate_algorithm,
      ca_subject,
      ca_algorithm,
    };
    Ok(builtin_token)
  }
}

impl From<BuiltinIdentityToken> for IdentityToken {
  fn from(builtin_token: BuiltinIdentityToken) -> Self {
    // First create the DataHolder
    let dh_builder = DataHolderBuilder::with_class_id(IDENTITY_TOKEN_CLASS_ID.to_string())
      .add_property_opt(
        CERT_SN_PROPERTY_NAME,
        builtin_token.certificate_subject,
        true,
      )
      .add_property_opt(
        CERT_ALGO_PROPERTY_NAME,
        builtin_token.certificate_algorithm.map(String::from),
        true,
      )
      .add_property_opt(CA_SN_PROPERTY_NAME, builtin_token.ca_subject, true)
      .add_property_opt(
        CA_ALGO_PROPERTY_NAME,
        builtin_token.ca_algorithm.map(String::from),
        true,
      );

    IdentityToken::from(dh_builder.build())
  }
}

const IDENTITY_STATUS_TOKEN_CLASS_ID: &str = "DDS:Auth:PKI-DH:1.0";

const OCSP_STATUS_PROPERTY_NAME: &str = "ocsp_status";

/// DDS:Auth:PKI-DH IdentityStatusToken type from section 9.3.2.2 of the
/// Security specification (v. 1.1)
pub struct BuiltinIdentityStatusToken {
  ocsp_status: Option<String>, // Optional according to spec
}

impl TryFrom<IdentityStatusToken> for BuiltinIdentityStatusToken {
  type Error = String;

  fn try_from(token: IdentityStatusToken) -> Result<Self, Self::Error> {
    let dh = token.data_holder;
    // Verify class id
    if dh.class_id != IDENTITY_STATUS_TOKEN_CLASS_ID {
      return Err(format!(
        "Invalid class ID. Got {}, expected {}",
        dh.class_id, IDENTITY_STATUS_TOKEN_CLASS_ID
      ));
    }

    // Extract OCSP status property if present
    let properties_map = dh.properties_as_map();

    let ocsp_status = if let Some(prop) = properties_map.get(OCSP_STATUS_PROPERTY_NAME) {
      Some(prop.value())
    } else {
      debug!("IdentityStatusToken did not contain OCSP status");
      None
    };

    let builtin_token = Self { ocsp_status };
    Ok(builtin_token)
  }
}

impl From<BuiltinIdentityStatusToken> for IdentityStatusToken {
  fn from(builtin_token: BuiltinIdentityStatusToken) -> Self {
    // First create the DataHolder
    let dh_builder = DataHolderBuilder::with_class_id(IDENTITY_STATUS_TOKEN_CLASS_ID.to_string())
      .add_property_opt(OCSP_STATUS_PROPERTY_NAME, builtin_token.ocsp_status, true);

    IdentityStatusToken::from(dh_builder.build())
  }
}

const AUTH_REQUEST_MESSAGE_TOKEN_CLASS_ID: &str = "DDS:Auth:PKI-DH:1.0+AuthReq";

const FUTURE_CHALLENGE_PROPERTY_NAME: &str = "future_challenge";

/// DDS:Auth:PKI-DH AuthRequestMessageToken type from section 9.3.2.4 of the
/// Security specification (v. 1.1)
pub struct BuiltinAuthRequestMessageToken {
  // In spec future_challenge is a property (string value), but it probably needs to be a binary
  // property
  future_challenge: Bytes, // In spec this is
}

impl TryFrom<AuthRequestMessageToken> for BuiltinAuthRequestMessageToken {
  type Error = String;

  fn try_from(token: AuthRequestMessageToken) -> Result<Self, Self::Error> {
    let dh = token.data_holder;
    // Verify class id
    if dh.class_id != AUTH_REQUEST_MESSAGE_TOKEN_CLASS_ID {
      return Err(format!(
        "Invalid class ID. Got {}, expected {}",
        dh.class_id, AUTH_REQUEST_MESSAGE_TOKEN_CLASS_ID
      ));
    }

    // Extract future challenge property
    let bin_properties_map = dh.binary_properties_as_map();

    let future_challenge =
      if let Some(prop) = bin_properties_map.get(FUTURE_CHALLENGE_PROPERTY_NAME) {
        let bytes = prop.value();
        // Check NONCE length is 256 bits / 32 bytes
        if bytes.len() != 32 {
          return Err(format!(
            "Invalid NONCE length. Got {} bytes, expected 32",
            bytes.len()
          ));
        }
        bytes
      } else {
        return Err("AuthRequestMessageToken did not contain future_challenge".to_string());
      };

    let builtin_token = Self { future_challenge };
    Ok(builtin_token)
  }
}

impl From<BuiltinAuthRequestMessageToken> for AuthRequestMessageToken {
  fn from(builtin_token: BuiltinAuthRequestMessageToken) -> Self {
    DataHolderBuilder::with_class_id(AUTH_REQUEST_MESSAGE_TOKEN_CLASS_ID.to_string())
      .add_binary_property(
        FUTURE_CHALLENGE_PROPERTY_NAME,
        builtin_token.future_challenge,
        true,
      )
      .build()
      .into()
  }
}

pub const HANDSHAKE_REQUEST_CLASS_ID: &[u8] = b"DDS:Auth:PKI-DH:1.0+Req";
pub const HANDSHAKE_REPLY_CLASS_ID: &[u8] = b"DDS:Auth:PKI-DH:1.0+Reply";
pub const HANDSHAKE_FINAL_CLASS_ID: &[u8] = b"DDS:Auth:PKI-DH:1.0+Final";

// Accepted values for the name of the key agreement algorithm in handshake
// messages
pub const DH_MODP_KAGREE_ALGO_NAME: &str = "DH+MODP-2048-256";
pub const ECDH_KAGREE_ALGO_NAME: &str = "ECDH+prime256v1-CEUM";

// Standard values for "signature algorithm"
// as a byte string accrding to Table 49
// in DDS Security Spec v1.1 Section "9.3.2.5.1 HandshakeRequestMessageToken
// objects"
pub const RSA_SIGNATURE_ALGO_NAME: &[u8] = b"RSASSA-PSS-SHA256";
pub const ECDSA_SIGNATURE_ALGO_NAME: &[u8] = b"ECDSA-SHA256";

// Recognize standard string constatns and convert to
// corresponging algorithm identifiers in ring library.
pub(crate) fn parse_signature_algo_name_to_ring(
  algo_name: &[u8],
) -> SecurityResult<&'static dyn ring::signature::VerificationAlgorithm> {
  match algo_name {
    RSA_SIGNATURE_ALGO_NAME => Ok(&ring::signature::RSA_PSS_2048_8192_SHA256),
    ECDSA_SIGNATURE_ALGO_NAME => Ok(&ring::signature::ECDSA_P256_SHA256_ASN1),
    _other =>
    // TODO: Log the algorithm name, but be careful,
    // the name is is arbitrary binary data from an unknown third party.
    {
      Err(create_security_error_and_log!(
        "Unknown signature algorithm name"
      ))
    }
  }
}

/// DDS:Auth:PKI-DH HandshakeMessageToken type from section 9.3.2.5 of the
/// Security specification (v. 1.1)
/// Works as all three token formats: HandshakeRequestMessageToken,
/// HandshakeReplyMessageToken and HandshakeFinalMessageToken
pub(in crate::security) struct BuiltinHandshakeMessageToken {
  pub class_id: Bytes,
  pub c_id: Option<Bytes>,
  pub c_perm: Option<Bytes>,
  pub c_pdata: Option<Bytes>,
  pub c_dsign_algo: Option<Bytes>,
  pub c_kagree_algo: Option<Bytes>,
  pub ocsp_status: Option<Bytes>,
  pub hash_c1: Option<Bytes>,
  pub dh1: Option<Bytes>,
  pub hash_c2: Option<Bytes>,
  pub dh2: Option<Bytes>,
  pub challenge1: Option<Bytes>,
  pub challenge2: Option<Bytes>,
  pub signature: Option<Bytes>,
}

impl BuiltinHandshakeMessageToken {
  // request message parser
  pub fn extract_request(self) -> SecurityResult<HandshakeRequest> {
    if self.class_id.as_ref() != HANDSHAKE_REQUEST_CLASS_ID {
      return Err(security_error(&format!(
        "Wrong class_id: {:?}. Expected {:?}",
        self.class_id,
        std::str::from_utf8(HANDSHAKE_REQUEST_CLASS_ID)
      )));
    }
    let c_id = self
      .c_id
      .ok_or_else(|| create_security_error_and_log!("c_id not found"))?;
    let c_perm = self
      .c_perm
      .ok_or_else(|| create_security_error_and_log!("c_perm not found"))?;
    let c_pdata = self
      .c_pdata
      .ok_or_else(|| create_security_error_and_log!("c_pdata not found"))?;
    let c_dsign_algo = self
      .c_dsign_algo
      .ok_or_else(|| create_security_error_and_log!("c_dsign_algo not found"))?;
    let c_kagree_algo = self
      .c_kagree_algo
      .ok_or_else(|| create_security_error_and_log!("c_kagree_algo not found"))?;
    let hash_c1 = self.hash_c1.map(|mh| mh.as_ref().try_into()).transpose()?;
    let challenge1 = self
      .challenge1
      .ok_or_else(|| create_security_error_and_log!("challenge1 not found"))
      .and_then(|b| Challenge::try_from(b.as_ref()))?;
    let dh1 = self
      .dh1
      .ok_or_else(|| create_security_error_and_log!("dh1 not found"))?;

    Ok(HandshakeRequest {
      c_id,
      c_perm,
      c_pdata,
      c_dsign_algo,
      c_kagree_algo,
      hash_c1,
      challenge1,
      dh1,
    })
  }

  // reply message parser
  pub fn extract_reply(self) -> SecurityResult<HandshakeReply> {
    if self.class_id.as_ref() != HANDSHAKE_REPLY_CLASS_ID {
      return Err(security_error(&format!(
        "Wrong class_id: {:?}. Expected {:?}",
        self.class_id,
        std::str::from_utf8(HANDSHAKE_REPLY_CLASS_ID)
      )));
    }
    let c_id = self
      .c_id
      .ok_or_else(|| create_security_error_and_log!("c_id not found"))?;
    let c_perm = self
      .c_perm
      .ok_or_else(|| create_security_error_and_log!("c_perm not found"))?;
    let c_pdata = self
      .c_pdata
      .ok_or_else(|| create_security_error_and_log!("c_pdata not found"))?;
    let c_dsign_algo = self
      .c_dsign_algo
      .ok_or_else(|| create_security_error_and_log!("c_dsign_algo not found"))?;
    let c_kagree_algo = self
      .c_kagree_algo
      .ok_or_else(|| create_security_error_and_log!("c_kagree_algo not found"))?;
    let hash_c1 = self.hash_c1.map(|mh| mh.as_ref().try_into()).transpose()?;
    let hash_c2 = self.hash_c2.map(|mh| mh.as_ref().try_into()).transpose()?;
    let challenge1 = self
      .challenge1
      .ok_or_else(|| create_security_error_and_log!("challenge1 not found"))
      .and_then(|b| Challenge::try_from(b.as_ref()))?;
    let challenge2 = self
      .challenge2
      .ok_or_else(|| create_security_error_and_log!("challenge2 not found"))
      .and_then(|b| Challenge::try_from(b.as_ref()))?;
    let dh1 = self
      .dh1
      .ok_or_else(|| create_security_error_and_log!("dh1 not found"))?;
    let dh2 = self
      .dh2
      .ok_or_else(|| create_security_error_and_log!("dh2 not found"))?;
    let signature = self
      .signature
      .ok_or_else(|| create_security_error_and_log!("signature not found"))?;

    Ok(HandshakeReply {
      c_id,
      c_perm,
      c_pdata,
      c_dsign_algo,
      c_kagree_algo,
      hash_c1,
      hash_c2,
      challenge1,
      challenge2,
      dh1,
      dh2,
      signature,
    })
  }

  // final message parser
  pub fn extract_final(self) -> SecurityResult<HandshakeFinal> {
    if self.class_id.as_ref() != HANDSHAKE_FINAL_CLASS_ID {
      return Err(security_error(&format!(
        "Wrong class_id: {:?}. Expected {:?}",
        self.class_id,
        std::str::from_utf8(HANDSHAKE_FINAL_CLASS_ID)
      )));
    }
    let hash_c1 = self.hash_c1.map(|mh| mh.as_ref().try_into()).transpose()?;
    let hash_c2 = self.hash_c2.map(|mh| mh.as_ref().try_into()).transpose()?;
    let challenge1 = self
      .challenge1
      .ok_or_else(|| create_security_error_and_log!("challenge1 not found"))
      .and_then(|b| Challenge::try_from(b.as_ref()))?;
    let challenge2 = self
      .challenge2
      .ok_or_else(|| create_security_error_and_log!("challenge2 not found"))
      .and_then(|b| Challenge::try_from(b.as_ref()))?;
    let dh1 = self
      .dh1
      .ok_or_else(|| create_security_error_and_log!("dh1 not found"))?;
    let dh2 = self
      .dh2
      .ok_or_else(|| create_security_error_and_log!("dh2 not found"))?;
    let signature = self
      .signature
      .ok_or_else(|| create_security_error_and_log!("signature not found"))?;

    Ok(HandshakeFinal {
      hash_c1,
      challenge1,
      dh1,
      hash_c2,
      challenge2,
      dh2,
      signature,
    })
  }
}

// Helper struct for parsing HandshakeRequest
// spec Table 49
pub(in crate::security) struct HandshakeRequest {
  pub c_id: Bytes,
  pub c_perm: Bytes,
  pub c_pdata: Bytes,
  pub c_dsign_algo: Bytes,
  pub c_kagree_algo: Bytes,
  pub hash_c1: Option<Sha256>,
  pub challenge1: Challenge,
  pub dh1: Bytes,
}

// Helper struct for parsing HandshakeReply
// Table 50
pub(in crate::security) struct HandshakeReply {
  pub c_id: Bytes,
  pub c_perm: Bytes,
  pub c_pdata: Bytes,
  pub c_dsign_algo: Bytes,
  pub c_kagree_algo: Bytes,
  pub hash_c1: Option<Sha256>,
  pub hash_c2: Option<Sha256>,
  pub challenge1: Challenge,
  pub challenge2: Challenge,
  pub dh1: Bytes,
  pub dh2: Bytes,
  pub signature: Bytes,
}

// Table 51
pub(in crate::security) struct HandshakeFinal {
  pub hash_c1: Option<Sha256>,
  pub hash_c2: Option<Sha256>,
  pub challenge1: Challenge,
  pub challenge2: Challenge,
  pub dh1: Bytes,
  pub dh2: Bytes,
  pub signature: Bytes,
}

impl TryFrom<HandshakeMessageToken> for BuiltinHandshakeMessageToken {
  type Error = String;

  fn try_from(token: HandshakeMessageToken) -> Result<Self, Self::Error> {
    let dh = token.data_holder;

    // Verify class id is one of expected
    if !([
      HANDSHAKE_REQUEST_CLASS_ID,
      HANDSHAKE_REPLY_CLASS_ID,
      HANDSHAKE_FINAL_CLASS_ID,
    ]
    .contains(&dh.class_id.as_bytes()))
    {
      return Err(format!("Invalid class ID '{}'", dh.class_id));
    }

    // Extract binary properties
    let bin_properties_map = dh.binary_properties_as_map();

    let c_id = bin_properties_map.get("c.id").map(|val| val.value());
    let c_perm = bin_properties_map.get("c.perm").map(|val| val.value());
    let c_pdata = bin_properties_map.get("c.pdata").map(|val| val.value());
    let c_dsign_algo = bin_properties_map
      .get("c.dsign_algo")
      .map(|val| val.value());
    let c_kagree_algo = bin_properties_map
      .get("c.kagree_algo")
      .map(|val| val.value());
    let ocsp_status = bin_properties_map.get("ocsp_status").map(|val| val.value());
    let hash_c1 = bin_properties_map.get("hash_c1").map(|val| val.value());
    let dh1 = bin_properties_map.get("dh1").map(|val| val.value());
    let hash_c2 = bin_properties_map.get("hash_c2").map(|val| val.value());
    let dh2 = bin_properties_map.get("dh2").map(|val| val.value());
    let challenge1 = bin_properties_map.get("challenge1").map(|val| val.value());
    let challenge2 = bin_properties_map.get("challenge2").map(|val| val.value());
    let signature = bin_properties_map.get("signature").map(|val| val.value());

    let builtin_token = Self {
      class_id: Bytes::copy_from_slice(dh.class_id.as_bytes()),
      c_id,
      c_perm,
      c_pdata,
      c_dsign_algo,
      c_kagree_algo,
      ocsp_status,
      hash_c1,
      dh1,
      hash_c2,
      dh2,
      challenge1,
      challenge2,
      signature,
    };
    Ok(builtin_token)
  }
}

impl From<BuiltinHandshakeMessageToken> for HandshakeMessageToken {
  fn from(builtin_token: BuiltinHandshakeMessageToken) -> Self {
    // TODO: make sure this .unwrap() does not panic
    // Better yet, class_id in DataHolder should be converted to Bytes or
    // Vec<u8>, as it is OMG IDL type string, which is not UTF-8, but
    // just an byte string (with null characters forbidden).
    DataHolderBuilder::with_class_id(String::from_utf8(builtin_token.class_id.to_vec()).unwrap())
      .add_binary_property_opt("c.id", builtin_token.c_id, true)
      .add_binary_property_opt("c.perm", builtin_token.c_perm, true)
      .add_binary_property_opt("c.pdata", builtin_token.c_pdata, true)
      .add_binary_property_opt("c.dsign_algo", builtin_token.c_dsign_algo, true)
      .add_binary_property_opt("c.kagree_algo", builtin_token.c_kagree_algo, true)
      .add_binary_property_opt("ocsp_status", builtin_token.ocsp_status, true)
      .add_binary_property_opt("hash_c1", builtin_token.hash_c1, true)
      .add_binary_property_opt("dh1", builtin_token.dh1, true)
      .add_binary_property_opt("hash_c2", builtin_token.hash_c2, true)
      .add_binary_property_opt("dh2", builtin_token.dh2, true)
      .add_binary_property_opt("challenge1", builtin_token.challenge1, true)
      .add_binary_property_opt("challenge2", builtin_token.challenge2, true)
      .add_binary_property_opt("signature", builtin_token.signature, true)
      .build()
      .into()
  }
}

const AUTHENTICATED_PEER_TOKEN_CLASS_ID: &str = "DDS:Auth:PKI-DH:1.0";

pub(in crate::security) const AUTHENTICATED_PEER_TOKEN_IDENTITY_CERTIFICATE_PROPERTY_NAME: &str =
  "c.id";
pub(in crate::security) const AUTHENTICATED_PEER_TOKEN_PERMISSIONS_DOCUMENT_PROPERTY_NAME: &str =
  "c.perm";

/// DDS:Auth:PKI-DH AuthenticatedPeerCredentialToken type from section 9.3.2.3
/// of the Security specification (v. 1.1)
/// The spec specifies the fields as properties (Strings), but they actually
/// should be binary properties. See <https://issues.omg.org/issues/DDSSEC12-110>
pub(in crate::security) struct BuiltinAuthenticatedPeerCredentialToken {
  pub c_id: Bytes,
  pub c_perm: Bytes,
}

impl TryFrom<AuthenticatedPeerCredentialToken> for BuiltinAuthenticatedPeerCredentialToken {
  type Error = SecurityError;

  fn try_from(token: AuthenticatedPeerCredentialToken) -> Result<Self, Self::Error> {
    let dh = token.data_holder;
    // Verify class id
    if dh.class_id != AUTHENTICATED_PEER_TOKEN_CLASS_ID {
      return Err(security_error(&format!(
        "Invalid class ID. Got {}, expected {}",
        dh.class_id, AUTHENTICATED_PEER_TOKEN_CLASS_ID
      )));
    }

    // Extract properties
    let bin_props_map = dh.binary_properties_as_map();

    let c_id = if let Some(prop) =
      bin_props_map.get(AUTHENTICATED_PEER_TOKEN_IDENTITY_CERTIFICATE_PROPERTY_NAME)
    {
      prop.value()
    } else {
      return Err(security_error(&format!(
        "No required {AUTHENTICATED_PEER_TOKEN_IDENTITY_CERTIFICATE_PROPERTY_NAME} binary property"
      )));
    };

    let c_perm = if let Some(prop) =
      bin_props_map.get(AUTHENTICATED_PEER_TOKEN_PERMISSIONS_DOCUMENT_PROPERTY_NAME)
    {
      prop.value()
    } else {
      return Err(security_error(&format!(
        "No required {AUTHENTICATED_PEER_TOKEN_PERMISSIONS_DOCUMENT_PROPERTY_NAME} binary property"
      )));
    };

    let builtin_token = Self { c_id, c_perm };
    Ok(builtin_token)
  }
}

impl From<BuiltinAuthenticatedPeerCredentialToken> for AuthenticatedPeerCredentialToken {
  fn from(builtin_token: BuiltinAuthenticatedPeerCredentialToken) -> Self {
    let dh_builder =
      DataHolderBuilder::with_class_id(AUTHENTICATED_PEER_TOKEN_CLASS_ID.to_string())
        .add_binary_property(
          AUTHENTICATED_PEER_TOKEN_IDENTITY_CERTIFICATE_PROPERTY_NAME,
          builtin_token.c_id,
          true,
        )
        .add_binary_property(
          AUTHENTICATED_PEER_TOKEN_PERMISSIONS_DOCUMENT_PROPERTY_NAME,
          builtin_token.c_perm,
          true,
        );
    AuthenticatedPeerCredentialToken::from(dh_builder.build())
  }
}
