use std::collections::BTreeMap;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use bytes::Bytes;
use x509_certificate::{signing::InMemorySigningKeyPair, Signer};
use cryptoki::{
  context::{CInitializeArgs, CInitializeFlags, Pkcs11},
  mechanism::{self, Mechanism},
  object::{Attribute, AttributeType, ObjectClass, ObjectHandle},
  session::{Session, UserType},
  slot::Slot,
  types::AuthPin,
};
use ring::digest;
use der::{asn1, Encode};

use crate::security::{
  authentication::authentication_builtin::types::CertificateAlgorithm,
  config::{parse_config_error, to_config_error_parse, ConfigError},
  types::{security_error, SecurityResult},
};

/// PrivateKey is the private key associaed with a Certificate (see neghbouring
/// module)
///
/// It can live within a .pem file (and normal memory), or in a Hardware
/// Security Module.

#[derive(Debug)]
pub(crate) enum PrivateKey {
  InMemory {
    priv_key: InMemorySigningKeyPair,
  },
  InHSM {
    key_algorithm: CertificateAlgorithm,
    // We store context and slot just in case dropping them would close them.
    #[allow(dead_code)]
    context: Pkcs11,
    #[allow(dead_code)]
    slot: Slot,
    session: Session,
    key_object_handle: ObjectHandle,
  },
}

// TODO: decrypt a password protected key
impl PrivateKey {
  pub fn from_pem(pem_data: impl AsRef<[u8]>) -> Result<Self, ConfigError> {
    let priv_key = InMemorySigningKeyPair::from_pkcs8_pem(pem_data.as_ref())
      .map_err(to_config_error_parse("Private key parse error"))?;

    Ok(PrivateKey::InMemory { priv_key })
  }

  // Process:
  //
  // decode object label in path
  // load HSM library
  // initialize library
  // get slots and enumerate
  // find slot where token is present and matches object label
  // open RO session to token and login with PIN
  // find private key objects
  // check that private key object supports "sign"
  // store library handle, slot handle, session handle to PrivateKey object
  // store object handle to PrivateKey
  // return PrivateKey

  pub fn from_pkcs11_uri_path_and_query(
    path_and_query: &str,
    key_algorithm: CertificateAlgorithm,
  ) -> Result<Self, ConfigError> {
    let interesting_attributes = vec![
      AttributeType::AllowedMechanisms,
      AttributeType::Class,
      AttributeType::Label,
      AttributeType::KeyType,
      AttributeType::Sign,
      AttributeType::Sensitive,
    ];
    // decode URI: path and possible query (PIN)
    //
    // example path_and_query, which is part of URI after scheme "pkcs11:"
    //
    // token=my_token_label?pin-value=1234&module-path=/usr/lib/softhsm/libsofthsm2.
    // so
    //
    // The "module-path" is a query-attribute like the PIN, and not a path-attribute
    // like "token", which may seem a bit strange choice, but that is what RFC
    // 7512 says.
    let (path, query) = path_and_query
      .split_once('?')
      .unwrap_or((path_and_query, ""));
    let path_attrs: BTreeMap<&str, &str> =
      path.split(';').filter_map(|a| a.split_once('=')).collect();
    let query_attrs: BTreeMap<&str, &str> =
      query.split('&').filter_map(|a| a.split_once('=')).collect();

    let token_label = path_attrs.get("object").ok_or(parse_config_error(
      "pkcs11 URI must specify attribute \"object\" i.e. token label".to_string(),
    ))?;

    let pin_value_opt = query_attrs.get("pin-value");

    let module_path: &str = query_attrs
      .get("module-path")
      .cloned()
      .unwrap_or("/usr/lib/softhsm/libsofthsm2.so"); // Some semi-reasnoable default value.

    info!("Opening PKCS#11 HSM client library {module_path}");
    let context = Pkcs11::new(module_path)?;
    context.initialize(CInitializeArgs::new(CInitializeFlags::OS_LOCKING_OK))?;
    debug!("PKCS#11 HSM library info: {:?}", context.get_library_info());

    let slots = context.get_all_slots()?;
    debug!("PKCS#11 HSM found {} slots.", slots.len());
    for (num, slot) in slots.iter().enumerate() {
      let slot_info = context.get_slot_info(*slot);
      match slot_info {
        Ok(si) if si.token_present() => {
          match context.get_token_info(*slot) {
            Ok(token_info) => {
              if token_info.label() == *token_label {
                info!("Found matching token \"{token_label}\" in slot {num}.");
                let session = context.open_ro_session(*slot)?;
                let secret_pin_opt: Option<AuthPin> =
                  pin_value_opt.map(|p| AuthPin::new((*p).into()));
                session.login(UserType::User, secret_pin_opt.as_ref())?; // bail on failure
                info!(
                  "Logged into token \"{}\" , using PIN = {:?}",
                  token_label,
                  secret_pin_opt.is_some()
                );

                // Now ,iterate though objects in the token.
                for (obj_num, obj) in session.find_objects(&[])?.iter().enumerate() {
                  let attr = session.get_attributes(*obj, &interesting_attributes)?;
                  // Check the attributes. Does this look like a private key?
                  if attr
                    .iter()
                    .any(|a| a == &Attribute::Class(ObjectClass::PRIVATE_KEY))
                    && attr.iter().any(|a| a == &Attribute::Sign(true))
                  {
                    // Is a private key and declares to support "sign" operation.
                    let object_label = attr.iter().find_map(|a| match a {
                      Attribute::Label(bytes) => Some(String::from_utf8_lossy(bytes)),
                      _ => None,
                    });
                    info!("Object {obj_num}: Found a Private Key. label={object_label:?}");

                    // Test that the signing operation works.
                    let test_signature_result =
                      session.sign(&key_algorithm.into(), *obj, b"This is just dummy data");
                    if test_signature_result.is_ok() {
                      debug!("Object {obj_num}: Test signing success.");
                      // Object looks like a legit private key, so we'll use that.
                      return Ok(PrivateKey::InHSM {
                        key_algorithm,
                        context,
                        slot: *slot,
                        session,
                        key_object_handle: *obj,
                      });
                    } else {
                      warn!(
                        "Object {obj_num}: Test signing fails: {test_signature_result:?}. Cannot \
                         use this as private key."
                      );
                    }
                  } else {
                    debug!("Object {obj_num}: Attributes do not match {attr:?}");
                  }
                } // for objects
              } else {
                debug!(
                  "Slot {}, token label={}: This is not the token we are looking for.",
                  num,
                  token_info.label()
                );
              }
            }
            Err(e) => warn!("Slot {num}: get_token_info() fails: {e:?}"),
          }
        } // Ok with token_present
        Ok(_) => info!("Slot {num} has no token"),
        Err(e) => warn!("Slot {num} get_slot_info() error: {e:?}"),
      }
    } // for slots

    Err(parse_config_error(
      "Did not find a suitable private key in the HSM".to_string(),
    ))
  }

  pub fn sign(&self, msg: &[u8]) -> SecurityResult<Bytes> {
    match self {
      PrivateKey::InMemory { priv_key } => priv_key
        .try_sign(msg)
        .map(|s| Bytes::copy_from_slice(s.as_ref()))
        .map_err(|e| security_error(&format!("Signing failure: {e:?}"))),

      PrivateKey::InHSM {
        key_algorithm,
        session,
        key_object_handle,
        ..
      } => {
        // DDS Security uses ASN.1-encoded ECDSA-SHA256 signatures.
        //
        // PKCS#11 (HSM) provides fixed-length ECDSA-signatures without SHA256.
        //
        // In order to
        // use HSM signing in DDS, we must first compute the SHA256 digest, then sign
        // using ECDSA. The result is two 32-byte integers (r,s) concatenated
        // together. These need to be ASN.1 DER-encoded according to RFC 3279
        // Section 2.2.3 as
        // ```
        // Ecdsa-Sig-Value  ::=  SEQUENCE  {
        //   r     INTEGER,
        //   s     INTEGER  }
        // ```
        //
        // Thanks to the
        // [ring crate documentation](https://docs.rs/ring/0.17.8/ring/signature/index.html)
        // for explaining this.

        // First, hash the message to be signed. Then sign the hash, not the message.
        let msg_digest = digest::digest(&digest::SHA256, msg);

        // Second, ask HSM to compute the signature
        let sign_mechanism = Mechanism::from(*key_algorithm);
        let hsm_signature_raw =
          session.sign(&sign_mechanism, *key_object_handle, msg_digest.as_ref())?;

        // Sanity check.
        if hsm_signature_raw.len() != 64 {
          return Err(security_error(&format!(
            "Expected signature len=64, got len={}",
            hsm_signature_raw.len()
          )));
        }

        // Third, convert raw signature to ASN.1 with DER
        let (r, s) = hsm_signature_raw.split_at(32); // safe, because of the length check above
        let mut hsm_signature_der = Vec::with_capacity(80); // typically needs about 70..72 bytes

        // Safety: We expect all the .unwrap() calls below to succeed, becaues the
        // possible errors are size overflows, and here the input sizes are fixed.
        let sequence_of_r_s = vec![
          asn1::UintRef::new(r).unwrap(),
          asn1::UintRef::new(s).unwrap(),
        ];
        sequence_of_r_s.encode(&mut hsm_signature_der).unwrap();

        Ok(Bytes::from(hsm_signature_der))
      }
    }
  } // fn
}

// Map our internal choice of certificate key algoritm to cryptoki Mechanism
impl From<CertificateAlgorithm> for Mechanism<'_> {
  fn from(value: CertificateAlgorithm) -> Self {
    match value {
      CertificateAlgorithm::ECPrime256v1 => Mechanism::Ecdsa,

      // DDS Security spec v1.1 Section "9.3.2.5.2 HandshakeReplyMessageToken" says
      //
      // "If the Participant Private Key is a RSA key, then [...]
      //  The digital signature shall be computed using the RSASSA-PSS
      //  algorithm specified in PKCS #1 (IETF 3447) RSA Cryptography
      //  Specifications Version 2.1 [44], using SHA256 as hash function, and
      //  MGF1 with SHA256 (mgf1sha256) as mask generation function."
      //
      // TODO: Make sure this is correctly mapped to `PkcsPssParams`
      CertificateAlgorithm::RSA2048 => Mechanism::Sha256RsaPkcsPss(mechanism::rsa::PkcsPssParams {
        hash_alg: mechanism::MechanismType::SHA256, // is this correct?
        mgf: mechanism::rsa::PkcsMgfType::MGF1_SHA256,
        s_len: 32.into(), /* is this correct?
                           * s_len is documented as
                           * "length, in bytes, of the salt value used in the PSS encoding;
                           * typical values are the length of the message hash and zero" */
      }),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  pub fn parse_private_key() {
    let priv_key_pem = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgaPBddesE0rHiP5/c
+djUjctfNoMAa5tNxOdged9AQtOhRANCAATKbyUP/dWap5kUbXky9qmhBc9ne0hg
EEJPTyIYWQldbZS8GH/4SOMViyWhL/BcSU48V0RidVPvyKqqiLyVaVmk
-----END PRIVATE KEY-----
"#;

    let key = PrivateKey::from_pem(priv_key_pem).unwrap();

    println!("{key:?}");
  }
}
