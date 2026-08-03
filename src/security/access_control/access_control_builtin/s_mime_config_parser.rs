//
// DDS Security spec v1.1
//
// Section "9.4.1.2 Domain Governance Document
//
// The domain governance document is an XML document that specifies how the
// domain should be secured.
//
// The domain governance document shall be signed by the Permissions CA. The
// signed document shall use S/MIME version 3.2 format as defined in IETF RFC
// 5761 using SignedData Content Type (section 2.4.2 of IETF RFC 5761)
// formatted as multipart/signed (section 3.4.3 of IETF RFC 5761). This
// corresponds to the mime-type application/pkcs7-signature. Additionally the
// signer certificate shall be included within the signature."
//

// This module is for decoding the said S/MIME encoding. The same encoding
// applies also for the DomainParticipant Permissions Document. (Section
// 9.4.1.3)

use bytes::Bytes;
use cms::{
  attr::MessageDigest,
  signed_data::{EncapsulatedContentInfo, SignedData},
};
use der::{Decode, Encode};
use ring::{digest, signature};

use crate::{
  create_security_error_and_log,
  security::{
    certificate::Certificate,
    config::{
      other_config_error, pkcs7_config_error, to_config_error_other, to_config_error_pkcs7,
      ConfigError,
    },
    types::SecurityResult,
    SecurityError,
  },
};

#[derive(Debug)]
pub struct SignedDocument {
  //input_bytes: Bytes,
  content: Bytes,
  signature_der: Bytes,
}

impl SignedDocument {
  pub fn from_bytes(input: &[u8]) -> Result<SignedDocument, ConfigError> {
    let parsed_mail =
      mailparse::parse_mail(input).map_err(to_config_error_other("S/MIME parse failure"))?;

    match parsed_mail.subparts.as_slice() {
      [doc_content, signature] => {
        let content = Vec::<u8>::from(doc_content.raw_bytes);

        // Since mailparse 0.16, the CRLF/LF that precedes the MIME boundary
        // delimiter is treated as part of the boundary (per RFC 2046) and is no
        // longer included in `raw_bytes`, so we must not strip a trailing
        // newline ourselves here. (mailparse <=0.15 appended it, requiring a
        // manual `.pop()`.)

        // OpenSSL has converted to MS-DOS line endings when MIME encoding, and
        // the contents has has been computed from that.
        //
        // We need to reconstruct the line endings to get a correct hash value.
        let content = bytes_unix2dos(content)?.into();
        // Now `content` should be byte-for-byte the same as what was
        // the original signing input.

        let signature_der = Bytes::from(
          signature
            .get_body_raw()
            .map_err(to_config_error_other("S/MIME signature read failure"))?,
        );

        Ok(SignedDocument {
          //input_bytes: Bytes::copy_from_slice(input),
          content,
          signature_der,
        })
      }
      parts => Err(other_config_error(format!(
        "Expected 2-part S/MIME document, found {} parts.",
        parts.len()
      ))),
    }
  }

  // Use given X.509 certificate (in PEM format) to verify signature
  // and check that the data matches the signature.
  //
  // If successful, returns reference to the verified document.
  pub fn verify_signature(&self, certificate: &Certificate) -> SecurityResult<impl AsRef<[u8]>> {
    // start parsing signature
    let signature_encap = EncapsulatedContentInfo::from_der(&self.signature_der)
      .map_err(to_config_error_pkcs7("Cannot parse PKCS#7 signature"))?;

    // The SignedData type is defined in RFC 5652 Section 5.1.
    // OpenSSL calls this "pkcs7-signedData (1.2.840.113549.1.7.2)"
    if signature_encap.econtent_type
      != der::asn1::ObjectIdentifier::new_unwrap("1.2.840.113549.1.7.2")
    {
      return Err(pkcs7_config_error("Expected to find SignedData object".to_owned()).into());
    }

    let signed_data = match signature_encap.econtent {
      None => Err(pkcs7_config_error(
        "SignedData: Empty container?".to_owned(),
      )),
      Some(sig) => sig
        .decode_as::<SignedData>()
        .map_err(to_config_error_pkcs7("Cannot decode SignedData")),
    }?;

    let signer_info = signed_data.signer_infos.0.get(0).ok_or(pkcs7_config_error(
      "SignerInfo list in SignedData is empty!".to_owned(),
    ))?;

    let (content_hash_in_signature, signed_attributes_der) = match &signer_info.signed_attrs {
      None => Err(pkcs7_config_error(
        "SignedData without signed attributes not implemented".to_owned(),
      )),
      Some(sas) => {
        //println!("signed_attrs bytes={:02x?}\ndebug=\n{:?}",sas.to_der(), sas );

        // RFC 5652, Section 5.3.  SignerInfo Type:
        // "If the [signedAttrs] field is present, it MUST contain [...]
        // A message-digest attribute [...]""
        match sas.iter().find(|attr| attr.oid ==
                  der::asn1::ObjectIdentifier::new_unwrap("1.2.840.113549.1.9.4")) 
                // id-messageDigest OBJECT IDENTIFIER ::= { iso(1) member-body(2)
                // us(840) rsadsi(113549) pkcs(1) pkcs9(9) 4 }
                {
                    None => Err(pkcs7_config_error("SignedAttrs has no MessageDigest".to_owned())),
                    Some(attr) => {
                      let value_0 = attr.values.get(0)
                        .ok_or(pkcs7_config_error("Empty Attribute".to_owned()))?;
                      let digest = value_0.decode_as::<MessageDigest>()
                        .map_err(to_config_error_pkcs7("Cannot decode MessageDigest"))?;
                      // Section 5.4.  Message Digest Calculation Process:
                      // "A separate encoding
                      // of the signedAttrs field is performed for message digest calculation.
                      // The IMPLICIT [0] tag in the signedAttrs is not used for the DER
                      // encoding, rather an EXPLICIT SET OF tag is used."
                      //
                      // Simple re-encoding to DER will do the EXPLICIT re-tagging by default.
                      let sas_der = sas.to_der()
                        .map_err(to_config_error_pkcs7("Cannot re-encode signed attributes"))?;

                      Ok(( digest, sas_der ))
                    }
                }
      }
    }?;

    // compute a digest of actual contents
    let computed_contents_digest = digest::digest(&digest::SHA256, &self.content);

    // Check that hash actually matches the content
    if content_hash_in_signature.as_bytes() != computed_contents_digest.as_ref() {
      return Err(
        pkcs7_config_error(format!(
          "Contents hash in signature does not match actual content.\nsignature: \
           {content_hash_in_signature:02x?}\ncontent: {computed_contents_digest:02x?}"
        ))
        .into(),
      );
    }

    // Section 5.4:
    // When the [signedAttrs] field is present, however, the result is the message
    // digest of the complete DER encoding of the SignedAttrs value
    // contained in the signedAttrs field.

    // TODO these oids appear when testing, but are they absolutely correct or can
    // there be more corresponding to the same algorithm?
    const ECDSA_WITH_SHA256_OID: &str = "1.2.840.10045.4.3.2";
    const RSA_PKCS1: &str = "1.2.840.113549.1.1.1";

    //TODO maybe find a better way to determine the algorithm
    let verify_algorithm: &'static dyn ring::signature::VerificationAlgorithm = match format!(
      "{}",
      signer_info.signature_algorithm.oid
    )
    .as_str()
    {
      ECDSA_WITH_SHA256_OID => &signature::ECDSA_P256_SHA256_ASN1,
      //TODO for some reason this is not  working, even though the basic RSA does
      RSA_PKCS1 => Err(create_security_error_and_log!("RSA-PSS not yet supported."))?, /* &signature::RSA_PSS_2048_8192_SHA256, */
      other => Err(create_security_error_and_log!(
        "Unknown signature algorithm oid: {other}"
      ))?,
    };

    certificate.verify_signed_data_with_algorithm(
      signed_attributes_der,
      signer_info.signature.as_bytes(),
      verify_algorithm,
    )?; // exit on verification error

    Ok(self.content.clone())
  }
}

fn bytes_unix2dos(unix: Vec<u8>) -> Result<Vec<u8>, ConfigError> {
  let string =
    String::from_utf8(unix).map_err(to_config_error_pkcs7("Input is not valid UTF-8"))?;
  Ok(Vec::from(
    newline_converter::unix2dos(&string).as_ref().as_bytes(),
  ))
}

#[cfg(test)]
mod tests {
  use super::{
    super::{domain_governance_document::*, domain_participant_permissions_document::*},
    *,
  };
  use crate::security::certificate::*;

  #[test]
  pub fn parse_example() {
    // How to generate test data:
    // Use
    // * valid XML input file example.xml
    // * Signing certificate cert.pem
    // * Private key of certificate cert.key.pem
    //
    // openssl smime -sign -in example.xml -out example.p7s -signer cert.pem -inkey
    // cert.key.pem -text
    //
    // The resulting example.p7s file is the signed "document".

    let document = r#"MIME-Version: 1.0
Content-Type: multipart/signed; protocol="application/x-pkcs7-signature"; micalg="sha-256"; boundary="----D7B1B9F5CFE597B428D44692C7027BCF"

This is an S/MIME signed message

------D7B1B9F5CFE597B428D44692C7027BCF
Content-Type: text/plain

<dds xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.omg.org/spec/DDS-SECURITY/20170901/omg_shared_ca_permissions.xsd">
  <permissions>
    <grant name="/talker/api">
      <subject_name>CN=/talker/api</subject_name>
      <validity>
        <not_before>2023-07-23T08:28:37</not_before>
        <not_after>2033-07-21T08:28:37</not_after>
      </validity>
      <allow_rule>
        <domains>
          <id>0</id>
        </domains>
        <publish>
          <topics>
            <topic>rq/*/_action/cancel_goalRequest</topic>
            <topic>rq/*/_action/get_resultRequest</topic>
            <topic>rq/*/_action/send_goalRequest</topic>
            <topic>rq/*Request</topic>
            <topic>rr/*/_action/cancel_goalReply</topic>
            <topic>rr/*/_action/get_resultReply</topic>
            <topic>rr/*/_action/send_goalReply</topic>
            <topic>rt/*/_action/feedback</topic>
            <topic>rt/*/_action/status</topic>
            <topic>rr/*Reply</topic>
            <topic>rt/*</topic>
          </topics>
        </publish>
        <subscribe>
          <topics>
            <topic>rq/*/_action/cancel_goalRequest</topic>
            <topic>rq/*/_action/get_resultRequest</topic>
            <topic>rq/*/_action/send_goalRequest</topic>
            <topic>rq/*Request</topic>
            <topic>rr/*/_action/cancel_goalReply</topic>
            <topic>rr/*/_action/get_resultReply</topic>
            <topic>rr/*/_action/send_goalReply</topic>
            <topic>rt/*/_action/feedback</topic>
            <topic>rt/*/_action/status</topic>
            <topic>rr/*Reply</topic>
            <topic>rt/*</topic>
          </topics>
        </subscribe>
      </allow_rule>
      <allow_rule>
        <domains>
          <id>0</id>
        </domains>
        <publish>
          <topics>
            <topic>ros_discovery_info</topic>
          </topics>
        </publish>
        <subscribe>
          <topics>
            <topic>ros_discovery_info</topic>
          </topics>
        </subscribe>
      </allow_rule>
      <default>DENY</default>
    </grant>
  </permissions>
</dds>

------D7B1B9F5CFE597B428D44692C7027BCF
Content-Type: application/x-pkcs7-signature; name="smime.p7s"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="smime.p7s"

MIIC+QYJKoZIhvcNAQcCoIIC6jCCAuYCAQExDzANBglghkgBZQMEAgEFADALBgkq
hkiG9w0BBwGgggE/MIIBOzCB4qADAgECAhR361786/qVPfJWWDw4Wg5cmJUwBTAK
BggqhkjOPQQDAjASMRAwDgYDVQQDDAdzcm9zMkNBMB4XDTIzMDcyMzA4MjgzNloX
DTMzMDcyMTA4MjgzNlowEjEQMA4GA1UEAwwHc3JvczJDQTBZMBMGByqGSM49AgEG
CCqGSM49AwEHA0IABMpvJQ/91ZqnmRRteTL2qaEFz2d7SGAQQk9PIhhZCV1tlLwY
f/hI4xWLJaEv8FxJTjxXRGJ1U+/IqqqIvJVpWaSjFjAUMBIGA1UdEwEB/wQIMAYB
Af8CAQEwCgYIKoZIzj0EAwIDSAAwRQIgEiyVGRc664+/TE/HImA4WNwsSi/alHqP
YB58BWINj34CIQDDiHhbVPRB9Uxts9CwglxYgZoUdGUAxreYIIaLO4yLqzGCAX4w
ggF6AgEBMCowEjEQMA4GA1UEAwwHc3JvczJDQQIUd+te/Ov6lT3yVlg8OFoOXJiV
MAUwDQYJYIZIAWUDBAIBBQCggeQwGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAc
BgkqhkiG9w0BCQUxDxcNMjMwNzI0MDgyODM3WjAvBgkqhkiG9w0BCQQxIgQgZMN5
ePtmuncArHGyik6XN2V2jXQfvALMN5VW3lfTBEYweQYJKoZIhvcNAQkPMWwwajAL
BglghkgBZQMEASowCwYJYIZIAWUDBAEWMAsGCWCGSAFlAwQBAjAKBggqhkiG9w0D
BzAOBggqhkiG9w0DAgICAIAwDQYIKoZIhvcNAwICAUAwBwYFKw4DAgcwDQYIKoZI
hvcNAwICASgwCgYIKoZIzj0EAwIERzBFAiEAlWvUBFIygrtc7cPc5aWp7RUBKATm
xIIOy2nxSv6UekICIEoA39P0Va694kOj2FSR2CqInHrmGghkuuR5au8Aoaab

------D7B1B9F5CFE597B428D44692C7027BCF--

"#;

    let cert_pem = r#"-----BEGIN CERTIFICATE-----
MIIBOzCB4qADAgECAhR361786/qVPfJWWDw4Wg5cmJUwBTAKBggqhkjOPQQDAjAS
MRAwDgYDVQQDDAdzcm9zMkNBMB4XDTIzMDcyMzA4MjgzNloXDTMzMDcyMTA4Mjgz
NlowEjEQMA4GA1UEAwwHc3JvczJDQTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IA
BMpvJQ/91ZqnmRRteTL2qaEFz2d7SGAQQk9PIhhZCV1tlLwYf/hI4xWLJaEv8FxJ
TjxXRGJ1U+/IqqqIvJVpWaSjFjAUMBIGA1UdEwEB/wQIMAYBAf8CAQEwCgYIKoZI
zj0EAwIDSAAwRQIgEiyVGRc664+/TE/HImA4WNwsSi/alHqPYB58BWINj34CIQDD
iHhbVPRB9Uxts9CwglxYgZoUdGUAxreYIIaLO4yLqw==
-----END CERTIFICATE-----
"#;
    let cert = Certificate::from_pem(cert_pem).unwrap();

    let dpp_signed = SignedDocument::from_bytes(document.as_bytes()).unwrap();

    let verified_dpp_xml = dpp_signed.verify_signature(&cert).unwrap();

    DomainParticipantPermissions::from_xml(&String::from_utf8_lossy(verified_dpp_xml.as_ref()))
      .unwrap();

    // getting here with no panic is success
  }

  #[test]
  pub fn parse_small_example() {
    let document = r#"MIME-Version: 1.0
Content-Type: multipart/signed; protocol="application/x-pkcs7-signature"; micalg="sha-256"; boundary="----26B97E9080C33B5E9E82D8FDC0946E23"

This is an S/MIME signed message

------26B97E9080C33B5E9E82D8FDC0946E23
Content-Type: text/plain

<?xml version="1.0" encoding="utf-8"?>
<dds xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:noNamespaceSchemaLocation="http://www.omg.org/spec/DDS-Security/20170801/omg_shared_ca_domain_governance.xsd">
  <domain_access_rules>
    <domain_rule>
      <domains>
        <id>0</id>
      </domains>

      <allow_unauthenticated_participants>false</allow_unauthenticated_participants>
      <enable_join_access_control>true</enable_join_access_control>
      <rtps_protection_kind>SIGN</rtps_protection_kind>
      <discovery_protection_kind>SIGN</discovery_protection_kind>
      <liveliness_protection_kind>SIGN</liveliness_protection_kind>

      <topic_access_rules>
        <topic_rule>
          <topic_expression>Square*</topic_expression>
          <enable_discovery_protection>true
          </enable_discovery_protection>
          <enable_liveliness_protection>false</enable_liveliness_protection>
          <enable_read_access_control>true
          </enable_read_access_control>
          <enable_write_access_control>true
          </enable_write_access_control>
          <metadata_protection_kind>ENCRYPT
          </metadata_protection_kind>
          <data_protection_kind>ENCRYPT
          </data_protection_kind>
        </topic_rule>
      </topic_access_rules>
    </domain_rule>
  </domain_access_rules>
</dds>

------26B97E9080C33B5E9E82D8FDC0946E23
Content-Type: application/x-pkcs7-signature; name="smime.p7s"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="smime.p7s"

MIIC+QYJKoZIhvcNAQcCoIIC6jCCAuYCAQExDzANBglghkgBZQMEAgEFADALBgkq
hkiG9w0BBwGgggE/MIIBOzCB4qADAgECAhR361786/qVPfJWWDw4Wg5cmJUwBTAK
BggqhkjOPQQDAjASMRAwDgYDVQQDDAdzcm9zMkNBMB4XDTIzMDcyMzA4MjgzNloX
DTMzMDcyMTA4MjgzNlowEjEQMA4GA1UEAwwHc3JvczJDQTBZMBMGByqGSM49AgEG
CCqGSM49AwEHA0IABMpvJQ/91ZqnmRRteTL2qaEFz2d7SGAQQk9PIhhZCV1tlLwY
f/hI4xWLJaEv8FxJTjxXRGJ1U+/IqqqIvJVpWaSjFjAUMBIGA1UdEwEB/wQIMAYB
Af8CAQEwCgYIKoZIzj0EAwIDSAAwRQIgEiyVGRc664+/TE/HImA4WNwsSi/alHqP
YB58BWINj34CIQDDiHhbVPRB9Uxts9CwglxYgZoUdGUAxreYIIaLO4yLqzGCAX4w
ggF6AgEBMCowEjEQMA4GA1UEAwwHc3JvczJDQQIUd+te/Ov6lT3yVlg8OFoOXJiV
MAUwDQYJYIZIAWUDBAIBBQCggeQwGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAc
BgkqhkiG9w0BCQUxDxcNMjMwODE0MDc1NDQxWjAvBgkqhkiG9w0BCQQxIgQgvAEn
eveae5s8eNw0HdhAcxPkLpHI3cdiMLrX5U6hwNoweQYJKoZIhvcNAQkPMWwwajAL
BglghkgBZQMEASowCwYJYIZIAWUDBAEWMAsGCWCGSAFlAwQBAjAKBggqhkiG9w0D
BzAOBggqhkiG9w0DAgICAIAwDQYIKoZIhvcNAwICAUAwBwYFKw4DAgcwDQYIKoZI
hvcNAwICASgwCgYIKoZIzj0EAwIERzBFAiAZQGxjfAoLlk99UWV5AYkHr1CGvOrn
X/iBDEnMibF4NAIhAPB45KRXnnC8QmjYByycsOo4uGDrrUZ4K+tWLBfOv8v9

------26B97E9080C33B5E9E82D8FDC0946E23--
"#;
    let cert_pem = r#"-----BEGIN CERTIFICATE-----
MIIBOzCB4qADAgECAhR361786/qVPfJWWDw4Wg5cmJUwBTAKBggqhkjOPQQDAjAS
MRAwDgYDVQQDDAdzcm9zMkNBMB4XDTIzMDcyMzA4MjgzNloXDTMzMDcyMTA4Mjgz
NlowEjEQMA4GA1UEAwwHc3JvczJDQTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IA
BMpvJQ/91ZqnmRRteTL2qaEFz2d7SGAQQk9PIhhZCV1tlLwYf/hI4xWLJaEv8FxJ
TjxXRGJ1U+/IqqqIvJVpWaSjFjAUMBIGA1UdEwEB/wQIMAYBAf8CAQEwCgYIKoZI
zj0EAwIDSAAwRQIgEiyVGRc664+/TE/HImA4WNwsSi/alHqPYB58BWINj34CIQDD
iHhbVPRB9Uxts9CwglxYgZoUdGUAxreYIIaLO4yLqw==
-----END CERTIFICATE-----
"#;

    let cert = Certificate::from_pem(cert_pem).unwrap();

    let dgd_signed = SignedDocument::from_bytes(document.as_bytes()).unwrap();

    let verified_dgd_xml = dgd_signed.verify_signature(&cert).unwrap();

    DomainGovernanceDocument::from_xml(&String::from_utf8_lossy(verified_dgd_xml.as_ref()))
      .unwrap();

    // getting here with no panic is success
  }

  #[test]
  #[should_panic(expected = "signature does not match")]
  pub fn parse_corrupt_dgd() {
    let document = r#"MIME-Version: 1.0
Content-Type: multipart/signed; protocol="application/x-pkcs7-signature"; micalg="sha-256"; boundary="----26B97E9080C33B5E9E82D8FDC0946E23"

This is an S/MIME signed message

------26B97E9080C33B5E9E82D8FDC0946E23
Content-Type: text/plain

<?xml version="1.0" encoding="utf-8"?>
<dds xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:noNamespaceSchemaLocation="http://www.omg.org/spec/DDS-Security/20170801/omg_shared_ca_domain_governance.xsd">
  <domain_access_rules>
    <domain_rule>
      <domains>
        <id>0</id>
      </domains>

      <allow_unauthenticated_participants>false</allow_unauthenticated_participants>
      <enable_join_access_control>true</enable_join_access_control>
      <rtps_protection_kind>SIGN</rtps_protection_kind>
      <discovery_protection_kind>SIGN</discovery_protection_kind>
      <liveliness_protection_kind>SIGN</liveliness_protection_kind>

      <topic_access_rules>
        <topic_rule>
          <topic_expression>*</topic_expression> <!-- evil hacker has been here -->
          <enable_discovery_protection>true
          </enable_discovery_protection>
          <enable_liveliness_protection>false</enable_liveliness_protection>
          <enable_read_access_control>true
          </enable_read_access_control>
          <enable_write_access_control>true
          </enable_write_access_control>
          <metadata_protection_kind>ENCRYPT
          </metadata_protection_kind>
          <data_protection_kind>ENCRYPT
          </data_protection_kind>
        </topic_rule>
      </topic_access_rules>
    </domain_rule>
  </domain_access_rules>
</dds>

------26B97E9080C33B5E9E82D8FDC0946E23
Content-Type: application/x-pkcs7-signature; name="smime.p7s"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="smime.p7s"

MIIC+QYJKoZIhvcNAQcCoIIC6jCCAuYCAQExDzANBglghkgBZQMEAgEFADALBgkq
hkiG9w0BBwGgggE/MIIBOzCB4qADAgECAhR361786/qVPfJWWDw4Wg5cmJUwBTAK
BggqhkjOPQQDAjASMRAwDgYDVQQDDAdzcm9zMkNBMB4XDTIzMDcyMzA4MjgzNloX
DTMzMDcyMTA4MjgzNlowEjEQMA4GA1UEAwwHc3JvczJDQTBZMBMGByqGSM49AgEG
CCqGSM49AwEHA0IABMpvJQ/91ZqnmRRteTL2qaEFz2d7SGAQQk9PIhhZCV1tlLwY
f/hI4xWLJaEv8FxJTjxXRGJ1U+/IqqqIvJVpWaSjFjAUMBIGA1UdEwEB/wQIMAYB
Af8CAQEwCgYIKoZIzj0EAwIDSAAwRQIgEiyVGRc664+/TE/HImA4WNwsSi/alHqP
YB58BWINj34CIQDDiHhbVPRB9Uxts9CwglxYgZoUdGUAxreYIIaLO4yLqzGCAX4w
ggF6AgEBMCowEjEQMA4GA1UEAwwHc3JvczJDQQIUd+te/Ov6lT3yVlg8OFoOXJiV
MAUwDQYJYIZIAWUDBAIBBQCggeQwGAYJKoZIhvcNAQkDMQsGCSqGSIb3DQEHATAc
BgkqhkiG9w0BCQUxDxcNMjMwODE0MDc1NDQxWjAvBgkqhkiG9w0BCQQxIgQgvAEn
eveae5s8eNw0HdhAcxPkLpHI3cdiMLrX5U6hwNoweQYJKoZIhvcNAQkPMWwwajAL
BglghkgBZQMEASowCwYJYIZIAWUDBAEWMAsGCWCGSAFlAwQBAjAKBggqhkiG9w0D
BzAOBggqhkiG9w0DAgICAIAwDQYIKoZIhvcNAwICAUAwBwYFKw4DAgcwDQYIKoZI
hvcNAwICASgwCgYIKoZIzj0EAwIERzBFAiAZQGxjfAoLlk99UWV5AYkHr1CGvOrn
X/iBDEnMibF4NAIhAPB45KRXnnC8QmjYByycsOo4uGDrrUZ4K+tWLBfOv8v9

------26B97E9080C33B5E9E82D8FDC0946E23--
"#;
    let cert_pem = r#"-----BEGIN CERTIFICATE-----
MIIBOzCB4qADAgECAhR361786/qVPfJWWDw4Wg5cmJUwBTAKBggqhkjOPQQDAjAS
MRAwDgYDVQQDDAdzcm9zMkNBMB4XDTIzMDcyMzA4MjgzNloXDTMzMDcyMTA4Mjgz
NlowEjEQMA4GA1UEAwwHc3JvczJDQTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IA
BMpvJQ/91ZqnmRRteTL2qaEFz2d7SGAQQk9PIhhZCV1tlLwYf/hI4xWLJaEv8FxJ
TjxXRGJ1U+/IqqqIvJVpWaSjFjAUMBIGA1UdEwEB/wQIMAYBAf8CAQEwCgYIKoZI
zj0EAwIDSAAwRQIgEiyVGRc664+/TE/HImA4WNwsSi/alHqPYB58BWINj34CIQDD
iHhbVPRB9Uxts9CwglxYgZoUdGUAxreYIIaLO4yLqw==
-----END CERTIFICATE-----
"#;

    let cert = Certificate::from_pem(cert_pem).unwrap();

    // Now evil hacker has modified DGD content
    let dgd_signed = SignedDocument::from_bytes(document.as_bytes()).unwrap();

    // We expect this to fail
    dgd_signed.verify_signature(&cert).unwrap();

    // It is an error if we get past .unwrap() above.
    unreachable!();
    // let dgd =
    //   DomainGovernanceDocument::from_xml(&
    // String::from_utf8_lossy(verified_dgd_xml.as_ref()))     .unwrap();
  }
}
