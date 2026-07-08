use chrono::Utc;

use crate::{
  create_security_error_and_log,
  dds::qos::QosPolicies,
  discovery::SpdpDiscoveredParticipantData,
  security::{
    access_control::{
      access_control_builtin::{
        domain_governance_document::DomainGovernanceDocument,
        domain_participant_permissions_document::DomainParticipantPermissions,
      },
      *,
    },
    authentication::{
      authentication_builtin::types::{
        AUTHENTICATED_PEER_TOKEN_IDENTITY_CERTIFICATE_PROPERTY_NAME,
        AUTHENTICATED_PEER_TOKEN_PERMISSIONS_DOCUMENT_PROPERTY_NAME,
        QOS_IDENTITY_CERTIFICATE_PROPERTY_NAME,
      },
      *,
    },
    certificate::{Certificate, DistinguishedName},
    config::*,
    *,
  },
};
use super::{
  domain_governance_document::{DomainRule, TopicRule},
  s_mime_config_parser::SignedDocument,
  types::{
    BuiltinPermissionsCredentialToken, BuiltinPermissionsToken,
    BuiltinPluginParticipantSecurityAttributes, QOS_GOVERNANCE_DOCUMENT_PROPERTY_NAME,
    QOS_PERMISSIONS_CERTIFICATE_PROPERTY_NAME, QOS_PERMISSIONS_DOCUMENT_PROPERTY_NAME,
  },
};

impl AccessControlBuiltin {
  fn check_participant(
    &self,
    permissions_handle: PermissionsHandle,
    domain_id: u16,
  ) -> SecurityResult<bool> {
    let grant = self.get_grant(&permissions_handle)?;
    let DomainRule {
      // corresponds to is_access_protected
      enable_join_access_control,
      topic_access_rules,
      ..
    } = self.get_domain_rule(&permissions_handle)?;

    let unprotected_topics = !enable_join_access_control
      || topic_access_rules.iter().any(
        |TopicRule {
           enable_read_access_control,
           enable_write_access_control,
           ..
         }| !(*enable_read_access_control && *enable_write_access_control),
      );

    // The specification seems to have a mistake here for check_create_participant. We should also check for protected topics for which we haver permissions. See https://issues.omg.org/issues/DDSSEC12-79
    let joinable_topics = unprotected_topics || grant.check_participant_join(domain_id);
    Ok(joinable_topics)
  }
}

// 9.4.3
impl ParticipantAccessControl for AccessControlBuiltin {
  fn validate_local_permissions(
    &mut self,
    _auth_plugin: &dyn Authentication,
    identity_handle: IdentityHandle,
    domain_id: u16,
    participant_qos: &QosPolicies,
  ) -> SecurityResult<PermissionsHandle> {
    let permissions_ca_certificate = participant_qos
      .get_property(QOS_PERMISSIONS_CERTIFICATE_PROPERTY_NAME)
      .and_then(|certificate_uri| {
        read_uri(&certificate_uri).map_err(|conf_err| {
          create_security_error_and_log!(
            "Failed to read the permissions certificate from {}: {:?}",
            certificate_uri,
            conf_err
          )
        })
      })
      .and_then(|certificate_contents_pem| {
        Certificate::from_pem(certificate_contents_pem)
          .map_err(|e| create_security_error_and_log!("{e:?}"))
      })?;

    let domain_rule = participant_qos
      .get_property(QOS_GOVERNANCE_DOCUMENT_PROPERTY_NAME)
      .and_then(|governance_uri| {
        read_uri(&governance_uri).map_err(|conf_err| {
          create_security_error_and_log!(
            "Failed to read the domain governance document from {}: {:?}",
            governance_uri,
            conf_err
          )
        })
      })
      .and_then(|governance_bytes| {
        SignedDocument::from_bytes(&governance_bytes)
          .map_err(SecurityError::from)
          .and_then(|signed_document| signed_document.verify_signature(&permissions_ca_certificate))
      })
      .and_then(|governance_xml| {
        DomainGovernanceDocument::from_xml(&String::from_utf8_lossy(governance_xml.as_ref()))
          .map_err(|e| create_security_error_and_log!("{e:?}"))
      })
      .and_then(|domain_governance_document| {
        domain_governance_document
          .find_rule(domain_id)
          .ok_or_else(|| {
            create_security_error_and_log!("Domain rule not found for the domain_id {}", domain_id)
          })
          .cloned()
      })?;

    let signed_permissions = participant_qos
      .get_property(QOS_PERMISSIONS_DOCUMENT_PROPERTY_NAME)
      .and_then(|permissions_uri| {
        read_uri(&permissions_uri).map_err(|conf_err| {
          create_security_error_and_log!(
            "Failed to read the domain participant permissions from {}: {:?}",
            permissions_uri,
            conf_err
          )
        })
      })?;
    let domain_participant_permissions = SignedDocument::from_bytes(&signed_permissions)
      .map_err(SecurityError::from)
      .and_then(|signed_document| signed_document.verify_signature(&permissions_ca_certificate))
      .and_then(|permissions_xml| {
        DomainParticipantPermissions::from_xml(&String::from_utf8_lossy(permissions_xml.as_ref()))
          .map_err(|e| create_security_error_and_log!("{e:?}"))
      })?;

    // Check the subject name in the identity certificate matches the one from the
    // permissions document.
    // First get the subject name from the certificate
    let subject_name: DistinguishedName = participant_qos
      .get_property(QOS_IDENTITY_CERTIFICATE_PROPERTY_NAME)
      .and_then(|certificate_uri| {
        read_uri(&certificate_uri).map_err(|conf_err| {
          create_security_error_and_log!(
            "Failed to read the identity certificate from {}: {:?}",
            certificate_uri,
            conf_err
          )
        })
      })
      .and_then(|certificate_contents_pem| {
        Certificate::from_pem(certificate_contents_pem)
          .map_err(|e| create_security_error_and_log!("{e:?}"))
      })
      .map(|cert| cert.subject_name().clone())?;

    // Then verify that we have permissions for this subject name
    if domain_participant_permissions
      .find_grant(&subject_name, &Utc::now())
      .is_none()
    {
      Err(create_security_error_and_log!(
        "The subject name '{}' from the ID certificate not found in the permissions document",
        subject_name
      ))?;
    }

    let permissions_handle = self.generate_permissions_handle();
    self.domain_rules.insert(permissions_handle, domain_rule);
    self.domain_participant_permissions.insert(
      permissions_handle,
      (subject_name, domain_participant_permissions),
    );
    self
      .signed_permissions_documents
      .insert(permissions_handle, signed_permissions);
    self
      .identity_to_permissions
      .insert(identity_handle, permissions_handle);
    self
      .permissions_ca_certificates
      .insert(permissions_handle, permissions_ca_certificate);
    Ok(permissions_handle)
  }

  fn validate_remote_permissions(
    &mut self,
    _auth_plugin: &dyn Authentication,
    local_identity_handle: IdentityHandle,
    _remote_identity_handle: IdentityHandle,
    remote_permissions_token: &PermissionsToken,
    remote_credential_token: &AuthenticatedPeerCredentialToken,
  ) -> SecurityResult<PermissionsHandle> {
    let local_permissions_handle = self.get_permissions_handle(&local_identity_handle)?;

    // Move the following check here from check_remote_ methods, as here we have
    // access to the tokens: "If the PluginClassName or the MajorVersion of the
    // local permissions_token differ from those in the
    // remote_permissions_token, the operation shall return FALSE."
    self
      .get_permissions_token(*local_permissions_handle)
      .and_then(|local_permissions_token| {
        PluginClassId::try_from(local_permissions_token.data_holder.class_id)
      })
      .and_then(|local_plugin_class_id| {
        PluginClassId::try_from(remote_permissions_token.data_holder.class_id.clone()).and_then(
          |remote_plugin_class_id| {
            local_plugin_class_id.matches_up_to_major_version(&remote_plugin_class_id)
          },
        )
      })?;

    let permissions_ca_certificate =
      self.get_permissions_ca_certificate(local_permissions_handle)?;

    let bin_prop_map = remote_credential_token
      .data_holder
      .binary_properties_as_map();

    // Extract remote identity certificate
    let remote_cert_bytes = bin_prop_map
      .get(AUTHENTICATED_PEER_TOKEN_IDENTITY_CERTIFICATE_PROPERTY_NAME)
      .map(|val| &val.value)
      .ok_or(security_error("Could not find remote_identity_certificate"))?;

    let remote_identity_certificate = Certificate::from_pem(remote_cert_bytes).map_err(|e| {
      security_error(&format!(
        "Could not parse remote identity certificate from data: {e:?}"
      ))
    })?;

    let remote_subject_name = remote_identity_certificate.subject_name();

    // Extract remote signed permissions document bytes
    let remote_permissions_bytes = bin_prop_map
      .get(AUTHENTICATED_PEER_TOKEN_PERMISSIONS_DOCUMENT_PROPERTY_NAME)
      .map(|val| &val.value)
      .ok_or(security_error(
        "Could not find remote_domain_participant_permissions",
      ))?;

    // Parse to signed document
    let signed_permissions = SignedDocument::from_bytes(remote_permissions_bytes).map_err(|e| {
      security_error(&format!(
        "Could not parse signed document from permissions data: {e:?}"
      ))
    })?;

    // Verify signature
    let permissions_xml_content = signed_permissions
      .verify_signature(permissions_ca_certificate)
      .map_err(|e| {
        security_error(&format!(
          "Could not verify signature on remote permissions document: {e}"
        ))
      })?;

    // Parse to permissions struct
    let remote_domain_participant_permissions = DomainParticipantPermissions::from_xml(
      &String::from_utf8_lossy(permissions_xml_content.as_ref()),
    )
    .map_err(|e| security_error(&format!("Could not parse permissions from XML: {e:?}")))?;

    // Check the subject name in the identity certificate matches the one from the
    // permissions document.
    if remote_domain_participant_permissions
      .find_grant(remote_subject_name, &Utc::now())
      .is_none()
    {
      Err(create_security_error_and_log!(
        "No valid grants with the subject name {:?} found",
        remote_subject_name
      ))?;
    }

    let domain_rule = self.get_domain_rule(local_permissions_handle).cloned()?;

    let permissions_handle = self.generate_permissions_handle();
    self.domain_rules.insert(permissions_handle, domain_rule);
    self.domain_participant_permissions.insert(
      permissions_handle,
      (
        remote_subject_name.clone(),
        remote_domain_participant_permissions,
      ),
    );
    Ok(permissions_handle)
  }

  fn check_create_participant(
    &self,
    permissions_handle: PermissionsHandle,
    domain_id: u16,
    _qos: &QosPolicies,
  ) -> SecurityResult<bool> {
    self.check_participant(permissions_handle, domain_id)
  }

  // Currently only mocked
  fn check_remote_participant(
    &self,
    permissions_handle: PermissionsHandle,
    domain_id: u16,
    _participant_data: Option<&SpdpDiscoveredParticipantData>,
  ) -> SecurityResult<bool> {
    // Move the following check to validate_remote_permissions from check_remote_
    // methods, as there we have access to the tokens: "If the PluginClassName
    // or the MajorVersion of the local permissions_token differ from those in
    // the remote_permissions_token, the operation shall return FALSE."

    self.check_participant(permissions_handle, domain_id)
  }

  fn get_permissions_token(&self, handle: PermissionsHandle) -> SecurityResult<PermissionsToken> {
    self
      .get_permissions_ca_certificate(&handle)
      .map(|_certificate| {
        // The CA subject name and algorithm identifier are optional properties
        // according to the spec. We do not set values for them, since this has
        // caused interoperability issues with FastDDS. In RustDDS we do not
        // use them for anything, since they don't provide any additional security.
        BuiltinPermissionsToken {
          permissions_ca_subject_name: None,
          permissions_ca_algorithm: None,
        }
        .into()
      })
  }

  fn get_permissions_credential_token(
    &self,
    handle: PermissionsHandle,
  ) -> SecurityResult<PermissionsCredentialToken> {
    self
      .get_signed_permissions_document(&handle)
      .cloned()
      .map(|permissions_document| {
        BuiltinPermissionsCredentialToken {
          permissions_document,
        }
        .into()
      })
  }

  fn set_listener(&self) -> SecurityResult<()> {
    Err(create_security_error_and_log!(
      "set_listener not supported. Use status events in DataReader/DataWriter instead."
    ))
  }

  fn get_participant_sec_attributes(
    &self,
    permissions_handle: PermissionsHandle,
  ) -> SecurityResult<ParticipantSecurityAttributes> {
    self.get_domain_rule(&permissions_handle).map(
      |DomainRule {
         allow_unauthenticated_participants,
         enable_join_access_control,
         discovery_protection_kind,
         liveliness_protection_kind,
         rtps_protection_kind,
         ..
       }| {
        let (is_rtps_protected, is_rtps_encrypted, is_rtps_origin_authenticated) =
          rtps_protection_kind.to_security_attributes_format();
        let (is_discovery_protected, is_discovery_encrypted, is_discovery_origin_authenticated) =
          discovery_protection_kind.to_security_attributes_format();
        let (is_liveliness_protected, is_liveliness_encrypted, is_liveliness_origin_authenticated) =
          liveliness_protection_kind.to_security_attributes_format();

        ParticipantSecurityAttributes {
          allow_unauthenticated_participants: *allow_unauthenticated_participants,
          is_access_protected: *enable_join_access_control,
          is_discovery_protected,
          is_liveliness_protected,
          is_rtps_protected,
          plugin_participant_attributes: BuiltinPluginParticipantSecurityAttributes {
            is_discovery_encrypted,
            is_discovery_origin_authenticated,
            is_liveliness_encrypted,
            is_liveliness_origin_authenticated,
            is_rtps_encrypted,
            is_rtps_origin_authenticated,
          }
          .into(),
          ac_participant_properties: Vec::new(),
        }
      },
    )
  }
}
