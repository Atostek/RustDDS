use std::{collections::HashMap, ops::Not};

use bytes::Bytes;
use chrono::Utc;

use crate::{
  create_security_error_and_log,
  rtps::constant::builtin_topic_names,
  security::{
    authentication::IdentityHandle,
    certificate::{Certificate, DistinguishedName},
    SecurityError, SecurityResult,
  },
};
use self::{
  domain_governance_document::{DomainRule, TopicRule},
  domain_participant_permissions_document::{Action, DomainParticipantPermissions, Grant},
  types::Entity,
};
use super::{AccessControl, PermissionsHandle};

//mod config_error; --> crate::security::config
mod domain_governance_document;
mod domain_participant_permissions_document;
//mod permissions_ca_certificate; --> crate::security::certificate
mod pkcs7_signed_data;
pub mod s_mime_config_parser;

mod local_entity_access_control;
mod participant_access_control;
mod remote_entity_access_control;
pub(in crate::security) mod types;

// A struct implementing the builtin Access control plugin
// See sections 8.4 and 9.4 of the Security specification (v. 1.1)
pub struct AccessControlBuiltin {
  domain_participant_permissions:
    HashMap<PermissionsHandle, (DistinguishedName, DomainParticipantPermissions)>,
  signed_permissions_documents: HashMap<PermissionsHandle, Bytes>,
  domain_rules: HashMap<PermissionsHandle, DomainRule>,
  permissions_ca_certificates: HashMap<PermissionsHandle, Certificate>,
  identity_to_permissions: HashMap<IdentityHandle, PermissionsHandle>,
  permissions_handle_counter: u32,
}

impl AccessControl for AccessControlBuiltin {}

impl AccessControlBuiltin {
  pub fn new() -> Self {
    Self {
      domain_participant_permissions: HashMap::new(),
      signed_permissions_documents: HashMap::new(),
      domain_rules: HashMap::new(),
      permissions_ca_certificates: HashMap::new(),
      identity_to_permissions: HashMap::new(),
      permissions_handle_counter: 0,
    }
  }

  fn generate_permissions_handle(&mut self) -> PermissionsHandle {
    self.permissions_handle_counter += 1;
    self.permissions_handle_counter
  }

  fn get_domain_rule(&self, permissions_handle: &PermissionsHandle) -> SecurityResult<&DomainRule> {
    self.domain_rules.get(permissions_handle).ok_or_else(|| {
      create_security_error_and_log!(
        "Could not find a domain rule for the PermissionsHandle {}",
        permissions_handle
      )
    })
  }

  fn get_permissions_document(
    &self,
    permissions_handle: &PermissionsHandle,
  ) -> SecurityResult<&(DistinguishedName, DomainParticipantPermissions)> {
    self
      .domain_participant_permissions
      .get(permissions_handle)
      .ok_or_else(|| {
        create_security_error_and_log!(
          "Could not find a permissions document for the PermissionsHandle {}",
          permissions_handle
        )
      })
  }

  fn get_grant(&self, permissions_handle: &PermissionsHandle) -> SecurityResult<&Grant> {
    self.get_permissions_document(permissions_handle).and_then(
      |(subject_name, permissions_document)| {
        permissions_document
          .find_grant(subject_name, &Utc::now())
          .ok_or_else(|| {
            create_security_error_and_log!(
              "Could not find a valid grant for the PermissionsHandle {}",
              permissions_handle
            )
          })
      },
    )
  }

  fn get_signed_permissions_document(
    &self,
    permissions_handle: &PermissionsHandle,
  ) -> SecurityResult<&Bytes> {
    self
      .signed_permissions_documents
      .get(permissions_handle)
      .ok_or_else(|| {
        create_security_error_and_log!(
          "Could not find a valid signed permissions document for the PermissionsHandle {}",
          permissions_handle
        )
      })
  }

  fn get_permissions_ca_certificate(
    &self,
    permissions_handle: &PermissionsHandle,
  ) -> SecurityResult<&Certificate> {
    self
      .permissions_ca_certificates
      .get(permissions_handle)
      .ok_or_else(|| {
        create_security_error_and_log!(
          "Could not find a permissions CA certificate for the PermissionsHandle {}",
          permissions_handle
        )
      })
  }

  fn get_permissions_handle(
    &self,
    identity_handle: &IdentityHandle,
  ) -> SecurityResult<&PermissionsHandle> {
    self
      .identity_to_permissions
      .get(identity_handle)
      .ok_or_else(|| {
        create_security_error_and_log!(
          "Could not find a PermissionsHandle for the IdentityHandle {}",
          identity_handle
        )
      })
  }

  // check_create_ and check_remote_ methods are very similar
  fn check_entity(
    &self,
    permissions_handle: PermissionsHandle,
    domain_id: u16,
    topic_name: &str,
    partitions: &[&str],
    data_tags: &[(&str, &str)],
    entity_kind: &Entity,
  ) -> SecurityResult<bool> {
    match topic_name {
      // Builtin entities always ok
      builtin_topic_names::DCPS_PARTICIPANT
      | builtin_topic_names::DCPS_PARTICIPANT_MESSAGE
      | builtin_topic_names::DCPS_PARTICIPANT_MESSAGE_SECURE
      | builtin_topic_names::DCPS_PARTICIPANT_SECURE
      | builtin_topic_names::DCPS_PARTICIPANT_STATELESS_MESSAGE
      | builtin_topic_names::DCPS_PARTICIPANT_VOLATILE_MESSAGE_SECURE
      | builtin_topic_names::DCPS_PUBLICATION
      | builtin_topic_names::DCPS_PUBLICATIONS_SECURE
      | builtin_topic_names::DCPS_SUBSCRIPTION
      | builtin_topic_names::DCPS_SUBSCRIPTIONS_SECURE
      | builtin_topic_names::DCPS_TOPIC => Ok(true),

      // General case
      topic_name => {
        let grant = self.get_grant(&permissions_handle)?;
        let domain_rule = self.get_domain_rule(&permissions_handle)?;

        let requested_access_is_unprotected = domain_rule
          .find_topic_rule(topic_name)
          .map(
            |TopicRule {
               enable_read_access_control,
               enable_write_access_control,
               ..
             }| match entity_kind {
              Entity::Datawriter => *enable_write_access_control,
              Entity::Datareader => *enable_read_access_control,
              Entity::Topic => *enable_read_access_control && *enable_write_access_control,
            },
          )
          .is_some_and(bool::not);

        let participant_has_write_access = grant
          .check_action(
            Action::Publish,
            domain_id,
            topic_name,
            partitions,
            data_tags,
          )
          .into();

        let participant_has_read_access = grant
          .check_action(
            Action::Subscribe,
            domain_id,
            topic_name,
            partitions,
            data_tags,
          )
          .into();

        let participant_has_requested_access = match entity_kind {
          Entity::Datawriter => participant_has_write_access,
          Entity::Datareader => participant_has_read_access,
          Entity::Topic => participant_has_write_access || participant_has_read_access,
        };

        let check_passed = requested_access_is_unprotected || participant_has_requested_access;
        Ok(check_passed)
      }
    }
  }
}
