use ring::{digest, hmac};

use crate::{
  create_security_error_and_log,
  security::{
    access_control::{
      access_control_builtin::types::{
        BuiltinPluginEndpointSecurityAttributes, BuiltinPluginParticipantSecurityAttributes,
      },
      types::*,
    },
    cryptographic::cryptographic_builtin::*,
  },
};
use super::{aes_gcm_gmac::keygen, builtin_key::*, key_material::*};

impl CryptographicBuiltin {
  fn generate_crypto_handle(&mut self) -> CryptoHandle {
    self.crypto_handle_counter += 1;
    self.crypto_handle_counter
  }

  fn get_or_generate_matched_remote_endpoint_crypto_handle(
    &mut self,
    remote_participant_crypto_handle: ParticipantCryptoHandle,
    local_endpoint_crypto_handle: EndpointCryptoHandle,
  ) -> EndpointCryptoHandle {
    // If a corresponding handle exists, get and return
    if let Some(remote_endpoint_crypto_handle) = self
      .matched_remote_endpoint
      .get(&local_endpoint_crypto_handle)
      .and_then(|remote_participant_to_remote_endpoint| {
        remote_participant_to_remote_endpoint.get(&remote_participant_crypto_handle)
      })
    {
      *remote_endpoint_crypto_handle
    } else {
      // Otherwise generate a new handle
      let remote_endpoint_crypto_handle = self.generate_crypto_handle();
      // Associate it with the remote participant
      self.endpoint_to_participant.insert(
        remote_endpoint_crypto_handle,
        remote_participant_crypto_handle,
      );
      // Associate it with the local endpoint
      self
        .matched_local_endpoint
        .insert(remote_endpoint_crypto_handle, local_endpoint_crypto_handle);
      // Insert it to the HashMap corresponding to the local endpoint
      if let Some(remote_participant_to_remote_endpoint) = self
        .matched_remote_endpoint
        .get_mut(&local_endpoint_crypto_handle)
      {
        remote_participant_to_remote_endpoint.insert(
          remote_participant_crypto_handle,
          remote_endpoint_crypto_handle,
        );
      } else {
        // Create a new HashMap if one does not yet exist
        self.matched_remote_endpoint.insert(
          local_endpoint_crypto_handle,
          HashMap::from([(
            remote_participant_crypto_handle,
            remote_endpoint_crypto_handle,
          )]),
        );
      }
      // Return the generated handle
      remote_endpoint_crypto_handle
    }
  }

  fn is_volatile(properties: &[Property]) -> bool {
    properties
      .iter()
      .find(|property| {
        property
          .name
          .eq(VOLATILE_ENDPOINT_RECOGNITION_PROPERTY_NAME)
      })
      .is_some_and(|property| {
        property
          .value
          .eq(VOLATILE_WRITER_RECOGNITION_PROPERTY_VALUE)
          || property
            .value
            .eq(VOLATILE_READER_RECOGNITION_PROPERTY_VALUE)
      })
  }

  // 9.5.2.1.2
  fn derive_volatile_key_materials(
    SharedSecretHandle {
      shared_secret,
      challenge1,
      challenge2,
    }: &SharedSecretHandle,
    use_256_bit_key: bool,
  ) -> SecurityResult<KeyMaterial_AES_GCM_GMAC_seq> {
    let transformation_kind = if use_256_bit_key {
      BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_AES256_GCM
    } else {
      BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_AES128_GCM
    };

    let salt_cookie: &[u8] = b"keyexchange salt".as_ref(); // Not a typo
    let key_cookie: &[u8] = b"key exchange key".as_ref();

    let master_salt = Self::hash_shared_secret(
      [challenge1.as_ref(), salt_cookie, challenge2.as_ref()],
      shared_secret,
    );

    let master_sender_key = Self::hash_shared_secret(
      [challenge2.as_ref(), key_cookie, challenge1.as_ref()],
      shared_secret,
    );

    Ok(KeyMaterial_AES_GCM_GMAC_seq::Two(
      KeyMaterial_AES_GCM_GMAC {
        transformation_kind,
        master_salt,
        sender_key_id: CryptoTransformKeyId::ZERO, // 9.5.2.1.2
        master_sender_key,
        // Leave receiver-specific key empty
        receiver_specific_key_id: CryptoTransformKeyId::ZERO,
        master_receiver_specific_key: BuiltinKey::None,
      },
      // The volatile topic has no payload protection
      KeyMaterial_AES_GCM_GMAC {
        transformation_kind: BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_NONE,
        master_salt: BuiltinKey::None,
        sender_key_id: CryptoTransformKeyId::ZERO,
        master_sender_key: BuiltinKey::None,
        receiver_specific_key_id: CryptoTransformKeyId::ZERO,
        master_receiver_specific_key: BuiltinKey::None,
      },
    ))
  }

  // Creates a hmac key out of the challenges and cookie and uses it to hash the
  // secret according to 9.5.2.1.2
  fn hash_shared_secret(hmac_key_plain: [&[u8]; 3], shared_secret: &SharedSecret) -> BuiltinKey {
    let hmac_key = hmac::Key::new(
      hmac::HMAC_SHA256,
      digest::digest(&digest::SHA256, hmac_key_plain.concat().as_ref()).as_ref(),
    );
    let hashed_secret = hmac::sign(&hmac_key, shared_secret.as_ref());
    // from_bytes handles truncation. HMAC_SHA256 gives 256 bit output so this never
    // fails.
    BuiltinKey::from_bytes(KeyLength::AES256, hashed_secret.as_ref()).unwrap()
  }

  fn use_256_bit_key(properties: &[Property]) -> bool {
    properties
      .iter()
      .find(|property| property.name.eq("dds.sec.crypto.keysize"))
      .is_none_or(|property| !property.value.eq("128"))
  }

  fn transformation_kind(
    is_protected: bool,
    is_encrypted: bool,
    use_256_bit_key: bool,
  ) -> BuiltinCryptoTransformationKind {
    match (is_protected, is_encrypted, use_256_bit_key) {
      (false, _, _) => BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_NONE,
      (true, false, false) => {
        BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_AES128_GMAC
      }
      (true, false, true) => {
        BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_AES256_GMAC
      }
      (true, true, false) => BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_AES128_GCM,
      (true, true, true) => BuiltinCryptoTransformationKind::CRYPTO_TRANSFORMATION_KIND_AES256_GCM,
    }
  }

  fn generate_key_id(&mut self) -> CryptoTransformKeyId {
    loop {
      let candidate = CryptoTransformKeyId::random();
      if !self.used_local_key_ids.contains(&candidate) {
        return candidate;
      }
      // Else there was a collision, retry
    }
  }

  fn generate_key_material(
    &mut self,
    transformation_kind: BuiltinCryptoTransformationKind,
  ) -> KeyMaterial_AES_GCM_GMAC {
    let key_length = KeyLength::from(transformation_kind);
    KeyMaterial_AES_GCM_GMAC {
      transformation_kind,

      master_salt: BuiltinKey::generate_random(key_length),

      sender_key_id: self.generate_key_id(),
      master_sender_key: keygen(key_length),
      // Leave receiver-specific key empty initially
      receiver_specific_key_id: CryptoTransformKeyId::ZERO,
      master_receiver_specific_key: BuiltinKey::None,
    }
  }

  fn generate_receiver_specific_key(
    &mut self,
    key_materials: KeyMaterial_AES_GCM_GMAC_seq,
    origin_authentication: bool,
  ) -> KeyMaterial_AES_GCM_GMAC_seq {
    if origin_authentication {
      let master_receiver_specific_key =
        keygen(key_materials.key_material().transformation_kind.into());
      key_materials
        .add_master_receiver_specific_key(self.generate_key_id(), master_receiver_specific_key)
    } else {
      key_materials.add_master_receiver_specific_key(CryptoTransformKeyId::ZERO, BuiltinKey::None)
    }
  }

  fn unregister_endpoint(&mut self, endpoint_info: EndpointInfo) {
    let endpoint_crypto_handle = endpoint_info.crypto_handle;
    self
      .common_encode_key_materials
      .remove(&endpoint_crypto_handle);
    self
      .receiver_specific_encode_key_materials
      .remove(&endpoint_crypto_handle);
    self.decode_key_materials.remove(&endpoint_crypto_handle);
    self
      .endpoint_encrypt_options
      .remove(&endpoint_crypto_handle);
    if let Some(participant_crypto_handle) =
      self.endpoint_to_participant.remove(&endpoint_crypto_handle)
    {
      if let Some(endpoint_info_set) = self
        .participant_to_endpoint_info
        .get_mut(&participant_crypto_handle)
      {
        endpoint_info_set.remove(&endpoint_info);
      }

      // If the endpoint is remote remove the association to the corresponding local
      // endpoint
      if let Some(matched_local_endpoint_crypto_handle) =
        self.matched_local_endpoint.remove(&endpoint_crypto_handle)
      {
        if let Some(remote_participant_to_remote_endpoint) = self
          .matched_remote_endpoint
          .get_mut(&matched_local_endpoint_crypto_handle)
        {
          remote_participant_to_remote_endpoint.remove(&participant_crypto_handle);
        }
      }
      // If the endpoint is local, unregister all associated remote entities as they serve no
      // purpose on their own. TODO: should we do this or just sever the association?
      else if let Some(remote_participant_to_remote_endpoint) =
        self.matched_remote_endpoint.remove(&endpoint_crypto_handle)
      {
        for remote_endpoint_crypto_handle in remote_participant_to_remote_endpoint.values() {
          self.unregister_endpoint(EndpointInfo {
            crypto_handle: *remote_endpoint_crypto_handle,
            kind: endpoint_info.kind.opposite(),
          });
        }
      }
    }
  }
}

/// Builtin CryptoKeyFactory implementation from section 9.5.3.1 of the Security
/// specification (v. 1.1)
impl CryptoKeyFactory for CryptographicBuiltin {
  fn register_local_participant(
    &mut self,
    _participant_identity: IdentityHandle,
    _participant_permissions: PermissionsHandle,
    participant_properties: &[Property],
    participant_security_attributes: ParticipantSecurityAttributes,
  ) -> SecurityResult<ParticipantCryptoHandle> {
    //TODO: this is only a mock implementation
    let plugin_participant_security_attributes =
      BuiltinPluginParticipantSecurityAttributes::try_from(
        participant_security_attributes.plugin_participant_attributes,
      )?;
    let crypto_handle = self.generate_crypto_handle();

    let key_material = self.generate_key_material(Self::transformation_kind(
      participant_security_attributes.is_rtps_protected,
      plugin_participant_security_attributes.is_rtps_encrypted,
      Self::use_256_bit_key(participant_properties),
    ));
    self
      .insert_common_encode_key_materials(
        crypto_handle,
        CommonEncodeKeyMaterials::Some(KeyMaterial_AES_GCM_GMAC_seq::One(key_material)),
      )
      .and(self.insert_participant_attributes(crypto_handle, participant_security_attributes))
      .and(Ok(crypto_handle))
  }

  fn register_matched_remote_participant(
    &mut self,
    local_participant_crypto_handle: ParticipantCryptoHandle,
    _remote_participant_identity: IdentityHandle,
    _remote_participant_permissions: PermissionsHandle,
    _shared_secret: SharedSecretHandle,
  ) -> SecurityResult<ParticipantCryptoHandle> {
    //TODO: this is only a mock implementation

    let local_participant_key_materials = self
      .get_common_encode_key_materials(&local_participant_crypto_handle)
      .cloned()
      .and_then(
        |common_encode_key_materials| match common_encode_key_materials {
          CommonEncodeKeyMaterials::Some(value) => Ok(value),
          CommonEncodeKeyMaterials::Volatile(_) => Err(create_security_error_and_log!(
            "The local_participant_crypto_handle {} points to volatile, but a participant cannot \
             be volatile",
            local_participant_crypto_handle
          )),
        },
      )?;

    let is_rtps_origin_authenticated = self
      .participant_encrypt_options
      .get(&local_participant_crypto_handle)
      .ok_or_else(|| {
        create_security_error_and_log!(
          "Participant encrypt options not found for the ParticipantCryptoHandle {}",
          local_participant_crypto_handle
        )
      })
      .and_then(|participant_security_attributes| {
        BuiltinPluginParticipantSecurityAttributes::try_from(
          participant_security_attributes.plugin_participant_attributes,
        )
      })
      .map(|plugin_participant_attributes| {
        plugin_participant_attributes.is_rtps_origin_authenticated
      })?;

    let remote_participant_crypto_handle = self.generate_crypto_handle();

    let key_materials = self.generate_receiver_specific_key(
      local_participant_key_materials,
      is_rtps_origin_authenticated,
    );

    self.insert_receiver_specific_encode_key_materials(
      remote_participant_crypto_handle,
      key_materials,
    )?;

    Ok(remote_participant_crypto_handle)
  }

  fn register_local_datawriter(
    &mut self,
    participant_crypto: ParticipantCryptoHandle,
    datawriter_properties: &[Property],
    datawriter_security_attributes: EndpointSecurityAttributes,
  ) -> SecurityResult<DatawriterCryptoHandle> {
    //TODO: this is only a mock implementation
    let plugin_endpoint_security_attributes = BuiltinPluginEndpointSecurityAttributes::try_from(
      datawriter_security_attributes.plugin_endpoint_attributes,
    )?;

    let local_datawriter_crypto_handle = self.generate_crypto_handle();

    let use_256_bit_key = Self::use_256_bit_key(datawriter_properties);

    // The key material for volatile datawriter is derived from the shared secret in
    // register_matched_remote_datareader
    if Self::is_volatile(datawriter_properties) {
      self.insert_common_encode_key_materials(
        local_datawriter_crypto_handle,
        CommonEncodeKeyMaterials::Volatile(use_256_bit_key),
      )?;
    } else {
      let submessage_transformation_kind = Self::transformation_kind(
        datawriter_security_attributes.is_submessage_protected,
        plugin_endpoint_security_attributes.is_submessage_encrypted,
        use_256_bit_key,
      );
      let payload_transformation_kind = Self::transformation_kind(
        datawriter_security_attributes.is_payload_protected,
        plugin_endpoint_security_attributes.is_payload_encrypted,
        use_256_bit_key,
      );

      let submessage_key_material = self.generate_key_material(submessage_transformation_kind);
      // If the transformation kinds match, key reuse is possible: 9.5.3.1
      let key_materials = if submessage_transformation_kind == payload_transformation_kind
      /* && additional configurable condition? */
      {
        KeyMaterial_AES_GCM_GMAC_seq::One(submessage_key_material)
      } else {
        KeyMaterial_AES_GCM_GMAC_seq::Two(
          submessage_key_material,
          self.generate_key_material(payload_transformation_kind),
        )
      };
      self.insert_common_encode_key_materials(
        local_datawriter_crypto_handle,
        CommonEncodeKeyMaterials::Some(key_materials),
      )?;
    }

    self.insert_endpoint_attributes(
      local_datawriter_crypto_handle,
      datawriter_security_attributes,
    )?;

    self.insert_endpoint_info(
      participant_crypto,
      EndpointInfo {
        crypto_handle: local_datawriter_crypto_handle,
        kind: EndpointKind::DataWriter,
      },
    );
    self
      .endpoint_to_participant
      .insert(local_datawriter_crypto_handle, participant_crypto);

    SecurityResult::Ok(local_datawriter_crypto_handle)
  }

  fn register_matched_remote_datareader(
    &mut self,
    local_datawriter_crypto_handle: DatawriterCryptoHandle,
    remote_participant_crypto_handle: ParticipantCryptoHandle,
    shared_secret: SharedSecretHandle,
    _relay_only: bool,
  ) -> SecurityResult<DatareaderCryptoHandle> {
    //TODO: this is only a mock implementation
    let common_encode_key_materials = self
      .get_common_encode_key_materials(&local_datawriter_crypto_handle)
      .cloned()?;

    // Find a handle for the remote datareader corresponding to the (remote
    // participant, local datawriter) pair, or generate a new one
    let remote_datareader_crypto_handle = self
      .get_or_generate_matched_remote_endpoint_crypto_handle(
        remote_participant_crypto_handle,
        local_datawriter_crypto_handle,
      );

    let receiver_specific_encode_key_materials = match common_encode_key_materials {
      CommonEncodeKeyMaterials::Volatile(use_256_bit_key) => {
        let volatile_key_materials =
          Self::derive_volatile_key_materials(&shared_secret, use_256_bit_key)?;

        // Instead of sending keys over the network like in other topics, the same key
        // material is used for decoding
        self.insert_decode_key_materials(
          remote_datareader_crypto_handle,
          volatile_key_materials.clone(),
        )?;
        volatile_key_materials
      }
      CommonEncodeKeyMaterials::Some(common_encode_key_materials) => {
        let is_submessage_origin_authenticated = self
          .endpoint_encrypt_options
          .get(&local_datawriter_crypto_handle)
          .ok_or_else(|| {
            create_security_error_and_log!(
              "Datawriter encrypt options not found for the DatawriterCryptoHandle {}",
              local_datawriter_crypto_handle
            )
          })
          .and_then(|datawriter_security_attributes| {
            BuiltinPluginEndpointSecurityAttributes::try_from(
              datawriter_security_attributes.plugin_endpoint_attributes,
            )
          })
          .map(|plugin_datawriter_attributes| {
            plugin_datawriter_attributes.is_submessage_origin_authenticated
          })?;

        self.generate_receiver_specific_key(
          common_encode_key_materials,
          is_submessage_origin_authenticated,
        )
      }
    };

    self.insert_receiver_specific_encode_key_materials(
      remote_datareader_crypto_handle,
      receiver_specific_encode_key_materials,
    )?;

    // Add endpoint info
    self.insert_endpoint_info(
      remote_participant_crypto_handle,
      EndpointInfo {
        crypto_handle: remote_datareader_crypto_handle,
        kind: EndpointKind::DataReader,
      },
    );

    // Copy the attributes
    if let Some(attributes) = self
      .endpoint_encrypt_options
      .get(&local_datawriter_crypto_handle)
      .cloned()
    {
      self
        .endpoint_encrypt_options
        .insert(remote_datareader_crypto_handle, attributes);
    }

    Ok(remote_datareader_crypto_handle)
  }

  fn register_local_datareader(
    &mut self,
    participant_crypto_handle: ParticipantCryptoHandle,
    datareader_properties: &[Property],
    datareader_security_attributes: EndpointSecurityAttributes,
  ) -> SecurityResult<DatareaderCryptoHandle> {
    //TODO: this is only a mock implementation
    let plugin_endpoint_security_attributes = BuiltinPluginEndpointSecurityAttributes::try_from(
      datareader_security_attributes.plugin_endpoint_attributes,
    )?;

    let local_datareader_crypto_handle = self.generate_crypto_handle();

    let use_256_bit_key = Self::use_256_bit_key(datareader_properties);
    // The key material for volatile datareader is derived from the shared secret in
    // register_matched_remote_datawriter
    if Self::is_volatile(datareader_properties) {
      self.insert_common_encode_key_materials(
        local_datareader_crypto_handle,
        CommonEncodeKeyMaterials::Volatile(use_256_bit_key),
      )?;
    } else {
      let key_material = self.generate_key_material(Self::transformation_kind(
        datareader_security_attributes.is_submessage_protected,
        plugin_endpoint_security_attributes.is_submessage_encrypted,
        use_256_bit_key,
      ));
      self.insert_common_encode_key_materials(
        local_datareader_crypto_handle,
        CommonEncodeKeyMaterials::Some(KeyMaterial_AES_GCM_GMAC_seq::One(key_material)),
      )?;
    }
    self.insert_endpoint_attributes(
      local_datareader_crypto_handle,
      datareader_security_attributes,
    )?;

    self.insert_endpoint_info(
      participant_crypto_handle,
      EndpointInfo {
        crypto_handle: local_datareader_crypto_handle,
        kind: EndpointKind::DataReader,
      },
    );
    self
      .endpoint_to_participant
      .insert(local_datareader_crypto_handle, participant_crypto_handle);
    SecurityResult::Ok(local_datareader_crypto_handle)
  }

  fn register_matched_remote_datawriter(
    &mut self,
    local_datareader_crypto_handle: DatareaderCryptoHandle,
    remote_participant_crypto_handle: ParticipantCryptoHandle,
    shared_secret: SharedSecretHandle,
  ) -> SecurityResult<DatawriterCryptoHandle> {
    //TODO: this is only a mock implementation
    let common_encode_key_materials = self
      .get_common_encode_key_materials(&local_datareader_crypto_handle)
      .cloned()?;

    // Find a handle for the remote datawriter corresponding to the (remote
    // participant, local datareader) pair, or generate a new one
    let remote_datawriter_crypto_handle: DatareaderCryptoHandle = self
      .get_or_generate_matched_remote_endpoint_crypto_handle(
        remote_participant_crypto_handle,
        local_datareader_crypto_handle,
      );

    let receiver_specific_encode_key_materials = match common_encode_key_materials {
      CommonEncodeKeyMaterials::Volatile(use_256_bit_key) => {
        let volatile_key_materials =
          Self::derive_volatile_key_materials(&shared_secret, use_256_bit_key)?;

        // Instead of sending keys over the network like in other topics, the same key
        // material is used for decoding
        self.insert_decode_key_materials(
          remote_datawriter_crypto_handle,
          volatile_key_materials.clone(),
        )?;
        volatile_key_materials
      }
      CommonEncodeKeyMaterials::Some(common_encode_key_materials) => {
        let is_submessage_origin_authenticated = self
          .endpoint_encrypt_options
          .get(&local_datareader_crypto_handle)
          .ok_or_else(|| {
            create_security_error_and_log!(
              "Datareader encrypt options not found for the handle {}",
              local_datareader_crypto_handle
            )
          })
          .and_then(|datareader_security_attributes| {
            BuiltinPluginEndpointSecurityAttributes::try_from(
              datareader_security_attributes.plugin_endpoint_attributes,
            )
          })
          .map(|plugin_datareader_attributes| {
            plugin_datareader_attributes.is_submessage_origin_authenticated
          })?;
        self.generate_receiver_specific_key(
          common_encode_key_materials,
          is_submessage_origin_authenticated,
        )
      }
    };
    self.insert_receiver_specific_encode_key_materials(
      remote_datawriter_crypto_handle,
      receiver_specific_encode_key_materials,
    )?;

    // Add endpoint info
    self.insert_endpoint_info(
      remote_participant_crypto_handle,
      EndpointInfo {
        crypto_handle: remote_datawriter_crypto_handle,
        kind: EndpointKind::DataWriter,
      },
    );

    // Copy the attributes
    if let Some(attributes) = self
      .endpoint_encrypt_options
      .get(&local_datareader_crypto_handle)
      .cloned()
    {
      self
        .endpoint_encrypt_options
        .insert(remote_datawriter_crypto_handle, attributes);
    }

    Ok(remote_datawriter_crypto_handle)
  }

  fn unregister_participant(
    &mut self,
    participant_crypto_handle: ParticipantCryptoHandle,
  ) -> SecurityResult<()> {
    //TODO: this is only a mock implementation
    self
      .participant_encrypt_options
      .remove(&participant_crypto_handle);
    if let Some(endpoint_info_set) = self
      .participant_to_endpoint_info
      .remove(&participant_crypto_handle)
    {
      for endpoint_info in endpoint_info_set {
        self.unregister_endpoint(endpoint_info);
      }
    }
    self
      .common_encode_key_materials
      .remove(&participant_crypto_handle);
    self
      .receiver_specific_encode_key_materials
      .remove(&participant_crypto_handle);
    self.decode_key_materials.remove(&participant_crypto_handle);
    Ok(())
  }

  fn unregister_datawriter(
    &mut self,
    datawriter_crypto_handle: DatawriterCryptoHandle,
  ) -> SecurityResult<()> {
    //TODO: this is only a mock implementation
    self.unregister_endpoint(EndpointInfo {
      crypto_handle: datawriter_crypto_handle,
      kind: EndpointKind::DataWriter,
    });
    Ok(())
  }

  fn unregister_datareader(
    &mut self,
    datareader_crypto_handle: DatareaderCryptoHandle,
  ) -> SecurityResult<()> {
    //TODO: this is only a mock implementation
    self.unregister_endpoint(EndpointInfo {
      crypto_handle: datareader_crypto_handle,
      kind: EndpointKind::DataReader,
    });
    Ok(())
  }
}
