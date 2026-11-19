use std::{
  collections::BTreeMap,
  sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
  time::Instant,
};

use chrono::Utc;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{
    participant::DomainParticipant,
    qos::HasQoSPolicy,
    statusevents::{DomainParticipantStatusEvent, LostReason, StatusChannelSender},
    topic::{Topic, TopicDescription},
  },
  rtps::{
    reader::ReaderIngredients, rtps_reader_proxy::RtpsReaderProxy,
    rtps_writer_proxy::RtpsWriterProxy,
  },
  structure::{
    duration::Duration,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, GUID},
    locator::Locator,
  },
};
use super::{
  sedp_messages::{
    topics_inconsistent, DiscoveredReaderData, DiscoveredTopicData, DiscoveredWriterData,
    ParticipantMessageData, ReaderProxy, SubscriptionBuiltinTopicData, TopicBuiltinTopicData,
    WriterProxy,
  },
  spdp_participant_data::SpdpDiscoveredParticipantData,
};
#[cfg(not(feature = "security"))]
use crate::no_security::EndpointSecurityInfo;
#[cfg(feature = "security")]
use crate::{discovery::secure_discovery::AuthenticationStatus, security::EndpointSecurityInfo};

// If remote participant does not specify lease duration, how long silence
// until we pronounce it dead.
const DEFAULT_PARTICIPANT_LEASE_DURATION: Duration = Duration::from_secs(60);

// How much longer to wait than lease duration before pronouncing lost.
const PARTICIPANT_LEASE_DURATION_TOLERANCE: Duration = Duration::from_secs(0);

// TODO: Let DiscoveryDB itself become thread-safe and support smaller-scope
// lock
pub(crate) struct DiscoveryDB {
  my_guid: GUID,
  participant_proxies: BTreeMap<GuidPrefix, SpdpDiscoveredParticipantData>,
  participant_last_life_signs: BTreeMap<GuidPrefix, Instant>,

  // Authentication statuses of participants
  #[cfg(feature = "security")]
  authentication_statuses: BTreeMap<GuidPrefix, AuthenticationStatus>,

  // local writer proxies for topics (topic name acts as key)
  local_topic_writers: BTreeMap<GUID, DiscoveredWriterData>,
  // local reader proxies for topics (topic name acts as key)
  local_topic_readers: BTreeMap<GUID, DiscoveredReaderData>,

  // remote readers and writers (via discovery)
  external_topic_readers: BTreeMap<GUID, DiscoveredReaderData>,
  external_topic_writers: BTreeMap<GUID, DiscoveredWriterData>,
  // Which locator lists of those are not what the reader or writer announced,
  // but the default locators of its participant. See update_subscription().
  // Only endpoints with such a list have an entry.
  default_locators_used: BTreeMap<GUID, DefaultLocatorsUsed>,

  // The "attic": remote readers and writers whose participant is not in
  // participant_proxies. Endpoints belong to a discovered participant (RTPS
  // 2.5 8.5.4.1, 8.5.5), so these are not matched with local endpoints.
  // - The endpoints of a participant that timed out are moved here.
  // - An endpoint whose SEDP sample Discovery processes while its participant
  //   is not known is entered here, e.g. because the participant was disposed
  //   or timed out after the sample was received.
  // They are kept because SEDP does not deliver them a second time. When the
  // participant is discovered (again), Discovery takes them out with
  // take_endpoints_from_attic() and processes them as new SEDP data. A dispose
  // of the participant, or of the endpoint, drops them.
  // The attic has the locators that the endpoints announced, without default
  // locators of the participant: it may return with other ones.
  external_topic_readers_attic: BTreeMap<GUID, DiscoveredReaderData>,
  external_topic_writers_attic: BTreeMap<GUID, DiscoveredWriterData>,

  // Database of topic updates:
  // Outer level key is topic name
  // Inner key is topic data sender.
  topics: BTreeMap<String, BTreeMap<GUID, (DiscoveredVia, DiscoveredTopicData)>>,

  // sender for notifying (potential) waiters in participant.find_topic() call
  topic_updated_sender: std::sync::mpsc::SyncSender<()>,

  participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
}

// Whether a remote reader or writer announced no unicast locators, or no
// multicast locators, so that it has the default ones of its participant
#[derive(Clone, Copy)]
struct DefaultLocatorsUsed {
  unicast: bool,
  multicast: bool,
}

// How did we discover this topic
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(crate) enum DiscoveredVia {
  Topic,        // explicitly, via the topic topic (does this actually occur?)
  Publication,  // we discovered there is a writer on this topic
  Subscription, // we discovered a reader on this topic
  SelfDefined,  // not discovered, but defined by the local DomainParticipant
}

fn move_by_guid_prefix<D>(
  guid_prefix: GuidPrefix,
  from: &mut BTreeMap<GUID, D>,
  to: &mut BTreeMap<GUID, D>,
) {
  let to_move: Vec<GUID> = from.range(guid_prefix.range()).map(|(g, _)| *g).collect();
  for guid in to_move {
    from.remove(&guid).map(|d| to.insert(guid, d));
  }
}

fn remove_by_guid_prefix<D>(guid_prefix: GuidPrefix, from: &mut BTreeMap<GUID, D>) -> Vec<D> {
  let to_remove: Vec<GUID> = from.range(guid_prefix.range()).map(|(g, _)| *g).collect();
  to_remove
    .into_iter()
    .filter_map(|guid| from.remove(&guid))
    .collect()
}

pub(crate) fn discovery_db_read(
  discovery_db: &Arc<RwLock<DiscoveryDB>>,
) -> RwLockReadGuard<'_, DiscoveryDB> {
  match discovery_db.read() {
    Ok(db) => db,
    Err(e) => panic!("RustDDS internal bug: DiscoveryDB is poisoned after a prior panic: {e:?}."),
  }
}

pub(crate) fn discovery_db_write(
  discovery_db: &Arc<RwLock<DiscoveryDB>>,
) -> RwLockWriteGuard<'_, DiscoveryDB> {
  match discovery_db.write() {
    Ok(db) => db,
    Err(e) => panic!("RustDDS internal bug: DiscoveryDB is poisoned after a prior panic: {e:?}."),
  }
}

impl DiscoveryDB {
  pub fn new(
    my_guid: GUID,
    topic_updated_sender: std::sync::mpsc::SyncSender<()>,
    participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
  ) -> Self {
    Self {
      my_guid,
      participant_proxies: BTreeMap::new(),
      participant_last_life_signs: BTreeMap::new(),
      #[cfg(feature = "security")]
      authentication_statuses: BTreeMap::new(),
      local_topic_writers: BTreeMap::new(),
      local_topic_readers: BTreeMap::new(),
      external_topic_readers: BTreeMap::new(),
      external_topic_writers: BTreeMap::new(),
      default_locators_used: BTreeMap::new(),
      external_topic_readers_attic: BTreeMap::new(),
      external_topic_writers_attic: BTreeMap::new(),
      topics: BTreeMap::new(),
      topic_updated_sender,
      participant_status_sender,
    }
  }

  fn send_participant_status(&self, event: DomainParticipantStatusEvent) {
    self
      .participant_status_sender
      .try_send(event)
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));
  }

  // Returns if participant was previously unknown
  pub fn update_participant(&mut self, data: &SpdpDiscoveredParticipantData) -> bool {
    debug!("update_participant: {:?}", data);
    let guid = data.participant_guid;

    // sanity check
    if guid.entity_id != EntityId::PARTICIPANT {
      error!("Discovered participant GUID entity_id is not for participant: {guid:?}");
      // Maybe we should discard the participant here?
      return false;
    }

    // We allow discovery to discover self, since our discovery readers
    // will receive our own announcements via broadcast. If we do not recognize
    // our own participant, there is confusion about unknown writers on the
    // discovery topics.
    //
    // if guid == self.my_guid {
    //   debug!("DiscoveryDB discovered self. Skipping.");
    //   return
    // }

    let mut new_participant = false;
    if !self.participant_proxies.contains_key(&guid.prefix) {
      info!("New remote participant: {:?}", data);
      new_participant = true;
      if guid == self.my_guid {
        info!("Remote participant {guid:?} is myself, but some reflection is good.");
        new_participant = false;
      }

      // This may be a rediscovery: Participant lease time timed out, but then
      // came back, e.g. by network outage. Its readers/writers are still in the
      // attic. The caller gets them with take_endpoints_from_attic() and
      // processes them as new SEDP data, so that they are matched again.
    }

    // In any case:
    // Update SpdpDiscoveredParticipantData to DB
    self.participant_proxies.insert(guid.prefix, data.clone());
    // Timestamp last life sign fro participant
    self
      .participant_last_life_signs
      .insert(guid.prefix, Instant::now());

    new_participant
  }

  pub fn participant_is_alive(&mut self, guid_prefix: GuidPrefix) {
    if let Some(ts) = self.participant_last_life_signs.get_mut(&guid_prefix) {
      let now = Instant::now();
      if now.duration_since(*ts) > std::time::Duration::from_secs(1) {
        debug!("Participant alive update for {guid_prefix:?}, but no full update.");
      }
      *ts = now;
    } else {
      info!(
        "Participant alive update for unknown {guid_prefix:?}. This is normal, if the message \
         does not repeat."
      );
    }
  }

  // active_disposal means that we received a discovery message announcing the
  // disposal of the participant. active_disposal=false means that the
  // participant timed out.
  pub fn remove_participant(&mut self, guid_prefix: GuidPrefix, active_disposal: bool) {
    info!("removing participant {guid_prefix:?}");
    self.participant_proxies.remove(&guid_prefix);
    self.participant_last_life_signs.remove(&guid_prefix);
    #[cfg(feature = "security")]
    self.authentication_statuses.remove(&guid_prefix);

    if active_disposal {
      self.remove_topic_reader_with_prefix(guid_prefix);
      self.remove_topic_writer_with_prefix(guid_prefix);
      // The participant does not come back, so neither do the endpoints it has
      // in the attic.
      remove_by_guid_prefix(guid_prefix, &mut self.external_topic_readers_attic);
      remove_by_guid_prefix(guid_prefix, &mut self.external_topic_writers_attic);
    } else {
      // move to attic
      move_by_guid_prefix(
        guid_prefix,
        &mut self.external_topic_readers,
        &mut self.external_topic_readers_attic,
      );
      move_by_guid_prefix(
        guid_prefix,
        &mut self.external_topic_writers,
        &mut self.external_topic_writers_attic,
      );
      // The default locators of this participant stay behind. If it returns,
      // its endpoints get the ones it announces then.
      for (guid, used) in self.default_locators_used.range(guid_prefix.range()) {
        let locator_lists = match (
          self.external_topic_readers_attic.get_mut(guid),
          self.external_topic_writers_attic.get_mut(guid),
        ) {
          (Some(reader), _) => (
            &mut reader.reader_proxy.unicast_locator_list,
            &mut reader.reader_proxy.multicast_locator_list,
          ),
          (None, Some(writer)) => (
            &mut writer.writer_proxy.unicast_locator_list,
            &mut writer.writer_proxy.multicast_locator_list,
          ),
          (None, None) => continue,
        };
        if used.unicast {
          locator_lists.0.clear();
        }
        if used.multicast {
          locator_lists.1.clear();
        }
      }
    }
    remove_by_guid_prefix(guid_prefix, &mut self.default_locators_used);
  }

  pub fn find_participant_proxy(
    &self,
    guid_prefix: GuidPrefix,
  ) -> Option<&SpdpDiscoveredParticipantData> {
    self.participant_proxies.get(&guid_prefix)
  }

  fn remove_topic_reader_with_prefix(&mut self, guid_prefix: GuidPrefix) {
    // TODO: Implement this using .drain_filter() in BTreeMap once it lands in
    // stable.
    let to_remove: Vec<GUID> = self
      .external_topic_readers
      .range(guid_prefix.range())
      .map(|(g, _)| *g)
      .collect();
    for guid in to_remove {
      self.external_topic_readers.remove(&guid);
    }
  }

  pub fn remove_topic_reader(&mut self, guid: GUID) {
    info!("remove_topic_reader {guid:?}");
    self.external_topic_readers.remove(&guid);
    self.default_locators_used.remove(&guid);
    self.external_topic_readers_attic.remove(&guid);
  }

  #[cfg(feature = "security")]
  pub fn get_topic_reader(&self, guid: &GUID) -> Option<&DiscoveredReaderData> {
    self.external_topic_readers.get(guid)
  }

  #[cfg(feature = "security")]
  pub fn get_topic_writer(&self, guid: &GUID) -> Option<&DiscoveredWriterData> {
    self.external_topic_writers.get(guid)
  }

  // A remote reader or writer that SEDP may dispose: one of those above, or
  // one in the attic, which remove_topic_reader() and remove_topic_writer()
  // drop too.
  #[cfg(feature = "security")]
  pub fn get_disposable_topic_reader(&self, guid: &GUID) -> Option<&DiscoveredReaderData> {
    self
      .external_topic_readers
      .get(guid)
      .or_else(|| self.external_topic_readers_attic.get(guid))
  }

  #[cfg(feature = "security")]
  pub fn get_disposable_topic_writer(&self, guid: &GUID) -> Option<&DiscoveredWriterData> {
    self
      .external_topic_writers
      .get(guid)
      .or_else(|| self.external_topic_writers_attic.get(guid))
  }

  fn remove_topic_writer_with_prefix(&mut self, guid_prefix: GuidPrefix) {
    // TODO: Implement this using .drain_filter() in BTreeMap once it lands in
    // stable.
    let to_remove: Vec<GUID> = self
      .external_topic_writers
      .range(guid_prefix.range())
      .map(|(g, _)| *g)
      .collect();
    for guid in to_remove {
      self.external_topic_writers.remove(&guid);
    }
  }

  pub fn remove_topic_writer(&mut self, guid: GUID) {
    self.external_topic_writers.remove(&guid);
    self.default_locators_used.remove(&guid);
    self.external_topic_writers_attic.remove(&guid);
  }

  // Delete participant proxies, if we have not heard of them within
  // lease_duration
  pub fn participant_cleanup(&mut self) -> Vec<(GuidPrefix, LostReason)> {
    let inow = Instant::now();

    let mut to_remove = Vec::new();
    // TODO: We are not cleaning up liast_life_signs table, but that should not be a
    // problem, except for a slight memory leak.
    for (&guid, sp) in &self.participant_proxies {
      let lease_duration = sp
        .lease_duration
        .unwrap_or(DEFAULT_PARTICIPANT_LEASE_DURATION);
      // let lease_duration = lease_duration + lease_duration; // double it
      match self.participant_last_life_signs.get(&guid) {
        Some(&last_life) => {
          // keep, if duration not exceeded
          let elapsed = Duration::from_std(inow.duration_since(last_life));
          if elapsed <= lease_duration + PARTICIPANT_LEASE_DURATION_TOLERANCE {
            // No timeout yet, we keep this, so do nothing.
          } else {
            info!(
              "participant cleanup - deleting participant proxy {guid:?}. lease_duration = \
               {lease_duration:?} elapsed = {elapsed:?}"
            );
            to_remove.push((
              guid,
              LostReason::Timeout {
                lease: lease_duration,
                elapsed,
              },
            ));
          }
        }
        None => {
          error!("Participant {guid:?} not in last_life_signs table?");
        }
      } // match
    } // for

    for (guid, _) in &to_remove {
      self.remove_participant(*guid, false); // false = removed due to timeout
    }

    to_remove
  }

  fn topic_has_writers_or_readers(&self, topic_name: &str) -> bool {
    // TODO: This entire function has silly implementation.
    // We should really have a separate map from Topic to Readers & Writers
    if self
      .local_topic_readers
      .iter()
      .any(|(_, p)| p.subscription_topic_data.topic_name() == topic_name)
    {
      return true;
    }

    if self
      .local_topic_writers
      .iter()
      .any(|(_, p)| p.publication_topic_data.topic_name == topic_name)
    {
      return true;
    }

    if self
      .external_topic_readers
      .values()
      .any(|p| p.subscription_topic_data.topic_name() == topic_name)
    {
      return true;
    }

    if self
      .external_topic_writers
      .values()
      .any(|p| p.publication_topic_data.topic_name == topic_name)
    {
      return true;
    }

    false
  }

  pub fn topic_cleanup(&mut self) {
    // removing topics that have no readers or writers
    let dead_topics: Vec<_> = self
      .topics
      .keys()
      .filter(|tn| !self.topic_has_writers_or_readers(tn))
      .cloned()
      .collect();
    for dt in &dead_topics {
      self.topics.remove(dt);
    }
  }

  pub fn update_local_topic_writer(&mut self, writer: DiscoveredWriterData) {
    self
      .local_topic_writers
      .insert(writer.writer_proxy.remote_writer_guid, writer);
  }

  pub fn remove_local_topic_writer(&mut self, guid: GUID) {
    self.local_topic_writers.remove(&guid);
  }

  // TODO: This is silly. Returns one of the parameters cloned, or None
  // TODO: Why are we here checking if discovery db already has this? What about
  // reader proxies in writers?

  // Here we make updates when a new Reader is found via Discovery.
  // * It may be one of our own.
  // * We may have seen it before.
  //
  // In case DiscoveredReaderData does not have locators, we try to get
  // them from the remote participant.
  //
  // The topic is updated to the topics table.
  //
  // If the remote participant is not known, the reader is kept in the attic
  // until the participant is discovered, and None is returned: it must not be
  // matched yet. See take_endpoints_from_attic().
  pub fn update_subscription(
    &mut self,
    data: &DiscoveredReaderData,
  ) -> Option<DiscoveredReaderData> {
    let guid = data.reader_proxy.remote_reader_guid;

    // fill in the default locators from participant, in case DRD did not provide
    // any
    let Some(default_locator_lists) = self.find_participant_proxy(guid.prefix).map(|pp| {
      debug!("Added participant locators to Reader {guid:?}");
      (
        pp.default_unicast_locators.clone(),
        pp.default_multicast_locators.clone(),
      )
    }) else {
      // The participant may not be known yet, or it may be gone already. A
      // reader matched now would then never be removed again.
      debug!(
        "No remote participant known for Reader {guid:?}. Keeping it until the participant is \
         discovered."
      );
      self.external_topic_readers_attic.insert(guid, data.clone());
      return None;
    };
    // Newer data than any in the attic
    self.external_topic_readers_attic.remove(&guid);
    self.note_default_locators_used(
      guid,
      &data.reader_proxy.unicast_locator_list,
      &data.reader_proxy.multicast_locator_list,
    );

    // Build enriched data with participant's default locators filled in, so that
    // later lookups via readers_on_topic() return usable locator information.
    let enriched = DiscoveredReaderData {
      reader_proxy: ReaderProxy::from(RtpsReaderProxy::from_discovered_reader_data(
        data,
        &default_locator_lists.0,
        &default_locator_lists.1,
      )),
      ..data.clone()
    };

    self.external_topic_readers.insert(guid, enriched.clone());
    debug!("External reader: {enriched:?}");

    // Now the topic update:
    let dtd = data.subscription_topic_data.to_topic_data();
    self.update_topic_data(
      &DiscoveredTopicData::new(Utc::now(), dtd),
      guid,
      DiscoveredVia::Subscription,
    );

    // TODO: Lookup the topic in DB, data sent by the same participant that sent the
    // reader update. If there is a DiscoveredVia::Topic record, use QosPolicies
    // from that record and modify by QoS given in the DRD.

    Some(enriched)
  }

  // TODO: This is silly. Returns one of the parameters cloned, or None
  //
  // As update_subscription(), a writer of an unknown participant is kept in the
  // attic until the participant is discovered, and None is returned.
  pub fn update_publication(
    &mut self,
    data: &DiscoveredWriterData,
  ) -> Option<DiscoveredWriterData> {
    let guid = data.writer_proxy.remote_writer_guid;

    // fill in the default locators from participant, in case DRD did not provide
    // any
    let Some(default_locator_lists) = self.find_participant_proxy(guid.prefix).map(|pp| {
      debug!("Added participant locators to Writer {guid:?}");
      (
        pp.default_unicast_locators.clone(),
        pp.default_multicast_locators.clone(),
      )
    }) else {
      debug!(
        "No remote participant known for Writer {guid:?}. Keeping it until the participant is \
         discovered."
      );
      self.external_topic_writers_attic.insert(guid, data.clone());
      return None;
    };
    // Newer data than any in the attic
    self.external_topic_writers_attic.remove(&guid);
    self.note_default_locators_used(
      guid,
      &data.writer_proxy.unicast_locator_list,
      &data.writer_proxy.multicast_locator_list,
    );

    // Build enriched data with participant's default locators filled in, so that
    // later lookups via writers_on_topic() return usable locator information.
    let enriched = DiscoveredWriterData {
      writer_proxy: WriterProxy::from(RtpsWriterProxy::from_discovered_writer_data(
        data,
        &default_locator_lists.0,
        &default_locator_lists.1,
      )),
      ..data.clone()
    };

    self.external_topic_writers.insert(guid, enriched.clone());
    debug!("External writer: {enriched:?}");

    // Now the topic update:
    let dtd = data.publication_topic_data.to_topic_data();
    self.update_topic_data(
      &DiscoveredTopicData::new(Utc::now(), dtd),
      guid,
      DiscoveredVia::Publication,
    );

    Some(enriched)
  }

  // Remembers which locator lists of a remote reader or writer, as announced,
  // are empty, so that the endpoint gets the default locators of its
  // participant.
  fn note_default_locators_used(
    &mut self,
    guid: GUID,
    announced_unicast: &[Locator],
    announced_multicast: &[Locator],
  ) {
    let used = DefaultLocatorsUsed {
      unicast: announced_unicast.is_empty(),
      multicast: announced_multicast.is_empty(),
    };
    if used.unicast || used.multicast {
      self.default_locators_used.insert(guid, used);
    } else {
      self.default_locators_used.remove(&guid);
    }
  }

  // Takes the readers and writers of this participant out of the attic. Call
  // this once the participant is in the DB, and process them as SEDP data
  // that arrives now: update_subscription() and update_publication() enter
  // them and return them, so that they can be matched with local endpoints.
  // With security, they have to be checked again before that.
  pub fn take_endpoints_from_attic(
    &mut self,
    guid_prefix: GuidPrefix,
  ) -> (Vec<DiscoveredReaderData>, Vec<DiscoveredWriterData>) {
    if self.find_participant_proxy(guid_prefix).is_none() {
      return (Vec::new(), Vec::new());
    }
    (
      remove_by_guid_prefix(guid_prefix, &mut self.external_topic_readers_attic),
      remove_by_guid_prefix(guid_prefix, &mut self.external_topic_writers_attic),
    )
  }

  // This is for local participant updating the topic table
  pub fn update_topic_data_p(&mut self, topic: &Topic) {
    let topic_data = DiscoveredTopicData::new(
      Utc::now(),
      TopicBuiltinTopicData::new(
        None,
        topic.name(),
        topic.get_type().name().to_owned(),
        &topic.qos(),
      ),
    );
    self.update_topic_data(&topic_data, self.my_guid, DiscoveredVia::SelfDefined);
  }

  // Topic update sends notifications, in case someone was waiting to find a
  // topic. Return value indicates whether the topic (name) was new to us. This
  // is used to add
  pub fn update_topic_data(
    &mut self,
    dtd: &DiscoveredTopicData,
    updater: GUID,
    discovered_via: DiscoveredVia,
  ) {
    trace!("Update topic data: {:?}", dtd);
    let topic_name = dtd.topic_data.name.clone();
    let mut notify = false;
    let mut inconsistency_event_to_send = None;

    if let Some(t) = self.topics.get_mut(&dtd.topic_data.name) {
      if let Some(old_dtd) = t.get_mut(&updater) {
        // already have it from the same source, do some checking(?) and merging
        if !topics_inconsistent(&dtd.topic_data, &old_dtd.1.topic_data) {
          // If this discovery was from Topic topic and the old was not, then update
          // TODO: Why do we have this logic? Where is the spec? Or ant reason for it?
          // Is it even triggered ever?
          if discovered_via == DiscoveredVia::Topic {
            *old_dtd = (discovered_via, dtd.clone()); // update QoS
            notify = true;
          } else {
            debug!(
              "Topic {:?} update ignored from {:?}. Already have this.",
              topic_name, updater
            );
            // TODO: Here we could warn about QoS changes.
            //
            // It is normal to have different (but compatible) QoS policies
            // for Reader and Writer. We end up here if the same Participant has
            // both Reader and Writer with different QoS. Also
            // different Readers could have different QoS,
            // as could Writers.
          }
        } else {
          error!(
            "Inconsistent topic update from {:?}: type was: {:?} new type: {:?}",
            updater, old_dtd.1.topic_data.type_name, dtd.topic_data.type_name,
          );
          inconsistency_event_to_send = Some(DomainParticipantStatusEvent::InconsistentTopic {
            previous_topic_data: Box::new((&old_dtd.1.topic_data).into()),
            previous_source: updater,
            discovered_topic_data: Box::new((&dtd.topic_data).into()),
            discovery_source: updater,
          });
        }
      } else {
        for (source_guid, (_via, existing_dtd)) in t.iter() {
          if topics_inconsistent(&existing_dtd.topic_data, &dtd.topic_data) {
            inconsistency_event_to_send = Some(DomainParticipantStatusEvent::InconsistentTopic {
              previous_topic_data: Box::new((&existing_dtd.topic_data).into()),
              previous_source: *source_guid,
              discovered_topic_data: Box::new((&dtd.topic_data).into()),
              discovery_source: updater,
            });
            // Note: There are potentially several sources of inconsistency
            // (loop), but we will report only one of them.
          }
        }
        // We have to topic, but not from this participant
        // TODO: Check that there is agreement about topic type name (at least)
        t.insert(updater, (discovered_via, dtd.clone())); // this should return None
        notify = true;
      }
    } else {
      // new topic to us
      let mut b = BTreeMap::new();
      b.insert(updater, (discovered_via, dtd.clone()));
      self.topics.insert(topic_name, b);
      self.send_participant_status(DomainParticipantStatusEvent::TopicDetected {
        name: dtd.topic_data.name.clone(),
        type_name: dtd.topic_data.type_name.clone(),
      });
    };
    if let Some(ev) = inconsistency_event_to_send {
      self.send_participant_status(ev);
    }
    if notify {
      self
        .topic_updated_sender
        .try_send(())
        // It is quite normal for this to fail due to channel full,
        // because usually there is no-one at the other end receiving.
        .unwrap_or_else(|e| trace!("update_topic_data: Notification send failed: {e:?}"));
    }
  }

  // local topic readers

  /// Builds the discovery record of a local reader.
  ///
  /// This locks the participant (`dpi`, for its locators and GUID), so it is
  /// a separate step from [`Self::update_local_topic_reader`]: build the record
  /// first, then take the Discovery DB lock and insert it. The two locks are
  /// not held together. Calling this while holding `discovery_db` could
  /// deadlock against `DomainParticipant::find_topic()`, which holds `dpi`
  /// while reading the DB.
  pub fn local_topic_reader_data(
    domain_participant: &DomainParticipant,
    topic: &Topic,
    reader: &ReaderIngredients,
    sec_info_opt: Option<EndpointSecurityInfo>,
  ) -> DiscoveredReaderData {
    let reader_guid = reader.guid;

    let reader_proxy = RtpsReaderProxy::from_reader(reader, domain_participant);

    let subscription_data = SubscriptionBuiltinTopicData::new(
      reader_guid,
      Some(domain_participant.guid()),
      topic.name(),
      topic.get_type().name().to_string(),
      &reader.qos_policy,
      sec_info_opt,
    );

    // TODO: possibly change content filter to dynamic value
    let content_filter = None;

    DiscoveredReaderData {
      reader_proxy: ReaderProxy::from(reader_proxy),
      subscription_topic_data: subscription_data,
      content_filter,
      user_data: Vec::new(),
    }
  }

  /// Inserts a record built by [`Self::local_topic_reader_data`].
  pub fn update_local_topic_reader(&mut self, discovered_reader_data: DiscoveredReaderData) {
    self.local_topic_readers.insert(
      discovered_reader_data.reader_proxy.remote_reader_guid,
      discovered_reader_data,
    );
  }

  pub fn remove_local_topic_reader(&mut self, guid: GUID) {
    self.local_topic_readers.remove(&guid);
  }

  pub fn get_local_topic_reader(&self, guid: GUID) -> Option<&DiscoveredReaderData> {
    self.local_topic_readers.get(&guid)
  }

  pub fn get_local_topic_writer(&self, guid: GUID) -> Option<&DiscoveredWriterData> {
    self.local_topic_writers.get(&guid)
  }

  pub fn get_all_local_topic_readers(&self) -> impl Iterator<Item = &DiscoveredReaderData> {
    self.local_topic_readers.values()
  }

  pub fn get_all_local_topic_writers(&self) -> impl Iterator<Item = &DiscoveredWriterData> {
    self.local_topic_writers.values()
  }

  pub fn get_all_external_topic_readers(&self) -> impl Iterator<Item = &DiscoveredReaderData> {
    self.external_topic_readers.values()
  }

  pub fn get_all_external_topic_writers(&self) -> impl Iterator<Item = &DiscoveredWriterData> {
    self.external_topic_writers.values()
  }

  // Note:
  // If multiple participants announce the same topic, this will
  // return duplicates, one per announcing participant.
  // The duplicates are not necessarily identical, but may have different QoS.
  pub fn all_user_topics(&self) -> impl Iterator<Item = &DiscoveredTopicData> {
    self
      .topics
      .iter()
      .filter(|(s, _)| !s.starts_with("DCPS"))
      .flat_map(|(_, gm)| gm.values().map(|dtd| &dtd.1))
  }

  // a Topic may have multiple definitions, because there may be multiple
  // participants publishing the topic information.
  // At least the QoS details may be different.
  // This just returns the first one found in the database, which is indexed by
  // GUID.
  pub fn get_topic(&self, topic_name: &str) -> Option<&DiscoveredTopicData> {
    self
      .topics
      .get(topic_name)
      .and_then(|m| m.values().next().map(|t| &t.1))
  }

  pub fn writers_on_topic_and_participant(
    &self,
    topic_name: &str,
    participant: GuidPrefix,
  ) -> Vec<DiscoveredWriterData> {
    let on_participant = self
      .external_topic_writers
      .range(participant.range())
      .map(|(_guid, dwd)| dwd);
    debug!("Writers on participant {participant:?} are {on_participant:?}");
    on_participant
      .filter(|dwd| dwd.publication_topic_data.topic_name == topic_name)
      .cloned()
      .collect()
  }

  pub fn readers_on_topic_and_participant(
    &self,
    topic_name: &str,
    participant: GuidPrefix,
  ) -> Vec<DiscoveredReaderData> {
    self
      .external_topic_readers
      .range(participant.range())
      .map(|(_guid, drd)| drd)
      .filter(|drd| drd.subscription_topic_data.topic_name() == topic_name)
      .cloned()
      .collect()
  }

  pub fn writers_on_topic(&self, topic_name: &str) -> Vec<&DiscoveredWriterData> {
    // Get external & local writers on the topic
    let exernal_writers = self
      .external_topic_writers
      .values()
      .filter(|wd| wd.publication_topic_data.topic_name() == topic_name);
    let local_writers = self
      .local_topic_writers
      .values()
      .filter(|wd| wd.publication_topic_data.topic_name() == topic_name);
    // Combine & return
    let combined: Vec<&DiscoveredWriterData> = exernal_writers.chain(local_writers).collect();
    combined
  }

  pub fn readers_on_topic(&self, topic_name: &str) -> Vec<&DiscoveredReaderData> {
    // Get external & local readers on the topic
    let exernal_readers = self
      .external_topic_readers
      .values()
      .filter(|rd| rd.subscription_topic_data.topic_name() == topic_name);
    let local_readers = self
      .local_topic_readers
      .values()
      .filter(|rd| rd.subscription_topic_data.topic_name() == topic_name);
    // Combine & return
    let combined: Vec<&DiscoveredReaderData> = exernal_readers.chain(local_readers).collect();
    combined
  }

  // // TODO: return iterator somehow?
  #[cfg(test)] // used only for testing
  pub fn get_local_topic_readers<T: TopicDescription>(
    &self,
    topic: &T,
  ) -> Vec<&DiscoveredReaderData> {
    let topic_name = topic.name();
    self
      .local_topic_readers
      .iter()
      .filter(|(_, p)| *p.subscription_topic_data.topic_name() == topic_name)
      .map(|(_, p)| p)
      .collect()
  }

  pub fn update_lease_duration(&mut self, data: &ParticipantMessageData) {
    let now = Instant::now();
    let prefix = data.guid;
    self
      .external_topic_writers
      .range_mut(prefix.range())
      .for_each(|(_guid, p)| p.last_updated = now);
  }

  #[cfg(feature = "security")]
  pub fn get_authentication_status(&self, guid_prefix: GuidPrefix) -> Option<AuthenticationStatus> {
    self.authentication_statuses.get(&guid_prefix).copied()
  }

  #[cfg(feature = "security")]
  pub fn update_authentication_status(
    &mut self,
    guid_prefix: GuidPrefix,
    status: AuthenticationStatus,
  ) {
    self.authentication_statuses.insert(guid_prefix, status);
  }
}

#[cfg(test)]
mod tests {
  use std::{sync::Mutex, time::Duration as StdDuration};

  use byteorder::LittleEndian;

  use super::*;
  use crate::{
    dds::{
      qos::QosPolicies,
      statusevents::{sync_status_channel, DataReaderStatus},
      topic::TopicKind,
      with_key::simpledatareader::ReaderCommand,
    },
    mio_source,
    polling::{wake_channel, LoopWaker},
    serialization::CDRSerializerAdapter,
    structure::guid::*,
    test::{
      random_data::RandomData,
      test_data::{reader_proxy_data, spdp_participant_data, subscription_builtin_topic_data},
      test_domain::test_domain_id,
    },
  };

  // SPDP data of a remote participant with this GUID prefix
  fn participant_data(guid_prefix: GuidPrefix) -> SpdpDiscoveredParticipantData {
    let mut data = spdp_participant_data().unwrap();
    data.participant_guid = GUID::new(guid_prefix, EntityId::PARTICIPANT);
    data
  }

  // An empty DiscoveryDB of some local participant
  fn empty_db() -> DiscoveryDB {
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      std::sync::mpsc::sync_channel::<()>(4);
    let (status_sender, _status_receiver) = sync_status_channel(16).unwrap();
    DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      status_sender,
    )
  }

  // SEDP data of a reader of the participant with this GUID prefix
  fn reader_of(guid_prefix: GuidPrefix) -> DiscoveredReaderData {
    let guid = GUID::new(
      guid_prefix,
      EntityId::new([0, 0, 1], EntityKind::READER_WITH_KEY_USER_DEFINED),
    );
    DiscoveredReaderData {
      reader_proxy: ReaderProxy::new(guid, false, vec![], vec![]),
      subscription_topic_data: SubscriptionBuiltinTopicData::new(
        guid,
        None,
        "topic".to_string(),
        "type".to_string(),
        &QosPolicies::qos_none(),
        None,
      ),
      content_filter: None,
      user_data: Vec::new(),
    }
  }

  // SEDP data of a writer of the participant with this GUID prefix
  fn writer_of(guid_prefix: GuidPrefix) -> DiscoveredWriterData {
    use crate::discovery::sedp_messages::PublicationBuiltinTopicData;

    let guid = GUID::new(
      guid_prefix,
      EntityId::new([0, 0, 2], EntityKind::WRITER_WITH_KEY_USER_DEFINED),
    );
    DiscoveredWriterData {
      last_updated: Instant::now(),
      writer_proxy: WriterProxy::new(guid, vec![], vec![]),
      publication_topic_data: PublicationBuiltinTopicData::new(
        guid,
        None,
        "topic".to_string(),
        "type".to_string(),
        None,
      ),
      user_data: Vec::new(),
    }
  }

  // What Discovery does with the endpoints that a participant has in the
  // attic when it is discovered: they are entered as new SEDP data. Returns the
  // entered ones, which Discovery announces to the local endpoints.
  fn enter_endpoints_from_attic(
    db: &mut DiscoveryDB,
    guid_prefix: GuidPrefix,
  ) -> (Vec<DiscoveredReaderData>, Vec<DiscoveredWriterData>) {
    let (readers, writers) = db.take_endpoints_from_attic(guid_prefix);
    (
      readers
        .iter()
        .filter_map(|r| db.update_subscription(r))
        .collect(),
      writers
        .iter()
        .filter_map(|w| db.update_publication(w))
        .collect(),
    )
  }

  // Is the reader, or the writer, among the external ones of the DB?
  fn reader_known(db: &DiscoveryDB, reader: &DiscoveredReaderData) -> bool {
    db.get_all_external_topic_readers()
      .any(|r| r.reader_proxy.remote_reader_guid == reader.reader_proxy.remote_reader_guid)
  }
  fn writer_known(db: &DiscoveryDB, writer: &DiscoveredWriterData) -> bool {
    db.get_all_external_topic_writers()
      .any(|w| w.writer_proxy.remote_writer_guid == writer.writer_proxy.remote_writer_guid)
  }

  // An endpoint that SEDP announces while its participant is not known, i.e.
  // before the participant's SPDP data or after the participant was removed,
  // is not entered until the participant is discovered, and it is dropped with
  // the participant.
  #[test]
  fn discdb_endpoints_wait_for_their_participant() {
    let mut db = empty_db();
    let reader = |prefix: &[u8; 12]| reader_of(GuidPrefix::new(prefix));
    let writer = |prefix: &[u8; 12]| writer_of(GuidPrefix::new(prefix));

    // SEDP before SPDP: the endpoints wait for their participant.
    let (early_reader, early_writer) = (reader(b"early_partcp"), writer(b"early_partcp"));
    let early = early_reader.reader_proxy.remote_reader_guid.prefix;
    assert!(db.update_subscription(&early_reader).is_none());
    assert!(db.update_publication(&early_writer).is_none());
    assert!(!reader_known(&db, &early_reader) && !writer_known(&db, &early_writer));
    assert_eq!(
      enter_endpoints_from_attic(&mut db, early),
      (vec![], vec![]),
      "endpoints were released while their participant is unknown"
    );
    db.update_participant(&participant_data(early));
    let (readers, writers) = enter_endpoints_from_attic(&mut db, early);
    assert_eq!(readers.len(), 1);
    assert_eq!(writers.len(), 1);
    assert!(reader_known(&db, &early_reader) && writer_known(&db, &early_writer));
    // The participant is known now, so updates are entered at once.
    assert!(db.update_subscription(&early_reader).is_some());
    assert!(db.update_publication(&early_writer).is_some());

    // SEDP after the participant was removed: the endpoints are not entered.
    let (late_reader, late_writer) = (reader(b"late_partcpt"), writer(b"late_partcpt"));
    let late = late_reader.reader_proxy.remote_reader_guid.prefix;
    db.update_participant(&participant_data(late));
    db.remove_participant(late, true);
    assert!(db.update_subscription(&late_reader).is_none());
    assert!(db.update_publication(&late_writer).is_none());
    assert!(!reader_known(&db, &late_reader) && !writer_known(&db, &late_writer));

    // A participant removed while its endpoints wait takes them with it.
    let (gone_reader, gone_writer) = (reader(b"gone_partcpt"), writer(b"gone_partcpt"));
    let gone = gone_reader.reader_proxy.remote_reader_guid.prefix;
    assert!(db.update_subscription(&gone_reader).is_none());
    assert!(db.update_publication(&gone_writer).is_none());
    db.remove_participant(gone, true);
    db.update_participant(&participant_data(gone));
    assert_eq!(enter_endpoints_from_attic(&mut db, gone), (vec![], vec![]));
    assert!(!reader_known(&db, &gone_reader) && !writer_known(&db, &gone_writer));

    // An endpoint disposed over SEDP while it waits is dropped too.
    let (disposed_reader, disposed_writer) = (reader(b"disp_partcpt"), writer(b"disp_partcpt"));
    let disposed = disposed_reader.reader_proxy.remote_reader_guid.prefix;
    assert!(db.update_subscription(&disposed_reader).is_none());
    assert!(db.update_publication(&disposed_writer).is_none());
    db.remove_topic_reader(disposed_reader.reader_proxy.remote_reader_guid);
    db.remove_topic_writer(disposed_writer.writer_proxy.remote_writer_guid);
    db.update_participant(&participant_data(disposed));
    assert_eq!(
      enter_endpoints_from_attic(&mut db, disposed),
      (vec![], vec![])
    );
  }

  // SEDP does not deliver the endpoints of a participant a second time, so
  // the endpoints of a participant that timed out are kept. When the
  // participant is discovered again, they are entered again and returned, so
  // that they can be matched with local endpoints.
  #[test]
  fn discdb_endpoints_of_a_timed_out_participant_return_with_it() {
    let mut db = empty_db();
    let prefix = GuidPrefix::new(b"timeout_part");
    let (reader, writer) = (reader_of(prefix), writer_of(prefix));
    db.update_participant(&participant_data(prefix));
    db.update_subscription(&reader);
    db.update_publication(&writer);
    assert!(reader_known(&db, &reader) && writer_known(&db, &writer));

    db.remove_participant(prefix, false); // false = timed out
    assert!(!reader_known(&db, &reader) && !writer_known(&db, &writer));

    db.update_participant(&participant_data(prefix));
    let (readers, writers) = enter_endpoints_from_attic(&mut db, prefix);
    assert_eq!(
      readers
        .iter()
        .map(|r| r.reader_proxy.remote_reader_guid)
        .collect::<Vec<_>>(),
      vec![reader.reader_proxy.remote_reader_guid],
      "the reader of the returned participant was not handed out for matching"
    );
    assert_eq!(
      writers
        .iter()
        .map(|w| w.writer_proxy.remote_writer_guid)
        .collect::<Vec<_>>(),
      vec![writer.writer_proxy.remote_writer_guid],
      "the writer of the returned participant was not handed out for matching"
    );
    assert!(reader_known(&db, &reader) && writer_known(&db, &writer));
  }

  // A reader or writer that announces no locators is reached at the default
  // locators of its participant. A participant that returns after a timeout
  // may announce new ones, e.g. because its address changed. Its endpoints
  // are then reached there, and no longer at the old locators.
  #[test]
  fn discdb_endpoints_of_a_returning_participant_use_its_new_default_locators() {
    use std::net::{Ipv4Addr, SocketAddr};

    let locator = |host: u8, port: u16| {
      Locator::from(SocketAddr::new(Ipv4Addr::new(192, 0, 2, host).into(), port))
    };
    let multicast = |group: u8| {
      Locator::from(SocketAddr::new(
        Ipv4Addr::new(239, 255, 0, group).into(),
        7401,
      ))
    };
    let participant_at = |prefix, host: u8, group: u8| {
      let mut data = participant_data(prefix);
      data.default_unicast_locators = vec![locator(host, 7411)];
      data.default_multicast_locators = vec![multicast(group)];
      data
    };

    let mut db = empty_db();
    let prefix = GuidPrefix::new(b"moving_partc");
    // The reader announces no locators. The writer announces a unicast locator
    // of its own, and no multicast locators.
    let reader = reader_of(prefix);
    let mut writer = writer_of(prefix);
    let writers_own = locator(77, 7477);
    writer.writer_proxy.unicast_locator_list = vec![writers_own];

    db.update_participant(&participant_at(prefix, 1, 1));
    let entered_reader = db.update_subscription(&reader).unwrap();
    let entered_writer = db.update_publication(&writer).unwrap();
    assert_eq!(
      (
        &entered_reader.reader_proxy.unicast_locator_list,
        &entered_reader.reader_proxy.multicast_locator_list,
        &entered_writer.writer_proxy.unicast_locator_list,
        &entered_writer.writer_proxy.multicast_locator_list,
      ),
      (
        &vec![locator(1, 7411)],
        &vec![multicast(1)],
        &vec![writers_own],
        &vec![multicast(1)],
      )
    );

    db.remove_participant(prefix, false); // false = timed out
    db.update_participant(&participant_at(prefix, 2, 2));
    let (readers, writers) = enter_endpoints_from_attic(&mut db, prefix);

    let reader_locators: Vec<_> = readers
      .iter()
      .chain(db.get_all_external_topic_readers())
      .map(|r| {
        (
          r.reader_proxy.unicast_locator_list.clone(),
          r.reader_proxy.multicast_locator_list.clone(),
        )
      })
      .collect();
    assert_eq!(
      reader_locators,
      vec![(vec![locator(2, 7411)], vec![multicast(2)]); 2],
      "the reader kept the default locators its participant had before it timed out"
    );
    let writer_locators: Vec<_> = writers
      .iter()
      .chain(db.get_all_external_topic_writers())
      .map(|w| {
        (
          w.writer_proxy.unicast_locator_list.clone(),
          w.writer_proxy.multicast_locator_list.clone(),
        )
      })
      .collect();
    assert_eq!(
      writer_locators,
      vec![(vec![writers_own], vec![multicast(2)]); 2],
      "the writer lost its own unicast locator, or kept the default multicast locators its \
       participant had before it timed out"
    );
  }

  // An endpoint that is disposed over SEDP while its participant is timed out
  // does not return with the participant.
  #[test]
  fn discdb_endpoint_disposed_while_its_participant_is_timed_out_does_not_return() {
    let mut db = empty_db();
    let prefix = GuidPrefix::new(b"timeout_part");
    let (reader, writer) = (reader_of(prefix), writer_of(prefix));
    db.update_participant(&participant_data(prefix));
    db.update_subscription(&reader);
    db.update_publication(&writer);
    assert!(reader_known(&db, &reader) && writer_known(&db, &writer));
    db.remove_participant(prefix, false); // false = timed out

    db.remove_topic_reader(reader.reader_proxy.remote_reader_guid);
    db.remove_topic_writer(writer.writer_proxy.remote_writer_guid);

    db.update_participant(&participant_data(prefix));
    assert_eq!(
      enter_endpoints_from_attic(&mut db, prefix),
      (vec![], vec![])
    );
    assert!(!reader_known(&db, &reader) && !writer_known(&db, &writer));
  }

  // A participant that is disposed after it timed out takes its endpoints with
  // it.
  #[test]
  fn discdb_dispose_of_a_timed_out_participant_drops_its_endpoints() {
    let mut db = empty_db();
    let prefix = GuidPrefix::new(b"timeout_part");
    let (reader, writer) = (reader_of(prefix), writer_of(prefix));
    db.update_participant(&participant_data(prefix));
    db.update_subscription(&reader);
    db.update_publication(&writer);
    assert!(reader_known(&db, &reader) && writer_known(&db, &writer));
    db.remove_participant(prefix, false); // false = timed out

    db.remove_participant(prefix, true); // true = disposed

    db.update_participant(&participant_data(prefix));
    assert_eq!(
      enter_endpoints_from_attic(&mut db, prefix),
      (vec![], vec![])
    );
    assert!(!reader_known(&db, &reader) && !writer_known(&db, &writer));
  }

  #[test]
  fn discdb_participant_operations() {
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      std::sync::mpsc::sync_channel::<()>(4);
    let (status_sender, _status_receiver) = sync_status_channel(16).unwrap();

    let mut discoverydb = DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      status_sender,
    );
    let mut data = spdp_participant_data().unwrap();
    data.lease_duration = Some(Duration::from(StdDuration::from_secs(1)));

    discoverydb.update_participant(&data);
    assert_eq!(discoverydb.participant_proxies.len(), 1);

    discoverydb.update_participant(&data);
    assert_eq!(discoverydb.participant_proxies.len(), 1);

    std::thread::sleep(StdDuration::from_secs(2));
    discoverydb.participant_cleanup();
    assert!(discoverydb.participant_proxies.is_empty());

    // TODO: more operations tests
  }

  #[test]
  fn discdb_writer_proxies() {
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      std::sync::mpsc::sync_channel::<()>(4);
    let (status_sender, _status_receiver) = sync_status_channel(16).unwrap();
    let _discoverydb = DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      status_sender,
    );
    let topic_name = String::from("some_topic");
    let type_name = String::from("RandomData");
    let _dreader = DiscoveredReaderData::default(topic_name, type_name);

    // TODO: more tests :)
  }

  #[test]
  fn discdb_subscription_operations() {
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      std::sync::mpsc::sync_channel::<()>(4);
    let (status_sender, _status_receiver) = sync_status_channel(16).unwrap();

    let mut discovery_db = DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      status_sender,
    );

    let domain_participant =
      DomainParticipant::new(test_domain_id(0)).expect("Failed to create publisher");
    let topic = domain_participant
      .create_topic(
        "Foobar".to_string(),
        "RandomData".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();
    let topic2 = domain_participant
      .create_topic(
        "Barfoo".to_string(),
        "RandomData".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();

    let publisher1 = domain_participant
      .create_publisher(&QosPolicies::qos_none())
      .unwrap();
    let dw = publisher1
      .create_datawriter::<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>>(&topic, None)
      .unwrap();

    let writer_data = DiscoveredWriterData::new(&dw, &topic, &domain_participant, None);

    discovery_db.update_local_topic_writer(writer_data);
    assert_eq!(discovery_db.local_topic_writers.len(), 1);

    let publisher2 = domain_participant
      .create_publisher(&QosPolicies::qos_none())
      .unwrap();
    let dw2 = publisher2
      .create_datawriter::<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>>(
        &topic2, None,
      )
      .unwrap();
    let writer_data2 = DiscoveredWriterData::new(&dw2, &topic2, &domain_participant, None);
    discovery_db.update_local_topic_writer(writer_data2);
    assert_eq!(discovery_db.local_topic_writers.len(), 2);

    // creating data
    let reader1 = reader_proxy_data().unwrap();
    let reader1sub = subscription_builtin_topic_data().unwrap();
    // reader1sub.set_key(reader1.remote_reader_guid);
    // reader1sub.set_topic_name(&topic.name());
    let dreader1 = DiscoveredReaderData {
      reader_proxy: reader1.clone(),
      subscription_topic_data: reader1sub.clone(),
      content_filter: None,
      user_data: Vec::new(),
    };
    discovery_db.update_subscription(&dreader1);

    let reader2 = reader_proxy_data().unwrap();
    let reader2sub = subscription_builtin_topic_data().unwrap();
    // reader2sub.set_key(reader2.remote_reader_guid);
    // reader2sub.set_topic_name(&topic2.name());
    let dreader2 = DiscoveredReaderData {
      reader_proxy: reader2,
      subscription_topic_data: reader2sub,
      content_filter: None,
      user_data: Vec::new(),
    };
    discovery_db.update_subscription(&dreader2);

    let reader3 = reader1;
    let reader3sub = reader1sub;
    let dreader3 = DiscoveredReaderData {
      reader_proxy: reader3,
      subscription_topic_data: reader3sub,
      content_filter: None,
      user_data: Vec::new(),
    };
    discovery_db.update_subscription(&dreader3);

    // TODO: there might be a need for different scenarios
  }

  #[test]
  fn discdb_external_endpoint_snapshots() {
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      std::sync::mpsc::sync_channel::<()>(4);
    let (status_sender, _status_receiver) = sync_status_channel(16).unwrap();

    let mut discovery_db = DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      status_sender,
    );

    // Nothing discovered yet.
    assert_eq!(discovery_db.get_all_external_topic_readers().count(), 0);
    assert_eq!(discovery_db.get_all_external_topic_writers().count(), 0);

    // Discover one external Reader via SEDP subscription data. Its participant
    // has to be known first.
    let reader = reader_proxy_data().unwrap();
    let reader_sub = subscription_builtin_topic_data().unwrap();
    let dreader = DiscoveredReaderData {
      reader_proxy: reader,
      subscription_topic_data: reader_sub,
      content_filter: None,
      user_data: Vec::new(),
    };
    discovery_db.update_participant(&participant_data(
      dreader.reader_proxy.remote_reader_guid.prefix,
    ));
    discovery_db.update_subscription(&dreader);

    // Discover one external Writer via SEDP publication data.
    let domain_participant =
      DomainParticipant::new(test_domain_id(0)).expect("Failed to create participant");
    let topic = domain_participant
      .create_topic(
        "Foobar".to_string(),
        "RandomData".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();
    let publisher = domain_participant
      .create_publisher(&QosPolicies::qos_none())
      .unwrap();
    let dw = publisher
      .create_datawriter::<RandomData, CDRSerializerAdapter<RandomData, LittleEndian>>(&topic, None)
      .unwrap();
    let writer_data = DiscoveredWriterData::new(&dw, &topic, &domain_participant, None);
    discovery_db.update_participant(&participant_data(domain_participant.guid_prefix()));
    discovery_db.update_publication(&writer_data);

    // Both accessors return the discovered endpoints as a snapshot.
    let readers = discovery_db
      .get_all_external_topic_readers()
      .cloned()
      .collect::<Vec<_>>();
    let writers = discovery_db
      .get_all_external_topic_writers()
      .cloned()
      .collect::<Vec<_>>();
    assert_eq!(readers.len(), 1);
    assert_eq!(writers.len(), 1);
    assert_eq!(
      readers[0].reader_proxy.remote_reader_guid,
      dreader.reader_proxy.remote_reader_guid
    );
    assert_eq!(
      writers[0].writer_proxy.remote_writer_guid,
      writer_data.writer_proxy.remote_writer_guid
    );
  }

  #[test]
  fn discdb_local_topic_reader() {
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      std::sync::mpsc::sync_channel::<()>(4);

    let dp = DomainParticipant::new(test_domain_id(0)).expect("Failed to create participant");
    let topic = dp
      .create_topic(
        "some topic name".to_string(),
        "Wazzup".to_string(),
        &QosPolicies::qos_none(),
        TopicKind::WithKey,
      )
      .unwrap();
    let (status_sender, _status_receiver) = sync_status_channel(16).unwrap();
    let mut discoverydb = DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      status_sender,
    );

    // Create reader ingredients
    let (notification_sender1, _notification_receiver1) =
      crate::mio_source::notification_channel(100);
    let (_notification_event_source1, notification_event_sender1) =
      mio_source::make_poll_channel().unwrap();
    let data_reader_waker1 = Arc::new(Mutex::new(None));

    let (status_sender1, _status_receiver1) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (_reader_commander1, reader_command_receiver1) =
      wake_channel::<ReaderCommand>(100, &LoopWaker::new());

    let topic_cache =
      dp.dds_cache()
        .write()
        .unwrap()
        .add_new_topic(topic.name(), topic.get_type(), &topic.qos());

    let reader1_ing = ReaderIngredients {
      guid: GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED),
      notification_sender: notification_sender1,
      status_sender: status_sender1,
      topic_name: topic.name(),
      topic_cache_handle: topic_cache.clone(),
      like_stateless: false,
      qos_policy: QosPolicies::qos_none(),
      data_reader_command_receiver: reader_command_receiver1,
      data_reader_waker: data_reader_waker1,
      poll_event_sender: notification_event_sender1,
      security_plugins: None,
      discovery_notify: None,
    };

    // Add the reader to the database and verify the info is updated
    discoverydb.update_local_topic_reader(DiscoveryDB::local_topic_reader_data(
      &dp,
      &topic,
      &reader1_ing,
      None,
    ));
    assert_eq!(discoverydb.local_topic_readers.len(), 1);
    assert_eq!(discoverydb.get_local_topic_readers(&topic).len(), 1);

    // Verify that the info does not change if the reader is added a second time
    discoverydb.update_local_topic_reader(DiscoveryDB::local_topic_reader_data(
      &dp,
      &topic,
      &reader1_ing,
      None,
    ));
    assert_eq!(discoverydb.local_topic_readers.len(), 1);
    assert_eq!(discoverydb.get_local_topic_readers(&topic).len(), 1);

    // Create second reader ingredients for the same topic
    let (notification_sender2, _notification_receiver2) =
      crate::mio_source::notification_channel(100);
    let (_notification_event_source2, notification_event_sender2) =
      mio_source::make_poll_channel().unwrap();
    let data_reader_waker2 = Arc::new(Mutex::new(None));

    let (status_sender2, _status_receiver2) = sync_status_channel::<DataReaderStatus>(4).unwrap();
    let (_reader_commander2, reader_command_receiver2) =
      wake_channel::<ReaderCommand>(100, &LoopWaker::new());

    let mut guid2 = GUID::dummy_test_guid(EntityKind::READER_NO_KEY_USER_DEFINED);
    guid2.prefix = GuidPrefix::new(b"Another fake"); // GUID needs to be different in order to be added

    let reader2_ing = ReaderIngredients {
      guid: guid2,
      notification_sender: notification_sender2,
      status_sender: status_sender2,
      topic_name: topic.name(),
      topic_cache_handle: topic_cache,
      like_stateless: false,
      qos_policy: QosPolicies::qos_none(),
      data_reader_command_receiver: reader_command_receiver2,
      data_reader_waker: data_reader_waker2,
      poll_event_sender: notification_event_sender2,
      security_plugins: None,
      discovery_notify: None,
    };

    // Add the second reader to the database and verify the info is updated
    discoverydb.update_local_topic_reader(DiscoveryDB::local_topic_reader_data(
      &dp,
      &topic,
      &reader2_ing,
      None,
    ));
    assert_eq!(discoverydb.get_local_topic_readers(&topic).len(), 2);
    assert_eq!(discoverydb.get_all_local_topic_readers().count(), 2);
  }
}
