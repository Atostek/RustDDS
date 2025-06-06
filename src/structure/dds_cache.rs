use std::{
  cmp::max,
  collections::{btree_map, BTreeMap, HashMap},
  iter,
  ops::Bound::{Excluded, Included},
  sync::{Arc, Mutex},
};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  create_error_internal,
  dds::{
    qos::{
      policy::{History, ResourceLimits},
      QosPolicies,
    },
    typedesc::TypeDesc,
    CreateError, CreateResult,
  },
  structure::{sequence_number::SequenceNumber, time::Timestamp},
  GUID,
};
use super::cache_change::CacheChange;

/// DDSCache contains all cacheChanges that are
/// received by this participant. It is for serving local Readers. Local
/// Writers each contain a HistoryBuffer to CacheChanges written by themselves.
///
/// Readers and Writers need separate caches, because the garbage collection
/// algorithms must be different. DDS spec 1.4 table at pp. 99-100 defines the
/// QoS History to be scoped over each Writer (individually), whereas History
/// for Readers is scoped over the entire Topic (or Instances in it). This leads
/// to situations where some sample would be garbage collected by a Reader's
/// History policy, but must be preserved by a Writer's History Policy.
///
/// Each topic that has been subscribed to
/// is contained in a separate TopicCache. One TopicCache contains
/// only CacheChanges of one serialized IDL datatype. -> all cache changes in
/// same TopicCache can be serialized/deserialized same way. Topic/TopicCache is
/// identified by its name, which must be unique in the whole Domain.
///
/// More specifically, the DDSCache stores handles (Arcs) to mutexes protecting
/// the actual TopicCaches. For a given topic, the Reader/Writer and
/// DataReader/DataWriter get a clone of the handle and
/// interact with the TopicCache through this handle.
#[derive(Debug, Default)]
pub struct DDSCache {
  topic_caches: HashMap<String, Arc<Mutex<TopicCache>>>,
}

impl DDSCache {
  pub fn new() -> Self {
    Self::default()
  }
  // Insert new topic if it does not exist.
  // If it exists already, update cache size limits.
  // Return a handle to the cache topic.
  // TODO: If we pick up a topic from Discovery, can someone DoS us by
  // sending super large limits in Topic QoS?
  pub(crate) fn add_new_topic(
    &mut self,
    topic_name: String,
    topic_data_type: TypeDesc,
    qos: &QosPolicies,
  ) -> Arc<Mutex<TopicCache>> {
    let topic_cache_handle = self
      .topic_caches
      .entry(topic_name.clone())
      .and_modify(|tc| tc.lock().unwrap().update_keep_limits(qos))
      .or_insert(Arc::new(Mutex::new(TopicCache::new(
        topic_name,
        topic_data_type,
        qos,
      ))));

    topic_cache_handle.clone()
  }

  pub(crate) fn get_existing_topic_cache(
    &self,
    topic_name: &str,
  ) -> CreateResult<Arc<Mutex<TopicCache>>> {
    // Return a clone of the pointer to the mutex on an existing topic cache
    match self.topic_caches.get(topic_name) {
      Some(tc) => Ok(tc.clone()),
      None => create_error_internal!("Topic cache for topic {topic_name} not found in DDS cache"),
    }
  }

  // TODO: Investigate why this is not used.
  // When do RTPS Topics die? Never?
  #[allow(dead_code)]
  pub fn remove_topic(&mut self, topic_name: &str) {
    if self.topic_caches.contains_key(topic_name) {
      self.topic_caches.remove(topic_name);
    }
  }

  pub fn garbage_collect(&mut self) {
    for tc in self.topic_caches.values_mut() {
      let mut tc = tc.lock().unwrap();
      if let Some((last_timestamp, _)) = tc.changes.iter().next_back() {
        if *last_timestamp > tc.changes_reallocated_up_to {
          tc.remove_changes_before(Timestamp::ZERO);
        }
      }
    }
  }
}

#[derive(Debug)]
pub(crate) struct TopicCache {
  topic_name: String,
  #[allow(dead_code)] // TODO: Which (future) feature needs this?
  topic_data_type: TypeDesc,
  #[allow(dead_code)]
  // TODO: The relevant data here is in min/max keep_samples. Is this still relevant?
  topic_qos: QosPolicies,
  min_keep_samples: History,
  max_keep_samples: i32, // from QoS, for quick, repeated access
  // TODO: Change this to Option<u32>, where None means "no limit".

  // The main content of the cache is in this map.
  // Timestamp is assumed to be unique id over all the CacheChanges.
  changes: BTreeMap<Timestamp, CacheChange>,

  // The underlying Bytes buffers are reallocated after some time (once for each) in
  // order to release the original receive buffer. The idea behind this is that if a CacheChange
  // persists in the TopicCaceh for some time, it should no longer hold onto the receive buffer,
  // so we can recycle the buffers. Otherwise, we (practically) leak memory.
  changes_reallocated_up_to: Timestamp,

  // sequence_numbers is an index to "changes" by GUID and SN
  sequence_numbers: BTreeMap<GUID, BTreeMap<SequenceNumber, Timestamp>>,

  // Keep track of how far we have "reliably" received samples from each Writer
  // This means that all data up to this point has either been received, or
  // we have been notified (GAP or HEARTBEAT) that is not available and never will.
  // Therefore, data before the marker SN can be handed off to a Reliable DataReader.
  // Initially, we consider the marker for each Writer (GUID) to be SequenceNumber::new(1)
  received_reliably_before: BTreeMap<GUID, SequenceNumber>,
}

impl TopicCache {
  pub fn new(topic_name: String, topic_data_type: TypeDesc, topic_qos: &QosPolicies) -> Self {
    let mut new_self = Self {
      topic_name,
      topic_data_type,
      topic_qos: topic_qos.clone(),
      min_keep_samples: History::KeepLast { depth: 1 }, /* dummy value, next call will overwrite
                                                         * this */
      max_keep_samples: 1, // dummy value, next call will overwrite this
      changes: BTreeMap::new(),
      changes_reallocated_up_to: Timestamp::ZERO,
      sequence_numbers: BTreeMap::new(),
      received_reliably_before: BTreeMap::new(),
    };

    new_self.update_keep_limits(topic_qos);

    new_self
  }

  pub fn update_keep_limits(&mut self, qos: &QosPolicies) {
    let min_keep_samples = qos
      .history()
      // default history setting from DDS spec v1.4 Section 2.2.3 "Supported QoS",
      // Table at p.99
      .unwrap_or(History::KeepLast { depth: 1 });

    // Look up some Topic-specific resource limit
    // and remove earliest samples until we are within limit.
    // This prevents cache from growing indefinitely.
    let max_keep_samples = qos
      .resource_limits()
      .unwrap_or(ResourceLimits {
        // This is some default limits out of the hat, if there was no QoS specification.
        // The purpose of these limits is to prevent excessive memory usage.
        max_samples: 64,
        max_instances: 64,
        max_samples_per_instance: 64,
      })
      .max_samples;
    // TODO: We cannot currently keep track of instance counts, because TopicCache
    // or DDSCache below do not know about instances.

    // If a definite minimum is specified, increase resource limit to at least that.
    let max_keep_samples = match min_keep_samples {
      History::KeepLast { depth: n } if n > max_keep_samples => n,
      _ => max_keep_samples,
    };

    // actual update. This is will only ever increase cache size.
    self.min_keep_samples = max(min_keep_samples, self.min_keep_samples);
    self.max_keep_samples = max(max_keep_samples, self.max_keep_samples);
  }

  // Returns true if the "reliably_received_before"-marker was actually moved
  // forward and false if not.
  pub fn mark_reliably_received_before(&mut self, writer: GUID, sn: SequenceNumber) -> bool {
    let prev_sn = self.received_reliably_before.insert(writer, sn);
    prev_sn.unwrap_or(SequenceNumber::new(1)) < sn
  }

  pub fn get_change(&self, instant: &Timestamp) -> Option<&CacheChange> {
    self.changes.get(instant)
  }

  pub fn add_change(&mut self, instant: &Timestamp, cache_change: CacheChange) {
    self
      .add_change_internal(instant, cache_change)
      .map(|cc_back| {
        warn!(
          "DDSCache insert failed topic={:?} cache_change={:?}",
          self.topic_name, cc_back
        );
      });
  }

  fn add_change_internal(
    &mut self,
    instant: &Timestamp,
    cache_change: CacheChange,
  ) -> Option<CacheChange> {
    // First, do garbage collection.
    // But not at every insert, just to save time and effort.
    // Some heuristic to decide if we should collect now.
    // let payload_size = max(1, cache_change.data_value.payload_size());
    let semi_random_number = i64::from(cache_change.sequence_number) as usize;
    // let fairly_large_constant = 0xffff;
    let modulus = 64;
    if modulus == 0 || semi_random_number % modulus == 0 {
      debug!("Garbage collecting topic {}", self.topic_name);
      self.remove_changes_before(Timestamp::ZERO);
      // remove limit is so low that it has no effect.
      // We actually only remove to keep cache size between min and max limits.
    }

    // Now to the actual adding business.
    if let Some(old_instant) = self.find_by_sn(&cache_change) {
      // Got duplicate DATA for a SN that we already have. It should be discarded.
      trace!(
        "add_change: discarding duplicate {:?} from {:?}. old timestamp = {:?}, new = {:?}",
        cache_change.sequence_number,
        cache_change.writer_guid,
        old_instant,
        instant,
      );
      // We are keeping this quiet, because e.g. FastDDS Discoery keeps sending the
      // same SequenceNumber in periodic updates.
      None
    } else {
      // This is a new (to us) SequenceNumber, this is the default processing path.
      self.insert_sn(*instant, &cache_change);
      self.changes.insert(*instant, cache_change).map(|old_cc| {
        // If this happens, cache changes were created at exactly same instant.
        // This is bad, since we are using instants as keys and assume that they
        // are unique.
        error!("DDSHistoryCache already contained element with key {instant:?} !!!");
        self.remove_sn(&old_cc);
        old_cc
      })
    }
  }

  fn find_by_sn(&self, cc: &CacheChange) -> Option<Timestamp> {
    self
      .sequence_numbers
      .get(&cc.writer_guid)
      .and_then(|snm| snm.get(&cc.sequence_number))
      .copied()
  }

  fn insert_sn(&mut self, instant: Timestamp, cc: &CacheChange) {
    self
      .sequence_numbers
      .entry(cc.writer_guid)
      .or_default()
      .insert(cc.sequence_number, instant);
  }

  pub fn get_changes_in_range_best_effort<'a>(
    &'a self,
    start_instant: Timestamp,
    end_instant: Timestamp,
  ) -> ChangesInRangeBestEffort<'a, impl FnMut(BestEffortMap<'a>) -> ChangesItem<'a>> {
    // Sanity check
    let start_instant = if start_instant <= end_instant {
      // sane case
      start_instant
    } else {
      // insane case
      error!(
        "get_changes_in_range_best_effort: Did my clock jump backwards? start={start_instant:?} \
         end={end_instant:?}"
      );
      end_instant
    };

    // Get result as tree range
    self
      .changes
      .range((Excluded(start_instant), Included(end_instant)))
      .map(|(i, c)| (*i, c))
  }

  pub fn get_changes_in_range_reliable<'a, 'b>(
    &'a self,
    last_read_sn: &'b BTreeMap<GUID, SequenceNumber>,
  ) -> ChangesInRangeReliable<
    'a,
    impl FnMut(ReliableFlat<'a>) -> ReliableFlatItem<'a> + use<'a, 'b>,
    impl FnMut(ReliableMap<'a>) -> Option<ChangesItem<'a>>,
  > {
    self
      .sequence_numbers
      .iter()
      .flat_map(|(guid, sn_map)| {
        let lower_bound_exc = last_read_sn
          .get(guid)
          .cloned()
          .unwrap_or(SequenceNumber::zero());
        let upper_bound_exc = self.reliable_before(*guid);
        // make sure lower < upper, so that `.range()` does not panic.
        let upper_bound_exc = max(upper_bound_exc, lower_bound_exc.plus_1());
        sn_map.range((Excluded(lower_bound_exc), Excluded(upper_bound_exc)))
      }) // we get iterator of Timestamp
      .filter_map(|(_sn, t)| self.get_change(t).map(|cc| (*t, cc)))
  }

  pub fn get_changes_in_range<'a>(
    &'a self,
    reliable: bool,
    latest_instant: Timestamp,
    last_read_sn: &'a BTreeMap<GUID, SequenceNumber>,
  ) -> impl Iterator<Item = (Timestamp, &'a CacheChange)> {
    if reliable {
      ChangesInRange::Reliable(self.get_changes_in_range_reliable(last_read_sn))
    } else {
      ChangesInRange::BestEffort(
        self.get_changes_in_range_best_effort(latest_instant, Timestamp::now()),
      )
    }
  }
  fn reliable_before(&self, writer: GUID) -> SequenceNumber {
    self
      .received_reliably_before
      .get(&writer)
      .cloned()
      .unwrap_or(SequenceNumber::default())
    // Sequence numbering starts at default(), so anything before that is always
    // received reliably, since no such samples exist.
  }

  fn remove_sn(&mut self, cc: &CacheChange) {
    let mut emptied = false;

    self.sequence_numbers.entry(cc.writer_guid).and_modify(|s| {
      s.remove(&cc.sequence_number);
      emptied = s.is_empty();
    });
    if emptied {
      self.sequence_numbers.remove(&cc.writer_guid);
    }
  }

  /// Garbarge collection:
  ///
  /// Remove changes before given Timestamp, but keep at least
  /// `self.min_keep_samples`.
  ///
  /// If we are over `self.max_keep_samples`, then remove the oldest samples
  /// until `max_keep_samples` is reached, regardless of `remove_before`.
  pub fn remove_changes_before(&mut self, remove_before: Timestamp) {
    // TODO: Currently, TopicCache does not know about Keys is Samples/CacheChanges,
    // because they are opaque binary blobs at this point. Therefore, we cannot
    // distinguish between instances, so cannot observe max instance counts.
    // We have to do just with min/max sample counts.

    let sample_count = self.changes.len();

    // We must remove at least enough to stay within max limit,
    // must_remove_count = sample_count - max_keep_samples
    let must_remove_count = sample_count.saturating_sub(self.max_keep_samples as usize);
    // Saturating sub takes care that this does not underflow below zero.

    let may_remove_count = match self.min_keep_samples {
      // We are erquested to KeepAll, so do not remove more than absolutely required.
      History::KeepAll => must_remove_count,

      // We are requested to keep some depth, so we may remove up to that.
      History::KeepLast { depth } => max(
        sample_count.saturating_sub(depth as usize),
        must_remove_count,
      ),
    };

    let oldest_timestamp_to_retain_opt: Option<Timestamp> = self
      .changes
      .keys()
      .enumerate()
      .skip_while(|(i, ts)|
        // Skip samples to be removed. Force "must" count, and then remove until
        // either limit timestamp or "may" count stops us
        *i < must_remove_count
        || (**ts < remove_before && *i < may_remove_count))
      .map(|(_, ts)| ts) // un-enumerate
      .next() // the next element would be the first to retain
      .copied();

    // In case `.next()` is `None`, then we just decided to to discard everything,
    // or, as a special case, the cache was empty to begin with, but the end
    // result is the same.

    let to_retain = if let Some(split_key) = oldest_timestamp_to_retain_opt {
      // split_off: Returns everything after the given key, including the key.
      self.changes.split_off(&split_key)
    } else {
      BTreeMap::new()
    };

    let to_remove = std::mem::replace(&mut self.changes, to_retain);

    // update also SequenceNumber map
    to_remove.values().for_each(|r| self.remove_sn(r));

    // Now, reallocate old cache changes
    let reallocate_timeout = crate::Duration::from_secs(5);
    let now = Timestamp::now();

    // Take max to avoid crash if clock jumps backward.
    let reallocate_limit = max(now - reallocate_timeout, self.changes_reallocated_up_to);

    self
      .changes
      .range_mut((
        Excluded(self.changes_reallocated_up_to),
        Included(reallocate_limit),
      ))
      .for_each(|(_, cc)| cc.reallocate());

    self.changes_reallocated_up_to = reallocate_limit;
  }

  pub fn topic_name(&self) -> String {
    self.topic_name.clone()
  }
}

type ChangesInRangeBestEffort<'a, F> = iter::Map<btree_map::Range<'a, Timestamp, CacheChange>, F>;
type ChangesInRangeReliable<'a, F1, F2> = iter::FilterMap<
  iter::FlatMap<
    btree_map::Iter<'a, GUID, BTreeMap<SequenceNumber, Timestamp>>,
    btree_map::Range<'a, SequenceNumber, Timestamp>,
    F1,
  >,
  F2,
>;

// type definitions for iterators
type BestEffortMap<'a> = (&'a Timestamp, &'a CacheChange);

type ReliableMap<'a> = (&'a SequenceNumber, &'a Timestamp);
type ReliableFlat<'a> = (&'a GUID, &'a BTreeMap<SequenceNumber, Timestamp>);
type ReliableFlatItem<'a> = btree_map::Range<'a, SequenceNumber, Timestamp>;

type ChangesItem<'a> = (Timestamp, &'a CacheChange);

pub(crate) enum ChangesInRange<'a, F, F1, F2> {
  BestEffort(ChangesInRangeBestEffort<'a, F>),
  Reliable(ChangesInRangeReliable<'a, F1, F2>),
}

impl<'a, F, F1, F2> Iterator for ChangesInRange<'a, F, F1, F2>
where
  F: FnMut(BestEffortMap<'a>) -> ChangesItem<'a>,
  F1: FnMut(ReliableFlat<'a>) -> ReliableFlatItem<'a>,
  F2: FnMut(ReliableMap<'a>) -> Option<ChangesItem<'a>>,
{
  type Item = ChangesItem<'a>;

  fn next(&mut self) -> Option<Self::Item> {
    match self {
      Self::Reliable(r) => r.next(),
      Self::BestEffort(b) => b.next(),
    }
  }
}

// -----------------------------------------------------------------------
// -----------------------------------------------------------------------
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::{
    sync::{Arc, RwLock},
    thread,
  };

  use super::DDSCache;
  use crate::{
    dds::{
      ddsdata::DDSData, qos::QosPolicies, typedesc::TypeDesc, with_key::datawriter::WriteOptions,
    },
    messages::submessages::elements::serialized_payload::SerializedPayload,
    structure::{cache_change::CacheChange, guid::GUID, sequence_number::SequenceNumber},
  };

  #[test]
  fn create_dds_cache_and_topic_cache() {
    // Create DDS cache
    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    // Set a topic name and some QoS policies
    let topic_name = String::from("ImJustATopic");
    let qos = QosPolicies::qos_none();

    // Add the new topic to DDS cache
    let topic_cache_handle = dds_cache.write().unwrap().add_new_topic(
      topic_name,
      TypeDesc::new("IDontKnowIfThisIsNecessary".to_string()),
      &qos,
    );

    // Create a cache change and add it to the topic cache
    let change1 = CacheChange::new(
      GUID::GUID_UNKNOWN,
      SequenceNumber::new(1),
      WriteOptions::default(),
      DDSData::new(SerializedPayload::default()),
    );
    topic_cache_handle
      .lock()
      .unwrap()
      .add_change(&crate::Timestamp::now(), change1);

    let topic_cache_handle2 = topic_cache_handle.clone();
    thread::spawn(move || {
      // Create two new cache changes and add them to topic cache
      let change2 = CacheChange::new(
        GUID::GUID_UNKNOWN,
        SequenceNumber::new(2),
        WriteOptions::default(),
        DDSData::new(SerializedPayload::default()),
      );
      let change3 = CacheChange::new(
        GUID::GUID_UNKNOWN,
        SequenceNumber::new(3),
        WriteOptions::default(),
        DDSData::new(SerializedPayload::default()),
      );

      topic_cache_handle2
        .lock()
        .unwrap()
        .add_change(&crate::Timestamp::now(), change2);
      topic_cache_handle2
        .lock()
        .unwrap()
        .add_change(&crate::Timestamp::now(), change3);
    })
    .join()
    .unwrap();

    // Verify that there are 3 cache changes in the topic cache
    assert_eq!(
      topic_cache_handle
        .lock()
        .unwrap()
        .get_changes_in_range_best_effort(
          crate::Timestamp::now() - crate::Duration::from_secs(23),
          crate::Timestamp::now()
        )
        .count(),
      3
    );
  }
}
