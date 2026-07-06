use std::{
  cmp::max,
  collections::{BTreeMap, BTreeSet},
  net::SocketAddr,
};

use bit_vec::BitVec;
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use crate::{
  dds::{participant::DomainParticipant, qos::QosPolicies},
  discovery::sedp_messages::DiscoveredReaderData,
  messages::submessages::submessage::AckSubmessage,
  network::util::{path_mtu_payload_for_peer, IfAddr},
  rtps::{
    constant::*,
    transmit::{InterfaceObservations, InterfaceSelector, RouteSelector, SendRoute},
  },
  structure::{
    guid::{EntityId, GUID},
    locator::Locator,
    sequence_number::{FragmentNumber, FragmentNumberSet, SequenceNumber, SequenceNumberRange},
  },
};
use super::reader::ReaderIngredients;

#[derive(Debug, PartialEq, Eq, Clone)]
/// ReaderProxy class represents the information an RTPS StatefulWriter
/// maintains on each matched RTPS Reader
//
// TODO: Maybe more of the members could be made private.
pub(crate) struct RtpsReaderProxy {
  /// Identifies the remote matched RTPS Reader that is represented by the
  /// ReaderProxy
  pub remote_reader_guid: GUID,
  /// Identifies the group to which the matched Reader belongs
  pub remote_group_entity_id: EntityId,
  /// List of unicast locators (transport, address, port combinations) that can
  /// be used to send messages to the matched RTPS Reader. The list may be empty
  pub unicast_locator_list: Vec<Locator>,
  /// List of multicast locators (transport, address, port combinations) that
  /// can be used to send messages to the matched RTPS Reader. The list may be
  /// empty
  pub multicast_locator_list: Vec<Locator>,

  /// Specifies whether the remote matched RTPS Reader expects in-line QoS to be
  /// sent along with any data.
  expects_in_line_qos: bool,
  /// Specifies whether the remote Reader is responsive to the Writer
  is_active: bool,

  // Reader has positively acked all SequenceNumbers _before_ this.
  // This is directly the same as readerSNState.base in ACKNACK submessage.
  pub all_acked_before: SequenceNumber,

  // List of SequenceNumbers to be sent to Reader. Both unsent and requested by ACKNACK.
  unsent_changes: BTreeSet<SequenceNumber>,

  // Messages that we are not going to send to this Reader.
  // We will send the SNs as GAP until they have been acked.
  // This is to be used in Reliable mode only.
  pending_gap: BTreeSet<SequenceNumber>,
  // true = send repair data messages due to NACKs, buffer messages by DataWriter
  // false = send data messages directly from DataWriter
  pub repair_mode: bool,
  qos: QosPolicies,
  frags_requested: BTreeMap<SequenceNumber, BitVec>,

  // Interface-aware transmit: the resolved send destination for this reader,
  // recomputed when locators or observations change. Defaults to the fallback
  // (legacy all-locators) route until resolved.
  send_route: SendRoute,

  // Per-peer path-MTU budget: the number of bytes available for RTPS
  // submessages in one datagram sent to this reader, derived from the local
  // egress interface's MTU (same-subnet peer) or the conservative default
  // (`FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE`) for peers behind a router / unresolved.
  // Recomputed alongside `send_route`. Used to bound submessage packing and
  // multi-fragment DATAFRAG datagrams. An overestimate only causes IP
  // fragmentation, never data loss.
  max_datagram_payload: usize,
}

impl RtpsReaderProxy {
  pub fn new(remote_reader_guid: GUID, qos: QosPolicies, expects_in_line_qos: bool) -> Self {
    Self {
      remote_reader_guid,
      remote_group_entity_id: EntityId::UNKNOWN,
      unicast_locator_list: Vec::default(),
      multicast_locator_list: Vec::default(),
      expects_in_line_qos,
      is_active: true,
      all_acked_before: SequenceNumber::zero(),
      unsent_changes: BTreeSet::new(),
      pending_gap: BTreeSet::new(),
      repair_mode: false,
      qos,
      frags_requested: BTreeMap::new(),
      send_route: SendRoute::default(),
      max_datagram_payload: FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE,
    }
  }

  // We get a (discovery) update on the properties of this remote Reader.
  // Update those properties that Discovery tells us, but keep run-time data.
  pub fn update(&mut self, update: &Self, topic: &str) {
    if self.remote_reader_guid != update.remote_reader_guid {
      error!("Update tried to change ReaderProxy GUID!"); // This is like
                                                          // changing primary
                                                          // key
                                                          // Refuse to update
    }
    if self.remote_group_entity_id != update.remote_group_entity_id {
      error!("Update tried to change ReaderProxy group entity id!"); // almost the same?
                                                                     // Refuse to update
    }

    if self.unicast_locator_list != update.unicast_locator_list
      || self.multicast_locator_list != update.multicast_locator_list
    {
      info!("Update changes Locators in ReaderProxy. topic={topic:?}");
      info!(
        "Unicast:\n  Old={:?}\n  New={:?}",
        self.unicast_locator_list, update.unicast_locator_list
      );
      info!(
        "Multicast:\n Old={:?}\n  New={:?}",
        self.multicast_locator_list, update.multicast_locator_list
      );
      let mut unicasts = update.unicast_locator_list.clone();
      unicasts.retain(Self::not_loopback);
      self.unicast_locator_list = unicasts;
      self
        .multicast_locator_list
        .clone_from(&update.multicast_locator_list);
    }

    self.expects_in_line_qos = update.expects_in_line_qos;

    // Apply QoS policies that are defined (only).
    // Undefined policies do not modify.
    let updated_qos = self.qos.modify_by(&update.qos);

    if self.qos != updated_qos {
      warn!("Update changes QoS in ReaderProxy topic={topic:?}.");
      info!(
        "  details:\n  Old: {:?}\n  New: {:?}",
        self.qos, updated_qos
      );
      self.qos = updated_qos;
    }
  }

  pub fn qos(&self) -> &QosPolicies {
    &self.qos
  }

  pub fn expects_inline_qos(&self) -> bool {
    self.expects_in_line_qos
  }

  pub fn unsent_changes_iter(
    &self,
  ) -> impl std::iter::DoubleEndedIterator<Item = SequenceNumber> + '_ {
    self.unsent_changes.iter().cloned()
  }

  // used to produce log messages
  pub fn unsent_changes_debug(&self) -> Vec<SequenceNumber> {
    self.unsent_changes_iter().collect()
  }

  pub fn first_unsent_change(&self) -> Option<SequenceNumber> {
    self.unsent_changes_iter().next()
  }

  pub fn mark_change_sent(&mut self, seq_num: SequenceNumber) {
    self.unsent_changes.remove(&seq_num);
  }

  // Changes are actually sent (via DATA/DATAFRAG) or reported missing as GAP
  pub fn remove_from_unsent_set_all_before(&mut self, before_seq_num: SequenceNumber) {
    // The handy split_off function "Returns everything after the given key,
    // including the key."
    self.unsent_changes = self.unsent_changes.split_off(&before_seq_num);
  }

  pub fn from_reader(reader: &ReaderIngredients, domain_participant: &DomainParticipant) -> Self {
    let mut self_locators = domain_participant.self_locators(); // This clones a map of locator lists.
    let (unicast_token, multicast_token) = if reader.guid.entity_id.kind().is_user_defined() {
      (USER_TRAFFIC_LISTENER_TOKEN, USER_TRAFFIC_MUL_LISTENER_TOKEN)
    } else {
      (DISCOVERY_LISTENER_TOKEN, DISCOVERY_MUL_LISTENER_TOKEN)
    };
    let unicast_locator_list = self_locators.remove(&unicast_token).unwrap_or_default();
    let multicast_locator_list = self_locators.remove(&multicast_token).unwrap_or_default();
    Self {
      remote_reader_guid: reader.guid,
      remote_group_entity_id: EntityId::UNKNOWN, // TODO
      unicast_locator_list,
      multicast_locator_list,
      expects_in_line_qos: false,
      is_active: true,
      all_acked_before: SequenceNumber::zero(),
      unsent_changes: BTreeSet::new(),
      pending_gap: BTreeSet::new(),
      repair_mode: false,
      qos: reader.qos_policy.clone(),
      frags_requested: BTreeMap::new(),
      send_route: SendRoute::default(),
      max_datagram_payload: FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE,
    }
  }

  fn discovered_or_default(drd: &[Locator], default: &[Locator]) -> Vec<Locator> {
    if drd.is_empty() {
      default.to_vec()
    } else {
      drd.to_vec()
    }
  }

  // OpenDDS seems to advertise also loopback address as its Locator over SPDP,
  // which is problematic, if we are not on the same host.
  fn not_loopback(l: &Locator) -> bool {
    let is_loopback = l.is_loopback();
    if is_loopback {
      info!("Ignoring loopback address {l:?}");
    }

    !is_loopback
  }

  pub fn from_discovered_reader_data(
    discovered_reader_data: &DiscoveredReaderData,
    default_unicast_locators: &[Locator],
    default_multicast_locators: &[Locator],
  ) -> Self {
    let mut unicast_locator_list = Self::discovered_or_default(
      &discovered_reader_data.reader_proxy.unicast_locator_list,
      default_unicast_locators,
    );
    unicast_locator_list.retain(Self::not_loopback);

    let multicast_locator_list = Self::discovered_or_default(
      &discovered_reader_data.reader_proxy.multicast_locator_list,
      default_multicast_locators,
    );

    Self {
      remote_reader_guid: discovered_reader_data.reader_proxy.remote_reader_guid,
      remote_group_entity_id: EntityId::UNKNOWN, // TODO
      unicast_locator_list,
      multicast_locator_list,
      expects_in_line_qos: discovered_reader_data.reader_proxy.expects_inline_qos,
      is_active: true,
      all_acked_before: SequenceNumber::zero(),
      unsent_changes: BTreeSet::new(),
      pending_gap: BTreeSet::new(),
      repair_mode: false,
      qos: discovered_reader_data.subscription_topic_data.qos(),
      frags_requested: BTreeMap::new(),
      send_route: SendRoute::default(),
      max_datagram_payload: FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE,
    }
  }

  /// The currently resolved [`SendRoute`] for this reader.
  pub fn send_route(&self) -> SendRoute {
    self.send_route
  }

  /// Recompute this reader's [`SendRoute`] from its advertised locators and the
  /// per-participant [`InterfaceObservations`], using `selector`.
  pub fn resolve_send_route(
    &mut self,
    observations: &InterfaceObservations,
    local_multicast_ifaces: &[InterfaceSelector],
    selector: &dyn RouteSelector,
  ) {
    let observed = observations.get(self.remote_reader_guid.prefix);
    self.send_route = selector.select(
      &self.unicast_locator_list,
      &self.multicast_locator_list,
      observed,
      local_multicast_ifaces,
    );
  }

  /// The per-peer datagram-payload budget (bytes for RTPS submessages in one
  /// datagram) resolved for this reader. Defaults to
  /// [`FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE`] until [`resolve_path_mtu`] runs.
  ///
  /// [`resolve_path_mtu`]: Self::resolve_path_mtu
  pub fn max_datagram_payload(&self) -> usize {
    self.max_datagram_payload
  }

  /// Recompute this reader's [`max_datagram_payload`](Self::max_datagram_payload)
  /// from its advertised unicast locators and the local interface table.
  ///
  /// We take the minimum budget over all of the reader's unicast locators so
  /// that whichever path the [`SendRoute`] ends up using, the datagram fits.
  /// Loopback locators were already stripped from `unicast_locator_list`, so a
  /// same-host peer is resolved via its LAN address (whose egress interface is
  /// the one actually configured for that subnet). When the reader advertises
  /// no unicast locators, we keep the conservative default.
  pub fn resolve_path_mtu(&mut self, local_interfaces: &[IfAddr]) {
    let budget = self
      .unicast_locator_list
      .iter()
      .filter(|l| l.is_udp())
      .map(|l| path_mtu_payload_for_peer(local_interfaces, SocketAddr::from(*l).ip()))
      .min();
    self.max_datagram_payload = budget.unwrap_or(FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE);
  }

  pub fn handle_ack_nack(
    &mut self,
    ack_submessage: &AckSubmessage,
    last_available: SequenceNumber,
  ) {
    match ack_submessage {
      AckSubmessage::AckNack(acknack) => {
        // Eliminate case that base = 0
        let new_all_acked_before = max(acknack.reader_sn_state.base(), SequenceNumber::from(1));
        // Sending acknack with sn_state base = 0 should not happen.
        // This is not allowed by  SequenceNumberSet
        // validity rules (RTPS Spec v2.5 "8.3.5.5 SequenceNumberSet")
        //
        // The correct way to acknowledge that nothing has been received is to
        // send ACKNACK with reader_sn_state.base = 1 and empty set contents.
        // This means everything before 1 has been received, but since
        // sequence numbering starts at 1 by definition
        // (in Section 8.3.5.4 SequenceNumber), it means "nothing"
        //
        // This is logged in `writer` object.

        // Ignore ACKNACK whose readerSNState.base claims ack beyond last_seq + 1 (the
        // writer's last change sequence number). Matches reliable writer/reader
        // semantics; a forged base would otherwise pin all_acked_before and
        // block real ACKNACKs (issue #405).
        let max_plausible_acked_before = last_available.plus_1();
        if new_all_acked_before > max_plausible_acked_before {
          warn!(
            "Ignoring ACKNACK: base {:?} implies ack through unsent data (last sent seq={:?}, max \
             plausible all-acked-before {:?}) reader={:x?}",
            new_all_acked_before,
            last_available,
            max_plausible_acked_before,
            self.remote_reader_guid
          );
          return;
        }

        // sanity check:
        if new_all_acked_before < self.all_acked_before {
          error!(
            "all_acked_before updated backwards! old={:?} new={:?}",
            self.all_acked_before, new_all_acked_before
          );
        }
        self.remove_from_unsent_set_all_before(new_all_acked_before); // update anyway
        self.all_acked_before = new_all_acked_before;

        // Insert the requested changes. These are (by construction) greater
        // then new_all_acked_before.
        for nack_sn in acknack.reader_sn_state.iter() {
          self.unsent_changes.insert(nack_sn);
        }
        // sanity check
        if let Some(&high) = self.unsent_changes.iter().next_back() {
          if high > last_available {
            warn!(
              "ReaderProxy {:?} asks for {:?} but I have only up to {:?}. Truncating request. \
               ACKNACK = {:?}",
              self.remote_reader_guid, self.unsent_changes, last_available, acknack
            );
            // Requesting something which is not yet available is unreasonable.
            // Ignore the request from last_available + 1 onwards.
            self.unsent_changes.split_off(&last_available.plus_1());
          }
        }
        // AckNack also clears pending_gap
        self.pending_gap = self.pending_gap.split_off(&self.all_acked_before);
      }

      AckSubmessage::NackFrag(_nack_frag) => {
        // TODO
        error!("NACKFRAG not implemented");
      }
    }
  }

  pub fn insert_pending_gap(&mut self, seq_num: SequenceNumber) {
    self.pending_gap.insert(seq_num);
  }

  pub fn set_pending_gap_up_to(&mut self, last_gap_sn: SequenceNumber) {
    // form SN range from 1 to last_gap_sn (inclusive)
    let gap_sn_range = SequenceNumberRange::new(SequenceNumber::new(1), last_gap_sn);
    // Convert to a set and insert the SNs to pending_gap
    let gap_sn_set = BTreeSet::from_iter(gap_sn_range);
    self.pending_gap.extend(gap_sn_set);
  }

  pub fn get_pending_gap(&self) -> &BTreeSet<SequenceNumber> {
    &self.pending_gap
  }

  /// this should be called every time a new CacheChange is set to RTPS writer
  /// HistoryCache
  pub fn notify_new_cache_change(&mut self, sequence_number: SequenceNumber) {
    if sequence_number == SequenceNumber::from(0) {
      error!(
        "new cache change with {:?}! bad! my GUID = {:?}",
        sequence_number, self.remote_reader_guid
      );
    }
    self.unsent_changes.insert(sequence_number);

    // Memory-safety backstop: never let this set grow without bound. In normal
    // operation it is pruned as samples are pushed (see Writer::process_pending)
    // or acknowledged (handle_ack_nack), but a best-effort flood sends no
    // ACKNACKs, so cap the set and drop the oldest (least-useful-to-resend)
    // entries if it ever exceeds the cap.
    while self.unsent_changes.len() > MAX_UNSENT_CHANGES_PER_READER {
      if let Some(&oldest) = self.unsent_changes.iter().next() {
        self.unsent_changes.remove(&oldest);
      } else {
        break;
      }
    }
  }

  #[cfg(test)]
  pub fn unsent_changes_count(&self) -> usize {
    self.unsent_changes.len()
  }

  pub fn acked_up_to_before(&self) -> SequenceNumber {
    self.all_acked_before
  }

  // Fragment handling

  pub fn mark_all_frags_requested(&mut self, seq_num: SequenceNumber, frag_count: u32) {
    // Insert all ones set with frag_count bits
    self
      .frags_requested
      // TODO: explain why unwrap below succeeds
      .insert(
        seq_num,
        BitVec::from_elem(frag_count.try_into().unwrap(), true),
      );
  }

  pub fn mark_frags_requested(&mut self, seq_num: SequenceNumber, frag_nums: &FragmentNumberSet) {
    let req_set = self
      .frags_requested
      .entry(seq_num)
      .or_insert_with(|| BitVec::with_capacity(64)); // default capacity out of hat

    for f in frag_nums.iter() {
      // -1 because FragmentNumbers start at 1
      let idx = usize::from(f) - 1;
      // The bit vector must be long enough to address `idx`. Grow it (filling
      // with `false`) whenever a requested fragment number exceeds the current
      // length, otherwise `set()` would be out of bounds. Note that a freshly
      // inserted `BitVec` has length 0 regardless of its capacity.
      if idx >= req_set.len() {
        req_set.grow(idx + 1 - req_set.len(), false);
      }
      req_set.set(idx, true);
    }
  }

  // This just removes the FragmentNumber entry from the set.
  pub fn mark_frag_sent(&mut self, seq_num: SequenceNumber, frag_num: &FragmentNumber) {
    let mut frag_map_emptied = false;
    if let Some(frag_map) = self.frags_requested.get_mut(&seq_num) {
      // -1 because FragmentNumbers start at 1
      frag_map.set(usize::from(*frag_num) - 1, false);
      frag_map_emptied = frag_map.none();
    }
    if frag_map_emptied {
      self.frags_requested.remove(&seq_num);
    }
  }

  // Note: The current implementation produces an iterator that iterates only
  // over one fragmented sample, but the upper layer should detect that
  // there are still other fragmented samples requested (if any)
  // and continue sending.
  pub fn frags_requested_iterator(&self) -> FragBitVecIterator {
    match self.frags_requested.iter().next() {
      None => FragBitVecIterator::new(SequenceNumber::default(), BitVec::new()), // empty iterator
      Some((sn, bv)) => FragBitVecIterator::new(*sn, bv.clone()),
    }
  }

  pub fn repair_frags_requested(&self) -> bool {
    self.frags_requested.values().any(|rf| rf.any())
  }
}

pub struct FragBitVecIterator {
  sequence_number: SequenceNumber,
  frag_count: FragmentNumber,
  bitvec: BitVec,
}

impl FragBitVecIterator {
  pub fn new(sequence_number: SequenceNumber, bv: BitVec) -> FragBitVecIterator {
    FragBitVecIterator {
      sequence_number,
      frag_count: FragmentNumber::new(1),
      bitvec: bv,
    }
  }
}

impl Iterator for FragBitVecIterator {
  type Item = (SequenceNumber, FragmentNumber);

  fn next(&mut self) -> Option<Self::Item> {
    // f indexes from 1, like FragmentNumber
    let mut f = u32::from(self.frag_count);
    while (f as usize) <= self.bitvec.len() && self.bitvec.get((f - 1) as usize) == Some(false) {
      f += 1;
    }
    if (f as usize) > self.bitvec.len() {
      None
    } else {
      self.frag_count = FragmentNumber::new(f + 1);
      Some((self.sequence_number, FragmentNumber::new(f)))
    }
  }
}

// pub enum ChangeForReaderStatusKind {
//   UNSENT,
//   NACKNOWLEDGED,
//   REQUESTED,
//   ACKNOWLEDGED,
//   UNDERWAY,
// }

// ///The RTPS ChangeForReader is an association class that maintains
// information of a CacheChange in the RTPS ///Writer HistoryCache as it
// pertains to the RTPS Reader represented by the ReaderProxy pub struct
// RTPSChangeForReader {   ///Indicates the status of a CacheChange relative to
// the RTPS Reader represented by the ReaderProxy.   pub kind:
// ChangeForReaderStatusKind,   ///Indicates whether the change is relevant to
// the RTPS Reader represented by the ReaderProxy.   pub is_relevant: bool,
// }

// impl RTPSChangeForReader {
//   pub fn new() -> RTPSChangeForReader {
//     RTPSChangeForReader {
//       kind: ChangeForReaderStatusKind::UNSENT,
//       is_relevant: true,
//     }
//   }
// }

#[cfg(test)]
mod bounded_unsent_tests {
  use super::*;
  use crate::structure::guid::GuidPrefix;

  fn test_proxy() -> RtpsReaderProxy {
    let guid = GUID::new(GuidPrefix::UNKNOWN, EntityId::UNKNOWN);
    RtpsReaderProxy::new(guid, QosPolicies::default(), false)
  }

  // Regression: the push path (Writer::process_pending) now prunes each sample
  // from unsent_changes via mark_change_sent, so a best-effort flood (no
  // ACKNACKs) does not leave one entry per sample behind. Model that here.
  #[test]
  fn unsent_changes_do_not_grow_when_pruned_on_push() {
    let mut rp = test_proxy();
    for i in 1..=50_000 {
      let sn = SequenceNumber::new(i);
      rp.notify_new_cache_change(sn);
      rp.mark_change_sent(sn); // Writer::process_pending does this on Complete/drop.
    }
    assert_eq!(
      rp.unsent_changes_count(),
      0,
      "unsent_changes should be empty after every sample is marked sent"
    );
  }

  // Regression / safety net: even if pruning never happened (pathological peer),
  // the hard cap must keep unsent_changes bounded.
  #[test]
  fn unsent_changes_bounded_by_hard_cap() {
    let mut rp = test_proxy();
    let n = (MAX_UNSENT_CHANGES_PER_READER as i64) * 4;
    for i in 1..=n {
      rp.notify_new_cache_change(SequenceNumber::new(i));
    }
    assert!(
      rp.unsent_changes_count() <= MAX_UNSENT_CHANGES_PER_READER,
      "unsent_changes exceeded cap {}: {} entries",
      MAX_UNSENT_CHANGES_PER_READER,
      rp.unsent_changes_count()
    );
  }
}

#[cfg(test)]
mod acknack_tests {
  use super::*;
  use crate::{
    messages::submessages::{ack_nack::AckNack, submessage::AckSubmessage},
    structure::{guid::GuidPrefix, sequence_number::SequenceNumberSet},
  };

  #[test]
  fn acknack_with_base_beyond_sent_data_does_not_advance_all_acked_before() {
    let guid = GUID::new(GuidPrefix::UNKNOWN, EntityId::UNKNOWN);
    let mut rp = RtpsReaderProxy::new(guid, QosPolicies::default(), false);
    rp.all_acked_before = SequenceNumber::from(10);

    let forged_base = SequenceNumber::new(4_611_686_018_427_387_993);
    let ack = AckNack {
      reader_id: EntityId::UNKNOWN,
      writer_id: EntityId::UNKNOWN,
      reader_sn_state: SequenceNumberSet::new_empty(forged_base),
      count: 1,
    };

    rp.handle_ack_nack(&AckSubmessage::AckNack(ack), SequenceNumber::from(100));

    assert_eq!(rp.all_acked_before, SequenceNumber::from(10));
  }

  #[test]
  fn acknack_with_valid_base_updates_all_acked_before() {
    let guid = GUID::new(GuidPrefix::UNKNOWN, EntityId::UNKNOWN);
    let mut rp = RtpsReaderProxy::new(guid, QosPolicies::default(), false);

    let ack = AckNack {
      reader_id: EntityId::UNKNOWN,
      writer_id: EntityId::UNKNOWN,
      reader_sn_state: SequenceNumberSet::new_empty(SequenceNumber::from(50)),
      count: 1,
    };

    rp.handle_ack_nack(&AckSubmessage::AckNack(ack), SequenceNumber::from(100));

    assert_eq!(rp.all_acked_before, SequenceNumber::from(50));
  }
}

#[cfg(test)]
mod route_tests {
  use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

  use super::*;
  use crate::{
    rtps::transmit::{DefaultRouteSelector, InterfaceObservations, InterfaceSelector},
    structure::guid::GuidPrefix,
  };

  fn udp(ip: [u8; 4], port: u16) -> Locator {
    Locator::UdpV4(SocketAddrV4::new(
      Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]),
      port,
    ))
  }

  fn iface(ip: [u8; 4]) -> InterfaceSelector {
    InterfaceSelector::Ip(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])))
  }

  fn proxy_with_prefix(prefix: GuidPrefix) -> RtpsReaderProxy {
    let guid = GUID::new(prefix, EntityId::UNKNOWN);
    RtpsReaderProxy::new(guid, QosPolicies::default(), false)
  }

  #[test]
  fn resolve_falls_back_without_observation() {
    let mut rp = proxy_with_prefix(GuidPrefix::UNKNOWN);
    rp.unicast_locator_list = vec![udp([10, 0, 0, 5], 7410)];
    let observations = InterfaceObservations::new();
    rp.resolve_send_route(
      &observations,
      &[iface([10, 0, 0, 1])],
      &DefaultRouteSelector,
    );
    assert!(rp.send_route().fallback);
  }

  #[test]
  fn resolve_narrows_with_observation() {
    let prefix = GuidPrefix::new(&[9; 12]);
    let mut rp = proxy_with_prefix(prefix);
    rp.unicast_locator_list = vec![udp([10, 0, 0, 5], 7410)];
    rp.multicast_locator_list = vec![udp([239, 255, 0, 1], 7401)];

    let mut observations = InterfaceObservations::new();
    observations.record(
      prefix,
      Some(iface([10, 0, 0, 1])),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 7410),
    );

    rp.resolve_send_route(
      &observations,
      &[iface([10, 0, 0, 1])],
      &DefaultRouteSelector,
    );

    let route = rp.send_route();
    assert!(!route.fallback);
    assert_eq!(route.unicast, Some(udp([10, 0, 0, 5], 7410)));
    assert_eq!(
      route.multicast,
      Some((udp([239, 255, 0, 1], 7401), iface([10, 0, 0, 1])))
    );
  }
}
