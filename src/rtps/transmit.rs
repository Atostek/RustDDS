//! Interface-aware transmit-locator selection.
//!
//! See `src/rtps/transmit_design.md` for the full design rationale.
//!
//! The goal of these types is to let a writer send exactly one datagram per
//! distinct destination "route" instead of blindly sending to every locator a
//! remote advertised on every local interface.
//!
//! Terminology:
//! - [`InterfaceSelector`] identifies a *local* egress interface.
//! - [`SendRoute`] is the resolved destination for one remote reader: at most
//!   one unicast locator and at most one interface-tagged multicast locator.
//! - [`RouteKey`] is the de-duplication key: two readers that resolve to the
//!   same `RouteKey` are served by a single datagram.
//! - [`InterfaceObservations`] records, per remote participant, on which local
//!   interface(s) we have actually seen its traffic arrive. This is the primary
//!   input to route selection.
//!
//! Safety guardrail: when we do not have enough information to narrow a remote's
//! route confidently, [`SendRoute::fallback`] is set and the writer falls back
//! to the legacy "send to every advertised locator on every interface" path, so
//! reachability is never reduced.

use std::{
  collections::BTreeMap,
  net::{IpAddr, SocketAddr},
  time::{Duration, Instant},
};

use crate::structure::{guid::GuidPrefix, locator::Locator};

/// Hysteresis margin for switching a remote participant's chosen multicast
/// egress interface. Once an interface is chosen it stays chosen until we have
/// not heard from it for at least this long *and* the participant's traffic is
/// arriving on a different interface. This keeps the route stable across
/// occasional stray packets on a secondary interface.
pub(crate) const STICKY_SWITCH_MARGIN: Duration = Duration::from_secs(30);

/// Identifies a local network interface to use as the egress for multicast.
///
/// Modelled as the interface's IP address for now; the enum leaves room for an
/// OS interface index variant should the IP prove insufficient (e.g. multiple
/// interfaces sharing an address, or IPv6 scope handling).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InterfaceSelector {
  Ip(IpAddr),
}

/// The resolved send destination for a single remote reader.
///
/// `fallback == true` means "we could not narrow this confidently; use the
/// legacy all-locators/all-interfaces path". In that case `unicast`/`multicast`
/// should be ignored by the sender.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SendRoute {
  pub unicast: Option<Locator>,
  pub multicast: Option<(Locator, InterfaceSelector)>,
  pub fallback: bool,
}

impl SendRoute {
  /// A route that instructs the sender to use the legacy behavior.
  pub fn fallback() -> Self {
    Self {
      unicast: None,
      multicast: None,
      fallback: true,
    }
  }
}

impl Default for SendRoute {
  fn default() -> Self {
    // Until a route is resolved, behave exactly like today.
    Self::fallback()
  }
}

/// De-duplication key for a concrete outbound datagram destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RouteKey {
  Unicast(Locator),
  Multicast(Locator, InterfaceSelector),
}

/// One local interface's observation record for a remote participant.
#[derive(Clone, Debug)]
pub struct InterfaceObservation {
  pub last_seen: Instant,
  /// Number of packets observed on this interface. Retained for diagnostics and
  /// possible future selection policies; the current sticky heuristic decides
  /// switches purely on `last_seen`.
  #[allow(dead_code)]
  pub count: u64,
  pub source: Option<SocketAddr>,
}

/// What we have observed about how to reach a single remote participant.
#[derive(Clone, Debug, Default)]
pub struct ObservedRoutes {
  /// Local interfaces on which we have seen this participant's traffic.
  by_iface: BTreeMap<InterfaceSelector, InterfaceObservation>,
  /// Most recent source socket address seen for this participant, regardless of
  /// whether the receiving interface could be determined.
  last_source: Option<SocketAddr>,
  /// The interface currently chosen for multicast egress to this participant.
  /// Updated with hysteresis (see [`STICKY_SWITCH_MARGIN`]) so it does not flip
  /// on a single stray packet arriving on another interface.
  current_interface: Option<InterfaceSelector>,
}

impl ObservedRoutes {
  fn record(&mut self, iface: Option<InterfaceSelector>, source: SocketAddr) {
    self.record_at(iface, source, Instant::now(), STICKY_SWITCH_MARGIN);
  }

  /// Testable core of [`Self::record`]: `now` and `margin` are injected so the
  /// hysteresis can be exercised deterministically.
  fn record_at(
    &mut self,
    iface: Option<InterfaceSelector>,
    source: SocketAddr,
    now: Instant,
    margin: Duration,
  ) {
    self.last_source = Some(source);
    if let Some(iface) = iface {
      self
        .by_iface
        .entry(iface)
        .and_modify(|o| {
          o.last_seen = now;
          o.count = o.count.saturating_add(1);
          o.source = Some(source);
        })
        .or_insert(InterfaceObservation {
          last_seen: now,
          count: 1,
          source: Some(source),
        });

      // Sticky choice: a switch is only ever triggered by receiving on a
      // non-current interface (the challenger, whose `last_seen` is `now`).
      // Displace the current interface only if we have not heard from it for at
      // least `margin`; otherwise the current interface stays chosen. A current
      // interface that keeps receiving traffic is thus never displaced.
      match self.current_interface {
        None => self.current_interface = Some(iface),
        Some(cur) if cur == iface => { /* refreshed the current choice; keep */ }
        Some(cur) => {
          let displace = self
            .by_iface
            .get(&cur)
            .is_none_or(|o| now.saturating_duration_since(o.last_seen) >= margin);
          if displace {
            self.current_interface = Some(iface);
          }
        }
      }
    }
  }

  /// The most recently observed source address, if any.
  pub fn last_source(&self) -> Option<SocketAddr> {
    self.last_source
  }

  /// The local interface currently chosen to reach this participant.
  ///
  /// This is sticky: it stays on the previously chosen interface until the
  /// participant's traffic has been arriving on a different interface and the
  /// old one has been silent for at least [`STICKY_SWITCH_MARGIN`] (see
  /// [`Self::record_at`]). `None` if we have never determined a receiving
  /// interface for this participant.
  pub fn best_interface(&self) -> Option<InterfaceSelector> {
    self.current_interface
  }

  /// Number of distinct local interfaces this participant has been seen on.
  #[cfg(test)]
  pub fn interface_count(&self) -> usize {
    self.by_iface.len()
  }
}

/// Per-remote-participant record of observed receive interfaces / source
/// addresses. Populated by the message receiver, consumed by route resolution.
#[derive(Debug, Default)]
pub struct InterfaceObservations {
  by_participant: BTreeMap<GuidPrefix, ObservedRoutes>,
}

impl InterfaceObservations {
  pub fn new() -> Self {
    Self::default()
  }

  /// Record that a packet from `prefix` arrived from `source`, on local
  /// interface `iface` (if it could be determined).
  pub fn record(&mut self, prefix: GuidPrefix, iface: Option<InterfaceSelector>, source: SocketAddr) {
    self
      .by_participant
      .entry(prefix)
      .or_default()
      .record(iface, source);
  }

  pub fn get(&self, prefix: GuidPrefix) -> Option<&ObservedRoutes> {
    self.by_participant.get(&prefix)
  }

  pub fn remove(&mut self, prefix: GuidPrefix) {
    self.by_participant.remove(&prefix);
  }
}

/// Strategy for turning advertised locators + observations into a [`SendRoute`].
///
/// Kept as a trait so the heuristic can evolve (or be swapped) without touching
/// the transmit path.
pub trait RouteSelector {
  fn select(
    &self,
    advertised_unicast: &[Locator],
    advertised_multicast: &[Locator],
    observed: Option<&ObservedRoutes>,
    local_multicast_ifaces: &[InterfaceSelector],
  ) -> SendRoute;
}

/// Conservative default policy.
///
/// - Without any observation for the remote, returns [`SendRoute::fallback`].
/// - With an observation, chooses the observed interface for multicast (only if
///   it is one of our local multicast interfaces) and the advertised unicast
///   locator that matches the observed source address.
/// - Whenever narrowing would risk dropping reachability (e.g. a multicast
///   locator is advertised but its interface cannot be determined, or several
///   unicast candidates cannot be disambiguated), it returns the fallback route
///   rather than guessing.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultRouteSelector;

fn first_reachable_udp(locators: &[Locator]) -> Option<Locator> {
  locators
    .iter()
    .copied()
    .find(|l| l.is_udp() && !l.is_loopback())
}

impl RouteSelector for DefaultRouteSelector {
  fn select(
    &self,
    advertised_unicast: &[Locator],
    advertised_multicast: &[Locator],
    observed: Option<&ObservedRoutes>,
    local_multicast_ifaces: &[InterfaceSelector],
  ) -> SendRoute {
    // No origin knowledge -> cannot narrow safely.
    let Some(obs) = observed else {
      return SendRoute::fallback();
    };

    let mc_advertised = first_reachable_udp(advertised_multicast);

    // Pick the egress interface only if it is genuinely one of ours.
    let chosen_iface = obs
      .best_interface()
      .filter(|iface| local_multicast_ifaces.contains(iface));

    let multicast = match (mc_advertised, chosen_iface) {
      (Some(mc), Some(iface)) => Some((mc, iface)),
      _ => None,
    };

    // If the remote advertises multicast but we cannot bind it to a local
    // interface, do not silently drop it: fall back to the legacy path.
    if mc_advertised.is_some() && multicast.is_none() {
      return SendRoute::fallback();
    }

    let unicast = select_unicast(advertised_unicast, obs);

    // Nothing we can confidently send to -> fallback (also covers the case of
    // several ambiguous unicast candidates and no multicast).
    if unicast.is_none() && multicast.is_none() {
      return SendRoute::fallback();
    }

    SendRoute {
      unicast,
      multicast,
      fallback: false,
    }
  }
}

/// Choose a single unicast locator, or `None` if the choice is ambiguous.
fn select_unicast(advertised_unicast: &[Locator], obs: &ObservedRoutes) -> Option<Locator> {
  let candidates: Vec<Locator> = advertised_unicast
    .iter()
    .copied()
    .filter(|l| l.is_udp() && !l.is_loopback())
    .collect();

  match candidates.len() {
    0 => None,
    1 => Some(candidates[0]),
    _ => {
      // Multiple advertised addresses: only pick one if the observed source
      // address disambiguates it. Otherwise stay ambiguous (caller falls back).
      let source_ip = obs.last_source().map(|sa| sa.ip());
      source_ip.and_then(|ip| {
        candidates
          .iter()
          .copied()
          .find(|l| SocketAddr::from(*l).ip() == ip)
      })
    }
  }
}

#[cfg(test)]
mod tests {
  use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

  use super::*;
  use crate::structure::guid::GuidPrefix;

  fn udp(ip: [u8; 4], port: u16) -> Locator {
    Locator::UdpV4(SocketAddrV4::new(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]), port))
  }

  fn sockaddr(ip: [u8; 4], port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), port)
  }

  fn iface(ip: [u8; 4]) -> InterfaceSelector {
    InterfaceSelector::Ip(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])))
  }

  #[test]
  fn no_observation_is_fallback() {
    let sel = DefaultRouteSelector;
    let route = sel.select(&[udp([10, 0, 0, 5], 7410)], &[], None, &[]);
    assert!(route.fallback);
  }

  #[test]
  fn single_unicast_with_observation_narrows() {
    let sel = DefaultRouteSelector;
    let mut obs = ObservedRoutes::default();
    obs.record(Some(iface([10, 0, 0, 1])), sockaddr([10, 0, 0, 5], 7410));
    let route = sel.select(
      &[udp([10, 0, 0, 5], 7410)],
      &[],
      Some(&obs),
      &[iface([10, 0, 0, 1])],
    );
    assert!(!route.fallback);
    assert_eq!(route.unicast, Some(udp([10, 0, 0, 5], 7410)));
    assert_eq!(route.multicast, None);
  }

  #[test]
  fn multicast_tagged_with_observed_interface() {
    let sel = DefaultRouteSelector;
    let mut obs = ObservedRoutes::default();
    obs.record(Some(iface([10, 0, 0, 1])), sockaddr([10, 0, 0, 5], 7410));
    let mc = udp([239, 255, 0, 1], 7401);
    let route = sel.select(
      &[udp([10, 0, 0, 5], 7410)],
      &[mc],
      Some(&obs),
      &[iface([10, 0, 0, 1])],
    );
    assert_eq!(route.multicast, Some((mc, iface([10, 0, 0, 1]))));
    assert!(!route.fallback);
  }

  #[test]
  fn advertised_multicast_but_unknown_interface_falls_back() {
    let sel = DefaultRouteSelector;
    let mut obs = ObservedRoutes::default();
    // Observation without a resolvable local interface (unicast only source).
    obs.record(None, sockaddr([10, 0, 0, 5], 7410));
    let mc = udp([239, 255, 0, 1], 7401);
    let route = sel.select(&[udp([10, 0, 0, 5], 7410)], &[mc], Some(&obs), &[iface([10, 0, 0, 1])]);
    assert!(route.fallback);
  }

  #[test]
  fn ambiguous_multi_unicast_without_source_match_falls_back() {
    let sel = DefaultRouteSelector;
    let mut obs = ObservedRoutes::default();
    obs.record(None, sockaddr([172, 16, 0, 9], 7410));
    // Two advertised addresses, neither matching the observed source IP.
    let route = sel.select(
      &[udp([10, 0, 0, 5], 7410), udp([192, 168, 1, 5], 7410)],
      &[],
      Some(&obs),
      &[],
    );
    assert!(route.fallback);
  }

  #[test]
  fn multi_unicast_disambiguated_by_source() {
    let sel = DefaultRouteSelector;
    let mut obs = ObservedRoutes::default();
    obs.record(Some(iface([10, 0, 0, 1])), sockaddr([192, 168, 1, 5], 7410));
    let route = sel.select(
      &[udp([10, 0, 0, 5], 7410), udp([192, 168, 1, 5], 7410)],
      &[],
      Some(&obs),
      &[iface([10, 0, 0, 1])],
    );
    assert_eq!(route.unicast, Some(udp([192, 168, 1, 5], 7410)));
    assert!(!route.fallback);
  }

  #[test]
  fn route_key_dedup() {
    use std::collections::BTreeSet;
    let mut set = BTreeSet::new();
    let k1 = RouteKey::Multicast(udp([239, 255, 0, 1], 7401), iface([10, 0, 0, 1]));
    let k2 = RouteKey::Multicast(udp([239, 255, 0, 1], 7401), iface([10, 0, 0, 1]));
    let k3 = RouteKey::Unicast(udp([10, 0, 0, 5], 7410));
    assert!(set.insert(k1));
    assert!(!set.insert(k2)); // duplicate
    assert!(set.insert(k3));
    assert_eq!(set.len(), 2);
  }

  #[test]
  fn observations_sticky_within_margin() {
    // Recording A, A, then B microseconds apart (default 30s margin) must NOT
    // flip the chosen interface: A was seen well within the margin.
    let mut obs = InterfaceObservations::new();
    let p = GuidPrefix::UNKNOWN;
    obs.record(p, Some(iface([10, 0, 0, 1])), sockaddr([10, 0, 0, 5], 7410));
    obs.record(p, Some(iface([10, 0, 0, 1])), sockaddr([10, 0, 0, 5], 7410));
    obs.record(p, Some(iface([192, 168, 1, 1])), sockaddr([192, 168, 1, 5], 7410));
    let recorded = obs.get(p).unwrap();
    assert_eq!(recorded.interface_count(), 2);
    // Sticky: the first-chosen interface stays.
    assert_eq!(recorded.best_interface(), Some(iface([10, 0, 0, 1])));
  }

  #[test]
  fn sticky_first_observation_is_chosen() {
    let mut obs = ObservedRoutes::default();
    let t0 = Instant::now();
    obs.record_at(
      Some(iface([10, 0, 0, 1])),
      sockaddr([10, 0, 0, 5], 7410),
      t0,
      Duration::from_secs(30),
    );
    assert_eq!(obs.best_interface(), Some(iface([10, 0, 0, 1])));
  }

  #[test]
  fn sticky_does_not_switch_within_margin() {
    let mut obs = ObservedRoutes::default();
    let margin = Duration::from_secs(30);
    let t0 = Instant::now();
    obs.record_at(Some(iface([10, 0, 0, 1])), sockaddr([10, 0, 0, 5], 7410), t0, margin);
    // Challenger arrives before the current interface has been silent for the
    // full margin -> no switch.
    obs.record_at(
      Some(iface([192, 168, 1, 1])),
      sockaddr([192, 168, 1, 5], 7410),
      t0 + Duration::from_secs(29),
      margin,
    );
    assert_eq!(obs.best_interface(), Some(iface([10, 0, 0, 1])));
  }

  #[test]
  fn sticky_switches_past_margin() {
    let mut obs = ObservedRoutes::default();
    let margin = Duration::from_secs(30);
    let t0 = Instant::now();
    obs.record_at(Some(iface([10, 0, 0, 1])), sockaddr([10, 0, 0, 5], 7410), t0, margin);
    // Current interface has been silent for >= margin when the challenger is
    // heard -> switch.
    obs.record_at(
      Some(iface([192, 168, 1, 1])),
      sockaddr([192, 168, 1, 5], 7410),
      t0 + Duration::from_secs(30),
      margin,
    );
    assert_eq!(obs.best_interface(), Some(iface([192, 168, 1, 1])));
  }

  #[test]
  fn sticky_busy_current_never_displaced() {
    let mut obs = ObservedRoutes::default();
    let margin = Duration::from_secs(30);
    let t0 = Instant::now();
    let current = iface([10, 0, 0, 1]);
    let challenger = iface([192, 168, 1, 1]);
    obs.record_at(Some(current), sockaddr([10, 0, 0, 5], 7410), t0, margin);
    // Interleave: the current interface keeps receiving every 10s, while an
    // intermittent challenger also shows up. The current interface is never
    // silent for a full margin, so it is never displaced.
    for k in 1..=6 {
      let t = t0 + Duration::from_secs(10 * k);
      obs.record_at(Some(challenger), sockaddr([192, 168, 1, 5], 7410), t, margin);
      obs.record_at(
        Some(current),
        sockaddr([10, 0, 0, 5], 7410),
        t + Duration::from_secs(1),
        margin,
      );
    }
    assert_eq!(obs.best_interface(), Some(current));
  }
}
