use std::{
  collections::HashMap,
  io,
  net::{IpAddr, Ipv4Addr, SocketAddr},
};

use log::{error, warn};

use crate::{
  rtps::{
    constant::{payload_budget_for_mtu, FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE},
    transmit::InterfaceSelector,
  },
  structure::locator::Locator,
};

/// Platform-neutral view of one network-interface address.
///
/// [`netdev`] gives us a single cross-platform enumeration; we normalize its
/// per-interface data into this small struct (one entry per assigned IP). The
/// inner helper functions then operate on `IfAddr` alone, which keeps them free
/// of any enumeration dependency and makes them trivially unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IfAddr {
  /// An IP address bound to the interface.
  pub ip: IpAddr,
  /// OS interface index. `0` when unknown / not applicable.
  pub index: u32,
  pub is_loopback: bool,
  /// Whether the interface is multicast-capable.
  pub is_multicast: bool,
  /// IPv4 subnet mask for `ip` (only populated for IPv4 addresses). Used to
  /// decide whether a peer is on the same subnet (i.e. reachable in one hop).
  pub netmask: Option<Ipv4Addr>,
  /// Interface MTU in bytes, when the OS reports it. Drives the per-peer
  /// datagram-payload budget for same-subnet peers.
  pub mtu: Option<u32>,
}

// ---------------------------------------------------------------------------
// Interface enumeration (cross-platform, via netdev)
// ---------------------------------------------------------------------------

/// Enumerate all interface addresses across platforms using [`netdev`].
///
/// `netdev::get_interfaces()` yields one [`netdev::Interface`] per interface
/// with its index, MTU, loopback/multicast flags and the list of assigned
/// IPv4/IPv6 prefixes. We flatten this into one [`IfAddr`] per assigned IP so
/// the existing locator/multicast/ifindex helpers keep working unchanged, while
/// the new fields (`netmask`, `mtu`) enable per-peer path-MTU resolution.
///
/// This is infallible in practice (netdev returns an empty list rather than an
/// error), but the signature is kept as `io::Result` for API compatibility with
/// the previous per-platform implementations.
fn enumerate_interfaces() -> io::Result<Vec<IfAddr>> {
  let mut result = Vec::new();

  for iface in netdev::get_interfaces() {
    let index = iface.index;
    let is_loopback = iface.is_loopback();
    let is_multicast = iface.is_multicast();
    let mtu = iface.mtu;

    for net in &iface.ipv4 {
      result.push(IfAddr {
        ip: IpAddr::V4(net.addr()),
        index,
        is_loopback,
        is_multicast,
        netmask: Some(net.netmask()),
        mtu,
      });
    }
    for net in &iface.ipv6 {
      result.push(IfAddr {
        ip: IpAddr::V6(net.addr()),
        index,
        is_loopback,
        is_multicast,
        // IPv4-only subnet matching for now; IPv6 peers fall back to default.
        netmask: None,
        mtu,
      });
    }
  }

  Ok(result)
}

// ---------------------------------------------------------------------------
// Per-peer path-MTU resolution
// ---------------------------------------------------------------------------

/// Snapshot of the local interface table, used by writers to resolve a per-peer
/// datagram budget. Built once (and refreshed on interface-set changes) and
/// shared with each `Writer` via the event loop.
pub fn local_interface_table() -> Vec<IfAddr> {
  match enumerate_interfaces() {
    Ok(ifaces) => ifaces,
    Err(e) => {
      error!("Cannot enumerate local interfaces for path-MTU resolution: {e:?}");
      Vec::new()
    }
  }
}

/// Resolve the per-peer UDP-payload budget (bytes available for RTPS submessages
/// in one datagram) for `peer_ip`, using the "local egress-interface MTU"
/// heuristic:
///
/// * If some local IPv4 interface's subnet contains `peer_ip`, the peer is
///   reachable in one hop, so we use that interface's MTU (minus headers).
///   Loopback peers (e.g. `127.0.0.1`) naturally match `lo`/`lo0` and inherit
///   its (often large) MTU.
/// * Otherwise the peer is behind a router (or IPv6 / unresolved), where the
///   real path MTU is unknown; fall back to [`FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE`]
///   (a conservative ~1500-byte-Ethernet budget).
///
/// An overestimate is a soft failure (IP fragmentation), never data loss, so
/// this errs toward the safe default when unsure.
pub fn path_mtu_payload_for_peer(ifaces: &[IfAddr], peer_ip: IpAddr) -> usize {
  if let IpAddr::V4(peer) = peer_ip {
    for ifa in ifaces {
      let (IpAddr::V4(local), Some(mask)) = (ifa.ip, ifa.netmask) else {
        continue;
      };
      if same_subnet_v4(local, mask, peer) {
        if let Some(mtu) = ifa.mtu {
          return payload_budget_for_mtu(mtu);
        }
        // Same subnet but MTU unknown: keep scanning in case another matching
        // interface reports one; otherwise we fall through to the default.
      }
    }
  }
  FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE
}

/// True when `local` and `peer` share the IPv4 subnet defined by `mask`.
fn same_subnet_v4(local: Ipv4Addr, mask: Ipv4Addr, peer: Ipv4Addr) -> bool {
  let m = u32::from(mask);
  (u32::from(local) & m) == (u32::from(peer) & m)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub fn get_local_multicast_locators(port: u16) -> Vec<Locator> {
  let saddr = SocketAddr::new("239.255.0.1".parse().unwrap(), port);
  vec![Locator::from(saddr)]
}

pub fn get_local_unicast_locators_filtered(
  port: u16,
  only_networks: Option<&[IpAddr]>,
) -> Vec<Locator> {
  match enumerate_interfaces() {
    Ok(ifaces) => {
      let result = get_local_unicast_locators_inner(&ifaces, port, only_networks);
      if result.is_empty() {
        if let Some(nets) = only_networks {
          warn!(
            "only_networks filter {:?} matched no unicast interfaces; this participant will be \
             invisible to peers.",
            nets,
          );
        }
      }
      result
    }
    Err(e) => {
      error!("Cannot get local network interfaces: {e:?}");
      vec![]
    }
  }
}

fn get_local_unicast_locators_inner(
  ifaces: &[IfAddr],
  port: u16,
  only_networks: Option<&[IpAddr]>,
) -> Vec<Locator> {
  ifaces
    .iter()
    .filter(|ifa| only_networks.is_none_or(|nets| nets.contains(&ifa.ip)))
    .map(|ifa| Locator::from(SocketAddr::new(ifa.ip, port)))
    .collect()
}

/// Enumerates local interfaces that we may use for multicasting.
///
/// The result of this function is used to set up senders and listeners.
/// When `only_networks` is `Some`, only interfaces with a matching IP are
/// included.
pub fn get_local_multicast_ip_addrs_filtered(
  only_networks: Option<&[IpAddr]>,
) -> io::Result<Vec<IpAddr>> {
  Ok(get_local_multicast_ip_addrs_inner(
    &enumerate_interfaces()?,
    only_networks,
  ))
}

/// Inner implementation of [`get_local_multicast_ip_addrs_filtered`], factored
/// out so tests can supply a mock interface list.
fn get_local_multicast_ip_addrs_inner(
  ifaces: &[IfAddr],
  only_networks: Option<&[IpAddr]>,
) -> Vec<IpAddr> {
  ifaces
    .iter()
    .filter(|ifa| ifa.is_multicast)
    .filter(|ifa| only_networks.is_none_or(|nets| nets.contains(&ifa.ip)))
    .map(|ifa| ifa.ip)
    .filter(IpAddr::is_ipv4)
    .collect()
}

/// Builds a mapping from OS interface index to an [`InterfaceSelector`].
///
/// Used to resolve the `ipi_ifindex` reported by `IP_PKTINFO`/`IPV6_PKTINFO`
/// into the interface identity our sender uses (its IP). Prefers an IPv4
/// address so the result matches the keys of the multicast sender sockets.
pub fn build_ifindex_to_interface_map() -> HashMap<u32, InterfaceSelector> {
  match enumerate_interfaces() {
    Ok(ifaces) => build_ifindex_map_inner(&ifaces),
    Err(e) => {
      error!("Cannot build interface-index map: {e:?}");
      HashMap::new()
    }
  }
}

fn build_ifindex_map_inner(ifaces: &[IfAddr]) -> HashMap<u32, InterfaceSelector> {
  let mut map: HashMap<u32, InterfaceSelector> = HashMap::new();
  for ifa in ifaces {
    if ifa.index == 0 {
      continue;
    }
    match map.get(&ifa.index) {
      // First address seen for this index.
      None => {
        map.insert(ifa.index, InterfaceSelector::Ip(ifa.ip));
      }
      // Prefer an IPv4 address over a previously stored non-IPv4 one, so the
      // result matches the (IPv4) multicast sender socket keys.
      Some(InterfaceSelector::Ip(existing)) if ifa.ip.is_ipv4() && !existing.is_ipv4() => {
        map.insert(ifa.index, InterfaceSelector::Ip(ifa.ip));
      }
      Some(_) => {}
    }
  }
  map
}

#[cfg(test)]
mod tests {
  use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

  use super::{
    build_ifindex_map_inner, get_local_multicast_ip_addrs_inner, get_local_unicast_locators_inner,
    path_mtu_payload_for_peer, IfAddr, InterfaceSelector,
  };
  use crate::{rtps::constant::FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE, structure::locator::Locator};

  fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
  }

  fn iface(ip: IpAddr, index: u32, is_loopback: bool, is_multicast: bool) -> IfAddr {
    IfAddr {
      ip,
      index,
      is_loopback,
      is_multicast,
      netmask: None,
      mtu: None,
    }
  }

  // Build an interface entry with an IPv4 subnet mask and MTU, for path-MTU
  // resolution tests.
  fn iface_v4(a: u8, b: u8, c: u8, d: u8, prefix: u8, mtu: u32, is_loopback: bool) -> IfAddr {
    let mask = if prefix == 0 {
      0u32
    } else {
      u32::MAX << (32 - u32::from(prefix))
    };
    IfAddr {
      ip: v4(a, b, c, d),
      index: 1,
      is_loopback,
      is_multicast: !is_loopback,
      netmask: Some(Ipv4Addr::from(mask)),
      mtu: Some(mtu),
    }
  }

  #[test]
  fn test_get_local_multicast_ip_addrs() {
    let ifaces = vec![
      // loopback: not multicast-capable
      iface(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, true, false),
      iface(IpAddr::V6(Ipv6Addr::LOCALHOST), 0, true, false),
      // eth0: multicast-capable, IPv4 + IPv6
      iface(v4(192, 168, 0, 137), 1, false, true),
      iface(
        IpAddr::V6(Ipv6Addr::new(0xfd73, 0x40a2, 0x1c3e, 0, 0, 0, 0, 0)),
        1,
        false,
        true,
      ),
    ];

    let ips = get_local_multicast_ip_addrs_inner(&ifaces, None);

    // Only IPv4 is currently supported for multicast enumeration.
    assert_eq!(ips.len(), 1, "should only contain the non-loopback iface");
    assert!(ips.contains(&v4(192, 168, 0, 137)));
  }

  #[test]
  fn no_multicast() {
    let mut ifaces = Vec::new();
    for index in 0..10 {
      ifaces.push(iface(
        v4(192, 168, 0, rand::random()),
        index + 1,
        false,
        false, // no multicast support
      ));
    }

    let ips = get_local_multicast_ip_addrs_inner(&ifaces, None);
    assert!(
      ips.is_empty(),
      "we only want interfaces w/ multicast support"
    );
  }

  #[test]
  fn empty_interfaces() {
    let ips = get_local_multicast_ip_addrs_inner(&[], None);
    assert!(
      ips.is_empty(),
      "blank iface list should result in empty list of ips"
    );
  }

  #[test]
  fn multicast_filter_respects_only_networks() {
    let ifaces = vec![
      iface(v4(192, 168, 0, 10), 1, false, true),
      iface(v4(10, 0, 0, 10), 2, false, true),
    ];

    let only_networks = [v4(10, 0, 0, 10)];
    let ips = get_local_multicast_ip_addrs_inner(&ifaces, Some(&only_networks));

    assert_eq!(ips, vec![v4(10, 0, 0, 10)]);
  }

  #[test]
  fn unicast_filter_respects_only_networks() {
    let only_networks = [v4(10, 0, 0, 10)];
    let ifaces = vec![
      iface(v4(192, 168, 0, 10), 1, false, true),
      iface(v4(10, 0, 0, 10), 2, false, true),
    ];

    let filtered = get_local_unicast_locators_inner(&ifaces, 7412, Some(&only_networks));

    assert_eq!(
      filtered,
      vec![Locator::from(SocketAddr::new(v4(10, 0, 0, 10), 7412))]
    );
  }

  #[test]
  fn ifindex_map_prefers_ipv4_and_skips_index_zero() {
    let ifaces = vec![
      // index 0 -> skipped
      iface(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, true, false),
      // index 3, IPv6 seen before IPv4: IPv4 should win
      iface(
        IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)),
        3,
        false,
        true,
      ),
      iface(v4(10, 0, 0, 7), 3, false, true),
    ];

    let map = build_ifindex_map_inner(&ifaces);
    assert!(!map.contains_key(&0), "index 0 must be skipped");
    assert_eq!(
      map.get(&3),
      Some(&InterfaceSelector::Ip(v4(10, 0, 0, 7))),
      "should prefer the IPv4 address"
    );
  }

  // A peer on the same /24 as a local interface uses that interface's MTU
  // (minus the 48-byte IPv4+UDP+RTPS header overhead).
  #[test]
  fn path_mtu_same_subnet_uses_iface_mtu() {
    let ifaces = vec![
      iface_v4(127, 0, 0, 1, 8, 16384, true),
      iface_v4(192, 168, 1, 10, 24, 1500, false),
    ];
    assert_eq!(
      path_mtu_payload_for_peer(&ifaces, v4(192, 168, 1, 55)),
      1500 - 48
    );
  }

  // A loopback peer matches the loopback interface and inherits its large MTU.
  #[test]
  fn path_mtu_loopback_peer_uses_loopback_mtu() {
    let ifaces = vec![
      iface_v4(127, 0, 0, 1, 8, 16384, true),
      iface_v4(192, 168, 1, 10, 24, 1500, false),
    ];
    assert_eq!(
      path_mtu_payload_for_peer(&ifaces, v4(127, 0, 0, 1)),
      16384 - 48
    );
  }

  // A peer not in any local subnet is assumed to be behind a router: use the
  // conservative Ethernet default.
  #[test]
  fn path_mtu_behind_router_uses_default() {
    let ifaces = vec![iface_v4(192, 168, 1, 10, 24, 1500, false)];
    assert_eq!(
      path_mtu_payload_for_peer(&ifaces, v4(10, 20, 30, 40)),
      FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE
    );
  }

  // An IPv6 peer has no IPv4 subnet to match: fall back to the default.
  #[test]
  fn path_mtu_ipv6_peer_uses_default() {
    let ifaces = vec![iface_v4(192, 168, 1, 10, 24, 9000, false)];
    let peer = IpAddr::V6(Ipv6Addr::LOCALHOST);
    assert_eq!(
      path_mtu_payload_for_peer(&ifaces, peer),
      FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE
    );
  }

  // A jumbo-frame interface yields a correspondingly large budget.
  #[test]
  fn path_mtu_jumbo_frame() {
    let ifaces = vec![iface_v4(10, 0, 0, 1, 8, 9000, false)];
    assert_eq!(path_mtu_payload_for_peer(&ifaces, v4(10, 1, 2, 3)), 9000 - 48);
  }

  // Empty table -> default.
  #[test]
  fn path_mtu_empty_table_uses_default() {
    assert_eq!(
      path_mtu_payload_for_peer(&[], v4(192, 168, 1, 1)),
      FALLBACK_MAX_AGGREGATED_DATAGRAM_SIZE
    );
  }
}
