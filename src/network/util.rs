use std::{
  collections::HashMap,
  io,
  net::{IpAddr, SocketAddr},
};

use log::{error, warn};

use crate::{rtps::transmit::InterfaceSelector, structure::locator::Locator};

/// Platform-neutral view of one network-interface address.
///
/// The interface-enumeration crates differ per platform (and expose different
/// data), so we normalize into this small struct. The inner helper functions
/// then operate on `IfAddr` alone, which keeps them free of any
/// platform/enumeration dependency and makes them trivially unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IfAddr {
  /// An IP address bound to the interface.
  ip: IpAddr,
  /// OS interface index. `0` when unknown / not applicable (e.g. Windows,
  /// where the index is not consumed).
  index: u32,
  is_loopback: bool,
  /// Whether the interface is multicast-capable. On platforms that do not
  /// expose the flag (Windows), this is approximated as "not loopback".
  is_multicast: bool,
}

// ---------------------------------------------------------------------------
// Platform-specific interface enumeration
// ---------------------------------------------------------------------------

/// Enumerate all interface addresses on Unix-like systems (Linux, macOS, BSD).
///
/// Uses `getifaddrs(3)` via the `nix` crate (already a dependency for
/// `recvmsg`/`IP_PKTINFO`), so no dedicated interface-enumeration crate is
/// needed here.
#[cfg(unix)]
fn enumerate_interfaces() -> io::Result<Vec<IfAddr>> {
  use nix::{
    ifaddrs::getifaddrs,
    net::if_::{if_nametoindex, InterfaceFlags},
  };

  let addrs = getifaddrs().map_err(|e| io::Error::from_raw_os_error(e as i32))?;

  // Cache name -> index so we call if_nametoindex once per interface.
  let mut index_cache: HashMap<String, u32> = HashMap::new();
  let mut result = Vec::new();

  for ifa in addrs {
    let Some(ip) = ifa.address.and_then(sockaddr_to_ipaddr) else {
      // Skip entries without an IP address (e.g. AF_PACKET link-layer on Linux).
      continue;
    };

    let index = *index_cache
      .entry(ifa.interface_name.clone())
      .or_insert_with(|| if_nametoindex(ifa.interface_name.as_str()).unwrap_or(0));

    result.push(IfAddr {
      ip,
      index,
      is_loopback: ifa.flags.contains(InterfaceFlags::IFF_LOOPBACK),
      is_multicast: ifa.flags.contains(InterfaceFlags::IFF_MULTICAST),
    });
  }

  Ok(result)
}

/// Convert a `nix` `SockaddrStorage` into a `std` [`IpAddr`], if it is an
/// IPv4 or IPv6 address.
#[cfg(unix)]
fn sockaddr_to_ipaddr(addr: nix::sys::socket::SockaddrStorage) -> Option<IpAddr> {
  if let Some(v4) = addr.as_sockaddr_in() {
    Some(IpAddr::V4(v4.ip()))
  } else {
    addr.as_sockaddr_in6().map(|v6| IpAddr::V6(v6.ip()))
  }
}

/// Enumerate all interface addresses on Windows.
///
/// Uses the `if-addrs` crate. Windows does not expose a multicast-capability
/// flag through this API, so non-loopback interfaces are assumed to be
/// multicast-capable; multicast joins/sends degrade gracefully (with a
/// warning) if that assumption is wrong. The interface index is not consumed
/// on Windows (it is only used by the Unix `IP_PKTINFO` receive path).
#[cfg(windows)]
fn enumerate_interfaces() -> io::Result<Vec<IfAddr>> {
  let ifaces = if_addrs::get_if_addrs()?;
  Ok(
    ifaces
      .into_iter()
      .map(|iface| {
        let is_loopback = iface.is_loopback();
        IfAddr {
          ip: iface.ip(),
          index: iface.index.unwrap_or(0),
          is_loopback,
          is_multicast: !is_loopback,
        }
      })
      .collect(),
  )
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
    IfAddr, InterfaceSelector,
  };
  use crate::structure::locator::Locator;

  fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
  }

  fn iface(ip: IpAddr, index: u32, is_loopback: bool, is_multicast: bool) -> IfAddr {
    IfAddr {
      ip,
      index,
      is_loopback,
      is_multicast,
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
}
