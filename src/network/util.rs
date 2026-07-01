use std::{
  collections::HashMap,
  io,
  net::{IpAddr, SocketAddr},
};

use log::{error, warn};

use crate::{rtps::transmit::InterfaceSelector, structure::locator::Locator};

pub fn get_local_multicast_locators(port: u16) -> Vec<Locator> {
  let saddr = SocketAddr::new("239.255.0.1".parse().unwrap(), port);
  vec![Locator::from(saddr)]
}

pub fn get_local_unicast_locators_filtered(
  port: u16,
  only_networks: Option<&[IpAddr]>,
) -> Vec<Locator> {
  match if_addrs::get_if_addrs() {
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
      error!("Cannot get local network interfaces: get_if_addrs() : {e:?}");
      vec![]
    }
  }
}

fn get_local_unicast_locators_inner(
  ifaces: &[if_addrs::Interface],
  port: u16,
  only_networks: Option<&[IpAddr]>,
) -> Vec<Locator> {
  ifaces
    .iter()
    .filter(|ip| only_networks.is_none_or(|nets| nets.contains(&ip.ip())))
    .map(|ip| Locator::from(SocketAddr::new(ip.ip(), port)))
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
  let interfaces = pnet::datalink::interfaces();
  Ok(get_local_multicast_ip_addrs_inner(
    interfaces,
    only_networks,
  ))
}

/// Inner implementation of [`get_local_multicast_ip_addrs_filtered`], factored
/// out so tests can supply a mock interface list.
fn get_local_multicast_ip_addrs_inner(
  interfaces: Vec<pnet::datalink::NetworkInterface>,
  only_networks: Option<&[IpAddr]>,
) -> Vec<IpAddr> {
  interfaces
    .into_iter()
    .filter(|ifaddr| ifaddr.is_multicast())
    .filter(|ifaddr| {
      only_networks.is_none_or(|nets| ifaddr.ips.iter().any(|ip_net| nets.contains(&ip_net.ip())))
    })
    .flat_map(|ifaddr| ifaddr.ips)
    .map(|ip_net| ip_net.ip())
    .filter(|ip| ip.is_ipv4())
    .collect()
}

/// Builds a mapping from OS interface index to an [`InterfaceSelector`].
///
/// Used to resolve the `ipi_ifindex` reported by `IP_PKTINFO`/`IPV6_PKTINFO`
/// into the interface identity our sender uses (its IP). Prefers an IPv4
/// address so the result matches the keys of the multicast sender sockets.
pub fn build_ifindex_to_interface_map() -> HashMap<u32, InterfaceSelector> {
  build_ifindex_map_inner(pnet::datalink::interfaces())
}

fn build_ifindex_map_inner(
  interfaces: Vec<pnet::datalink::NetworkInterface>,
) -> HashMap<u32, InterfaceSelector> {
  let mut map = HashMap::new();
  for iface in interfaces {
    if iface.index == 0 {
      continue;
    }
    let ip = iface
      .ips
      .iter()
      .map(|n| n.ip())
      .find(IpAddr::is_ipv4)
      .or_else(|| iface.ips.first().map(|n| n.ip()));
    if let Some(ip) = ip {
      map.insert(iface.index, InterfaceSelector::Ip(ip));
    }
  }
  map
}

#[cfg(test)]
mod tests {
  use std::{
    ffi::c_int,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
  };

  use pnet::{
    datalink::{InterfaceType, NetworkInterface},
    ipnetwork::{IpNetwork, Ipv4Network, Ipv6Network},
  };

  use crate::structure::locator::Locator;

  /// Mocks the `get_local_multicast_ip_addrs` function.
  #[test]
  fn test_get_local_multicast_ip_addrs() {
    let eth0 = interface(
      "eth0",
      1,
      &[
        IpNetwork::V4(Ipv4Network::new(Ipv4Addr::new(192, 168, 0, 137), 24).unwrap()),
        IpNetwork::V6(
          Ipv6Network::new(Ipv6Addr::new(0xfd73, 0x40a2, 0x1c3e, 0, 0, 0, 0, 0), 64).unwrap(),
        ),
      ],
      &[pnet_sys::IFF_MULTICAST],
    );

    let interfaces = vec![loopback(), eth0];

    let ips = super::get_local_multicast_ip_addrs_inner(interfaces, None);

    // TODO: uncomment if IPv6 becomes supported :(
    // assert_eq!(ips.len(), 2, "should only contain the non-loopback iface");
    assert_eq!(ips.len(), 1, "should only contain the non-loopback iface");
    assert!(ips.contains(&Ipv4Addr::new(192, 168, 0, 137).into()));

    // TODO: uncomment if ipv6
    // assert!(ips.contains(&Ipv6Addr::new(0xfd73, 0x40a2, 0x1c3e, 0, 0, 0, 0,
    // 0).into()))
  }

  /// Tries a number of interfaces, none of which support multicast.
  ///
  /// This should result in an empty list of IP addresses.
  #[test]
  fn no_multicast() {
    let mut interfaces = Vec::new();

    for index in 0..10 {
      interfaces.push(interface(
        format!("eth{index}"),
        index,
        &[IpNetwork::V4(
          Ipv4Network::new(Ipv4Addr::new(192, 168, 0, rand::random()), 24).unwrap(),
        )],
        &[],
      ));
    }

    let ips = super::get_local_multicast_ip_addrs_inner(interfaces, None);

    assert!(
      ips.is_empty(),
      "we only want interfaces w/ multicast support"
    );
  }

  #[test]
  fn empty_interfaces() {
    let ips = super::get_local_multicast_ip_addrs_inner(Vec::new(), None);
    assert!(
      ips.is_empty(),
      "blank iface list should result in empty list of ips"
    );
  }

  #[test]
  fn multicast_filter_respects_only_networks() {
    let interfaces = vec![
      interface(
        "eth0",
        1,
        &[IpNetwork::V4(
          Ipv4Network::new(Ipv4Addr::new(192, 168, 0, 10), 24).unwrap(),
        )],
        &[pnet_sys::IFF_MULTICAST],
      ),
      interface(
        "eth1",
        2,
        &[IpNetwork::V4(
          Ipv4Network::new(Ipv4Addr::new(10, 0, 0, 10), 24).unwrap(),
        )],
        &[pnet_sys::IFF_MULTICAST],
      ),
    ];

    let only_networks = [IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10))];
    let ips = super::get_local_multicast_ip_addrs_inner(interfaces, Some(&only_networks));

    assert_eq!(ips, vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10))]);
  }

  #[test]
  fn unicast_filter_respects_only_networks() {
    let only_networks = [IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10))];
    let ifaces = vec![
      if_addrs::Interface {
        name: "eth0".to_string(),
        addr: if_addrs::IfAddr::V4(if_addrs::Ifv4Addr {
          ip: Ipv4Addr::new(192, 168, 0, 10),
          netmask: Ipv4Addr::new(255, 255, 255, 0),
          prefixlen: 24,
          broadcast: None,
        }),
        index: None,
        oper_status: if_addrs::IfOperStatus::Up,
      },
      if_addrs::Interface {
        name: "eth1".to_string(),
        addr: if_addrs::IfAddr::V4(if_addrs::Ifv4Addr {
          ip: Ipv4Addr::new(10, 0, 0, 10),
          netmask: Ipv4Addr::new(255, 255, 255, 0),
          prefixlen: 24,
          broadcast: None,
        }),
        index: None,
        oper_status: if_addrs::IfOperStatus::Up,
      },
    ];

    let filtered = super::get_local_unicast_locators_inner(&ifaces, 7412, Some(&only_networks));

    assert_eq!(
      filtered,
      vec![Locator::from(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10)),
        7412,
      ))]
    );
  }

  #[test]
  fn ifindex_map_prefers_ipv4_and_skips_index_zero() {
    use super::InterfaceSelector;

    let interfaces = vec![
      loopback(), // index 0 -> skipped
      interface(
        "eth0",
        3,
        &[
          IpNetwork::V6(Ipv6Network::new(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1), 64).unwrap()),
          IpNetwork::V4(Ipv4Network::new(Ipv4Addr::new(10, 0, 0, 7), 24).unwrap()),
        ],
        &[pnet_sys::IFF_MULTICAST],
      ),
    ];

    let map = super::build_ifindex_map_inner(interfaces);
    assert!(!map.contains_key(&0), "index 0 must be skipped");
    assert_eq!(
      map.get(&3),
      Some(&InterfaceSelector::Ip(IpAddr::V4(Ipv4Addr::new(
        10, 0, 0, 7
      )))),
      "should prefer the IPv4 address"
    );
  }

  fn loopback() -> NetworkInterface {
    NetworkInterface {
      name: "lo".to_string(),
      description: "loopback".to_string(),
      index: 0,
      mac: None,
      ips: vec![
        IpNetwork::V4(Ipv4Network::new(Ipv4Addr::LOCALHOST, 24).unwrap()),
        IpNetwork::V6(Ipv6Network::new(Ipv6Addr::LOCALHOST, 64).unwrap()),
      ],
      flags: pnet_sys::IFF_MULTICAST as InterfaceType & pnet_sys::IFF_LOOPBACK as InterfaceType,
    }
  }

  fn interface(
    name: impl AsRef<str>,
    index: u32,
    ips: &[IpNetwork],
    flags: &[c_int],
  ) -> NetworkInterface {
    NetworkInterface {
      name: name.as_ref().into(),
      description: String::new(),
      index,
      mac: None,
      ips: ips.to_vec(),
      flags: flags
        .iter()
        .fold(0, |acc, &flag| acc | flag as InterfaceType),
    }
  }
}
