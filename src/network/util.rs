use std::{
  io,
  net::{IpAddr, SocketAddr},
};

use log::{error, warn};

use crate::structure::locator::Locator;

pub fn get_local_multicast_locators(port: u16) -> Vec<Locator> {
  let saddr = SocketAddr::new("239.255.0.1".parse().unwrap(), port);
  vec![Locator::from(saddr)]
}

pub fn get_local_unicast_locators_filtered(
  port: u16,
  only_networks: Option<&[String]>,
) -> Vec<Locator> {
  match if_addrs::get_if_addrs() {
    Ok(ifaces) => {
      let result = get_local_unicast_locators_inner(ifaces, port, only_networks);
      if result.is_empty() && only_networks.is_some() {
        warn!(
          "only_networks filter {:?} matched no unicast interfaces; this participant will be \
           invisible to peers.",
          only_networks.unwrap(),
        );
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
  ifaces: Vec<if_addrs::Interface>,
  port: u16,
  only_networks: Option<&[String]>,
) -> Vec<Locator> {
  ifaces
    .iter()
    .filter(|ip| !ip.is_loopback())
    .filter(|ip| only_networks.map_or(true, |nets| nets.contains(&ip.name)))
    .map(|ip| Locator::from(SocketAddr::new(ip.ip(), port)))
    .collect()
}

/// Enumerates local interfaces that we may use for multicasting.
///
/// The result of this function is used to set up senders and listeners.
/// When `only_networks` is `Some`, only the named interfaces are included.
pub fn get_local_multicast_ip_addrs_filtered(
  only_networks: Option<&[String]>,
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
  only_networks: Option<&[String]>,
) -> Vec<IpAddr> {
  interfaces
    .into_iter()
    .filter(|ifaddr| !ifaddr.is_loopback())
    .filter(|ifaddr| ifaddr.is_multicast())
    .filter(|ifaddr| only_networks.map_or(true, |nets| nets.contains(&ifaddr.name)))
    .flat_map(|ifaddr| ifaddr.ips)
    .map(|ip_net| ip_net.ip())
    .filter(|ip| ip.is_ipv4())
    .collect()
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

    let only_networks = [String::from("eth1")];
    let ips = super::get_local_multicast_ip_addrs_inner(interfaces, Some(&only_networks));

    assert_eq!(ips, vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10))]);
  }

  #[test]
  fn unicast_filter_respects_only_networks() {
    let only_networks = [String::from("eth1")];
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

    let filtered = super::get_local_unicast_locators_inner(ifaces, 7412, Some(&only_networks));

    assert_eq!(
      filtered,
      vec![Locator::from(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10)),
        7412,
      ))]
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
