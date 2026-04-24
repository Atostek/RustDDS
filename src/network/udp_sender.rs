use std::{
  io,
  net::{IpAddr, SocketAddr, UdpSocket},
};
#[cfg(test)]
use std::net::Ipv4Addr;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
#[cfg(windows)]
use local_ip_address::list_afinet_netifas;

use crate::{network::util::get_local_multicast_ip_addrs_filtered, structure::locator::Locator};

// We need one multicast sender socket per interface

#[derive(Debug)]
pub struct UDPSender {
  unicast_socket: mio_08::net::UdpSocket,
  multicast_sockets: Vec<mio_08::net::UdpSocket>,
}

impl UDPSender {
  #[cfg(test)]
  pub fn new(sender_port: u16) -> io::Result<Self> {
    Self::new_with_networks(sender_port, None)
  }

  pub fn new_with_networks(sender_port: u16, only_networks: Option<&[IpAddr]>) -> io::Result<Self> {
    let unicast_socket = {
      let saddr: SocketAddr = SocketAddr::new("0.0.0.0".parse().unwrap(), sender_port);
      mio_08::net::UdpSocket::bind(saddr)?
    };

    // We set multicasting loop on so that we can hear other DomainParticipant
    // instances running on the same host.
    unicast_socket
      .set_multicast_loop_v4(true)
      .unwrap_or_else(|e| {
        error!("Cannot set multicast loop on: {e:?}");
      });

    let mut multicast_sockets = Vec::with_capacity(1);
    for multicast_if_ipaddr in get_local_multicast_ip_addrs_filtered(only_networks)? {
      // beef: specify output interface
      trace!("UDPSender: Multicast sender on interface {multicast_if_ipaddr:?}");

      let mc_socket = match multicast_if_ipaddr {
        // ipv4 requires a little more work
        IpAddr::V4(a) => {
          let raw_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
          raw_socket.set_multicast_if_v4(&a)?;

          // Handle windows.
          //
          // TODO: Check if necessary.
          if cfg!(windows) {
            raw_socket.set_reuse_address(true)?;
          }

          // bind to the multicast interface
          raw_socket.bind(&SockAddr::from(SocketAddr::new(multicast_if_ipaddr, 0)))?;

          // make multicast sock
          let mc_socket = UdpSocket::from(raw_socket);
          mc_socket.set_multicast_loop_v4(true).unwrap_or_else(|e| {
            error!("Cannot set IPv4 multicast loop. err: {e}");
          });
          mc_socket
        }

        // ipv6
        IpAddr::V6(addr) => {
          let raw_socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;

          // note: you don't need to use set_multicast_if for ipv6 multicast.
          // it comes for free!
          raw_socket.bind(&SocketAddr::new(addr.into(), 0).into())?;

          // make multicast sock
          let mc_socket = UdpSocket::from(raw_socket);
          mc_socket.set_multicast_loop_v6(true).unwrap_or_else(|e| {
            error!("Cannot set IPv6 multicast loop. err: {e}");
          });

          mc_socket
        }
      };

      multicast_sockets.push(mio_08::net::UdpSocket::from_std(mc_socket));
    } // end for

    let sender = Self {
      unicast_socket,
      multicast_sockets,
    };
    info!("UDPSender::new() --> {sender:?}");
    Ok(sender)
  }

  #[cfg(test)]
  pub fn new_with_random_port() -> io::Result<Self> {
    Self::new(0)
  }

  pub fn send_to_locator_list(&self, buffer: &[u8], ll: &[Locator]) {
    for loc in ll {
      self.send_to_locator(buffer, loc);
    }
  }

  fn send_to_udp_socket(&self, buffer: &[u8], socket: &mio_08::net::UdpSocket, addr: &SocketAddr) {
    match socket.send_to(buffer, *addr) {
      Ok(bytes_sent) => {
        if bytes_sent == buffer.len() { // ok
        } else {
          error!(
            "send_to_udp_socket - send_to tried {} bytes, sent only {}",
            buffer.len(),
            bytes_sent
          );
        }
      }
      Err(e) => {
        warn!(
          "send_to_udp_socket - send_to {} : {:?} len={}",
          addr,
          e,
          buffer.len()
        );
      }
    }
  }

  pub fn send_to_locator(&self, buffer: &[u8], locator: &Locator) {
    if buffer.len() > 1500 {
      warn!("send_to_locator: Message size = {}", buffer.len());
    }
    let send = |socket_address: SocketAddr| {
      if socket_address.ip().is_multicast() {
        for socket in &self.multicast_sockets {
          self.send_to_udp_socket(buffer, socket, &socket_address);
        }
      } else {
        self.send_to_udp_socket(buffer, &self.unicast_socket, &socket_address);
      }
    };

    match locator {
      Locator::UdpV4(socket_address) => send(SocketAddr::from(*socket_address)),
      Locator::UdpV6(socket_address) => send(SocketAddr::from(*socket_address)),
      Locator::Invalid | Locator::Reserved => {
        error!("send_to_locator: Cannot send to {locator:?}");
      }
      Locator::Other { kind, .. } =>
      // This is normal, as other implementations can define their own kinds.
      // We get those from Discovery.
      {
        trace!("send_to_locator: Unknown LocatorKind: {kind:?}");
      }
    }
  }

  #[cfg(test)]
  pub fn send_to_all(&self, buffer: &[u8], addresses: &[SocketAddr]) {
    let buf_len = buffer.len();

    for address in addresses.iter() {
      // try sending the addr a message
      match self.unicast_socket.send_to(buffer, *address) {
        Ok(bytes_sent) => {
          // error if we didn't send the whole buffer.
          if bytes_sent != buffer.len() {
            panic!("tried to send `{buf_len}` bytes, sent only `{bytes_sent}`!");
          }
        }

        // it's a problem if we couldn't send anything - so we'll panic!
        Err(e) => {
          panic!("Unable to send to `{address}`. err: {e}");
        }
      }
    }
  }

  #[cfg(test)]
  pub fn send_multicast(self, buffer: &[u8], address: Ipv4Addr, port: u16) -> io::Result<usize> {
    if address.is_multicast() {
      let address = SocketAddr::new(IpAddr::V4(address), port);
      let mut size = 0;
      for s in self.multicast_sockets {
        size = s.send_to(buffer, address)?;
      }
      Ok(size)
    } else {
      io::Result::Err(io::Error::new(
        io::ErrorKind::Other,
        "Not a multicast address",
      ))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::network::udp_listener::*;

  #[test]
  fn udps_single_send() {
    let listener = UDPListener::new_unicast("127.0.0.1", 10201).unwrap();
    let sender = UDPSender::new(11201).expect("failed to create UDPSender");

    let data: Vec<u8> = vec![0, 1, 2, 3, 4];

    let addrs = vec![SocketAddr::new("127.0.0.1".parse().unwrap(), 10201)];
    sender.send_to_all(&data, &addrs);

    let rec_data = listener.get_message();

    assert_eq!(rec_data.len(), 5);
    assert_eq!(rec_data, data);
  }

  #[test]
  fn udps_multi_send() {
    let listener_1 = UDPListener::new_unicast("127.0.0.1", 10301).unwrap();
    let listener_2 = UDPListener::new_unicast("127.0.0.1", 10302).unwrap();
    let sender = UDPSender::new(11301).expect("failed to create UDPSender");

    let data: Vec<u8> = vec![5, 4, 3, 2, 1, 0];

    let addrs = vec![
      SocketAddr::new("127.0.0.1".parse().unwrap(), 10301),
      SocketAddr::new("127.0.0.1".parse().unwrap(), 10302),
    ];
    sender.send_to_all(&data, &addrs);

    let rec_data_1 = listener_1.get_message();
    let rec_data_2 = listener_2.get_message();

    assert_eq!(rec_data_1.len(), 6);
    assert_eq!(rec_data_1, data);
    assert_eq!(rec_data_2.len(), 6);
    assert_eq!(rec_data_2, data);
  }
}
