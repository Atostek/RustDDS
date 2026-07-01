use std::{
  cell::RefCell,
  collections::HashMap,
  io,
  net::{IpAddr, SocketAddr, UdpSocket},
};
#[cfg(test)]
use std::net::Ipv4Addr;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
#[cfg(windows)]
use local_ip_address::list_afinet_netifas;

use crate::{
  network::util::get_local_multicast_ip_addrs_filtered,
  rtps::{
    outbound::{ControlQueue, Datagram, SendOutcome, SocketId, CONTROL_QUEUE_WARN_LEN},
    transmit::InterfaceSelector,
  },
  structure::locator::Locator,
};

// We need one multicast sender socket per interface

#[derive(Debug)]
pub struct UDPSender {
  unicast_socket: mio_08::net::UdpSocket,
  // One multicast sender socket per local interface, keyed by the interface it
  // was bound to (its `InterfaceSelector`). This lets us target a single
  // interface instead of sending on all of them.
  multicast_sockets: Vec<(InterfaceSelector, mio_08::net::UdpSocket)>,

  // nonblocking-transmit: per-socket never-dropped control queue. A control
  // datagram is enqueued only when its socket is currently congested
  // (WouldBlock) or already has queued control ahead of it; otherwise it is
  // sent immediately. Drained on write readiness by `flush_control`.
  // (see src/rtps/nonblocking_transmit_design.md)
  control_queues: RefCell<HashMap<SocketId, ControlQueue>>,
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
          // nonblocking-transmit: mio requires the socket be non-blocking, and
          // we must never let a full kernel buffer stall the event loop.
          mc_socket.set_nonblocking(true).unwrap_or_else(|e| {
            error!("Cannot set IPv4 multicast socket non-blocking. err: {e}");
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
          // nonblocking-transmit: see IPv4 branch above.
          mc_socket.set_nonblocking(true).unwrap_or_else(|e| {
            error!("Cannot set IPv6 multicast socket non-blocking. err: {e}");
          });

          mc_socket
        }
      };

      multicast_sockets.push((
        InterfaceSelector::Ip(multicast_if_ipaddr),
        mio_08::net::UdpSocket::from_std(mc_socket),
      ));
    } // end for

    let sender = Self {
      unicast_socket,
      multicast_sockets,
      control_queues: RefCell::new(HashMap::new()),
    };
    info!("UDPSender::new() --> {sender:?}");
    Ok(sender)
  }

  #[cfg(test)]
  pub fn new_with_random_port() -> io::Result<Self> {
    Self::new(0)
  }

  // --- nonblocking-transmit: socket enumeration & raw non-blocking send ------

  fn socket_ref(&self, id: SocketId) -> Option<&mio_08::net::UdpSocket> {
    match id {
      SocketId::Unicast => Some(&self.unicast_socket),
      SocketId::Multicast(i) => self.multicast_sockets.get(i).map(|(_, s)| s),
    }
  }

  /// All sender sockets, so `DPEventLoop` can arm write readiness on each.
  pub(crate) fn socket_ids(&self) -> Vec<SocketId> {
    let mut v = vec![SocketId::Unicast];
    v.extend((0..self.multicast_sockets.len()).map(SocketId::Multicast));
    v
  }

  /// Raw fd of a sender socket, for registering write readiness in the poll.
  #[cfg(unix)]
  pub(crate) fn socket_raw_fd(&self, id: SocketId) -> Option<RawFd> {
    self.socket_ref(id).map(AsRawFd::as_raw_fd)
  }

  /// One non-blocking datagram send. Never blocks; classifies the result.
  fn raw_send(&self, id: SocketId, addr: SocketAddr, buffer: &[u8]) -> SendOutcome {
    let Some(socket) = self.socket_ref(id) else {
      error!("raw_send: no socket for {id:?}");
      return SendOutcome::Dropped;
    };
    match socket.send_to(buffer, addr) {
      Ok(bytes_sent) => {
        if bytes_sent != buffer.len() {
          error!(
            "raw_send: {id:?} tried {} bytes, sent only {bytes_sent}",
            buffer.len()
          );
        }
        SendOutcome::Sent
      }
      Err(e) if e.kind() == io::ErrorKind::WouldBlock => SendOutcome::WouldBlock,
      Err(e) => {
        warn!("raw_send: {id:?} to {addr} : {e:?} len={}", buffer.len());
        SendOutcome::Dropped
      }
    }
  }

  fn control_queue_nonempty(&self, id: SocketId) -> bool {
    self
      .control_queues
      .borrow()
      .get(&id)
      .is_some_and(|q| !q.is_empty())
  }

  // --- nonblocking-transmit: control path (never dropped, high priority) -----

  // Enqueue-or-send one control datagram to a single socket. If the socket
  // already has queued control we must preserve order and just enqueue;
  // otherwise we try an immediate send and only enqueue on WouldBlock.
  fn control_send_one(&self, id: SocketId, addr: SocketAddr, buffer: &[u8]) {
    let mut queues = self.control_queues.borrow_mut();
    let queue = queues.entry(id).or_default();
    if queue.is_empty() {
      match self.raw_send(id, addr, buffer) {
        SendOutcome::Sent | SendOutcome::Dropped => {}
        SendOutcome::WouldBlock => queue.push_back(Datagram {
          addr,
          bytes: buffer.to_vec(),
        }),
      }
    } else {
      queue.push_back(Datagram {
        addr,
        bytes: buffer.to_vec(),
      });
      if queue.len() == CONTROL_QUEUE_WARN_LEN {
        warn!(
          "nonblocking-transmit: control queue for {id:?} reached {} datagrams; \
           link may be wedged (nothing dropped)",
          queue.len()
        );
      }
    }
  }

  /// Try to flush a socket's queued control datagrams (called on write
  /// readiness). Returns `true` if the queue is now empty.
  pub(crate) fn flush_control(&self, id: SocketId) -> bool {
    let mut queues = self.control_queues.borrow_mut();
    let Some(queue) = queues.get_mut(&id) else {
      return true;
    };
    while let Some(front) = queue.front() {
      let addr = front.addr;
      let outcome = self.raw_send(id, addr, &front.bytes);
      match outcome {
        SendOutcome::Sent | SendOutcome::Dropped => {
          queue.pop_front();
        }
        SendOutcome::WouldBlock => return false,
      }
    }
    true
  }

  /// Sockets that currently have queued (undelivered) control datagrams.
  pub(crate) fn pending_control_sockets(&self) -> Vec<SocketId> {
    self
      .control_queues
      .borrow()
      .iter()
      .filter(|(_, q)| !q.is_empty())
      .map(|(id, _)| *id)
      .collect()
  }

  fn locator_socket_addr(&self, locator: &Locator, ctx: &str) -> Option<SocketAddr> {
    match locator {
      Locator::UdpV4(sa) => Some(SocketAddr::from(*sa)),
      Locator::UdpV6(sa) => Some(SocketAddr::from(*sa)),
      Locator::Invalid | Locator::Reserved => {
        error!("{ctx}: Cannot send to {locator:?}");
        None
      }
      Locator::Other { kind, .. } => {
        // Normal: other implementations define their own kinds (from Discovery).
        trace!("{ctx}: Unknown LocatorKind: {kind:?}");
        None
      }
    }
  }

  pub fn send_to_locator_list(&self, buffer: &[u8], ll: &[Locator]) {
    for loc in ll {
      self.send_to_locator(buffer, loc);
    }
  }

  /// Control-path send to a locator. A multicast locator fans out to every
  /// multicast interface (legacy reachability). Datagrams are queued (never
  /// dropped) if the socket is congested.
  pub fn send_to_locator(&self, buffer: &[u8], locator: &Locator) {
    if buffer.len() > 1500 {
      warn!("send_to_locator: Message size = {}", buffer.len());
    }
    let Some(socket_address) = self.locator_socket_addr(locator, "send_to_locator") else {
      return;
    };
    if socket_address.ip().is_multicast() {
      for id in 0..self.multicast_sockets.len() {
        self.control_send_one(SocketId::Multicast(id), socket_address, buffer);
      }
    } else {
      self.control_send_one(SocketId::Unicast, socket_address, buffer);
    }
  }

  /// The set of local interfaces on which this sender can emit multicast.
  /// Used by route resolution to validate an observed interface is usable.
  pub fn multicast_interfaces(&self) -> Vec<InterfaceSelector> {
    self
      .multicast_sockets
      .iter()
      .map(|(iface, _)| *iface)
      .collect()
  }

  fn multicast_socket_id_for(&self, interface: &InterfaceSelector) -> Option<SocketId> {
    self
      .multicast_sockets
      .iter()
      .position(|(iface, _)| iface == interface)
      .map(SocketId::Multicast)
  }

  /// Control-path multicast send out of a single, specific local interface.
  /// Falls back to all interfaces if the requested one is unknown, so a
  /// stale/misresolved interface never silently drops traffic.
  pub fn send_to_multicast_locator_via(
    &self,
    buffer: &[u8],
    locator: &Locator,
    interface: &InterfaceSelector,
  ) {
    if buffer.len() > 1500 {
      warn!(
        "send_to_multicast_locator_via: Message size = {}",
        buffer.len()
      );
    }
    let Some(socket_address) =
      self.locator_socket_addr(locator, "send_to_multicast_locator_via")
    else {
      return;
    };

    if !socket_address.ip().is_multicast() {
      // Not a multicast destination; treat as a plain unicast send.
      self.control_send_one(SocketId::Unicast, socket_address, buffer);
      return;
    }

    match self.multicast_socket_id_for(interface) {
      Some(id) => self.control_send_one(id, socket_address, buffer),
      None => {
        trace!("send_to_multicast_locator_via: interface {interface:?} not found, sending on all");
        for id in 0..self.multicast_sockets.len() {
          self.control_send_one(SocketId::Multicast(id), socket_address, buffer);
        }
      }
    }
  }

  // --- nonblocking-transmit: bulk path (flow-controlled, backpressured) ------

  // One bulk datagram attempt to a socket. Control has strict priority: if the
  // socket still has queued control, report WouldBlock so the writer backs off
  // and resumes only after control has drained.
  fn bulk_send_one(&self, id: SocketId, addr: SocketAddr, buffer: &[u8]) -> SendOutcome {
    if self.control_queue_nonempty(id) {
      return SendOutcome::WouldBlock;
    }
    self.raw_send(id, addr, buffer)
  }

  /// Bulk send to a locator. Returns the sockets that could not accept the
  /// datagram (WouldBlock), so the caller can stop and arm write readiness.
  pub(crate) fn try_send_to_locator(&self, buffer: &[u8], locator: &Locator) -> Vec<SocketId> {
    if buffer.len() > 1500 {
      warn!("try_send_to_locator: Message size = {}", buffer.len());
    }
    let mut blocked = Vec::new();
    let Some(socket_address) = self.locator_socket_addr(locator, "try_send_to_locator") else {
      return blocked;
    };
    if socket_address.ip().is_multicast() {
      for id in (0..self.multicast_sockets.len()).map(SocketId::Multicast) {
        if self.bulk_send_one(id, socket_address, buffer) == SendOutcome::WouldBlock {
          blocked.push(id);
        }
      }
    } else if self.bulk_send_one(SocketId::Unicast, socket_address, buffer)
      == SendOutcome::WouldBlock
    {
      blocked.push(SocketId::Unicast);
    }
    blocked
  }

  /// Bulk multicast send out of a single interface (fallback: all). Returns the
  /// sockets that could not accept the datagram (WouldBlock).
  pub(crate) fn try_send_to_multicast_locator_via(
    &self,
    buffer: &[u8],
    locator: &Locator,
    interface: &InterfaceSelector,
  ) -> Vec<SocketId> {
    if buffer.len() > 1500 {
      warn!(
        "try_send_to_multicast_locator_via: Message size = {}",
        buffer.len()
      );
    }
    let mut blocked = Vec::new();
    let Some(socket_address) =
      self.locator_socket_addr(locator, "try_send_to_multicast_locator_via")
    else {
      return blocked;
    };
    if !socket_address.ip().is_multicast() {
      if self.bulk_send_one(SocketId::Unicast, socket_address, buffer) == SendOutcome::WouldBlock {
        blocked.push(SocketId::Unicast);
      }
      return blocked;
    }
    let ids: Vec<SocketId> = match self.multicast_socket_id_for(interface) {
      Some(id) => vec![id],
      None => (0..self.multicast_sockets.len())
        .map(SocketId::Multicast)
        .collect(),
    };
    for id in ids {
      if self.bulk_send_one(id, socket_address, buffer) == SendOutcome::WouldBlock {
        blocked.push(id);
      }
    }
    blocked
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
      for (_iface, s) in self.multicast_sockets {
        size = s.send_to(buffer, address)?;
      }
      Ok(size)
    } else {
      io::Result::Err(io::Error::other("Not a multicast address"))
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
