use std::{
  collections::HashMap,
  io,
  net::{IpAddr, Ipv4Addr, SocketAddr},
};

use log::{debug, error, info, trace, warn};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use bytes::{Bytes, BytesMut};

use crate::{
  network::util::{
    build_ifindex_to_interface_map, get_local_multicast_ip_addrs_filtered,
    get_local_multicast_locators, get_local_unicast_locators_filtered,
  },
  rtps::transmit::InterfaceSelector,
  serialization::padding_needed_for_alignment_4,
  structure::locator::Locator,
};

/// Metadata captured about where a received datagram came from and how it
/// reached us.
#[derive(Clone, Copy, Debug)]
pub struct PacketOrigin {
  /// Remote source socket address, if it could be determined.
  pub source: Option<SocketAddr>,
  /// Local interface the datagram was received on, if it could be determined
  /// (requires `IP_PKTINFO`; `None` on platforms/paths where it is
  /// unavailable).
  pub local_if: Option<InterfaceSelector>,
}

impl PacketOrigin {
  /// An origin with no captured metadata (forces the legacy send fallback).
  #[allow(dead_code)] // Used by tests and the loopback path; harmless if unused in a given build.
  pub const UNKNOWN: Self = Self {
    source: None,
    local_if: None,
  };
}

const MAX_MESSAGE_SIZE: usize = 64 * 1024; // This is max we can get from UDP.
const MESSAGE_BUFFER_ALLOCATION_CHUNK: usize = 256 * 1024; // must be >= MAX_MESSAGE_SIZE
static_assertions::const_assert!(MESSAGE_BUFFER_ALLOCATION_CHUNK > MAX_MESSAGE_SIZE);

/// Listens to messages coming to specified host port combination.
/// Only messages from added listen addressed are read when get_all_messages is
/// called.
#[derive(Debug)]
pub struct UDPListener {
  socket: mio_06::net::UdpSocket,
  receive_buffer: BytesMut,
  multicast_group: Option<Ipv4Addr>,
  has_multicast_join: bool,
  // Cached OS interface-index -> local interface map, used to resolve the
  // receiving interface reported by IP_PKTINFO. Built once at construction.
  ifindex_map: HashMap<u32, InterfaceSelector>,
}

impl Drop for UDPListener {
  fn drop(&mut self) {
    if let Some(mcg) = self.multicast_group {
      self
        .socket
        .leave_multicast_v4(&mcg, &Ipv4Addr::UNSPECIFIED)
        .unwrap_or_else(|e| {
          error!("leave_multicast_group: {e:?}");
        });
    }
  }
}

impl UDPListener {
  fn new_listening_socket(
    host: &str,
    port: u16,
    reuse_addr: bool,
    recv_buffer_size: usize,
  ) -> io::Result<mio_06::net::UdpSocket> {
    let raw_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

    if recv_buffer_size > 0 {
      raw_socket
        .set_recv_buffer_size(recv_buffer_size)
        .unwrap_or_else(|e| {
          warn!("Failed to set SO_RCVBUF to {recv_buffer_size}: {e}. Using OS default.");
        });

      // Verify the effective size (getsockopt). The kernel silently clamps
      // SO_RCVBUF to a per-socket ceiling (macOS: `kern.ipc.maxsockbuf`, which
      // defaults to 8 MiB; Linux: `net.core.rmem_max`). Note Linux reports back
      // ~2x the requested value (bookkeeping overhead). Warn only when the
      // effective size is materially below what we asked for, so silent clamping
      // that can cause packet loss under load is visible in the logs.
      match raw_socket.recv_buffer_size() {
        Ok(effective) => {
          if effective < recv_buffer_size {
            warn!(
              "SO_RCVBUF clamped: requested {recv_buffer_size} bytes but got {effective} bytes. \
               The kernel limits per-socket receive buffers; raise it to reduce packet loss under \
               load (macOS: `sudo sysctl -w kern.ipc.maxsockbuf=<bytes>`, Linux: `sudo sysctl -w \
               net.core.rmem_max=<bytes>`)."
            );
          } else {
            debug!("SO_RCVBUF requested {recv_buffer_size} bytes, effective {effective} bytes.");
          }
        }
        Err(e) => {
          debug!("Could not read back SO_RCVBUF: {e}");
        }
      }
    }

    // We set ReuseAddr so that other DomainParticipants on this host can
    // bind to the same multicast address and port.
    // To have an effect on bind, this must be done before bind call, so must be
    // done below Rust std::net::UdpSocket level.
    if reuse_addr {
      raw_socket.set_reuse_address(true)?;
    }

    // MacOS requires this also
    #[cfg(not(any(target_os = "solaris", target_os = "illumos", windows)))]
    {
      if reuse_addr {
        raw_socket.set_reuse_port(true)?;
      }
    }

    // Ask the kernel to attach IP_PKTINFO to received datagrams so we can learn
    // which local interface each one arrived on. Best-effort: if it fails we
    // simply lose interface metadata and fall back to the legacy send path.
    #[cfg(unix)]
    {
      if let Err(e) = nix::sys::socket::setsockopt(
        &raw_socket,
        nix::sys::socket::sockopt::Ipv4PacketInfo,
        &true,
      ) {
        warn!(
          "Could not enable IP_PKTINFO on listener socket: {e}. Interface-aware transmit disabled \
           for this socket."
        );
      }
    }

    let address = SocketAddr::new(host.parse().map_err(io::Error::other)?, port);

    if let Err(e) = raw_socket.bind(&SockAddr::from(address)) {
      info!("new_socket - cannot bind socket: {e:?}");
      return Err(e);
    }

    let std_socket = std::net::UdpSocket::from(raw_socket);
    std_socket
      .set_nonblocking(true)
      .map_err(|e| io::Error::other(format!("Failed to set std socket to non blocking: {e}")))?;

    let mio_socket = mio_06::net::UdpSocket::from_socket(std_socket)
      .map_err(|e| io::Error::other(format!("Unable to create mio socket: {e}")))?;
    info!(
      "UDPListener: new socket with address {:?}",
      mio_socket.local_addr()
    );

    Ok(mio_socket)
  }

  pub fn to_locator_address(&self, only_networks: Option<&[IpAddr]>) -> io::Result<Vec<Locator>> {
    let local_port = self.socket.local_addr()?.port();

    match self.multicast_group {
      Some(_ipv4_addr) if self.has_multicast_join => Ok(get_local_multicast_locators(local_port)),
      Some(_ipv4_addr) => Ok(vec![]),
      None => Ok(get_local_unicast_locators_filtered(
        local_port,
        only_networks,
      )),
    }
  }

  #[cfg(test)]
  pub fn new_unicast(host: &str, port: u16) -> io::Result<Self> {
    Self::new_unicast_with_buf_size(host, port, 0)
  }

  pub fn new_unicast_with_buf_size(
    host: &str,
    port: u16,
    recv_buffer_size: usize,
  ) -> io::Result<Self> {
    let mio_socket = Self::new_listening_socket(host, port, false, recv_buffer_size)?;

    Ok(Self {
      socket: mio_socket,
      receive_buffer: BytesMut::with_capacity(MESSAGE_BUFFER_ALLOCATION_CHUNK),
      multicast_group: None,
      has_multicast_join: false,
      ifindex_map: build_ifindex_to_interface_map(),
    })
  }

  #[cfg(test)]
  pub fn new_multicast(host: &str, port: u16, multicast_group: Ipv4Addr) -> io::Result<Self> {
    Self::new_multicast_with_buf_size(host, port, multicast_group, 0, None)
  }

  pub fn new_multicast_with_buf_size(
    host: &str,
    port: u16,
    multicast_group: Ipv4Addr,
    recv_buffer_size: usize,
    only_networks: Option<&[IpAddr]>,
  ) -> io::Result<Self> {
    if !multicast_group.is_multicast() {
      return io::Result::Err(io::Error::other("Not a multicast address"));
    }

    let mio_socket = Self::new_listening_socket(host, port, true, recv_buffer_size)?;
    let mut joined_multicast = false;

    for multicast_if_ipaddr in get_local_multicast_ip_addrs_filtered(only_networks)? {
      match multicast_if_ipaddr {
        IpAddr::V4(a) => mio_socket
          .join_multicast_v4(&multicast_group, &a)
          .map(|()| {
            joined_multicast = true;
          })
          .unwrap_or_else(|e| {
            warn!(
              "join_multicast_v4 failed: {e:?}. multicast_group [{multicast_group:?}] interface \
               [{a:?}]"
            );
          }),

        IpAddr::V6(addr) => {
          mio_socket
            .join_multicast_v6(&addr, 0)
            .map(|()| {
              joined_multicast = true;
            })
            .unwrap_or_else(|e| {
              warn!(
                "join_multicast_v6 failed. err: {e}. mcast group: [{multicast_group:?}], \
                 addr:[{addr:?}]"
              );
            });
        }
      }
    }

    if !joined_multicast {
      warn!(
        "No multicast joins succeeded for group {multicast_group:?}; multicast locator will not \
         be advertised."
      );
    }

    Ok(Self {
      socket: mio_socket,
      receive_buffer: BytesMut::with_capacity(MESSAGE_BUFFER_ALLOCATION_CHUNK),
      multicast_group: Some(multicast_group),
      has_multicast_join: joined_multicast,
      ifindex_map: build_ifindex_to_interface_map(),
    })
  }

  pub fn mio_socket(&mut self) -> &mut mio_06::net::UdpSocket {
    &mut self.socket
  }

  #[cfg(test)]
  pub fn port(&self) -> u16 {
    match self.socket.local_addr() {
      Ok(add) => add.port(),
      _ => 0,
    }
  }

  // TODO: remove this. It is used only for tests.
  // We cannot read a single packet only, because we use edge-triggered polls.
  #[cfg(test)]
  pub fn get_message(&self) -> Vec<u8> {
    let mut buf: [u8; MAX_MESSAGE_SIZE] = [0; MAX_MESSAGE_SIZE];

    // try getting the message several times
    for _ in 0..10 {
      match self.socket.recv(&mut buf) {
        Ok(nbytes) => {
          assert!(nbytes > 0, "tests should always read data");

          return buf[..nbytes].to_vec();
        }
        Err(e) => {
          // handle EAGAIN on UNIX platforms.
          //
          // this means we need to wait for the kernel to give us the data.
          if e.kind() == io::ErrorKind::WouldBlock {
            std::thread::sleep(core::time::Duration::from_millis(50));
            continue;
          }

          panic!("test helper (`get_message`) failed! err: {e}");
        }
      }
    }

    panic!("test helper didn't recv message after ten attempts.");
  }

  /// Drain up to `max_messages` datagrams waiting in the socket, each paired
  /// with its [`PacketOrigin`] (source address + receiving interface, when
  /// available). Pass `usize::MAX` to read everything currently queued. Used
  /// by the event loop to cap how much bulk traffic one socket can process
  /// per poll iteration, so a flood on one socket cannot starve the
  /// (single-threaded) loop from servicing discovery/control sockets. Relies
  /// on the listener being registered level-triggered, so undrained data
  /// re-fires on the next poll.
  pub fn messages_bounded(&mut self, max_messages: usize) -> Vec<(Bytes, PacketOrigin)> {
    let mut messages = Vec::with_capacity(4);

    loop {
      if messages.len() >= max_messages {
        return messages;
      }
      // Loop invariant. Note that capacity() may be large, but .len() == 0.
      assert_eq!(self.receive_buffer.len(), 0);

      // Ensure that receive buffer has enough capacity for a message
      if self.receive_buffer.capacity() < MAX_MESSAGE_SIZE {
        self.receive_buffer = BytesMut::with_capacity(MESSAGE_BUFFER_ALLOCATION_CHUNK);
        debug!("ensure_receive_buffer_capacity - reallocated receive_buffer");
      }
      unsafe {
        // This is safe, because we just checked that there is enough capacity,
        // or allocated more.
        // We do not read undefined data, because the recv()
        // will overwrite this space and truncate the rest away.
        self.receive_buffer.set_len(MAX_MESSAGE_SIZE);
      }
      trace!(
        "ensure_receive_buffer_capacity - {} bytes left",
        self.receive_buffer.capacity()
      );
      let (nbytes, origin) = match self.recv_one() {
        Ok(Some(received)) => received,
        Ok(None) => {
          // WouldBlock: nothing (more) to read.
          self.receive_buffer.clear();
          return messages;
        }
        Err(e) => {
          self.receive_buffer.clear();
          warn!("socket recv() error: {e:?}");
          return messages;
        }
      };
      // Something was received.
      // The buffer length is still MAX_MESSAGE_SIZE, set before the receive so
      // the kernel had room to write into. Shrink it back to the number of bytes
      // actually received, so that the padding + split below only consume this
      // datagram's worth of the chunk. Without this, every datagram (however
      // small) would carve off a full MAX_MESSAGE_SIZE slot, wasting the chunk
      // and defeating the packing this alignment logic assumes.
      unsafe {
        // Safe: recv_one wrote `nbytes` valid bytes at the front of the buffer,
        // and nbytes <= MAX_MESSAGE_SIZE == the current len.
        self.receive_buffer.set_len(nbytes);
      }

      // Now, append some extra data to align the buffer end, so the next piece will
      // be aligned also. This assumes that the initial buffer was aligned to begin
      // with. This is because RTPS data is optimized to align to 4-byte boundaries.
      let pad = padding_needed_for_alignment_4(self.receive_buffer.len());
      if pad != 0 {
        self
          .receive_buffer
          .extend_from_slice(&[0xCC, 0xCC, 0xCC, 0xCC][..pad]);
        // Funny value 0xCC encourages a fast crash in case these bytes
        // are ever accessed, as they should not.
      }

      // Now split away the used portion.
      let mut message = self.receive_buffer.split_to(self.receive_buffer.len());
      message.truncate(nbytes); // discard (hide) padding
      messages.push((Bytes::from(message), origin)); // freeze bytes and push
    } // loop

    // unreachable!(); // But why does this cause a warning? (rustc 1.66.0)
    // Answer: https://github.com/rust-lang/rust/issues/46500
  }

  /// Receive a single datagram into `self.receive_buffer`, capturing its
  /// [`PacketOrigin`]. Returns `Ok(None)` when the socket would block.
  #[cfg(unix)]
  fn recv_one(&mut self) -> io::Result<Option<(usize, PacketOrigin)>> {
    use std::{io::IoSliceMut, os::unix::io::AsRawFd};

    use nix::{
      errno::Errno,
      sys::socket::{recvmsg, ControlMessageOwned, MsgFlags, SockaddrStorage},
    };

    let fd = self.socket.as_raw_fd();
    let mut cmsg_space = nix::cmsg_space!(nix::libc::in_pktinfo);

    // Read the datagram and pull out the Copy metadata; the borrow of
    // `receive_buffer` (through `iov`) ends when this block ends.
    let (nbytes, source, ifindex, spec_dst) = {
      let mut iov = [IoSliceMut::new(
        &mut self.receive_buffer[..MAX_MESSAGE_SIZE],
      )];
      let msg =
        match recvmsg::<SockaddrStorage>(fd, &mut iov, Some(&mut cmsg_space), MsgFlags::empty()) {
          Ok(m) => m,
          Err(Errno::EAGAIN) => return Ok(None),
          Err(e) => return Err(io::Error::from_raw_os_error(e as i32)),
        };

      let nbytes = msg.bytes;
      let source = msg.address.and_then(sockaddr_storage_to_socketaddr);

      let mut ifindex = 0u32;
      let mut spec_dst: Option<IpAddr> = None;
      for cmsg in msg.cmsgs()? {
        if let ControlMessageOwned::Ipv4PacketInfo(info) = cmsg {
          ifindex = info.ipi_ifindex as u32;
          let addr = Ipv4Addr::from(u32::from_be(info.ipi_spec_dst.s_addr));
          if !addr.is_unspecified() {
            spec_dst = Some(IpAddr::V4(addr));
          }
        }
      }
      (nbytes, source, ifindex, spec_dst)
    };

    // Prefer the exact local destination address (matches sender interface
    // keys directly); otherwise resolve the interface index.
    let local_if = spec_dst
      .map(InterfaceSelector::Ip)
      .or_else(|| self.ifindex_map.get(&ifindex).copied());

    Ok(Some((nbytes, PacketOrigin { source, local_if })))
  }

  /// Non-Unix fallback: capture the source address only (no interface info).
  #[cfg(not(unix))]
  fn recv_one(&mut self) -> io::Result<Option<(usize, PacketOrigin)>> {
    match self
      .socket
      .recv_from(&mut self.receive_buffer[..MAX_MESSAGE_SIZE])
    {
      Ok((nbytes, source)) => Ok(Some((
        nbytes,
        PacketOrigin {
          source: Some(source),
          local_if: None,
        },
      ))),
      Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
      Err(e) => Err(e),
    }
  }

  #[cfg(test)] // normally done in .drop()
  pub fn leave_multicast(&self, address: &Ipv4Addr) -> io::Result<()> {
    if address.is_multicast() {
      return self
        .socket
        .leave_multicast_v4(address, &Ipv4Addr::UNSPECIFIED);
    }
    io::Result::Err(io::Error::other("Not a multicast address"))
  }
}

#[cfg(unix)]
fn sockaddr_storage_to_socketaddr(addr: nix::sys::socket::SockaddrStorage) -> Option<SocketAddr> {
  use std::net::{SocketAddrV4, SocketAddrV6};
  if let Some(v4) = addr.as_sockaddr_in() {
    Some(SocketAddr::V4(SocketAddrV4::new(v4.ip(), v4.port())))
  } else {
    addr.as_sockaddr_in6().map(|v6| {
      SocketAddr::V6(SocketAddrV6::new(
        v6.ip(),
        v6.port(),
        v6.flowinfo(),
        v6.scope_id(),
      ))
    })
  }
}

#[cfg(test)]
mod tests {
  // use std::os::unix::io::AsRawFd;
  // use nix::sys::socket::setsockopt;
  // use nix::sys::socket::sockopt::IpMulticastLoop;
  use std::{thread, time};

  use super::*;
  use crate::network::udp_sender::*;

  #[test]
  fn udpl_single_address() {
    let listener = UDPListener::new_unicast("127.0.0.1", 10001).unwrap();
    let sender = UDPSender::new_with_random_port().expect("failed to create UDPSender");

    let data: Vec<u8> = vec![0, 1, 2, 3, 4];

    let addrs = vec![SocketAddr::new("127.0.0.1".parse().unwrap(), 10001)];
    sender.send_to_all(&data, &addrs);

    let rec_data = listener.get_message();

    assert_eq!(rec_data.len(), 5); // It appears that this test may randomly fail.
    assert_eq!(rec_data, data);
  }

  #[test]
  fn udpl_multicast_address() {
    let listener =
      UDPListener::new_multicast("0.0.0.0", 10002, Ipv4Addr::new(239, 255, 0, 1)).unwrap();
    let sender = UDPSender::new_with_random_port().unwrap();

    // setsockopt(sender.socket.as_raw_fd(), IpMulticastLoop, &true)
    //  .expect("Unable set IpMulticastLoop option on socket");

    let data: Vec<u8> = vec![2, 4, 6];

    sender
      .send_multicast(&data, Ipv4Addr::new(239, 255, 0, 1), 10002)
      .expect("Failed to send multicast");

    thread::sleep(time::Duration::from_secs(1));

    let rec_data = listener.get_message();

    listener
      .leave_multicast(&Ipv4Addr::new(239, 255, 0, 1))
      .unwrap();

    assert_eq!(rec_data.len(), 3);
    assert_eq!(rec_data, data);
  }
}
