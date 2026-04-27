use std::net::{SocketAddrV4, SocketAddrV6};
pub use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use speedy::{Context, Readable, Reader, Writable, Writer};

mod kind {
  pub const INVALID: i32 = -1;
  pub const RESERVED: i32 = 0;
  pub const UDP_V4: i32 = 1;
  pub const UDP_V6: i32 = 2;
}

const INVALID_PORT: u16 = 0;
const INVALID_ADDRESS: [u8; 16] = [0; 16];

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Locator {
  Invalid,
  Reserved,
  UdpV4(SocketAddrV4),
  UdpV6(SocketAddrV6),
  Other {
    kind: i32,
    port: u32,
    address: [u8; 16],
  },
}

impl Locator {
  pub fn is_udp(&self) -> bool {
    matches!(self, Self::UdpV4(_) | Self::UdpV6(_))
  }

  pub fn is_loopback(&self) -> bool {
    match self {
      Locator::UdpV4(socket_address) => socket_address.ip().is_loopback(),
      Locator::UdpV6(socket_address) => socket_address.ip().is_loopback(),
      _ => false,
    }
  }
}

impl From<Locator> for SocketAddr {
  fn from(locator: Locator) -> Self {
    match locator {
      Locator::UdpV4(socket_address) => socket_address.into(),
      Locator::UdpV6(socket_address) => socket_address.into(),
      Locator::Invalid | Locator::Reserved | Locator::Other { .. } => {
        let ip = Ipv6Addr::from(INVALID_ADDRESS).into();
        Self::new(ip, INVALID_PORT)
      }
    }
  }
}

impl From<SocketAddr> for Locator {
  fn from(socket_address: SocketAddr) -> Self {
    if socket_address.ip().is_unspecified() {
      return Self::Invalid;
    }
    match socket_address {
      SocketAddr::V4(socket_address) => Self::UdpV4(socket_address),
      SocketAddr::V6(socket_address) => Self::UdpV6(socket_address),
    }
  }
}

// use repr::Locator to serialize

impl<'a, C: Context> Readable<'a, C> for Locator {
  fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
    let repr = repr::Locator::read_from(reader)?;
    Ok(repr.into())
  }

  /// Wire size of [`repr::Locator`]. Used by `speedy` when deserializing
  /// `Vec<Locator>` so the length prefix is checked against the remaining
  /// buffer *before* allocating (see e.g. issue #404).
  fn minimum_bytes_needed() -> usize {
    std::mem::size_of::<repr::Locator>()
  }
}

impl<C: Context> Writable<C> for Locator {
  fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
    let repr = repr::Locator::from(*self);
    repr.write_to(writer)
  }
}

impl From<repr::Locator> for Locator {
  fn from(repr: repr::Locator) -> Self {
    match repr.kind {
      kind::INVALID => Self::Invalid,
      kind::RESERVED => Self::Reserved,
      kind::UDP_V4 => {
        let ip = Ipv4Addr::new(
          repr.address[12],
          repr.address[13],
          repr.address[14],
          repr.address[15],
        );
        // repr.port is 32 bits, but we just truncate it to u16
        let socket_address = SocketAddrV4::new(ip, repr.port as u16);

        Self::UdpV4(socket_address)
      }
      kind::UDP_V6 => {
        let ip = Ipv6Addr::from(repr.address);
        let socket_address = SocketAddrV6::new(ip, repr.port as u16, 0, 0);

        Self::UdpV6(socket_address)
      }
      kind => Self::Other {
        kind,
        port: repr.port,
        address: repr.address,
      },
    }
  }
}

impl From<Locator> for repr::Locator {
  fn from(locator: Locator) -> Self {
    let (kind, port, address) = match locator {
      Locator::Invalid => (kind::INVALID, INVALID_PORT.into(), INVALID_ADDRESS),
      Locator::Reserved => (kind::RESERVED, INVALID_PORT.into(), INVALID_ADDRESS),
      Locator::UdpV4(socket_address) => {
        let kind = kind::UDP_V4;
        let port = socket_address.port();
        let address = socket_address.ip().to_ipv6_compatible().octets();
        (kind, port.into(), address)
      }
      Locator::UdpV6(socket_address) => {
        let kind = kind::UDP_V6;
        let port = socket_address.port();
        let address = socket_address.ip().octets();
        (kind, port.into(), address)
      }
      Locator::Other {
        kind,
        port,
        address,
      } => (kind, port, address),
    };

    Self {
      kind,
      port,
      address,
    }
  }
}

pub(crate) mod repr {

  use speedy::{Readable, Writable};

  #[derive(Writable, Readable)]
  pub struct Locator {
    pub kind: i32,
    pub port: u32,
    pub address: [u8; 16],
  }
}

#[cfg(test)]
mod tests {
  use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

  use speedy::{Endianness, Readable, Writable};
  use test_case::test_case;

  use super::{repr, Locator};

  #[test_case(
    &[
      0x01, 0x00, 0x00, 0x00,  // LocatorKind_t::LOCATOR_KIND_UDP_V4
      0x90, 0x1F, 0x00, 0x00,  // Locator_t::port(8080),
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x7F, 0x00, 0x00, 0x01   // Locator_t::address[12:15]
    ]
    =>  Locator::from(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 8080))
    ; "IPv4 deserialize"
  )]
  #[test_case(
    &[
      0x01, 0x00, 0x00, 0x00,  // LocatorKind_t::LOCATOR_KIND_UDP_V4
      0xf2, 0x1c, 0x00, 0x01,  // not representable
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x0A, 0x00, 0x00, 0x0F   // Locator_t::address[12:15]
    ]
    =>  Locator::from(SocketAddr::new(Ipv4Addr::new(0x0A, 0, 0, 0x0F).into(), 0x1cf2))
    ; "IPv4 fuzz"
  )]
  // test body
  fn deserialize_le(little_endian: &[u8]) -> Locator {
    repr::Locator::read_from_buffer_with_ctx(Endianness::LittleEndian, little_endian)
      .unwrap()
      .into() // repr::Locator -> Locator
  }

  #[test_case(SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0) => Locator::Invalid ; "unspecified IPv6")]
  fn from_socket_address(socket_addr: impl Into<Locator>) -> Locator {
    socket_addr.into()
  }

  #[test_case(
    Locator::Invalid,
    [
      0xFF, 0xFF, 0xFF, 0xFF,  // LocatorKind_t::LOCATOR_KIND_INVALID
      0x00, 0x00, 0x00, 0x00,  // Locator_t::LOCATOR_PORT_INVALID,
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x00, 0x00, 0x00, 0x00   // Locator_t::address[12:15]
    ],
    [
      0xFF, 0xFF, 0xFF, 0xFF,  // LocatorKind_t::LOCATOR_KIND_UDP_V4
      0x00, 0x00, 0x00, 0x00,  // Locator_t::LOCATOR_PORT_INVALID,
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x00, 0x00, 0x00, 0x00   // Locator_t::address[12:15]
    ]
    ; "invalid"
  )]
  #[test_case(
    Locator::from(SocketAddr::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0).into(), 7171)),
    [
      0xFF, 0xFF, 0xFF, 0xFF,  // LocatorKind_t::LOCATOR_KIND_INVALID
      0x00, 0x00, 0x00, 0x00,  // Locator_t::LOCATOR_PORT_INVALID,
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x00, 0x00, 0x00, 0x00   // Locator_t::address[12:15]
    ],
    [
      0xFF, 0xFF, 0xFF, 0xFF,  // LocatorKind_t::LOCATOR_KIND_INVALID
      0x00, 0x00, 0x00, 0x00,  // Locator_t::LOCATOR_PORT_INVALID,
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x00, 0x00, 0x00, 0x00   // Locator_t::address[12:15]
    ]
    ; "invalid IPv6"
  )]
  #[test_case(
    Locator::from(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 8080)),
    [
      0x00, 0x00, 0x00, 0x01,  // LocatorKind_t::LOCATOR_KIND_UDP_V4
      0x00, 0x00, 0x1F, 0x90,  // Locator_t::port(8080),
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x7F, 0x00, 0x00, 0x01   // Locator_t::address[12:15]
    ],
    [
      0x01, 0x00, 0x00, 0x00,  // LocatorKind_t::LOCATOR_KIND_UDP_V4
      0x90, 0x1F, 0x00, 0x00,  // Locator_t::port(8080),
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x7F, 0x00, 0x00, 0x01   // Locator_t::address[12:15]
    ]
    ; "IPv4"
  )]
  #[test_case(
    Locator::from(SocketAddr::new(Ipv6Addr::new(0xFF00, 0x4501, 0, 0, 0, 0, 0, 0x0032).into(), 7171)),
    [
      0x00, 0x00, 0x00, 0x02,  // LocatorKind_t::LOCATOR_KIND_UDP_V6
      0x00, 0x00, 0x1C, 0x03,  // Locator_t::port(7171),
      0xFF, 0x00, 0x45, 0x01,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x00, 0x00, 0x00, 0x32   // Locator_t::address[12:15]
    ],
    [
      0x02, 0x00, 0x00, 0x00,  // LocatorKind_t::LOCATOR_KIND_UDP_V6
      0x03, 0x1C, 0x00, 0x00,  // Locator_t::port(7171),
      0xFF, 0x00, 0x45, 0x01,  // Locator_t::address[0:3]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[4:7]
      0x00, 0x00, 0x00, 0x00,  // Locator_t::address[8:11]
      0x00, 0x00, 0x00, 0x32   // Locator_t::address[12:15]
    ]
    ; "IPv6"
  )]
  fn serialization(locator: Locator, big_endian: [u8; 24], little_endian: [u8; 24]) {
    assert_eq!(
      locator
        .write_to_vec_with_ctx(Endianness::BigEndian)
        .unwrap(),
      big_endian,
    );
    assert_eq!(
      locator
        .write_to_vec_with_ctx(Endianness::LittleEndian)
        .unwrap(),
      little_endian
    );
  }

  /// Regression: `Vec<Locator>` must not call `with_capacity` from a forged
  /// length alone (issue #404).
  #[test]
  fn vec_rejects_length_prefix_inconsistent_with_buffer() {
    let count_only = [0x80u8, 0x94, 0x20, 0x44];
    let result = Vec::<Locator>::read_from_buffer_with_ctx(Endianness::LittleEndian, &count_only);
    assert!(result.is_err());
  }
}
