//! Regression test: SEDP endpoint announcements must carry a source timestamp
//! (an INFO_TS submessage before the DATA).
//!
//! Discovery writes its samples without a timestamp, and RustDDS used to send
//! them without INFO_TS. Fast DDS then gives the samples source time 0. Its
//! reader history orders samples by sequence number within one writer and by
//! source time across writers, so samples from two RustDDS participants
//! compare equal. A late sample can then be inserted after a later sequence
//! number of its own writer, and Fast DDS's in-order notification skips it for
//! good: with Fast DDS 2.14 a ROS 2 subscriber that joined after a RustDDS
//! node announced its writers sometimes never matched one of them.
//!
//! The test listens on the domain's discovery multicast port like any other
//! participant would and checks every SEDP publication DATA it sees there.
//! Only this test's participants use the domain.
use std::{
  net::{Ipv4Addr, SocketAddrV4, UdpSocket},
  time::{Duration, Instant},
};

use rustdds::{policy, DomainParticipant, QosPolicyBuilder, TopicKind};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};

mod common;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Ping {
  seq: u32,
}

/// This test's offset from `RUSTDDS_TEST_DOMAIN_ID`: a domain of its own.
const DOMAIN_OFFSET: u16 = 50;
const SEDP_PUBLICATIONS_WRITER: [u8; 4] = [0x00, 0x00, 0x03, 0xc2];

/// The discovery multicast port of a domain (RTPS 9.6.1.1: PB + DG * domain).
fn spdp_multicast_port(domain_id: u16) -> u16 {
  7400 + 250 * domain_id
}

fn multicast_listener(port: u16) -> UdpSocket {
  let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
  socket.set_reuse_address(true).unwrap();
  #[cfg(unix)]
  socket.set_reuse_port(true).unwrap();
  socket
    .bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port).into())
    .unwrap();
  let socket: UdpSocket = socket.into();
  socket
    .join_multicast_v4(&Ipv4Addr::new(239, 255, 0, 1), &Ipv4Addr::UNSPECIFIED)
    .unwrap();
  socket
    .set_read_timeout(Some(Duration::from_millis(100)))
    .unwrap();
  socket
}

/// For each SEDP publications DATA in an RTPS datagram: whether a valid
/// INFO_TS precedes it.
fn sedp_publication_data(datagram: &[u8]) -> Vec<bool> {
  let mut found = Vec::new();
  if datagram.len() < 20 || &datagram[0..4] != b"RTPS" {
    return found;
  }
  let mut timestamp_valid = false;
  let mut offset = 20;
  while offset + 4 <= datagram.len() {
    let (id, flags) = (datagram[offset], datagram[offset + 1]);
    let length_bytes = [datagram[offset + 2], datagram[offset + 3]];
    let length = if flags & 0x01 != 0 {
      u16::from_le_bytes(length_bytes)
    } else {
      u16::from_be_bytes(length_bytes)
    } as usize;
    let body = &datagram[offset + 4..(offset + 4 + length).min(datagram.len())];
    match id {
      // INFO_TS; the I flag (0x02) invalidates the current timestamp.
      0x09 => timestamp_valid = flags & 0x02 == 0,
      // DATA: extraFlags (2), octetsToInlineQos (2), readerId (4), writerId (4).
      0x15 if body.len() >= 12 && body[8..12] == SEDP_PUBLICATIONS_WRITER => {
        found.push(timestamp_valid)
      }
      _ => {}
    }
    if length == 0 {
      break; // the last submessage extends to the end of the datagram
    }
    offset += 4 + length;
  }
  found
}

#[test]
fn sedp_announcements_carry_a_source_timestamp() {
  let domain_id = common::test_domain_id(DOMAIN_OFFSET);
  let listener = multicast_listener(spdp_multicast_port(domain_id));

  // Two participants, so that each has a remote SEDP reader to announce to.
  let announcer = DomainParticipant::new(domain_id).unwrap();
  let _peer = DomainParticipant::new(domain_id).unwrap();

  let qos = QosPolicyBuilder::new()
    .reliability(policy::Reliability::Reliable {
      max_blocking_time: rustdds::Duration::from_secs(1),
    })
    .build();
  let publisher = announcer.create_publisher(&qos).unwrap();
  let mut writers = Vec::new();

  let mut announcements = Vec::new();
  let deadline = Instant::now() + Duration::from_secs(20);
  let mut buffer = [0u8; 65536];
  while Instant::now() < deadline && announcements.len() < 5 {
    // Keep creating writers: each is announced over SEDP once the peer is known.
    if writers.len() < 20 {
      let topic = announcer
        .create_topic(
          format!("sedp_source_timestamp_test_{}", writers.len()),
          "Ping".to_string(),
          &qos,
          TopicKind::NoKey,
        )
        .unwrap();
      writers.push(
        publisher
          .create_datawriter_no_key_cdr::<Ping>(&topic, None)
          .unwrap(),
      );
    }
    if let Ok(size) = listener.recv(&mut buffer) {
      announcements.extend(sedp_publication_data(&buffer[..size]));
    }
  }

  assert!(
    !announcements.is_empty(),
    "saw no SEDP publication DATA from the announcer on the multicast port"
  );
  let without = announcements.iter().filter(|stamped| !**stamped).count();
  assert_eq!(
    without,
    0,
    "{without} of {} SEDP publication DATA had no INFO_TS",
    announcements.len()
  );
}
