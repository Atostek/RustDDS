//! Core types for the nonblocking-transmit outbound path.
//!
//! See `src/rtps/nonblocking_transmit_design.md`. These types are shared by
//! `UDPSender` (which owns the sockets and the never-dropped control queues),
//! the `Writer` (bulk DATA producer, resumable), and `DPEventLoop` (which owns
//! the poll and the per-socket round-robin of writers willing to send bulk).

use std::{collections::VecDeque, net::SocketAddr};

/// Identifies one physical sender socket owned by `UDPSender`.
///
/// `Multicast(i)` indexes into `UDPSender::multicast_sockets` (one socket per
/// local multicast-capable interface).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum SocketId {
  Unicast,
  Multicast(usize),
}

/// Outcome of a single non-blocking datagram send attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SendOutcome {
  /// The datagram was handed to the kernel.
  Sent,
  /// The kernel send buffer is full. For control this means "keep it queued";
  /// for bulk this means "stop and back off, resume on writable".
  WouldBlock,
  /// A permanent error (bad address, encode failure, unknown locator kind).
  /// Never retried.
  Dropped,
}

/// Traffic class deciding the per-socket queueing policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrafficClass {
  /// Control/discovery (HEARTBEAT, ACKNACK, GAP, SPDP/SEDP): high priority,
  /// never dropped, drained before bulk data.
  Control,
  /// Bulk user DATA/DATAFRAG: flow-controlled; on WouldBlock the producer stops
  /// and is resumed on write readiness (or, for repair, simply retried later).
  Bulk,
}

/// A queued outbound datagram. Used only for the never-dropped control queue;
/// bulk data is regenerated on demand and never buffered here.
#[derive(Debug)]
pub(crate) struct Datagram {
  pub addr: SocketAddr,
  pub bytes: Vec<u8>,
}

/// A per-socket FIFO of control datagrams that must never be dropped.
pub(crate) type ControlQueue = VecDeque<Datagram>;

/// Soft high-watermark for a per-socket control queue. Exceeding it only logs a
/// warning (a persistently wedged socket usually means the peer/link is dead);
/// nothing is ever dropped.
pub(crate) const CONTROL_QUEUE_WARN_LEN: usize = 1024;
