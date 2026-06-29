use crate::{
  rtps::{reader, writer},
  structure::guid::EntityId,
};

// Payload for the single shared timer of a `DPEventLoop`.
//
// Previously every Reader and Writer owned its own `mio_extras` timer (each
// spawning a background thread), and the loop also had separate timers for
// preemptive ACKNACKs and DDSCache garbage collection. Now a single shared
// timer carries all of these; the payload identifies which endpoint or task a
// fired timeout belongs to so the event loop can dispatch it.
pub(crate) enum DpTimerEvent {
  Reader {
    entity_id: EntityId,
    event: reader::TimedEvent,
  },
  Writer {
    entity_id: EntityId,
    event: writer::TimedEvent,
  },
  PreemptiveAcknack,
  CacheGc,
}
