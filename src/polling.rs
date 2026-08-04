// Currently, just helpers for mio library

// TODO: Expand this to become an iternal API for polling operations:
// * sockets (send/recv)
// * timers
// * inter-thread channels
//
// Then we cold implement them either on top of mio-0.6, mio-0.8 or something
// else

use std::{cell::RefCell, rc::Rc};

use mio_extras::{timer, timer::Timer};

// A timer handle that can be shared (cloned) among the endpoints/tasks that run
// on a single event-loop thread. The event loop owns the underlying `Timer`
// and registers it with the `Poll` exactly once, while endpoints schedule
// timeouts on the same timer through their cloned handle. This is sound because
// an event loop and all the endpoints it drives run on the same thread (the
// loop is `!Send`), so no synchronization is required.
//
// Sharing a single timer this way avoids the previous design where every
// endpoint (and periodic task) owned a separate `mio_extras` `Timer`, each of
// which spawns its own background OS thread.
pub(crate) type SharedTimer<E> = Rc<RefCell<Timer<E>>>;

// Constructor for a shared timer. Because a single shared timer now holds all
// the in-flight timeouts for an entire event loop (every Reader, Writer and
// periodic task), its `capacity` (the hard cap on simultaneously scheduled
// timeouts) must be sized generously.
//
// (The default `mio_extras` timer has 256 wheel slots and capacity 65536, which
// uses a lot of memory; we pick smaller but still generous values. Each timer
// also spawns its own background OS thread, which is exactly why we now share
// one timer per event loop instead of one per endpoint.)
pub(crate) fn new_shared_timer<E>() -> SharedTimer<E> {
  // tick_duration: the timer-wheel resolution. `mio_extras` rounds every
  // scheduled timeout UP to at least one tick, so its 100 ms default silently
  // inflates all sub-100 ms timeouts. Several reliability timeouts are meant to
  // be much shorter than that (reliable repair reschedules itself at
  // `deadline/5`, ~200 µs by default, and DATAFRAG repair continues at 1 ms).
  // With a 100 ms tick those fire ~100 ms late, so recovering the reliable
  // startup backlog (samples written during the ~0.5 s discovery window, before
  // the reader matched) crawls at ~1 batch per tick and takes seconds. A 1 ms
  // tick keeps those timeouts prompt while still being coarse enough to be
  // cheap. The timer thread parks until the next real deadline, so a finer tick
  // does not increase idle wakeups.
  //
  // num_slots: number of timer wheel slots. capacity: maximum number of
  // timeouts that may be in flight at once. Longer timeouts (periodic
  // heartbeat, cache cleaning, discovery) simply wrap the wheel and remain
  // correct via absolute-tick comparison.
  let inner = timer::Builder::default()
    .tick_duration(std::time::Duration::from_millis(1))
    .num_slots(1024)
    .capacity(8192)
    .build();
  Rc::new(RefCell::new(inner))
}
