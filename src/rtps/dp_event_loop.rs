use std::{
  cell::RefCell,
  collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
  net::IpAddr,
  rc::Rc,
  sync::{Arc, RwLock},
  time::{Duration, Instant},
};

use chrono::Utc;
use log::{debug, error, info, trace, warn};
use mio1::{Events, Interest, Poll, Token};

use crate::{
  dds::{
    qos::policy,
    result::{CreateError, CreateResult},
    statusevents::{DomainParticipantStatusEvent, StatusChannelSender},
  },
  discovery::{
    discovery::DiscoveryCommand,
    discovery_db::{discovery_db_read, DiscoveryDB},
    sedp_messages::{DiscoveredReaderData, DiscoveredWriterData},
  },
  messages::submessages::submessages::AckSubmessage,
  network::{
    constant::SPDP_LOCALHOST_PEER_COUNT,
    udp_listener::UDPListener,
    udp_sender::UDPSender,
    util::{local_interface_table, localhost_spdp_peer_locators, IfAddr},
  },
  polling::{
    new_shared_timer, timeout_until, wake_channel, LoopWaker, ReadyReceiver, SharedTimer,
    WakeReceiver, WakeSender,
  },
  //qos::HasQoSPolicy,
  rtps::{
    constant::*,
    message_receiver::MessageReceiver,
    outbound::SocketId,
    reader::{Reader, ReaderIngredients},
    rtps_reader_proxy::RtpsReaderProxy,
    rtps_writer_proxy::RtpsWriterProxy,
    timed_event::DpTimerEvent,
    transmit::InterfaceObservations,
    writer::{Writer, WriterIngredients},
  },
  structure::{
    dds_cache::DDSCache,
    entity::RTPSEntity,
    guid::{EntityId, GuidPrefix, TokenDecode, GUID},
  },
  //QosPolicyBuilder,
  //QosPolicies,
  EndpointDescription,
};
#[cfg(feature = "security")]
use crate::{
  discovery::secure_discovery::AuthenticationStatus,
  security::{security_plugins::SecurityPluginsHandle, EndpointSecurityInfo},
  security_warn,
};
#[cfg(not(feature = "security"))]
use crate::no_security::security_plugins::SecurityPluginsHandle;

#[derive(Clone, Debug)]
pub struct DomainInfo {
  pub domain_participant_guid: GUID,
  pub domain_id: u16,
  pub participant_id: u16,
}

pub(crate) enum EventLoopCommand {
  Stop,
  PrepareStop,
}

pub struct DPEventLoop {
  domain_info: DomainInfo,
  poll: Poll,
  dds_cache: Arc<RwLock<DDSCache>>,
  discovery_db: Arc<RwLock<DiscoveryDB>>,
  udp_listeners: HashMap<Token, UDPListener>,
  message_receiver: MessageReceiver, // This contains our Readers

  // If security is enabled, this contains the security plugins
  #[cfg(feature = "security")]
  security_plugins_opt: Option<SecurityPluginsHandle>,

  // Adding readers
  add_reader_receiver: TokenReceiverPair<ReaderIngredients>,
  remove_reader_receiver: TokenReceiverPair<GUID>,

  // Writers
  add_writer_receiver: TokenReceiverPair<WriterIngredients>,
  remove_writer_receiver: TokenReceiverPair<GUID>,
  stop_poll_receiver: WakeReceiver<EventLoopCommand>,
  // GuidPrefix sent in this channel needs to be RTPSMessage source_guid_prefix. Writer needs this
  // to locate RTPSReaderProxy if negative acknack.
  ack_nack_receiver: WakeReceiver<(GuidPrefix, AckSubmessage)>,
  /// Entity ids whose command queues or doorbells have been rung. One waker
  /// serves every queue, so the id is the reason.
  reader_ready: ReadyReceiver<EntityId>,
  writer_ready: ReadyReceiver<EntityId>,
  waker: LoopWaker,

  writers: HashMap<EntityId, Writer>,
  udp_sender: Rc<UDPSender>,

  // nonblocking-transmit: per-socket round-robin of writers that have bulk DATA
  // to send but hit WouldBlock. Served on write readiness, control first.
  // `writable_armed` tracks which sender sockets currently have writable poll
  // interest registered (armed on demand, disarmed when queues drain).
  // (see src/rtps/nonblocking_transmit_design.md)
  bulk_ready: BTreeMap<SocketId, VecDeque<EntityId>>,
  writable_armed: BTreeSet<SocketId>,

  // Interface-aware transmit: per-remote observed receive interfaces/addresses,
  // shared (intra-thread) with the MessageReceiver that populates it.
  interface_observations: Rc<RefCell<InterfaceObservations>>,

  // Snapshot of the local interface table (IP, IPv4 subnet, MTU, flags) used to
  // resolve each matched reader's per-peer path-MTU budget. Shared (read-only)
  // with every Writer. Rebuilt on interface-set changes (same trigger points as
  // the send-route recompute).
  local_interfaces: Rc<[IfAddr]>,

  // One timer shared by all Readers, Writers and the periodic loop tasks.
  // Endpoints hold cloned handles to schedule timeouts; the loop owns it,
  // registers it once and drains it. This replaces one OS thread per timer.
  shared_timer: SharedTimer<DpTimerEvent>,

  participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,

  discovery_update_notification_receiver: WakeReceiver<DiscoveryNotificationType>,
  discovery_command_sender: WakeSender<DiscoveryCommand>,

  // Same-host loopback feature (participant-builder `same_host_loopback` knob):
  // when true, the SPDP writer announces to the localhost peers and writers may
  // route same-host peers over loopback. See
  // `src/rtps/loopback_same_host_design.md`.
  same_host_loopback: bool,
}

impl DPEventLoop {
  // This pub(crate) , because it should be constructed only by DomainParticipant.
  #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
  pub(crate) fn new(
    domain_info: DomainInfo,
    dds_cache: Arc<RwLock<DDSCache>>,
    udp_listeners: HashMap<Token, UDPListener>,
    discovery_db: Arc<RwLock<DiscoveryDB>>,
    participant_guid_prefix: GuidPrefix,
    add_reader_receiver: TokenReceiverPair<ReaderIngredients>,
    remove_reader_receiver: TokenReceiverPair<GUID>,
    add_writer_receiver: TokenReceiverPair<WriterIngredients>,
    remove_writer_receiver: TokenReceiverPair<GUID>,
    stop_poll_receiver: WakeReceiver<EventLoopCommand>,
    discovery_update_notification_receiver: WakeReceiver<DiscoveryNotificationType>,
    discovery_command_sender: WakeSender<DiscoveryCommand>,
    spdp_liveness_sender: WakeSender<GuidPrefix>,
    participant_status_sender: StatusChannelSender<DomainParticipantStatusEvent>,
    security_plugins_opt: Option<SecurityPluginsHandle>,
    only_networks: Option<Arc<[IpAddr]>>,
    socket_send_buffer_size: usize,
    same_host_loopback: bool,
    reader_ready: ReadyReceiver<EntityId>,
    writer_ready: ReadyReceiver<EntityId>,
    waker: LoopWaker,
  ) -> CreateResult<Self> {
    macro_rules! try_init {
      ($result:expr, $msg:literal) => {
        match $result {
          Ok(v) => v,
          Err(e) => {
            return Err(CreateError::OutOfResources {
              reason: format!("{}: {:?}", $msg, e),
            });
          }
        }
      };
    }

    let poll = try_init!(Poll::new(), "Unable to create new poll");
    // Listeners are edge-triggered. The loop reads each one until WouldBlock,
    // so a datagram left in the socket cannot sit unreported.
    let mut udp_listeners = udp_listeners;
    for (token, listener) in &mut udp_listeners {
      try_init!(
        poll
          .registry()
          .register(listener.mio_socket(), *token, Interest::READABLE,),
        "Failed to register listener"
      );
    }

    // Command queues, doorbells and the timer share `waker` (installed when the
    // loop starts). The acknack queue is drained on that same wake. Both ends
    // of it are this thread. 1024 absorbs a burst of ACKNACKs from one poll's
    // worth of datagrams; a still larger one is discarded.
    let (acknack_sender, acknack_receiver) =
      wake_channel::<(GuidPrefix, AckSubmessage)>(1024, &waker);

    // Deadlines live in the loop. Reader/Writer timeouts are scheduled later
    // through cloned handles passed into Reader::new / Writer::new.
    let shared_timer = new_shared_timer::<DpTimerEvent>();
    {
      let mut t = shared_timer.borrow_mut();
      t.set_timeout(PREEMPTIVE_ACKNACK_PERIOD, DpTimerEvent::PreemptiveAcknack);
      t.set_timeout(CACHE_CLEAN_PERIOD, DpTimerEvent::CacheGc);
    }

    // port number 0 means OS chooses an available port number.
    let udp_sender = try_init!(
      UDPSender::new_with_networks(0, only_networks.as_deref(), socket_send_buffer_size),
      "UDPSender construction fail"
    );

    #[cfg(not(feature = "security"))]
    let security_plugins_opt = security_plugins_opt.and(None); // make sure it is None an consume value

    let interface_observations = Rc::new(RefCell::new(InterfaceObservations::new()));
    let local_interfaces: Rc<[IfAddr]> = Rc::from(local_interface_table());

    Ok(Self {
      domain_info,
      poll,
      dds_cache,
      discovery_db,
      udp_listeners,
      udp_sender: Rc::new(udp_sender),
      message_receiver: MessageReceiver::new(
        participant_guid_prefix,
        acknack_sender,
        spdp_liveness_sender,
        security_plugins_opt.clone(),
        Rc::clone(&interface_observations),
      ),
      interface_observations,
      local_interfaces,
      #[cfg(feature = "security")]
      security_plugins_opt,
      add_reader_receiver,
      remove_reader_receiver,
      add_writer_receiver,
      remove_writer_receiver,
      stop_poll_receiver,
      reader_ready,
      writer_ready,
      waker,
      writers: HashMap::new(),
      bulk_ready: BTreeMap::new(),
      writable_armed: BTreeSet::new(),
      shared_timer,
      ack_nack_receiver: acknack_receiver,
      discovery_update_notification_receiver,
      participant_status_sender,
      discovery_command_sender,
      same_host_loopback,
    })
  }

  pub fn event_loop(self) {
    let mut events = Events::with_capacity(16); // too small capacity just delays events to next poll

    let mut poll_alive = Instant::now();
    let mut ev_wrapper = self;
    let mut preparing_to_stop = false;

    if let Err(e) = ev_wrapper.waker.install(&ev_wrapper.poll, LOOP_WAKE_TOKEN) {
      error!("dp_event_loop failed to install waker: {e}");
      return;
    }

    // Endpoint removals are collected during a wake and applied only after
    // ready-queue commands from that same wake have been dispatched.
    let mut readers_to_remove: Vec<GUID> = Vec::new();
    let mut writers_to_remove: Vec<GUID> = Vec::new();

    // Anything enqueued before the waker existed would not have woken us.
    if ev_wrapper.drain_queues(
      &mut readers_to_remove,
      &mut writers_to_remove,
      &mut preparing_to_stop,
    ) {
      return;
    }
    for reader_guid in readers_to_remove.drain(..) {
      ev_wrapper.remove_local_reader(reader_guid);
    }
    for writer_guid in writers_to_remove.drain(..) {
      ev_wrapper.remove_local_writer(&writer_guid);
    }

    // loop starts here
    loop {
      // The poll wakes for a socket, the shared waker, or the sooner of the next
      // deadline and the outbound fallback. Non-unix has no write-readiness
      // registration, so a short cap flushes queued sends.
      let idle_cap = if cfg!(unix) || !ev_wrapper.has_pending_outbound() {
        Duration::from_millis(2000)
      } else {
        Duration::from_millis(2)
      };
      let poll_timeout = timeout_until(ev_wrapper.shared_timer.borrow().next_deadline(), idle_cap);
      match ev_wrapper.poll.poll(&mut events, Some(poll_timeout)) {
        Ok(_) => {}
        Err(e) => {
          error!("dp_event_loop poll failed: {e:?}");
          break;
        }
      }

      // liveness watchdog
      let now = Instant::now();
      if now > poll_alive + Duration::from_secs(2) {
        debug!("Poll loop alive");
        poll_alive = now;
      }

      if events.is_empty() {
        debug!("dp_event_loop idling.");
      } else {
        for event in events.iter() {
          let token = Token(event.token().0);
          match EntityId::from_token(token) {
            TokenDecode::FixedToken(fixed_token) => match fixed_token {
              DISCOVERY_LISTENER_TOKEN
              | DISCOVERY_MUL_LISTENER_TOKEN
              | USER_TRAFFIC_LISTENER_TOKEN
              | USER_TRAFFIC_MUL_LISTENER_TOKEN => {
                // Edge-triggered: read until WouldBlock. A cap would leave
                // datagrams sitting with no further readiness event.
                let udp_messages = ev_wrapper.udp_listeners.get_mut(&token).map_or_else(
                  || {
                    error!("No listener with token {:?}", token);
                    vec![]
                  },
                  |l| l.messages_bounded(usize::MAX),
                );
                for (packet, origin) in udp_messages {
                  ev_wrapper
                    .message_receiver
                    .handle_received_packet(&packet, origin);
                }
              }
              LOOP_WAKE_TOKEN => {
                // Queues are drained below, once, for this whole iteration.
              }
              fixed_unknown => {
                if let Some(sid) = sender_writable_socket_id(fixed_unknown) {
                  ev_wrapper.on_socket_writable(sid);
                } else {
                  error!(
                    "Unknown event.token {:?} = 0x{:x?} , decoded as {:?}",
                    token, token.0, fixed_unknown
                  );
                }
              }
            },
            // Endpoint commands and doorbells are no longer poll tokens. They
            // arrive through the ready queues drained below.
            TokenDecode::Entity(eid) => {
              error!("Unexpected entity poll token {eid:?}");
            }
            TokenDecode::AltEntity(eid) => {
              error!("Unexpected AltEntity poll token {eid:?}");
            }
          }
        }
      }

      ev_wrapper.drain_timers(preparing_to_stop);
      if ev_wrapper.drain_queues(
        &mut readers_to_remove,
        &mut writers_to_remove,
        &mut preparing_to_stop,
      ) {
        return;
      }
      // Removals wait until commands queued in this same wake have run.
      for reader_guid in readers_to_remove.drain(..) {
        ev_wrapper.remove_local_reader(reader_guid);
      }
      for writer_guid in writers_to_remove.drain(..) {
        ev_wrapper.remove_local_writer(&writer_guid);
      }

      // nonblocking-transmit: service the per-socket outbound queues and keep
      // write-readiness interest in sync with what is pending.
      ev_wrapper.service_outbound();
    } // loop
  } // fn

  // Returns true when the loop should exit.
  fn drain_queues(
    &mut self,
    readers_to_remove: &mut Vec<GUID>,
    writers_to_remove: &mut Vec<GUID>,
    preparing_to_stop: &mut bool,
  ) -> bool {
    use std::sync::mpsc::TryRecvError;
    loop {
      match self.stop_poll_receiver.try_recv() {
        Ok(EventLoopCommand::PrepareStop) => {
          info!("dp_event_loop preparing to stop.");
          *preparing_to_stop = true;
        }
        Ok(EventLoopCommand::Stop) => {
          info!("Stopping dp_event_loop");
          return true;
        }
        Err(TryRecvError::Empty) => break,
        Err(TryRecvError::Disconnected) => {
          error!("Application thread has exited abnormally. Stopping RustDDS event loop.");
          return true;
        }
      }
    }

    self.handle_reader_action(readers_to_remove);
    self.handle_writer_action(writers_to_remove);
    self.handle_writer_acknack_action();

    while let Ok(dnt) = self.discovery_update_notification_receiver.try_recv() {
      use DiscoveryNotificationType::*;
      match dnt {
        WriterUpdated {
          discovered_writer_data,
        } => self.remote_writer_discovered(&discovered_writer_data),
        WriterLost { writer_guid } => self.remote_writer_lost(writer_guid),
        ReaderUpdated {
          discovered_reader_data,
        } => self.remote_reader_discovered(&discovered_reader_data),
        ReaderLost { reader_guid } => self.remote_reader_lost(reader_guid),
        ParticipantUpdated { guid_prefix } => self.update_participant(guid_prefix),
        ParticipantLost { guid_prefix } => self.remote_participant_lost(guid_prefix),
        AssertTopicLiveliness {
          writer_guid,
          manual_assertion,
        } => {
          self
            .writers
            .get_mut(&writer_guid.entity_id)
            .map(|w| w.handle_heartbeat_tick(manual_assertion));
        }
        #[cfg(feature = "security")]
        ParticipantAuthenticationStatusChanged { guid_prefix } => {
          self.on_remote_participant_authentication_status_changed(guid_prefix);
        }
      }
    }

    // Adds have been applied. Ready ids for those endpoints can be handled now,
    // still before the deferred removals.
    for eid in self.reader_ready.drain_unique() {
      self.message_receiver.reader_mut(eid).map_or_else(
        || debug!("Command event for unknown reader {eid:?}"),
        Reader::process_command,
      );
    }
    let writer_ids = self.writer_ready.drain_unique();
    for eid in writer_ids {
      let (blocked, local_readers) = match self.writers.get_mut(&eid) {
        None => {
          debug!("Command event for unknown writer {eid:?}");
          (BTreeSet::new(), vec![])
        }
        Some(writer) => {
          writer.process_pending();
          (writer.take_blocked_sockets(), writer.local_readers())
        }
      };
      for sid in blocked {
        self.mark_writer_willing(sid, eid);
      }
      self.message_receiver.notify_data_to_readers(local_readers);
    }
    false
  }

  fn drain_timers(&mut self, preparing_to_stop: bool) {
    let expired: Vec<DpTimerEvent> = {
      let mut timer = self.shared_timer.borrow_mut();
      let mut v = Vec::new();
      while let Some(e) = timer.poll() {
        v.push(e);
      }
      v
    };
    for timed_event in expired {
      match timed_event {
        DpTimerEvent::PreemptiveAcknack => {
          self.message_receiver.send_preemptive_acknacks();
          self
            .shared_timer
            .borrow_mut()
            .set_timeout(PREEMPTIVE_ACKNACK_PERIOD, DpTimerEvent::PreemptiveAcknack);
        }
        DpTimerEvent::CacheGc => {
          debug!("Clean DDSCache on timer");
          self.dds_cache.write().unwrap().garbage_collect();
          self
            .shared_timer
            .borrow_mut()
            .set_timeout(CACHE_CLEAN_PERIOD, DpTimerEvent::CacheGc);
        }
        DpTimerEvent::Reader { entity_id, event } => {
          if let Some(reader) = self.message_receiver.reader_mut(entity_id) {
            reader.handle_timed_event(event);
          } else if !preparing_to_stop {
            trace!("Timed event for unknown reader {entity_id:?}");
          }
        }
        DpTimerEvent::Writer { entity_id, event } => {
          if let Some(writer) = self.writers.get_mut(&entity_id) {
            writer.handle_timed_event(event);
          } else if !preparing_to_stop {
            trace!("Timed event for unknown writer {entity_id:?}");
          }
        }
      }
    }
  }

  // --- nonblocking-transmit helpers -----------------------------------------

  // Enqueue a writer on a socket's round-robin bulk queue (no duplicates).
  fn mark_writer_willing(&mut self, sid: SocketId, eid: EntityId) {
    let queue = self.bulk_ready.entry(sid).or_default();
    if !queue.contains(&eid) {
      queue.push_back(eid);
    }
  }

  // Is there any queued control or bulk work waiting for a socket to drain?
  fn has_pending_outbound(&self) -> bool {
    !self.udp_sender.pending_control_sockets().is_empty()
      || self.bulk_ready.values().any(|q| !q.is_empty())
  }

  // Write readiness fired for one sender socket: flush its control queue first
  // (strict priority), then serve willing bulk writers round-robin until the
  // socket fills again or its queue empties.
  fn on_socket_writable(&mut self, sid: SocketId) {
    self.udp_sender.flush_control(sid);
    if self.udp_sender.pending_control_sockets().contains(&sid) {
      // Still congested after control; wait for the next writable edge.
      return;
    }
    while let Some(eid) = self.bulk_ready.get_mut(&sid).and_then(VecDeque::pop_front) {
      let blocked = match self.writers.get_mut(&eid) {
        Some(writer) => {
          writer.process_pending();
          writer.take_blocked_sockets()
        }
        None => BTreeSet::new(),
      };
      let sid_blocked = blocked.contains(&sid);
      for s in blocked {
        self.mark_writer_willing(s, eid);
      }
      if sid_blocked {
        // Socket filled again; the writer has been re-queued. Stop and wait for
        // the next writable edge.
        break;
      }
    }
  }

  fn service_outbound(&mut self) {
    #[cfg(unix)]
    self.reconcile_writable_interest();
    #[cfg(not(unix))]
    self.drain_outbound_fallback();
  }

  // Arm write-readiness poll interest for sockets that have queued work, and
  // disarm it for sockets that have drained. mio 1.2 is edge-triggered, so a
  // socket that is already writable may not notify until it transitions. After
  // arming we attempt the pending send immediately; WouldBlock waits for the
  // next edge.
  #[cfg(unix)]
  fn reconcile_writable_interest(&mut self) {
    let newly_armed = self.sync_writable_interest();
    for sid in newly_armed {
      self.on_socket_writable(sid);
    }
    // The immediate attempt may have drained the socket. Drop interest if so.
    let _ = self.sync_writable_interest();
  }

  #[cfg(unix)]
  fn sync_writable_interest(&mut self) -> Vec<SocketId> {
    use mio1::unix::SourceFd;
    let mut newly_armed = Vec::new();
    let pending_control = self.udp_sender.pending_control_sockets();
    for sid in self.udp_sender.socket_ids() {
      let want =
        pending_control.contains(&sid) || self.bulk_ready.get(&sid).is_some_and(|q| !q.is_empty());
      let armed = self.writable_armed.contains(&sid);
      match (want, armed) {
        (true, false) => {
          if let Some(fd) = self.udp_sender.socket_raw_fd(sid) {
            let mut source = SourceFd(&fd);
            match self.poll.registry().register(
              &mut source,
              sender_writable_token(sid),
              Interest::WRITABLE,
            ) {
              Ok(()) => {
                self.writable_armed.insert(sid);
                newly_armed.push(sid);
              }
              Err(e) => error!("nonblocking-transmit: failed to arm writable for {sid:?}: {e}"),
            }
          }
        }
        (false, true) => {
          if let Some(fd) = self.udp_sender.socket_raw_fd(sid) {
            let mut source = SourceFd(&fd);
            let _ = self.poll.registry().deregister(&mut source);
          }
          self.writable_armed.remove(&sid);
        }
        _ => {}
      }
    }
    newly_armed
  }

  // Fallback for platforms without EventedFd: flush/serve every socket each loop
  // iteration (the loop uses a short poll timeout while anything is pending).
  #[cfg(not(unix))]
  fn drain_outbound_fallback(&mut self) {
    for sid in self.udp_sender.socket_ids() {
      self.on_socket_writable(sid);
    }
  }

  #[cfg(feature = "security")] // Currently used only with security.
                               // Just remove attribute if used also without.
  fn send_participant_status(&self, event: DomainParticipantStatusEvent) {
    self
      .participant_status_sender
      .try_send(event)
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));
  }

  fn handle_reader_action(&mut self, pending_removals: &mut Vec<GUID>) {
    while let Ok(new_reader_ing) = self.add_reader_receiver.receiver.try_recv() {
      trace!("add reader {:?}", new_reader_ing.guid);
      let guid = new_reader_ing.guid;
      self.add_local_reader(new_reader_ing);
      self.inform_discovery_about_new_local_endpoint(guid);
    }
    // Only collect removals. A command for this reader may already be in the
    // ready queue of this same wake. Removing immediately would turn that into
    // an "unknown reader" miss. The caller applies these after ready-queue
    // dispatch.
    while let Ok(old_reader_guid) = self.remove_reader_receiver.receiver.try_recv() {
      pending_removals.push(old_reader_guid);
    }
  }

  fn handle_writer_action(&mut self, pending_removals: &mut Vec<GUID>) {
    while let Ok(new_writer_ingredients) = self.add_writer_receiver.receiver.try_recv() {
      let guid = new_writer_ingredients.guid;
      self.add_local_writer(new_writer_ingredients);
      self.inform_discovery_about_new_local_endpoint(guid);
    }
    while let Ok(writer_guid) = self.remove_writer_receiver.receiver.try_recv() {
      pending_removals.push(writer_guid);
    }
  }

  fn handle_writer_acknack_action(&mut self) {
    while let Ok((acknack_sender_prefix, acknack_submessage)) = self.ack_nack_receiver.try_recv() {
      let writer_guid = GUID::new_with_prefix_and_id(
        self.domain_info.domain_participant_guid.prefix,
        acknack_submessage.writer_id(),
      );
      if let Some(found_writer) = self.writers.get_mut(&writer_guid.entity_id) {
        if found_writer.is_reliable() {
          found_writer.handle_ack_nack(acknack_sender_prefix, &acknack_submessage);
        }
      } else {
        // Note: when testing against FastDDS Shapes demo, this else branch is
        // repeatedly triggered. The resulting log entry contains the following
        // EntityId: {[0, 3, 0] EntityKind::WRITER_NO_KEY_BUILT_IN}.
        // In this case a writer cannot be found, because FastDDS sends
        // pre-emptive acknacks about a built-in topic defined in DDS Xtypes
        // specification, which RustDDS does not implement. So even though the acknack
        // cannot be handled, it is not a problem in this case.
        debug!(
          "Couldn't handle acknack/nackfrag! Did not find local RTPS writer with GUID: \
           {writer_guid:x?}"
        );
        continue;
      }
    }
  }

  fn update_participant(&mut self, participant_guid_prefix: GuidPrefix) {
    debug!(
      "update_participant {:?} myself={}",
      participant_guid_prefix,
      participant_guid_prefix == self.domain_info.domain_participant_guid.prefix
    );

    let db = discovery_db_read(&self.discovery_db);
    // new Remote Participant discovered
    let discovered_participant =
      if let Some(dpd) = db.find_participant_proxy(participant_guid_prefix) {
        dpd
      } else {
        error!("Participant was updated, but DB does not have it. Strange.");
        return;
      };

    // Select which builtin endpoints of the remote participant are updated to local
    // readers & writers
    #[cfg(not(feature = "security"))]
    let (readers_init_list, writers_init_list) = (
      STANDARD_BUILTIN_READERS_INIT_LIST.to_vec(),
      STANDARD_BUILTIN_WRITERS_INIT_LIST.to_vec(),
    );

    #[cfg(feature = "security")]
    let (readers_init_list, writers_init_list) = if self.security_plugins_opt.is_none() {
      // No security enabled, just the standard endpoints
      let readers_init_list = STANDARD_BUILTIN_READERS_INIT_LIST.to_vec();
      let writers_init_list = STANDARD_BUILTIN_WRITERS_INIT_LIST.to_vec();

      (readers_init_list, writers_init_list)
    } else {
      // Security enabled. The endpoints are selected based on the authentication
      // status of the remote participant
      let mut readers_init_list = vec![];
      let mut writers_init_list = vec![];

      match db.get_authentication_status(participant_guid_prefix) {
        Some(AuthenticationStatus::Authenticating) => {
          // Add just the stateless endpoint used for authentication
          readers_init_list.extend_from_slice(AUTHENTICATION_BUILTIN_READERS_INIT_LIST);
          writers_init_list.extend_from_slice(AUTHENTICATION_BUILTIN_WRITERS_INIT_LIST);
        }
        Some(AuthenticationStatus::Authenticated) => {
          // Match all builtin endpoints
          readers_init_list.extend_from_slice(STANDARD_BUILTIN_READERS_INIT_LIST);
          writers_init_list.extend_from_slice(STANDARD_BUILTIN_WRITERS_INIT_LIST);
          readers_init_list.extend_from_slice(SECURE_BUILTIN_READERS_INIT_LIST);
          writers_init_list.extend_from_slice(SECURE_BUILTIN_WRITERS_INIT_LIST);
        }
        Some(AuthenticationStatus::Unauthenticated) => {
          // Match only the regular builtin endpoints (see Security spec section 8.8.2.1)
          readers_init_list.extend_from_slice(STANDARD_BUILTIN_READERS_INIT_LIST);
          writers_init_list.extend_from_slice(STANDARD_BUILTIN_WRITERS_INIT_LIST);
        }
        _ => {
          // Not adding any endpoints when authentication status is Rejected
          // or None
        }
      }
      (readers_init_list, writers_init_list)
    };

    // Never create send-destinations (built-in reader proxies) toward our *own*
    // participant. We already know our own endpoints locally, so a self reader
    // proxy would only ever be a reliable writer target that never ACKs, causing
    // an endless retransmit flood onto the metatraffic multicast group (the self
    // proxy's loopback unicast is split into the observation-gated bucket, so its
    // route falls back to multicast). Reflection in the DiscoveryDB (recognizing
    // our own announcements) is kept; only self send-matching is skipped.

    // Update local writers, i.e. reader_proxies inside them
    for (writer_eid, reader_eid, reader_endpoint_set_elem, reader_qos) in &readers_init_list {
      if let Some(writer) = self.writers.get_mut(writer_eid) {
        debug!("update_discovery_writer - {:?}", writer.topic_name());

        if discovered_participant
          .available_builtin_endpoints
          .contains(*reader_endpoint_set_elem)
        {
          let reader_proxy =
            discovered_participant.get_builtin_reader_proxy(*reader_eid, reader_qos);

          // Get the QoS for the built-in topic from the local writer
          let mut reader_qos = reader_qos.clone();

          // special case by RTPS 2.3 / 2.5 spec Section
          // "8.4.13.3 BuiltinParticipantMessageWriter and
          // BuiltinParticipantMessageReader QoS"
          if *reader_eid == EntityId::P2P_BUILTIN_PARTICIPANT_MESSAGE_READER
            && discovered_participant
              .builtin_endpoint_qos
              .is_some_and(|beq| beq.is_best_effort())
          {
            reader_qos.reliability = Some(policy::Reliability::BestEffort);
            // This notifies our `writer` that the reader over the wire is
            // BestEffort, and will therefore not send ACKNACKs. Now the
            // `writer` knows not to expect them, and avoid stalling.
          };

          writer.update_reader_proxy(&reader_proxy, &reader_qos);
          debug!(
            "update_discovery writer - endpoint {:?} - {:?}",
            reader_endpoint_set_elem, discovered_participant.participant_guid
          );
        }
      }
    }
    // update local readers.
    // list to be looped over is the same as above, but now
    // EntityIds are for announcers
    for (writer_eid, reader_eid, writer_endpoint_set_elem, writer_qos) in &writers_init_list {
      if let Some(reader) = self.message_receiver.available_readers.get_mut(reader_eid) {
        debug!("try update_discovery_reader - {:?}", reader.topic_name());

        if discovered_participant
          .available_builtin_endpoints
          .contains(*writer_endpoint_set_elem)
        {
          let writer_proxy = discovered_participant.get_builtin_writer_proxy(*writer_eid);

          reader.update_writer_proxy(writer_proxy, writer_qos);
          debug!(
            "update_discovery_reader - endpoint {:?} - {:?}",
            *writer_endpoint_set_elem, discovered_participant.participant_guid
          );
        }
      }
    } // for

    // Fresh SPDP traffic from this participant may have updated our interface
    // observations; refresh the interface-aware send routes of any writers that
    // already have matched readers behind this participant. Access `writers`
    // directly (not via &mut self) so it stays disjoint from the `db` borrow.
    for writer in self.writers.values_mut() {
      writer.recompute_routes_for(participant_guid_prefix);
    }

    debug!("update_participant - finished for {participant_guid_prefix:?}");
  }

  fn remote_participant_lost(&mut self, participant_guid_prefix: GuidPrefix) {
    info!(
      "remote_participant_lost guid_prefix={:?}",
      participant_guid_prefix
    );
    // Discovery has already removed Participant from Discovery DB
    // Now we have to remove any ReaderProxies and WriterProxies belonging
    // to that participant, so that we do not send messages to them anymore.

    for writer in self.writers.values_mut() {
      writer.participant_lost(participant_guid_prefix);
    }

    for reader in self.message_receiver.available_readers.values_mut() {
      reader.participant_lost(participant_guid_prefix);
    }

    // Forget interface observations for the departed participant.
    self
      .interface_observations
      .borrow_mut()
      .remove(participant_guid_prefix);

    #[cfg(feature = "security")]
    if let Some(security_plugins_handle) = &self.security_plugins_opt {
      security_plugins_handle
        .get_plugins()
        .unregister_remote_participant(&participant_guid_prefix)
        .unwrap_or_else(|e| error!("{e}"));
    }
  }

  fn remote_reader_discovered(&mut self, remote_reader: &DiscoveredReaderData) {
    debug!(
      "remote_reader_discovered on {:?}",
      remote_reader.subscription_topic_data.topic_name
    );
    self
      .participant_status_sender
      .try_send(DomainParticipantStatusEvent::ReaderDetected {
        reader: EndpointDescription {
          updated_time: Utc::now(),
          guid: remote_reader.reader_proxy.remote_reader_guid,
          topic_name: remote_reader.subscription_topic_data.topic_name.clone(),
          type_name: remote_reader.subscription_topic_data.type_name().clone(),
          qos: remote_reader.subscription_topic_data.qos(),
          user_data: remote_reader.user_data.clone(),
        },
      })
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));

    for writer in self.writers.values_mut() {
      if remote_reader.subscription_topic_data.topic_name() == writer.topic_name() {
        #[cfg(not(feature = "security"))]
        let match_to_reader = true;
        #[cfg(feature = "security")]
        let match_to_reader = if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
          // Security is enabled.
          let local_writer_guid = writer.guid();
          let remote_reader_guid = remote_reader.reader_proxy.remote_reader_guid;

          // Check do we have compatible security with the remote
          let local_writer_sec_info_opt = plugins_handle
            .get_plugins()
            .get_writer_sec_attributes(writer.guid(), writer.topic_name().clone())
            .map(EndpointSecurityInfo::from)
            .ok();
          let remote_reader_sec_info_opt = remote_reader
            .subscription_topic_data
            .security_info()
            .clone();

          let compatible = check_are_endpoints_securities_compatible(
            local_writer_sec_info_opt,
            remote_reader_sec_info_opt,
          );
          if !compatible {
            security_warn!(
              "Local writer {:?} and remote reader {:?} have incompatible security, ignoring the \
               remote.",
              writer.guid(),
              remote_reader_guid
            );
            false // match_to_reader
          } else {
            // Signal Secure discovery to exchange keys with the remote
            // TODO: do this only at first encounter with the remote / before keys have been
            // sent, not every time
            self
              .discovery_command_sender
              .send(DiscoveryCommand::StartKeyExchangeWithRemoteEndpoint {
                local_endpoint_guid: local_writer_guid,
                remote_endpoint_guid: remote_reader_guid,
              })
              .unwrap_or_else(|e| {
                error!(
                  "Could not signal Secure Discovery to start the key exchange with remote reader \
                   {remote_reader_guid:?}. Reason: {e}."
                );
              });
            true // match_to_reader
          }
        } else {
          // No security enabled. Always match
          true // match_to_reader
        };

        if match_to_reader {
          // Should we check if the participant has published a QoS for the topic?
          let requested_qos = remote_reader.subscription_topic_data.qos();
          writer.update_reader_proxy(
            &RtpsReaderProxy::from_discovered_reader_data(remote_reader, &[], &[]),
            &requested_qos,
          );
        }
      }
    }
  }

  fn remote_reader_lost(&mut self, reader_guid: GUID) {
    for writer in self.writers.values_mut() {
      writer.reader_lost(reader_guid);
    }
  }

  fn remote_writer_discovered(&mut self, remote_writer: &DiscoveredWriterData) {
    self
      .participant_status_sender
      .try_send(DomainParticipantStatusEvent::WriterDetected {
        writer: EndpointDescription {
          updated_time: Utc::now(),
          guid: remote_writer.writer_proxy.remote_writer_guid,
          topic_name: remote_writer.publication_topic_data.topic_name.clone(),
          type_name: remote_writer.publication_topic_data.type_name.clone(),
          qos: remote_writer.publication_topic_data.qos(),
          user_data: remote_writer.user_data.clone(),
        },
      })
      .unwrap_or_else(|e| error!("Cannot report participant status: {e:?}"));

    // update writer proxies in local readers
    for reader in self.message_receiver.available_readers.values_mut() {
      if &remote_writer.publication_topic_data.topic_name == reader.topic_name() {
        #[cfg(not(feature = "security"))]
        let match_to_writer = true;
        #[cfg(feature = "security")]
        let match_to_writer = if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
          // Security is enabled.
          let local_reader_guid = reader.guid();
          let remote_writer_guid = remote_writer.writer_proxy.remote_writer_guid;

          // Check do we have compatible security with the remote
          let local_reader_sec_info_opt = plugins_handle
            .get_plugins()
            .get_reader_sec_attributes(local_reader_guid, reader.topic_name().clone())
            .map(EndpointSecurityInfo::from)
            .ok();
          let remote_writer_sec_info_opt =
            remote_writer.publication_topic_data.security_info.clone();

          let compatible = check_are_endpoints_securities_compatible(
            local_reader_sec_info_opt,
            remote_writer_sec_info_opt,
          );

          if !compatible {
            security_warn!(
              "Local reader {:?} and remote writer {:?} have incompatible security, ignoring the \
               remote.",
              local_reader_guid,
              remote_writer_guid
            );
            false // match_to_writer
          } else {
            // Signal Secure discovery to exchange keys with the remote
            // TODO: do this only at first encounter with the remote / before keys have been
            // sent, not every time
            if let Err(e) = self.discovery_command_sender.send(
              DiscoveryCommand::StartKeyExchangeWithRemoteEndpoint {
                local_endpoint_guid: local_reader_guid,
                remote_endpoint_guid: remote_writer_guid,
              },
            ) {
              error!(
                "Could not signal Secure Discovery to start the key exchange with remote writer \
                 {remote_writer_guid:?}. Reason: {e}."
              );
            }
            true // match_to_writer
          }
        } else {
          // No security enabled. Always match
          true // match_to_writer
        };

        if match_to_writer {
          let offered_qos = remote_writer.publication_topic_data.qos();
          // Should we check if the participant has published a QoS for the topic?
          reader.update_writer_proxy(
            RtpsWriterProxy::from_discovered_writer_data(remote_writer, &[], &[]),
            &offered_qos,
          );
        }
      }
    }
  }

  fn remote_writer_lost(&mut self, writer_guid: GUID) {
    for reader in self.message_receiver.available_readers.values_mut() {
      reader.remove_writer_proxy(writer_guid);
    }
  }

  fn add_local_reader(&mut self, reader_ing: ReaderIngredients) {
    // The reader schedules its timeouts on the loop's shared timer (already
    // registered in `new()`), so there is no per-reader timer to register.
    let mut new_reader = Reader::new(
      reader_ing,
      self.udp_sender.clone(),
      self.shared_timer.clone(),
      self.participant_status_sender.clone(),
    );

    // Reader commands share the poll waker. The ready queue records the entity
    // id, so there is nothing to register under the reader token.
    new_reader.set_requested_deadline_check_timer();
    trace!("Add reader: {new_reader:?}");
    self.message_receiver.add_reader(new_reader);
  }

  fn remove_local_reader(&mut self, reader_guid: GUID) {
    if self.message_receiver.remove_reader(reader_guid).is_some() {
      // The timer is shared and stays with the loop. Reader commands share the
      // poll waker, so there is no per-reader registration to drop. A timeout
      // already scheduled for this reader is ignored on dispatch (lookup miss).

      #[cfg(feature = "security")]
      if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
        // Security is enabled. Unregister the reader with the crypto plugin.
        // Currently the unregister method is called for every reader, and errors are
        // ignored. If this is inconvenient, add a check if the reader has been
        // registered/is secure, and unregister only if it is so
        let _ = plugins_handle
          .get_plugins()
          .unregister_local_reader(&reader_guid);
      }
    } else {
      warn!("Tried to remove nonexistent Reader {reader_guid:?}");
    }
  }

  fn add_local_writer(&mut self, writer_ing: WriterIngredients) {
    // The writer schedules its timeouts on the loop's shared timer (already
    // registered in `new()`), so there is no per-writer timer to register.
    let mut new_writer = Writer::new(
      writer_ing,
      self.udp_sender.clone(),
      self.shared_timer.clone(),
      self.participant_status_sender.clone(),
      Rc::clone(&self.interface_observations),
      Rc::clone(&self.local_interfaces),
    );

    // Same-host loopback feature (gated by the `same_host_loopback` knob):
    // - the built-in SPDP writer additionally announces to the localhost SPDP peers
    //   so same-host participants discover each other with no external network / no
    //   loopback multicast;
    // - every writer may route a confirmed same-host peer over loopback.
    // See `src/rtps/loopback_same_host_design.md`.
    new_writer.set_prefer_loopback_same_host(self.same_host_loopback);
    if self.same_host_loopback
      && new_writer.guid().entity_id == EntityId::SPDP_BUILTIN_PARTICIPANT_WRITER
    {
      new_writer.set_extra_unicast_destinations(localhost_spdp_peer_locators(
        self.domain_info.domain_id,
        self.domain_info.participant_id,
        SPDP_LOCALHOST_PEER_COUNT,
      ));
    }

    // The doorbell is a shared ready-queue plus the poll waker, not a
    // registration under the writer token.
    self.writers.insert(new_writer.guid().entity_id, new_writer);
  }

  fn remove_local_writer(&mut self, writer_guid: &GUID) {
    if self.writers.remove(&writer_guid.entity_id).is_some() {
      // The timer is shared and stays with the loop. Stale timeouts are ignored
      // on dispatch (lookup miss).

      #[cfg(feature = "security")]
      if let Some(plugins_handle) = self.security_plugins_opt.as_ref() {
        // Security is enabled. Unregister the writer with the crypto plugin.
        // Currently the unregister method is called for every writer, and errors are
        // ignored. If this is inconvenient, add a check if the writer has been
        // registered/is secure, and unregister only if it is so
        let _ = plugins_handle
          .get_plugins()
          .unregister_local_writer(writer_guid);
      }
    }
  }

  #[cfg(feature = "security")]
  fn on_remote_participant_authentication_status_changed(&mut self, remote_guidp: GuidPrefix) {
    let auth_status = discovery_db_read(&self.discovery_db).get_authentication_status(remote_guidp);

    auth_status.map(|status| {
      self.send_participant_status(DomainParticipantStatusEvent::Authentication {
        participant: remote_guidp,
        status,
      });
    });

    match auth_status {
      Some(AuthenticationStatus::Authenticated) => {
        // The participant has been authenticated
        // First connect the built-in endpoints
        self.update_participant(remote_guidp);
        // Then start the key exchange
        if let Err(e) = self.discovery_command_sender.send(
          DiscoveryCommand::StartKeyExchangeWithRemoteParticipant {
            participant_guid_prefix: remote_guidp,
          },
        ) {
          error!(
            "Could not signal Discovery to start the key exchange with remote. Reason: {e}. \
             Remote: {remote_guidp:?}"
          );
        }
      }
      Some(AuthenticationStatus::Authenticating) => {
        // The following call should connect the endpoints used for authentication
        self.update_participant(remote_guidp);
      }
      Some(AuthenticationStatus::Rejected) => {
        // TODO: disconnect endpoints from the participant?
        info!(
          "Status Rejected in on_remote_participant_authentication_status_changed with \
           {remote_guidp:?}. TODO!"
        );
      }
      other => {
        info!(
          "Status {other:?}, in on_remote_participant_authentication_status_changed. What to do?"
        );
      }
    }
  }

  fn inform_discovery_about_new_local_endpoint(&self, guid: GUID) {
    let discovery_command = if guid.entity_id.kind().is_writer() {
      DiscoveryCommand::AddLocalWriter { guid }
    } else {
      DiscoveryCommand::AddLocalReader { guid }
    };

    if let Err(e) = self.discovery_command_sender.try_send(discovery_command) {
      log::error!(
        "Failed to inform Discovery about the new endpoint: {e}. Endpoint guid: {guid:?}"
      );
      // Improvement TODO: that's it, just an error log entry on failing to
      // inform discovery?
    }
  }
}

#[cfg(feature = "security")]
fn check_are_endpoints_securities_compatible(
  local_info_opt: Option<EndpointSecurityInfo>,
  remote_info_opt: Option<EndpointSecurityInfo>,
) -> bool {
  let (local_info, remote_info) = match (local_info_opt, remote_info_opt) {
    (None, None) => {
      // Neither has security info. Pass?
      return true;
    }
    (Some(_info), None) | (None, Some(_info)) => {
      // Only one of the endpoints has security info. Reject.
      return false;
    }
    (Some(local_info), Some(remote_info)) => (local_info, remote_info),
  };

  // See Security specification section 7.2.8 EndpointSecurityInfo
  if local_info.endpoint_security_attributes.is_valid()
    && local_info.plugin_endpoint_security_attributes.is_valid()
    && remote_info.endpoint_security_attributes.is_valid()
    && remote_info.plugin_endpoint_security_attributes.is_valid()
  {
    // When all masks are valid, values need to be equal
    local_info == remote_info
  } else {
    // From the spec:
    // "If the is_valid is set to zero on either of the masks, the comparison
    // between the local and remote setting for the EndpointSecurityInfo shall
    // ignore the attribute"

    // TODO: Does it actually make sense to ignore the masks if they're not valid?
    // Seems a bit strange. Currently we require that all masks are valid
    false
  }
}

// -----------------------------------------------------------
// -----------------------------------------------------------
// -----------------------------------------------------------

#[cfg(test)]
mod tests {
  use std::{sync::Mutex, thread};

  use super::*;
  use crate::{
    dds::{
      qos::QosPolicies,
      statusevents::{sync_status_channel, DataReaderStatus},
      typedesc::TypeDesc,
      with_key::simpledatareader::ReaderCommand,
    },
    mio_source,
    polling::{ready_channel, wake_channel, wake_channel_unbounded, LoopWaker},
  };

  //#[test]
  // TODO: Investigate why this fails in the github CI pipeline
  // Then re-enable this test.
  #[allow(dead_code)]
  fn dpew_add_and_remove_readers() {
    // Test sending 'add reader' and 'remove reader' commands to DP event loop
    // TODO: There are no assertions in this test case. Does in actually test
    // anything?

    // Create DP communication channels
    let waker = LoopWaker::new();
    let (sender_add_reader, receiver_add) = wake_channel::<ReaderIngredients>(8, &waker);
    let (sender_remove_reader, receiver_remove) = wake_channel::<GUID>(4, &waker);
    let (_add_writer_sender, add_writer_receiver) = wake_channel::<WriterIngredients>(8, &waker);
    let (_remove_writer_sender, remove_writer_receiver) = wake_channel::<GUID>(4, &waker);
    let (stop_poll_sender, stop_poll_receiver) = wake_channel_unbounded(&waker);
    let (_discovery_update_notification_sender, discovery_update_notification_receiver) =
      wake_channel(8, &waker);
    let discovery_waker = LoopWaker::new();
    let (discovery_command_sender, _discovery_command_receiver) =
      wake_channel::<DiscoveryCommand>(64, &discovery_waker);
    let (spdp_liveness_sender, _spdp_liveness_receiver) = wake_channel(8, &discovery_waker);
    let (_reader_ready_tx, reader_ready) = ready_channel(&waker);
    let (_writer_ready_tx, writer_ready) = ready_channel(&waker);
    let (participant_status_sender, _participant_status_receiver) =
      sync_status_channel(16).unwrap();

    let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
    let dds_cache_clone = Arc::clone(&dds_cache);
    let (discovery_db_event_sender, _discovery_db_event_receiver) =
      std::sync::mpsc::sync_channel(4);

    let discovery_db = Arc::new(RwLock::new(DiscoveryDB::new(
      GUID::new_participant_guid(),
      discovery_db_event_sender,
      participant_status_sender.clone(),
    )));

    let domain_info = DomainInfo {
      domain_participant_guid: GUID::default(),
      domain_id: 0,
      participant_id: 0,
    };

    // Start event loop
    let child = thread::spawn(move || {
      let dp_event_loop = DPEventLoop::new(
        domain_info,
        dds_cache_clone,
        HashMap::new(),
        discovery_db,
        GuidPrefix::default(),
        TokenReceiverPair {
          token: ADD_READER_TOKEN,
          receiver: receiver_add,
        },
        TokenReceiverPair {
          token: REMOVE_READER_TOKEN,
          receiver: receiver_remove,
        },
        TokenReceiverPair {
          token: ADD_WRITER_TOKEN,
          receiver: add_writer_receiver,
        },
        TokenReceiverPair {
          token: REMOVE_WRITER_TOKEN,
          receiver: remove_writer_receiver,
        },
        stop_poll_receiver,
        discovery_update_notification_receiver,
        discovery_command_sender,
        spdp_liveness_sender,
        participant_status_sender,
        None,
        None,
        0,
        true,
        reader_ready,
        writer_ready,
        waker,
      )
      .expect("DPEventLoop::new in test");
      dp_event_loop.event_loop();
    });

    // Create a topic cache
    let topic_cache = dds_cache.write().unwrap().add_new_topic(
      "test".to_string(),
      TypeDesc::new("test_type".to_string()),
      &QosPolicies::qos_none(),
    );

    let num_of_readers = 3;

    // Send some 'add reader' commands
    let mut reader_guids = Vec::new();
    for i in 0..num_of_readers {
      let new_guid = GUID::default();

      // Create mechanisms for notifications, statuses & commands
      let (notification_sender, _notification_receiver) =
        crate::mio_source::notification_channel(100);
      let (_notification_event_source, notification_event_sender) =
        mio_source::make_poll_channel().unwrap();
      let data_reader_waker = Arc::new(Mutex::new(None));

      let (status_sender, _status_receiver) = sync_status_channel::<DataReaderStatus>(4).unwrap();

      let (_reader_command_sender, reader_command_receiver) =
        wake_channel::<ReaderCommand>(10, &LoopWaker::new());

      let new_reader_ing = ReaderIngredients {
        guid: new_guid,
        notification_sender,
        status_sender,
        topic_cache_handle: topic_cache.clone(),
        topic_name: "test".to_string(),
        like_stateless: false,
        qos_policy: QosPolicies::qos_none(),
        data_reader_command_receiver: reader_command_receiver,
        data_reader_waker: data_reader_waker.clone(),
        poll_event_sender: notification_event_sender,
        security_plugins: None,
        discovery_notify: None,
      };

      reader_guids.push(new_reader_ing.guid);
      info!("\nSent reader number {}: {:?}\n", i, new_reader_ing);
      sender_add_reader.send(new_reader_ing).unwrap();
      std::thread::sleep(Duration::new(0, 100));
    }

    // Send a command to remove the second reader
    info!("\nremoving the second\n");
    let some_guid = reader_guids[1];
    sender_remove_reader.send(some_guid).unwrap();
    std::thread::sleep(Duration::new(0, 100));

    info!("\nsending end token\n");
    stop_poll_sender.send(EventLoopCommand::Stop).unwrap();
    child.join().unwrap();
  }

  // TODO: Rewrite / remove this test - all asserts in it use
  // DataReader::get_requested_deadline_missed_status which is
  // currently commented out

  // #[test]
  // fn dpew_test_reader_commands() {
  //   let somePolicies = QosPolicies {
  //     durability: None,
  //     presentation: None,
  //     deadline: Some(Deadline(DurationDDS::from_millis(500))),
  //     latency_budget: None,
  //     ownership: None,
  //     liveliness: None,
  //     time_based_filter: None,
  //     reliability: None,
  //     destination_order: None,
  //     history: None,
  //     resource_limits: None,
  //     lifespan: None,
  //   };
  //   let dp = DomainParticipant::new(0).expect("Failed to create
  // participant");   let sub = dp.create_subscriber(&somePolicies).unwrap();

  //   let topic_1 = dp
  //     .create_topic("TOPIC_1", "something", &somePolicies,
  // TopicKind::WithKey)     .unwrap();
  //   let _topic_2 = dp
  //     .create_topic("TOPIC_2", "something", &somePolicies,
  // TopicKind::WithKey)     .unwrap();
  //   let _topic_3 = dp
  //     .create_topic("TOPIC_3", "something", &somePolicies,
  // TopicKind::WithKey)     .unwrap();

  //   // Adding readers
  //   let (sender_add_reader, receiver_add) = mio_channel::channel::<Reader>();
  //   let (_sender_remove_reader, receiver_remove) =
  // mio_channel::channel::<GUID>();

  //   let (_add_writer_sender, add_writer_receiver) = mio_channel::channel();
  //   let (_remove_writer_sender, remove_writer_receiver) =
  // mio_channel::channel();

  //   let (_stop_poll_sender, stop_poll_receiver) = mio_channel::channel();

  //   let (_discovery_update_notification_sender,
  // discovery_update_notification_receiver) =     mio_channel::channel();

  //   let dds_cache = Arc::new(RwLock::new(DDSCache::new()));
  //   let discovery_db = Arc::new(RwLock::new(DiscoveryDB::new()));

  //   let domain_info = DomainInfo {
  //     domain_participant_guid: GUID::default(),
  //     domain_id: 0,
  //     participant_id: 0,
  //   };

  //   let dp_event_loop = DPEventLoop::new(
  //     domain_info,
  //     HashMap::new(),
  //     dds_cache,
  //     discovery_db,
  //     GuidPrefix::default(),
  //     TokenReceiverPair {
  //       token: ADD_READER_TOKEN,
  //       receiver: receiver_add,
  //     },
  //     TokenReceiverPair {
  //       token: REMOVE_READER_TOKEN,
  //       receiver: receiver_remove,
  //     },
  //     TokenReceiverPair {
  //       token: ADD_WRITER_TOKEN,
  //       receiver: add_writer_receiver,
  //     },
  //     TokenReceiverPair {
  //       token: REMOVE_WRITER_TOKEN,
  //       receiver: remove_writer_receiver,
  //     },
  //     stop_poll_receiver,
  //     discovery_update_notification_receiver,
  //   );

  //   let (sender_stop, receiver_stop) = mio_channel::channel::<i32>();
  //   dp_event_loop
  //     .poll
  //     .register(
  //       &receiver_stop,
  //       STOP_POLL_TOKEN,
  //       Ready::readable(),
  //       PollOpt::edge(),
  //     )
  //     .expect("Failed to register receivers.");

  //   let child = thread::spawn(move ||
  // DPEventLoop::event_loop(dp_event_loop));

  //   //TODO IF THIS IS SET TO 1 TEST SUCCEEDS
  //   let n = 1;

  //   let mut reader_guids = Vec::new();
  //   let mut data_readers: Vec<DataReader<RandomData,
  // CDRDeserializerAdapter<RandomData>>> = vec![];   let _topics: Vec<Topic>
  // = vec![];   for i in 0..n {
  //     //topics.push(topic);
  //     let new_guid = GUID::default();

  //     let (send, _rec) = crate::mio_source::notification_channel(100);
  //     let (status_sender, status_receiver_DataReader) =
  //       mio_extras::channel::sync_channel::<DataReaderStatus>(1000);
  //     let (reader_commander, reader_command_receiver) =
  //       mio_extras::channel::sync_channel::<ReaderCommand>(1000);

  //     let mut new_reader = Reader::new(
  //       new_guid,
  //       send,
  //       status_sender,
  //       Arc::new(RwLock::new(DDSCache::new())),
  //       "test".to_string(),
  //       QosPolicies::qos_none(),
  //       reader_command_receiver,
  //     );

  //     let somePolicies = QosPolicies {
  //       durability: None,
  //       presentation: None,
  //       deadline: Some(Deadline(DurationDDS::from_millis(50))),
  //       latency_budget: None,
  //       ownership: None,
  //       liveliness: None,
  //       time_based_filter: None,
  //       reliability: None,
  //       destination_order: None,
  //       history: None,
  //       resource_limits: None,
  //       lifespan: None,
  //     };

  //     let mut datareader = sub
  //       .create_datareader::<RandomData, CDRDeserializerAdapter<RandomData>>(
  //         topic_1.clone(),
  //         Some(somePolicies.clone()),
  //       )
  //       .unwrap();

  //     datareader.set_status_change_receiver(status_receiver_DataReader);
  //     datareader.set_reader_commander(reader_commander);
  //     data_readers.push(datareader);

  //     //new_reader.set_qos(&somePolicies).unwrap();
  //     new_reader.matched_writer_add(GUID::default(),
  // EntityId::UNKNOWN, vec![], vec![]);     reader_guids.
  // push(new_reader.guid().clone());     info!("\nSent reader number {}:
  // {:?}\n", i, &new_reader);     sender_add_reader.send(new_reader).
  // unwrap();     std::thread::sleep(Duration::from_millis(100));
  //   }
  //   thread::sleep(Duration::from_millis(100));

  //   let status = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status);
  //   assert_eq!(
  //     status.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(3, 3)
  //     )),
  //   );
  //   thread::sleep(Duration::from_millis(150));

  //   let status2 = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status2);
  //   assert_eq!(
  //     status2.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(6, 3)
  //     ))
  //   );

  //   let status3 = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status3);
  //   assert_eq!(
  //     status3.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(6, 0)
  //     ))
  //   );

  //   thread::sleep(Duration::from_millis(50));

  //   let status4 = data_readers
  //     .get_mut(0)
  //     .unwrap()
  //     .get_requested_deadline_missed_status();
  //   info!("Received status change: {:?}", status4);
  //   assert_eq!(
  //     status4.unwrap(),
  //     Some(RequestedDeadlineMissedStatus::from_count(
  //       CountWithChange::start_from(7, 1)
  //     ))
  //   );

  //   info!("\nsending end token\n");
  //   sender_stop.send(0).unwrap();
  //   child.join().unwrap();
  // }
}
