// PollEventSource and PollEventSender are an event communication channel that
// carries no data. The receiver is always a mio 1.2 `Source`, and also a mio
// 0.8 `Source` when that optional compatibility feature is enabled.
//
// The internal reactors do not use this channel.

macro_rules! socketpair_bridge {
  ($modname:ident, $mio:ident) => {
    mod $modname {
      use std::{
        io,
        io::{Read, Write},
        sync::{Arc, Mutex},
      };
      #[cfg(not(target_os = "windows"))]
      use std::os::fd::OwnedFd;
      #[cfg(target_os = "windows")]
      use std::{thread::sleep, time::Duration};

      #[allow(unused_imports)]
      use log::{debug, error, info};
      #[cfg(not(target_os = "windows"))]
      use socketpair::{socketpair_stream, SocketpairStream};

      pub struct Source {
        rec: Mutex<$mio::net::TcpStream>,
      }

      #[derive(Clone)]
      pub struct Sender {
        send: Arc<Mutex<$mio::net::TcpStream>>,
      }

      #[cfg(not(target_os = "windows"))]
      fn set_non_blocking(s: SocketpairStream) -> io::Result<SocketpairStream> {
        let owned_fd = OwnedFd::from(s);
        let std_socket = std::net::TcpStream::from(owned_fd);
        std_socket.set_nonblocking(true)?;
        Ok(SocketpairStream::from(OwnedFd::from(std_socket)))
      }

      #[cfg(not(target_os = "windows"))]
      pub fn make() -> io::Result<(Source, Sender)> {
        let (rec_sps, send_sps) = socketpair_stream()?;
        let rec_sps = set_non_blocking(rec_sps)?;
        let send_sps = set_non_blocking(send_sps)?;
        let rec = $mio::net::TcpStream::from_std(std::net::TcpStream::from(OwnedFd::from(rec_sps)));
        let send =
          $mio::net::TcpStream::from_std(std::net::TcpStream::from(OwnedFd::from(send_sps)));
        Ok((
          Source {
            rec: Mutex::new(rec),
          },
          Sender {
            send: Arc::new(Mutex::new(send)),
          },
        ))
      }

      #[cfg(target_os = "windows")]
      pub fn make() -> io::Result<(Source, Sender)> {
        let listener = $mio::net::TcpListener::bind("127.0.0.1:0".parse().unwrap())?;
        let addr = listener.local_addr()?;
        let rec = $mio::net::TcpStream::connect(addr)?;
        let mut accepted = listener.accept();
        while accepted.is_err() {
          sleep(Duration::from_millis(100));
          accepted = listener.accept();
        }
        let (send, _addr_send) = accepted?;
        debug!("IPC socket: {addr:#?} <-> {_addr_send:#?}");
        Ok((
          Source {
            rec: Mutex::new(rec),
          },
          Sender {
            send: Arc::new(Mutex::new(send)),
          },
        ))
      }

      impl Sender {
        pub fn send(&self) {
          match self.send.lock().unwrap().write(&[0xcc]) {
            Ok(_b) => {}
            Err(e) => info!("PollEventSender.send: {e}"),
          }
        }
      }

      impl Source {
        pub fn drain(&self) {
          let mut buf = Vec::with_capacity(16);
          match self.rec.lock().unwrap().read_to_end(&mut buf) {
            Ok(_) => (),
            Err(err) => match err.kind() {
              io::ErrorKind::WouldBlock => {}
              other_kind => info!("PollEventSource.drain(): {other_kind}"),
            },
          }
        }
      }

      impl $mio::event::Source for Source {
        fn register(
          &mut self,
          registry: &$mio::Registry,
          token: $mio::Token,
          interests: $mio::Interest,
        ) -> io::Result<()> {
          self
            .rec
            .lock()
            .unwrap()
            .register(registry, token, interests)
        }

        fn reregister(
          &mut self,
          registry: &$mio::Registry,
          token: $mio::Token,
          interests: $mio::Interest,
        ) -> io::Result<()> {
          self
            .rec
            .lock()
            .unwrap()
            .reregister(registry, token, interests)
        }

        fn deregister(&mut self, registry: &$mio::Registry) -> io::Result<()> {
          self.rec.lock().unwrap().deregister(registry)
        }
      }
    }
  };
}

#[cfg(feature = "mio_08")]
socketpair_bridge!(bridge_08, mio_08);
socketpair_bridge!(bridge_1, mio1);

pub struct PollEventSource {
  #[cfg(feature = "mio_08")]
  mio08: bridge_08::Source,
  mio1: bridge_1::Source,
}

#[derive(Clone)]
pub struct PollEventSender {
  #[cfg(feature = "mio_08")]
  mio08: bridge_08::Sender,
  mio1: bridge_1::Sender,
}

pub fn make_poll_channel() -> std::io::Result<(PollEventSource, PollEventSender)> {
  #[cfg(feature = "mio_08")]
  let (mio08_source, mio08_sender) = bridge_08::make()?;
  let (mio1_source, mio1_sender) = bridge_1::make()?;
  Ok((
    PollEventSource {
      #[cfg(feature = "mio_08")]
      mio08: mio08_source,
      mio1: mio1_source,
    },
    PollEventSender {
      #[cfg(feature = "mio_08")]
      mio08: mio08_sender,
      mio1: mio1_sender,
    },
  ))
}

impl PollEventSender {
  pub fn send(&self) {
    #[cfg(feature = "mio_08")]
    self.mio08.send();
    self.mio1.send();
  }
}

impl PollEventSource {
  /// Drain sent bytes so the socketpair can trigger again. Call before acting
  /// on the event.
  pub fn drain(&self) {
    #[cfg(feature = "mio_08")]
    self.mio08.drain();
    self.mio1.drain();
  }
}

#[cfg(feature = "mio_08")]
impl mio_08::event::Source for PollEventSource {
  fn register(
    &mut self,
    registry: &mio_08::Registry,
    token: mio_08::Token,
    interests: mio_08::Interest,
  ) -> std::io::Result<()> {
    self.mio08.register(registry, token, interests)
  }

  fn reregister(
    &mut self,
    registry: &mio_08::Registry,
    token: mio_08::Token,
    interests: mio_08::Interest,
  ) -> std::io::Result<()> {
    self.mio08.reregister(registry, token, interests)
  }

  fn deregister(&mut self, registry: &mio_08::Registry) -> std::io::Result<()> {
    self.mio08.deregister(registry)
  }
}

impl mio1::event::Source for PollEventSource {
  fn register(
    &mut self,
    registry: &mio1::Registry,
    token: mio1::Token,
    interests: mio1::Interest,
  ) -> std::io::Result<()> {
    self.mio1.register(registry, token, interests)
  }

  fn reregister(
    &mut self,
    registry: &mio1::Registry,
    token: mio1::Token,
    interests: mio1::Interest,
  ) -> std::io::Result<()> {
    self.mio1.reregister(registry, token, interests)
  }

  fn deregister(&mut self, registry: &mio1::Registry) -> std::io::Result<()> {
    self.mio1.deregister(registry)
  }
}

/// Bounded reader-notification queue with optional mio 0.6 readiness.
#[derive(Clone)]
pub(crate) struct NotificationSender {
  sender: std::sync::mpsc::SyncSender<()>,
  #[cfg(feature = "mio_06")]
  legacy_sender: mio_extras::channel::SyncSender<()>,
}

pub(crate) struct NotificationReceiver {
  receiver: std::sync::mpsc::Receiver<()>,
  #[cfg(feature = "mio_06")]
  legacy_receiver: mio_extras::channel::Receiver<()>,
}

pub(crate) fn notification_channel(capacity: usize) -> (NotificationSender, NotificationReceiver) {
  let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
  #[cfg(feature = "mio_06")]
  let (legacy_sender, legacy_receiver) = mio_extras::channel::sync_channel(capacity);
  (
    NotificationSender {
      sender,
      #[cfg(feature = "mio_06")]
      legacy_sender,
    },
    NotificationReceiver {
      receiver,
      #[cfg(feature = "mio_06")]
      legacy_receiver,
    },
  )
}

impl NotificationSender {
  pub(crate) fn try_send(&self, value: ()) -> Result<(), std::sync::mpsc::TrySendError<()>> {
    self.sender.try_send(value)?;
    #[cfg(feature = "mio_06")]
    let _ = self.legacy_sender.try_send(());
    Ok(())
  }
}

impl NotificationReceiver {
  pub(crate) fn try_recv(&self) -> Result<(), std::sync::mpsc::TryRecvError> {
    self.receiver.try_recv()
  }
}

#[cfg(feature = "mio_06")]
impl mio_06::Evented for NotificationReceiver {
  fn register(
    &self,
    poll: &mio_06::Poll,
    token: mio_06::Token,
    interest: mio_06::Ready,
    opts: mio_06::PollOpt,
  ) -> std::io::Result<()> {
    self.legacy_receiver.register(poll, token, interest, opts)
  }

  fn reregister(
    &self,
    poll: &mio_06::Poll,
    token: mio_06::Token,
    interest: mio_06::Ready,
    opts: mio_06::PollOpt,
  ) -> std::io::Result<()> {
    self.legacy_receiver.reregister(poll, token, interest, opts)
  }

  fn deregister(&self, poll: &mio_06::Poll) -> std::io::Result<()> {
    self.legacy_receiver.deregister(poll)
  }
}
