// PollEventSource and PollEventSender are an event communication
// channel. PollEventSource is a mio-0.8 event::Source for Poll,
// so it can be Registered in mio-0.8.
// These Events carry no data.
//
// When the "mio_08" feature is disabled, these types degrade to no-op stubs:
// RustDDS still uses the async (waker) and mio-0.6 notification paths, so the
// mio-0.8 socketpair-based signalling is simply not built (and neither the
// `mio` 0.8 nor `socketpair` crates are pulled in).

#[cfg(feature = "mio_08")]
mod imp {
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
  use log::{debug, error, info, trace, warn};
  #[cfg(target_os = "windows")]
  use mio_08::net::TcpListener;
  #[cfg(not(target_os = "windows"))]
  use socketpair::*;
  use mio_08::{net::TcpStream, *};

  // This is the event receiver end. It is a "Source" in the terminology of mio.
  pub struct PollEventSource {
    rec_mio_socket: Mutex<mio_08::net::TcpStream>,
  }

  #[derive(Clone)]
  pub struct PollEventSender {
    send_mio_socket: Arc<Mutex<mio_08::net::TcpStream>>,
    // Sender has Arc to support Clone, whereas Receiver has not.
  }

  #[cfg(not(target_os = "windows"))]
  fn set_non_blocking(s: SocketpairStream) -> io::Result<SocketpairStream> {
    let owned_fd = OwnedFd::from(s);
    let std_socket = std::net::TcpStream::from(owned_fd);
    std_socket.set_nonblocking(true)?;

    Ok(SocketpairStream::from(OwnedFd::from(std_socket)))
  }

  #[cfg(not(target_os = "windows"))]
  pub fn make_poll_channel() -> io::Result<(PollEventSource, PollEventSender)> {
    let (rec_sps, send_sps) = socketpair_stream()?;
    let rec_sps = set_non_blocking(rec_sps)?;
    let send_sps = set_non_blocking(send_sps)?;

    let rec_mio_socket = TcpStream::from_std(std::net::TcpStream::from(OwnedFd::from(rec_sps)));
    let send_mio_socket = TcpStream::from_std(std::net::TcpStream::from(OwnedFd::from(send_sps)));

    Ok((
      PollEventSource {
        rec_mio_socket: Mutex::new(rec_mio_socket),
      },
      PollEventSender {
        send_mio_socket: Arc::new(Mutex::new(send_mio_socket)),
      },
    ))
  }

  #[cfg(target_os = "windows")]
  pub fn make_poll_channel() -> io::Result<(PollEventSource, PollEventSender)> {
    let listener = match TcpListener::bind("127.0.0.1:0".parse().unwrap()) {
      Ok(listener) => listener,
      Err(err) => {
        error!("Failed to make listener!: {err}");
        return Err(err);
      }
    };

    let addr = match listener.local_addr() {
      Ok(listener) => listener,
      Err(err) => {
        error!("Failed to retrieve local address: {err}");
        return Err(err);
      }
    };

    let rec_mio_socket = match TcpStream::connect(addr) {
      Ok(tcp_stream) => tcp_stream,
      Err(err) => {
        error!("Failed to connect tcp stream: {err}");
        return Err(err);
      }
    };

    let mut send_mio_socket = listener.accept();
    while send_mio_socket.is_err() {
      sleep(Duration::from_millis(100));
      send_mio_socket = listener.accept();
    }
    let (send_mio_socket, _addr_send) = send_mio_socket?;
    debug!("IPC socket: {addr:#?} <-> {_addr_send:#?}");
    Ok((
      PollEventSource {
        rec_mio_socket: Mutex::new(rec_mio_socket),
      },
      PollEventSender {
        send_mio_socket: Arc::new(Mutex::new(send_mio_socket)),
      },
    ))
  }

  impl PollEventSender {
    pub fn send(&self) {
      match self.send_mio_socket.lock().unwrap().write(&[0xcc]) {
        Ok(_b) => { // presumably wrote something
        }
        Err(e) => {
          info!("PollEventSender.send: {e}");
        }
      }
    }
  }

  impl PollEventSource {
    /// drain the sent events so that buffers do not fill up
    // and triggering can happen again. This should be called every
    // time just before acting on the events.
    pub fn drain(&self) {
      // receive and discard all available data from recv_socket
      let mut buf = Vec::with_capacity(16);
      match self.rec_mio_socket.lock().unwrap().read_to_end(&mut buf) {
        Ok(_) => (),
        Err(err) => {
          match err.kind() {
            io::ErrorKind::WouldBlock => {} // This is the expected case
            other_kind => {
              info!("PollEventSource.drain(): {other_kind}");
            }
          }
        }
      }
    }
  }

  impl event::Source for PollEventSource {
    fn register(
      &mut self,
      registry: &Registry,
      token: Token,
      interests: Interest,
    ) -> io::Result<()> {
      self
        .rec_mio_socket
        .lock()
        .unwrap()
        .register(registry, token, interests)
    }

    fn reregister(
      &mut self,
      registry: &Registry,
      token: Token,
      interests: Interest,
    ) -> io::Result<()> {
      self
        .rec_mio_socket
        .lock()
        .unwrap()
        .reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
      self.rec_mio_socket.lock().unwrap().deregister(registry)
    }
  }
}

// No-op stubs used when the "mio_08" feature is disabled. They preserve the
// internal API surface (fields, `make_poll_channel`, `send`, `drain`) so the
// rest of the crate compiles unchanged, while pulling in neither `mio` 0.8 nor
// `socketpair`. Notifications instead flow via the async waker and mio-0.6.
#[cfg(not(feature = "mio_08"))]
mod imp {
  use std::io;

  pub struct PollEventSource;

  #[derive(Clone)]
  pub struct PollEventSender;

  pub fn make_poll_channel() -> io::Result<(PollEventSource, PollEventSender)> {
    Ok((PollEventSource, PollEventSender))
  }

  impl PollEventSender {
    #[inline]
    pub fn send(&self) {}
  }

  impl PollEventSource {
    #[inline]
    pub fn drain(&self) {}
  }
}

pub use imp::*;
