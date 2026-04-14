//! Interoperability test program for `RustDDS` library

#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::{
  io,
  time::{Duration, Instant},
};
#[cfg(feature = "security")]
use std::path::Path;

use log::{debug, error, trace, warn, LevelFilter};
use log4rs::{
  append::console::ConsoleAppender,
  config::{Appender, Root},
  Config,
};
use rustdds::{
  dds::statusevents, with_key::Sample, DomainParticipantBuilder, Keyed, QosPolicyBuilder,
  StatusEvented, TopicDescription, TopicKind,
};
use rustdds::policy::{Deadline, Durability, History, Reliability}; /* import all QoS
                                                                     * policies directly */
use serde::{Deserialize, Serialize};
use clap::{Arg, ArgMatches, Command}; // command line argument processing
use mio_06::{Events, Poll, PollOpt, Ready, Token}; // polling
use mio_extras::channel; // pollable channel
use rand::prelude::*;
#[cfg(feature = "security")]
use rustdds::DomainParticipantSecurityConfigFiles;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ShapeType {
  color: String,
  x: i32,
  y: i32,
  shape_size: i32,
}

impl Keyed for ShapeType {
  type K = String;
  fn key(&self) -> String {
    self.color.clone()
  }
}

const DA_WIDTH: i32 = 240;
const DA_HEIGHT: i32 = 270;

const STOP_PROGRAM: Token = Token(0);
const READER_READY: Token = Token(1);
const READER_STATUS_READY: Token = Token(2);
const WRITER_STATUS_READY: Token = Token(3);

#[allow(clippy::too_many_lines)]
fn main() {
  configure_logging();
  let matches = get_matches();

  // Process command line arguments
  let topic_name = matches
    .get_one::<String>("topic")
    .cloned()
    .unwrap_or("Square".to_owned());
  let domain_id = matches.get_one::<u16>("domain_id").unwrap();
  let color = matches
    .get_one::<String>("color")
    .cloned()
    .unwrap_or("BLUE".to_owned());
  // Change some logging to make automatic tests work
  let is_auto_test = std::env::var("auto_test").is_ok();

  // Build the DomainParticipant
  let dp_builder = DomainParticipantBuilder::new(*domain_id);
  #[cfg(feature = "security")]
  let dp_builder = if let Some(sec_dir_path) = matches.get_one::<String>("security") {
    match (
      matches.get_one::<String>("pkcs11-token"),
      matches.get_one::<String>("pkcs11-library"),
    ) {
      (Some(token_label), Some(hsm_lib_path)) => dp_builder.builtin_security(
        DomainParticipantSecurityConfigFiles::with_ros_default_names_and_hsm(
          Path::new(sec_dir_path),
          Path::new(hsm_lib_path),
          token_label.clone(),
          matches.get_one::<String>("pkcs11-pin").cloned(),
        ),
      ),
      (_, _) => dp_builder.builtin_security(
        DomainParticipantSecurityConfigFiles::with_ros_default_names(
          Path::new(sec_dir_path),
          "no_pwd".to_string(),
        ),
      ),
    }
  } else {
    dp_builder
  };
  #[cfg(not(feature = "security"))]
  if matches.contains_id("security") {
    error!("the security command line option was given, but the security feature is not enabled!");
  }

  let domain_participant = dp_builder
    .build()
    .unwrap_or_else(|e| panic!("DomainParticipant construction failed: {e:?}"));

  let mut qos_b = QosPolicyBuilder::new()
    .reliability(if matches.get_flag("reliable") {
      Reliability::Reliable {
        max_blocking_time: rustdds::Duration::ZERO,
      }
    } else {
      Reliability::BestEffort
    })
    .durability(
      match matches.get_one::<String>("durability").map(String::as_str) {
        Some("l") => Durability::TransientLocal,
        Some("t") => Durability::Transient,
        Some("p") => Durability::Persistent,
        _ => Durability::Volatile,
      },
    )
    .history(match matches.get_one::<i32>("history_depth") {
      None => History::KeepAll,
      Some(d) => {
        if *d < 0 {
          History::KeepAll
        } else {
          History::KeepLast { depth: *d }
        }
      }
    });
  let deadline_policy = matches
    .get_one::<f64>("deadline")
    .map(|dl| Deadline(rustdds::Duration::from_frac_seconds(*dl)));

  if let Some(dl) = deadline_policy {
    qos_b = qos_b.deadline(dl);
  }

  assert!(
    !matches.contains_id("partition"),
    "QoS policy Partition is not yet implemented."
  );

  assert!(
    !matches.contains_id("interval"),
    "QoS policy Time Based Filter is not yet implemented."
  );

  assert!(
    !matches.contains_id("ownership_strength"),
    "QoS policy Ownership Strength is not yet implemented."
  );

  assert!(
    !matches.contains_id("representation"),
    "QoS policy Representation is not yet implemented."
  );

  let qos = qos_b.build();

  let loop_delay: Duration = match deadline_policy {
    None => Duration::from_millis(200), // This is the default rate
    Some(Deadline(dd)) => Duration::from(dd).mul_f32(0.8), // slightly faster than deadline
  };

  let topic = domain_participant
    .create_topic(
      topic_name,
      "ShapeType".to_string(),
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {e:?}"));

  if is_auto_test {
    // Make automation tests happy
    println!("Create topic: {}", topic.name());
    println!("Create reader for topic: {}", topic.name());
  } else {
    println!(
      "Topic name is {}. Type is {}.",
      topic.name(),
      topic.get_type().name()
    );
  }

  // Set Ctrl-C handler
  let (stop_sender, stop_receiver) = channel::channel();
  ctrlc::set_handler(move || {
    stop_sender.send(()).unwrap_or(());
    // ignore errors, as we are quitting anyway
  })
  .expect("Error setting Ctrl-C handler");
  println!("Press Ctrl-C to quit.");

  let poll = Poll::new().unwrap();
  let mut events = Events::with_capacity(4);

  poll
    .register(
      &stop_receiver,
      STOP_PROGRAM,
      Ready::readable(),
      PollOpt::edge(),
    )
    .unwrap();

  let is_publisher = matches.get_flag("publisher");
  let is_subscriber = matches.get_flag("subscriber");

  let mut writer_opt = if is_publisher {
    debug!("Publisher");
    let publisher = domain_participant.create_publisher(&qos).unwrap();
    let mut writer = publisher
      .create_datawriter_cdr::<ShapeType>(&topic, None) // None = get qos policy from publisher
      .unwrap();
    poll
      .register(
        writer.as_status_evented(),
        WRITER_STATUS_READY,
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();
    Some(writer)
  } else {
    None
  };

  let mut reader_opt = if is_subscriber {
    debug!("Subscriber");
    let subscriber = domain_participant.create_subscriber(&qos).unwrap();
    let mut reader = subscriber
      .create_datareader_cdr::<ShapeType>(&topic, Some(qos))
      .unwrap();
    poll
      .register(&reader, READER_READY, Ready::readable(), PollOpt::edge())
      .unwrap();
    poll
      .register(
        reader.as_status_evented(),
        READER_STATUS_READY,
        Ready::readable(),
        PollOpt::edge(),
      )
      .unwrap();
    debug!("Created DataReader");
    Some(reader)
  } else {
    None
  };

  let mut shape_sample = ShapeType {
    color: color.clone(),
    x: 0,
    y: 0,
    shape_size: 21,
  };
  let mut random_gen = rand::rng();
  // a bit complicated lottery to ensure we do not end up with zero velocity.
  let mut x_vel = if rand::random() {
    random_gen.random_range(1..5)
  } else {
    random_gen.random_range(-5..-1)
  };
  let mut y_vel = if rand::random() {
    random_gen.random_range(1..5)
  } else {
    random_gen.random_range(-5..-1)
  };

  let mut last_write = Instant::now();

  loop {
    poll.poll(&mut events, Some(loop_delay)).unwrap();
    for event in &events {
      match event.token() {
        STOP_PROGRAM => {
          if stop_receiver.try_recv().is_ok() {
            println!("Done.");
            return;
          }
        }
        READER_READY => {
          match reader_opt {
            Some(ref mut reader) => {
              loop {
                trace!("DataReader triggered");
                match reader.take_next_sample() {
                  Ok(Some(sample)) => match sample.into_value() {
                    Sample::Value(sample) => println!(
                      "{:10.10} {:10.10} {:3.3} {:3.3} [{}]",
                      topic.name(),
                      sample.color,
                      sample.x,
                      sample.y,
                      sample.shape_size,
                    ),
                    Sample::Dispose(key) => println!("Disposed key {key:?}"),
                  },
                  Ok(None) => break, // no more data
                  Err(e) => println!("DataReader error {e:?}"),
                } // match
              }
            }
            None => {
              error!("Where is my reader?");
            }
          }
        }
        READER_STATUS_READY => match reader_opt {
          Some(ref mut reader) => {
            while let Some(status) = reader.try_recv_status() {
              if is_auto_test {
                match status {
                  // These prints make dds-rtps automation tests happy
                  statusevents::DataReaderStatus::SubscriptionMatched { .. } => {
                    println!("on_subscription_matched()");
                  }
                  statusevents::DataReaderStatus::LivelinessChanged { .. } => {
                    println!("on_liveliness_changed()");
                  }
                  statusevents::DataReaderStatus::RequestedDeadlineMissed { .. } => {
                    println!("on_requested_deadline_missed()");
                  }
                  statusevents::DataReaderStatus::RequestedIncompatibleQos { .. } => {
                    println!("on_requested_incompatible_qos()");
                  }
                  _ => {}
                }
              } else {
                println!("DataReader status: {status:?}");
              }
            }
          }
          None => {
            error!("Where is my reader?");
          }
        },

        WRITER_STATUS_READY => match writer_opt {
          Some(ref mut writer) => {
            while let Some(status) = writer.try_recv_status() {
              if is_auto_test {
                match status {
                  // These prints make dds-rtps automation tests happy
                  statusevents::DataWriterStatus::LivelinessLost { .. } => {
                    println!("on_liveliness_lost()");
                  }
                  statusevents::DataWriterStatus::OfferedDeadlineMissed { .. } => {
                    println!("on_offered_deadline_missed()");
                  }
                  statusevents::DataWriterStatus::OfferedIncompatibleQos { .. } => {
                    println!("on_offered_incompatible_qos()");
                  }
                  statusevents::DataWriterStatus::PublicationMatched { .. } => {
                    println!("on_publication_matched()");
                  }
                }
              } else {
                println!("DataWriter status: {status:?}");
              }
            }
          }
          None => {
            error!("Where is my writer?");
          }
        },
        other_token => {
          println!("Polled event is {other_token:?}. WTF?");
        }
      }
    }

    let r = move_shape(shape_sample, x_vel, y_vel);
    shape_sample = r.0;
    x_vel = r.1;
    y_vel = r.2;

    // write to DDS
    trace!("Writing shape color {}", &color);
    match writer_opt {
      Some(ref mut writer) => {
        let now = Instant::now();
        if last_write + loop_delay < now {
          writer
            .write(shape_sample.clone(), None)
            .unwrap_or_else(|e| error!("DataWriter write failed: {e:?}"));
          last_write = now;
        }
      }
      None => {
        if is_publisher {
          error!("Where is my writer?");
        } else { // never mind
        }
      }
    }
  } // loop
}

fn configure_logging() {
  // initialize logging, preferably from config file
  log4rs::init_file(
    "logging-config.yaml",
    log4rs::config::Deserializers::default(),
  )
  .unwrap_or_else(|e| {
    match e.downcast_ref::<io::Error>() {
      // Config file did not work. If it is a simple "No such file or directory", then
      // substitute some default config.
      Some(os_err) if os_err.kind() == io::ErrorKind::NotFound => {
        println!("No config file found in current working directory.");
        let stdout = ConsoleAppender::builder().build();
        let conf = Config::builder()
          .appender(Appender::builder().build("stdout", Box::new(stdout)))
          .build(Root::builder().appender("stdout").build(LevelFilter::Error))
          .unwrap();
        log4rs::init_config(conf).unwrap();
      }
      // Give up.
      other_error => panic!("Config problem: {other_error:?}"),
    }
  });
}

#[allow(clippy::too_many_lines)]
fn get_matches() -> ArgMatches {
  Command::new("RustDDS-interop")
    .version("0.2.2")
    .author("Juhana Helovuo <juhe@iki.fi>")
    .about("Command-line \"shapes\" interoperability test.")
    .arg(
      Arg::new("domain_id")
        .short('d')
        .value_name("id")
        .value_parser(clap::value_parser!(u16))
        .default_value("0")
        .help("Sets the DDS domain id number"),
    )
    .arg(
      Arg::new("topic")
        .short('t')
        .value_name("name")
        .help("Sets the topic name")
        .required(true),
    )
    .arg(
      Arg::new("color")
        .short('c')
        .value_name("color")
        .default_value("BLUE")
        .help("Color to publish (or filter)"),
    )
    .arg(
      Arg::new("durability")
        .short('D')
        .value_name("durability")
        .help("Set durability")
        .value_parser(["v", "l", "t", "p"]),
    )
    .arg(
      Arg::new("publisher")
        .help("Act as publisher")
        .short('P')
        .action(clap::ArgAction::SetTrue)
        .required_unless_present("subscriber"),
    )
    .arg(
      Arg::new("subscriber")
        .help("Act as subscriber")
        .short('S')
        .action(clap::ArgAction::SetTrue)
        .required_unless_present("publisher"),
    )
    .arg(
      Arg::new("best_effort")
        .help("BEST_EFFORT reliability")
        .short('b')
        .action(clap::ArgAction::SetTrue)
        .conflicts_with("reliable"),
    )
    .arg(
      Arg::new("reliable")
        .help("RELIABLE reliability")
        .short('r')
        .action(clap::ArgAction::SetTrue)
        .conflicts_with("best_effort"),
    )
    .arg(
      Arg::new("history_depth")
        .help("Keep history depth")
        .short('k')
        .value_parser(clap::value_parser!(i32))
        .default_value("1")
        .value_name("depth"),
    )
    .arg(
      Arg::new("deadline")
        .help("Set a 'deadline' with interval (seconds)")
        .short('f')
        .value_name("interval"),
    )
    .arg(
      Arg::new("partition")
        .help("Set a 'partition' string")
        .short('p')
        .value_name("partition"),
    )
    .arg(
      Arg::new("interval")
        .help("Apply 'time based filter' with interval (seconds)")
        .short('i')
        .value_name("interval"),
    )
    .arg(
      Arg::new("ownership_strength")
        .help("Set ownership strength [-1: SHARED]")
        .short('s')
        .value_name("strength"),
    )
    .arg(
      Arg::new("security")
        .help(
          "Path to directory containing security configuration files. Setting this enables \
           security.",
        )
        .long("security")
        .value_name("security"),
    )
    .arg(
      Arg::new("pkcs11-library")
        .help("Path to a library implementing PKCS#11 client.")
        .long("pkcs11-library")
        .value_name("pkcs11-library")
        .requires("pkcs11-token"),
    )
    .arg(
      Arg::new("pkcs11-token")
        .help("Token label for PKCS#11")
        .long("pkcs11-token")
        .value_name("pkcs11-token")
        .requires("security")
        .requires("pkcs11-library"),
    )
    .arg(
      Arg::new("pkcs11-pin")
        .help("PIN to access PKCS#11 token")
        .long("pkcs11-pin")
        .value_name("pkcs11-pin")
        .requires("pkcs11-token"),
    )
    .arg(
      Arg::new("representation")
        .help("Data representation type [1: XCDR, 2: XCDR2]")
        .value_parser(["1", "2"])
        .short('x')
        .value_name("representation"),
    )
    .get_matches()
}

#[allow(clippy::similar_names)]
fn move_shape(shape: ShapeType, xv: i32, yv: i32) -> (ShapeType, i32, i32) {
  let half_size = shape.shape_size / 2 + 1;
  let mut x = shape.x + xv;
  let mut y = shape.y + yv;

  let mut xv_new = xv;
  let mut yv_new = yv;

  if x < half_size {
    x = half_size;
    xv_new = -xv;
  }
  if x > DA_WIDTH - half_size {
    x = DA_WIDTH - half_size;
    xv_new = -xv;
  }
  if y < half_size {
    y = half_size;
    yv_new = -yv;
  }
  if y > DA_HEIGHT - half_size {
    y = DA_HEIGHT - half_size;
    yv_new = -yv;
  }
  (
    ShapeType {
      color: shape.color,
      x,
      y,
      shape_size: shape.shape_size,
    },
    xv_new,
    yv_new,
  )
}
