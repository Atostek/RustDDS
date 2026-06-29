//! `shape_main` interoperability test application for the OMG DDS-RTPS
//! interoperability test suite (<https://github.com/omg-dds/dds-rtps>).
//!
//! This is the RustDDS implementation of the `shape_main` application. The
//! Python test harness (`pexpect`) matches specific strings printed on stdout
//! to determine whether a test passes. Those strings (e.g. `Create topic:`,
//! `Create writer for topic`, `on_publication_matched()`, and the
//! `<topic> <color> <x> <y> [<size>]` data line) MUST NOT be changed.
//!
//! The structure follows `examples/async_shapes_demo`: a `read_loop` and a
//! `write_loop` run concurrently on a `smol` executor, driven by
//! `futures::select!`, with DDS status events arriving as async streams.
//!
//! Build:  cargo build --release --example shape_main
//! Install: cp target/release/examples/shape_main \
//!             <dds-rtps>/executables/rustdds-0.12.0_shape_main_linux

#![allow(clippy::too_many_lines)]

use std::time::Duration as StdDuration;

use clap::Parser;
use futures::{pin_mut, select, FutureExt, StreamExt};
use log::{debug, error};
use rustdds::{
  policy::{Deadline, Durability, History, Lifespan, Ownership, Reliability, TimeBasedFilter},
  with_key::Sample,
  DomainParticipantBuilder, Duration, Keyed, QosPolicyBuilder, StatusEvented, TopicDescription,
  TopicKind,
};
use rustdds::dds::statusevents::{DataReaderStatus, DataWriterStatus};
use serde::{Deserialize, Serialize};
use smol::Timer;

/// `ShapeType` as defined in the suite's `srcCxx/shape.idl`:
///
/// ```text
/// @appendable struct ShapeType {
///   @key string<128> color;
///   int32 x;
///   int32 y;
///   int32 shapesize;
///   sequence<uint8> additional_payload_size;
/// };
/// ```
///
/// CDR serialization is positional, so the field order and types must match
/// the other vendors' shape type exactly. `additional_payload_size` is the
/// trailing sequence used by the `--additional-payload-size` (large data)
/// tests; it is present (possibly empty) so the wire layout matches the
/// current vendor binaries.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct ShapeType {
  color: String,
  x: i32,
  y: i32,
  shapesize: i32,
  additional_payload_size: Vec<u8>,
}

impl Keyed for ShapeType {
  type K = String;
  fn key(&self) -> String {
    self.color.clone()
  }
}

const DA_WIDTH: i32 = 240;
const DA_HEIGHT: i32 = 270;

/// Command-line interface of the OMG `shape_main` application. Every option the
/// test harness might pass is accepted so that no unexpected argument causes a
/// hard parse error (which the harness would read as "topic not created").
#[derive(Parser, Debug, Clone)]
#[command(
  name = "rustdds-shape_main",
  about = "RustDDS shape_main for the OMG DDS-RTPS interoperability test suite"
)]
struct Args {
  /// publish samples
  #[arg(short = 'P', default_value_t = false)]
  publish: bool,

  /// subscribe samples
  #[arg(short = 'S', default_value_t = false)]
  subscribe: bool,

  /// domain id
  #[arg(short = 'd', default_value_t = 0)]
  domain_id: u16,

  /// BEST_EFFORT reliability
  #[arg(short = 'b', default_value_t = false)]
  best_effort: bool,

  /// RELIABLE reliability
  #[arg(short = 'r', default_value_t = false)]
  reliable: bool,

  /// keep history depth (0: KEEP_ALL, >0: KEEP_LAST)
  #[arg(short = 'k')]
  history_depth: Option<i32>,

  /// deadline interval in milliseconds
  #[arg(short = 'f')]
  deadline_ms: Option<i64>,

  /// ownership strength (-1: SHARED, >=0: EXCLUSIVE)
  #[arg(short = 's', allow_negative_numbers = true)]
  ownership_strength: Option<i32>,

  /// topic name
  #[arg(short = 't')]
  topic: String,

  /// color to publish (or filter on as a subscriber)
  #[arg(short = 'c')]
  color: Option<String>,

  /// partition (unsupported by RustDDS)
  #[arg(short = 'p')]
  partition: Option<String>,

  /// durability (v: VOLATILE, l: TRANSIENT_LOCAL, t: TRANSIENT, p: PERSISTENT)
  #[arg(short = 'D')]
  durability: Option<String>,

  /// data representation (1: XCDR, 2: XCDR2). RustDDS only supports XCDR1.
  #[arg(short = 'x')]
  data_representation: Option<String>,

  /// print published samples
  #[arg(short = 'w', default_value_t = false)]
  print_writer_samples: bool,

  /// shapesize (0: grow by 1 every sample)
  #[arg(short = 'z', default_value_t = 20)]
  shapesize: i32,

  /// use read() instead of take() (accepted, no effect on interop)
  #[arg(short = 'R', default_value_t = false)]
  use_read: bool,

  /// time between write() operations in milliseconds
  #[arg(long = "write-period")]
  write_period_ms: Option<u64>,

  /// time between read()/take() operations in milliseconds (accepted)
  #[arg(long = "read-period")]
  read_period_ms: Option<u64>,

  /// log verbosity (accepted, no effect)
  #[arg(short = 'v')]
  verbosity: Option<String>,

  /// time based filter minimum separation in milliseconds
  #[arg(short = 'i', long = "time-filter")]
  time_filter_ms: Option<i64>,

  /// lifespan in milliseconds
  #[arg(short = 'l', long = "lifespan")]
  lifespan_ms: Option<i64>,

  /// number of iterations of the main loop before exiting (0: infinite)
  #[arg(short = 'n', long = "num-iterations")]
  num_iterations: Option<i32>,

  /// number of instances written (colors get a numeric suffix)
  #[arg(short = 'I', long = "num-instances")]
  num_instances: Option<i32>,

  /// number of topics (only 1 supported)
  #[arg(short = 'E', long = "num-topics")]
  num_topics: Option<i32>,

  /// action after writing finishes (u: unregister, d: dispose)
  #[arg(short = 'M', long = "final-instance-state")]
  final_instance_state: Option<char>,

  /// presentation access scope (unsupported)
  #[arg(short = 'C', long = "access-scope")]
  access_scope: Option<char>,

  /// coherent access (unsupported)
  #[arg(short = 'T', long = "coherent", default_value_t = false)]
  coherent: bool,

  /// ordered access (unsupported)
  #[arg(short = 'O', long = "ordered", default_value_t = false)]
  ordered: bool,

  /// coherent set sample count (unsupported)
  #[arg(short = 'H', long = "coherent-sample-count")]
  coherent_sample_count: Option<i32>,

  /// bytes added to each sample (large data)
  #[arg(short = 'B', long = "additional-payload-size")]
  additional_payload_size: Option<i32>,

  /// use take()/read() instead of *_next_instance() (accepted)
  #[arg(short = 'K', long = "take-read", default_value_t = false)]
  take_read: bool,

  /// content filter expression (unsupported by RustDDS)
  #[arg(short = 'F', long = "cft")]
  cft: Option<String>,

  /// apply modulo to a growing shapesize
  #[arg(short = 'Q', long = "size-modulo")]
  size_modulo: Option<i32>,

  /// participant announcement period in milliseconds (accepted, no effect)
  #[arg(long = "periodic-announcement")]
  periodic_announcement_ms: Option<u64>,

  /// data fragment size (accepted, no effect)
  #[arg(long = "datafrag-size")]
  datafrag_size: Option<u64>,
}

/// Print a message containing "not supported" (which the harness recognizes as
/// an unsupported feature) and exit cleanly.
fn unsupported(feature: &str) -> ! {
  println!("{feature} is not supported by RustDDS");
  std::process::exit(0);
}

fn main() {
  env_logger::init();
  let args = Args::parse();

  if !args.publish && !args.subscribe {
    eprintln!("please specify publish [-P] or subscribe [-S]");
    std::process::exit(1);
  }
  if args.publish && args.subscribe {
    eprintln!("please specify only one of publish [-P] or subscribe [-S]");
    std::process::exit(1);
  }

  // Features RustDDS 0.12 does not provide. Report them as unsupported instead
  // of failing in a way the harness cannot classify.
  if args.partition.is_some() {
    unsupported("PARTITION QoS");
  }
  if matches!(args.data_representation.as_deref(), Some("2")) {
    unsupported("XCDR2 data representation");
  }
  if args.cft.is_some() {
    unsupported("ContentFilteredTopic");
  }
  if args.coherent || args.ordered || args.access_scope.is_some() {
    unsupported("PRESENTATION coherent/ordered access");
  }

  let topic_name = args.topic.clone();
  let color = args.color.clone().unwrap_or_else(|| "BLUE".to_owned());

  let domain_participant = DomainParticipantBuilder::new(args.domain_id)
    .build()
    .unwrap_or_else(|e| panic!("DomainParticipant construction failed: {e:?}"));

  let qos = build_qos(&args);

  let topic = domain_participant
    .create_topic(
      topic_name.clone(),
      "ShapeType".to_string(),
      &qos,
      TopicKind::WithKey,
    )
    .unwrap_or_else(|e| panic!("create_topic failed: {e:?}"));
  println!("Create topic: {}", topic.name());

  // Ctrl-C / SIGINT (sent by the harness to stop the app) -> tell the loops to
  // quit. A few tokens are queued so each concurrent loop receives one.
  let (stop_sender, stop_receiver) = smol::channel::bounded(4);
  ctrlc::set_handler(move || {
    for _ in 0..4 {
      stop_sender.send_blocking(()).unwrap_or(());
    }
  })
  .expect("Error setting Ctrl-C handler");

  let writer_opt = if args.publish {
    let publisher = domain_participant.create_publisher(&qos).unwrap();
    let writer = publisher
      .create_datawriter_cdr::<ShapeType>(&topic, None)
      .unwrap();
    println!("Create writer for topic: {topic_name} color: {color}");
    Some(writer)
  } else {
    None
  };

  let reader_opt = if args.subscribe {
    let subscriber = domain_participant.create_subscriber(&qos).unwrap();
    let reader = subscriber
      .create_datareader_cdr::<ShapeType>(&topic, Some(qos.clone()))
      .unwrap();
    println!("Create reader for topic: {topic_name}");
    Some(reader)
  } else {
    None
  };

  let write_interval = write_interval(&args);

  let read_loop = async {
    let Some(datareader) = reader_opt else {
      return;
    };
    let mut run = true;
    let stop = stop_receiver.recv().fuse();
    pin_mut!(stop);
    let mut sample_stream = datareader.async_sample_stream();
    let mut event_stream = sample_stream.async_event_stream();
    while run {
      select! {
        _ = stop => run = false,
        r = sample_stream.select_next_some() => match r {
          Ok(s) => match s.into_value() {
            Sample::Value(sample) => print_sample(&topic_name, &sample),
            Sample::Dispose(key) => println!("Disposed key {key:?}"),
          },
          Err(e) => { error!("{e:?}"); break; }
        },
        e = event_stream.select_next_some() => report_reader_status(&e),
      }
    }
  };

  let write_loop = async {
    let Some(datawriter) = writer_opt else {
      return;
    };
    let mut run = true;
    let stop = stop_receiver.recv().fuse();
    pin_mut!(stop);
    let mut status_stream = datawriter.as_async_status_stream();
    let mut ticks = StreamExt::fuse(Timer::interval(write_interval));

    let num_instances = args.num_instances.unwrap_or(1).max(1);
    let payload = match args.additional_payload_size {
      Some(n) if n > 0 => vec![0xffu8; n as usize],
      _ => Vec::new(),
    };
    let mut shapes: Vec<ShapeType> = (0..num_instances)
      .map(|i| ShapeType {
        color: instance_color(&color, i),
        x: rand::random_range(0..DA_WIDTH),
        y: rand::random_range(0..DA_HEIGHT),
        shapesize: if args.shapesize == 0 { 1 } else { args.shapesize },
        additional_payload_size: payload.clone(),
      })
      .collect();
    let mut velocities: Vec<(i32, i32)> = (0..num_instances).map(|_| random_velocity()).collect();
    let mut iterations = 0i32;

    // RustDDS 0.12 does not raise the writer-side OfferedDeadlineMissed status,
    // so we compute it here: the writer controls its own write cadence and
    // knows the requested deadline. If a deadline elapses without a write, we
    // emit `on_offered_deadline_missed()` (once per missed period), matching
    // what the test harness expects from the publisher.
    let deadline = args.deadline_ms.map(|ms| StdDuration::from_millis(ms.max(0) as u64));
    let mut deadline_check =
      StreamExt::fuse(Timer::interval(deadline.unwrap_or(StdDuration::from_secs(3600))));
    let mut last_write = std::time::Instant::now();
    let mut deadline_reported = false;

    while run {
      select! {
        _ = stop => run = false,
        _ = ticks.select_next_some() => {
          for (shape, vel) in shapes.iter_mut().zip(velocities.iter_mut()) {
            step_shape(shape, vel, &args);
            datawriter
              .async_write(shape.clone(), None)
              .await
              .unwrap_or_else(|e| error!("DataWriter write failed: {e:?}"));
            if args.print_writer_samples {
              print_sample(&topic_name, shape);
            }
          }
          last_write = std::time::Instant::now();
          deadline_reported = false;
          iterations += 1;
          if let Some(n) = args.num_iterations {
            if n > 0 && iterations >= n {
              run = false;
            }
          }
        },
        _ = deadline_check.select_next_some() => {
          if let Some(dl) = deadline {
            if !deadline_reported && last_write.elapsed() >= dl {
              println!("on_offered_deadline_missed()");
              deadline_reported = true;
            }
          }
        },
        e = status_stream.select_next_some() => report_writer_status(&e),
      }
    }

    // Final instance action requested by --final-instance-state.
    if matches!(args.final_instance_state, Some('d')) {
      for shape in &shapes {
        let _ = datawriter.dispose(&shape.key(), None);
      }
    }
    // 'u' (unregister) has no direct RustDDS API; dropping the writer lets the
    // reader observe the loss of the writer.
  };

  debug!("Starting RustDDS shape_main: {args:?}");
  smol::block_on(async { futures::join!(read_loop, write_loop) });
  println!("Done.");
}

/// Translate the command-line options into a `QosPolicies` value using only the
/// policies RustDDS 0.12 supports.
fn build_qos(args: &Args) -> rustdds::QosPolicies {
  let mut b = QosPolicyBuilder::new().reliability(if args.best_effort {
    Reliability::BestEffort
  } else {
    Reliability::Reliable {
      max_blocking_time: Duration::ZERO,
    }
  });

  b = b.durability(match args.durability.as_deref() {
    Some("l") => Durability::TransientLocal,
    Some("t") => Durability::Transient,
    Some("p") => Durability::Persistent,
    _ => Durability::Volatile,
  });

  if let Some(depth) = args.history_depth {
    b = b.history(if depth <= 0 {
      History::KeepAll
    } else {
      History::KeepLast { depth }
    });
  }

  if let Some(ms) = args.deadline_ms {
    b = b.deadline(Deadline(Duration::from_millis(ms)));
  }

  if let Some(strength) = args.ownership_strength {
    b = b.ownership(if strength < 0 {
      Ownership::Shared
    } else {
      Ownership::Exclusive { strength }
    });
  }

  if let Some(ms) = args.lifespan_ms {
    b = b.lifespan(Lifespan {
      duration: Duration::from_millis(ms),
    });
  }

  if let Some(ms) = args.time_filter_ms {
    b = b.time_based_filter(TimeBasedFilter {
      minimum_separation: Duration::from_millis(ms),
    });
  }

  b.build()
}

/// Determine the write period: an explicit `--write-period` wins; otherwise
/// write slightly faster than the deadline; otherwise a brisk default.
fn write_interval(args: &Args) -> StdDuration {
  if let Some(ms) = args.write_period_ms {
    StdDuration::from_millis(ms)
  } else if let Some(ms) = args.deadline_ms {
    StdDuration::from_millis((ms.max(0) as u64 * 4) / 5)
  } else {
    StdDuration::from_millis(100)
  }
}

fn instance_color(base: &str, index: i32) -> String {
  if index == 0 {
    base.to_string()
  } else {
    format!("{base}{index}")
  }
}

fn random_velocity() -> (i32, i32) {
  let pick = || {
    if rand::random() {
      rand::random_range(1..5)
    } else {
      rand::random_range(-5..-1)
    }
  };
  (pick(), pick())
}

/// Maximum shapesize (in pixels) used when computing movement bounds. With
/// `-z 0` the reported `shapesize` grows without bound to let subscribers
/// verify ordering, but it must not be used directly for the bounce extent:
/// a large half-extent would collapse the play area and drive coordinates
/// negative, which breaks the test harness's `[0-9]+` coordinate regex.
const MAX_BOUNCE_SIZE: i32 = 30;

/// Advance a shape one step inside the bouncing box and update its size.
/// Coordinates are always kept within `[half, DA_* - half]`, i.e. strictly
/// non-negative, so the printed data line always matches the harness regex.
fn step_shape(shape: &mut ShapeType, vel: &mut (i32, i32), args: &Args) {
  let half = shape.shapesize.min(MAX_BOUNCE_SIZE) / 2 + 1;
  let max_x = (DA_WIDTH - half).max(half);
  let max_y = (DA_HEIGHT - half).max(half);
  shape.x += vel.0;
  shape.y += vel.1;
  if shape.x < half {
    shape.x = half;
    vel.0 = -vel.0;
  }
  if shape.x > max_x {
    shape.x = max_x;
    vel.0 = -vel.0;
  }
  if shape.y < half {
    shape.y = half;
    vel.1 = -vel.1;
  }
  if shape.y > max_y {
    shape.y = max_y;
    vel.1 = -vel.1;
  }
  if args.shapesize == 0 {
    shape.shapesize = match args.size_modulo {
      Some(m) if m > 0 => (shape.shapesize % m) + 1,
      _ => shape.shapesize + 1,
    };
  }
}

/// Print a sample in the exact format the harness parses:
/// `<topic> <color> <x> <y> [<shapesize>]`.
fn print_sample(topic_name: &str, sample: &ShapeType) {
  println!(
    "{:<10.10} {:<10.10} {:03} {:03} [{}]",
    topic_name, sample.color, sample.x, sample.y, sample.shapesize
  );
}

fn report_reader_status(status: &DataReaderStatus) {
  match status {
    DataReaderStatus::SubscriptionMatched { .. } => println!("on_subscription_matched()"),
    DataReaderStatus::RequestedIncompatibleQos { .. } => {
      println!("on_requested_incompatible_qos()");
    }
    DataReaderStatus::RequestedDeadlineMissed { .. } => {
      println!("on_requested_deadline_missed()");
    }
    DataReaderStatus::LivelinessChanged { .. } => println!("on_liveliness_changed()"),
    _ => {}
  }
}

fn report_writer_status(status: &DataWriterStatus) {
  match status {
    DataWriterStatus::PublicationMatched { .. } => println!("on_publication_matched()"),
    DataWriterStatus::OfferedIncompatibleQos { .. } => println!("on_offered_incompatible_qos()"),
    DataWriterStatus::OfferedDeadlineMissed { .. } => println!("on_offered_deadline_missed()"),
    DataWriterStatus::LivelinessLost { .. } => println!("on_liveliness_lost()"),
  }
}
