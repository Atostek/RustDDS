//! Minimal reproduction: a writer publishing large samples faster than its
//! reader keeps up.
//!
//! Spawns a subscriber in a child process and publishes 300 KB images on a
//! Reliable, KeepLast(6) topic, with `max_blocking_time` 0, over loopback. The
//! publisher also sends a small message every 10 ms, which the subscriber
//! echoes back on another topic, and reports their round trip time. Unix only,
//! because `pause` uses the `kill` command.
//!
//! `cargo run --release --example slow_reader_mwe -- overload`
//!   Publishes images at 2000 Hz for 3 s, faster than the subscriber keeps up,
//!   then times dropping the DomainParticipant. Expected: the small messages
//!   keep a round trip of a few milliseconds, and the drop takes well under a
//!   second. The publisher holds unacknowledged images, about 2 GB at the end.
//!
//! `cargo run --release --example slow_reader_mwe -- pause`
//!   Publishes images at 100 Hz for 8 s and stops the subscriber process
//!   (SIGSTOP) from 2 s to 4 s. Expected: the subscriber receives and echoes
//!   again after it is resumed.

use std::{
  process::{Child, Command},
  thread::sleep,
  time::{Duration, Instant},
};

use rustdds::{
  policy::{History, Reliability},
  DomainParticipant, QosPolicies, QosPolicyBuilder, Topic, TopicKind,
};
use serde::{Deserialize, Serialize};

// Not the default domain, to keep away from other DDS applications.
const DOMAIN_ID: u16 = 77;
const IMAGE_SIZE: usize = 300_000;
const TICK_PERIOD: Duration = Duration::from_millis(10);

#[derive(Serialize, Deserialize)]
struct Image {
  seq: u32,
  // CDR (de)serializes a String as one block, a Vec<u8> byte by byte.
  data: String,
}

#[derive(Serialize, Deserialize)]
struct Tick {
  // Microseconds since the publisher started.
  sent_us: u64,
}

fn main() {
  env_logger::init();
  match std::env::args().nth(1).as_deref() {
    Some("overload") => publish(2000, Duration::from_secs(3), false),
    Some("pause") => publish(100, Duration::from_secs(8), true),
    Some("subscriber") => subscribe(),
    _ => eprintln!("usage: slow_reader_mwe overload|pause"),
  }
}

fn qos() -> QosPolicies {
  QosPolicyBuilder::new()
    .history(History::KeepLast { depth: 6 })
    .reliability(Reliability::Reliable {
      max_blocking_time: rustdds::Duration::ZERO,
    })
    .build()
}

// Returns the image, tick and echo topics.
fn create_topics(participant: &DomainParticipant) -> (Topic, Topic, Topic) {
  let topic = |name: &str, type_name: &str| {
    participant
      .create_topic(
        name.to_string(),
        type_name.to_string(),
        &qos(),
        TopicKind::NoKey,
      )
      .unwrap()
  };
  (
    topic("slow_reader_mwe_image", "Image"),
    topic("slow_reader_mwe_tick", "Tick"),
    topic("slow_reader_mwe_echo", "Tick"),
  )
}

fn publish(image_rate_hz: u32, duration: Duration, pause_subscriber: bool) {
  let mut child = Command::new(std::env::current_exe().unwrap())
    .arg("subscriber")
    .spawn()
    .unwrap();

  let participant = DomainParticipant::new(DOMAIN_ID).unwrap();
  let (image_topic, tick_topic, echo_topic) = create_topics(&participant);
  let publisher = participant.create_publisher(&qos()).unwrap();
  let subscriber = participant.create_subscriber(&qos()).unwrap();
  let image_writer = publisher
    .create_datawriter_no_key_cdr::<Image>(&image_topic, None)
    .unwrap();
  let tick_writer = publisher
    .create_datawriter_no_key_cdr::<Tick>(&tick_topic, None)
    .unwrap();
  let mut echo_reader = subscriber
    .create_datareader_no_key_cdr::<Tick>(&echo_topic, None)
    .unwrap();

  // Wait for the subscriber's image and tick readers.
  while participant
    .discovered_readers()
    .iter()
    .filter(|r| {
      r.subscription_topic_data
        .topic_name()
        .starts_with("slow_reader_mwe")
    })
    .count()
    < 2
  {
    sleep(Duration::from_millis(50));
  }
  sleep(Duration::from_millis(500));
  println!("publisher: writing images at {image_rate_hz} Hz for {duration:?}");

  let data = "x".repeat(IMAGE_SIZE);
  let image_period = Duration::from_secs(1) / image_rate_hz;
  let start = Instant::now();
  let (mut next_image, mut next_tick) = (start, start);
  let mut next_report = start + Duration::from_secs(1);
  let mut seq = 0;
  let (mut written, mut failed, mut echoes) = (0, 0, 0);
  let mut max_round_trip = Duration::ZERO;
  let mut paused = false;
  // Ticks continue for 2 s after the images, to show whether echoes recover.
  while start.elapsed() < duration + Duration::from_secs(2) {
    let now = Instant::now();
    let writing_images = now - start < duration;
    if writing_images && now >= next_image {
      // With max_blocking_time 0, a write fails when the send window is full.
      let image = Image {
        seq,
        data: data.clone(),
      };
      match image_writer.write(image, None) {
        Ok(()) => written += 1,
        Err(_) => failed += 1,
      }
      seq += 1;
      next_image += image_period;
    }
    if now >= next_tick {
      let sent_us = (now - start).as_micros() as u64;
      let _ = tick_writer.write(Tick { sent_us }, None);
      next_tick += TICK_PERIOD;
    }
    while let Ok(Some(echo)) = echo_reader.take_next_sample() {
      let round_trip = start.elapsed() - Duration::from_micros(echo.value().sent_us);
      max_round_trip = max_round_trip.max(round_trip);
      echoes += 1;
    }
    if pause_subscriber {
      let t = now - start;
      if !paused && t >= Duration::from_secs(2) && t < Duration::from_secs(4) {
        signal(&child, "STOP");
        paused = true;
      } else if paused && t >= Duration::from_secs(4) {
        signal(&child, "CONT");
        paused = false;
      }
    }
    if now >= next_report {
      println!(
        "publisher: {:.0} s: images written {written}, failed {failed}; echoes {echoes}, max \
         round trip {max_round_trip:.1?}",
        (now - start).as_secs_f64()
      );
      (written, failed, echoes) = (0, 0, 0);
      max_round_trip = Duration::ZERO;
      next_report = now + Duration::from_secs(1);
    }
    // Wake at least every millisecond to take echoes promptly.
    let mut wake = next_tick
      .min(next_report)
      .min(Instant::now() + Duration::from_millis(1));
    if writing_images {
      wake = wake.min(next_image);
    }
    sleep(wake.saturating_duration_since(Instant::now()));
  }

  let drop_start = Instant::now();
  drop((
    image_writer,
    tick_writer,
    echo_reader,
    publisher,
    subscriber,
  ));
  drop((image_topic, tick_topic, echo_topic, participant));
  println!(
    "publisher: dropping the participant took {:?}",
    drop_start.elapsed()
  );
  child.kill().unwrap();
  child.wait().unwrap();
}

fn signal(child: &Child, signal: &str) {
  println!("publisher: kill -{signal} subscriber");
  Command::new("kill")
    .arg(format!("-{signal}"))
    .arg(child.id().to_string())
    .status()
    .unwrap();
}

fn subscribe() {
  let participant = DomainParticipant::new(DOMAIN_ID).unwrap();
  let (image_topic, tick_topic, echo_topic) = create_topics(&participant);
  let subscriber = participant.create_subscriber(&qos()).unwrap();
  let publisher = participant.create_publisher(&qos()).unwrap();
  let mut image_reader = subscriber
    .create_datareader_no_key_cdr::<Image>(&image_topic, None)
    .unwrap();
  let mut tick_reader = subscriber
    .create_datareader_no_key_cdr::<Tick>(&tick_topic, None)
    .unwrap();
  let echo_writer = publisher
    .create_datawriter_no_key_cdr::<Tick>(&echo_topic, None)
    .unwrap();

  let mut images = 0;
  let mut next_report = Instant::now() + Duration::from_secs(1);
  loop {
    while let Ok(Some(_)) = image_reader.take_next_sample() {
      images += 1;
    }
    while let Ok(Some(tick)) = tick_reader.take_next_sample() {
      let _ = echo_writer.write(tick.into_value(), None);
    }
    if Instant::now() >= next_report {
      println!("subscriber: {images} images in the last second");
      images = 0;
      next_report = Instant::now() + Duration::from_secs(1);
    }
    sleep(Duration::from_millis(1));
  }
}
