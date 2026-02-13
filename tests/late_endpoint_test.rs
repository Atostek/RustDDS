/// Regression test: endpoints created after SEDP discovery must be able to
/// exchange data. Previously, `discovery_db` stored endpoint data without
/// filling in the participant's default locators, so late-created endpoints
/// would get reader/writer proxies with empty locator lists and silently
/// drop all data.
use std::time::{Duration, Instant};

use rustdds::{policy, DomainParticipant, QosPolicyBuilder, TopicKind};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Ping {
  seq: u32,
}

#[test]
fn late_writer_can_reach_early_reader() {
  // Participant A: creates a reader immediately.
  let participant_a = DomainParticipant::new(51).unwrap();
  let qos = QosPolicyBuilder::new()
    .reliability(policy::Reliability::Reliable {
      max_blocking_time: rustdds::Duration::from_secs(1),
    })
    .durability(policy::Durability::Volatile)
    .history(policy::History::KeepAll)
    .build();

  let topic_a = participant_a
    .create_topic(
      "late_endpoint_test_topic".to_string(),
      "Ping".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .unwrap();
  let subscriber = participant_a.create_subscriber(&qos).unwrap();
  let mut reader = subscriber
    .create_datareader_no_key_cdr::<Ping>(&topic_a, None)
    .unwrap();

  // Participant B: wait for SEDP discovery to complete with A, then create a
  // late writer.
  let participant_b = DomainParticipant::new(51).unwrap();
  std::thread::sleep(Duration::from_secs(3));

  let topic_b = participant_b
    .create_topic(
      "late_endpoint_test_topic".to_string(),
      "Ping".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .unwrap();
  let publisher = participant_b.create_publisher(&qos).unwrap();
  let writer = publisher
    .create_datawriter_no_key_cdr::<Ping>(&topic_b, None)
    .unwrap();

  // Wait for the late writer to be matched.
  std::thread::sleep(Duration::from_secs(2));

  // Write data from the late writer.
  writer.write(Ping { seq: 42 }, None).unwrap();

  // Read from A.
  let deadline = Instant::now() + Duration::from_secs(5);
  while Instant::now() < deadline {
    if let Ok(Some(sample)) = reader.take_next_sample() {
      assert_eq!(sample.into_value().seq, 42);
      return; // success
    }
    std::thread::sleep(Duration::from_millis(50));
  }
  panic!("late writer's data never arrived at the early reader within 5 seconds");
}

#[test]
fn late_reader_can_receive_from_early_writer() {
  // Participant A: creates a writer immediately.
  let participant_a = DomainParticipant::new(52).unwrap();
  let qos = QosPolicyBuilder::new()
    .reliability(policy::Reliability::Reliable {
      max_blocking_time: rustdds::Duration::from_secs(1),
    })
    .durability(policy::Durability::Volatile)
    .history(policy::History::KeepAll)
    .build();

  let topic_a = participant_a
    .create_topic(
      "late_endpoint_test_topic_2".to_string(),
      "Ping".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .unwrap();
  let publisher = participant_a.create_publisher(&qos).unwrap();
  let writer = publisher
    .create_datawriter_no_key_cdr::<Ping>(&topic_a, None)
    .unwrap();

  // Participant B: wait for SEDP discovery to complete with A, then create a
  // late reader.
  let participant_b = DomainParticipant::new(52).unwrap();
  std::thread::sleep(Duration::from_secs(3));

  let topic_b = participant_b
    .create_topic(
      "late_endpoint_test_topic_2".to_string(),
      "Ping".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .unwrap();
  let subscriber = participant_b.create_subscriber(&qos).unwrap();
  let mut reader = subscriber
    .create_datareader_no_key_cdr::<Ping>(&topic_b, None)
    .unwrap();

  // Wait for the late reader to be matched.
  std::thread::sleep(Duration::from_secs(2));

  // Write data from the early writer (after matching).
  writer.write(Ping { seq: 99 }, None).unwrap();

  // Read from B's late reader.
  let deadline = Instant::now() + Duration::from_secs(5);
  while Instant::now() < deadline {
    if let Ok(Some(sample)) = reader.take_next_sample() {
      assert_eq!(sample.into_value().seq, 99);
      return; // success
    }
    std::thread::sleep(Duration::from_millis(50));
  }
  panic!("early writer's data never arrived at the late reader within 5 seconds");
}
