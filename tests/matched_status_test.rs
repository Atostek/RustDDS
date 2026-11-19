//! `DataWriter::get_publication_matched_status` and
//! `DataReader::get_subscription_matched_status` between two participants.
use std::time::{Duration, Instant};

use rustdds::{policy, DomainParticipant, QosPolicyBuilder, TopicKind};
use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Ping {
  seq: u32,
}

const TIMEOUT: Duration = Duration::from_secs(10);

/// Calls `poll` until it returns a value, for at most [`TIMEOUT`].
fn wait_for<T>(mut poll: impl FnMut() -> Option<T>) -> Option<T> {
  let deadline = Instant::now() + TIMEOUT;
  loop {
    if let Some(value) = poll() {
      return Some(value);
    }
    if Instant::now() >= deadline {
      return None;
    }
    std::thread::sleep(Duration::from_millis(20));
  }
}

#[test]
fn matched_status_follows_the_remote_endpoint() {
  let domain_id = common::test_domain_id(49);
  let qos = QosPolicyBuilder::new()
    .reliability(policy::Reliability::Reliable {
      max_blocking_time: rustdds::Duration::from_secs(1),
    })
    .build();
  let topic_name = "matched_status_test_topic".to_string();

  // Participant A has the writer.
  let participant_a = DomainParticipant::new(domain_id).unwrap();
  let topic_a = participant_a
    .create_topic(
      topic_name.clone(),
      "Ping".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .unwrap();
  let publisher = participant_a.create_publisher(&qos).unwrap();
  let writer = publisher
    .create_datawriter_no_key_cdr::<Ping>(&topic_a, None)
    .unwrap();

  let unmatched = writer.get_publication_matched_status();
  assert_eq!(
    (unmatched.total.count(), unmatched.current.count()),
    (0, 0),
    "matched before there is a reader"
  );

  // Participant B has the reader.
  let participant_b = DomainParticipant::new(domain_id).unwrap();
  let topic_b = participant_b
    .create_topic(topic_name, "Ping".to_string(), &qos, TopicKind::NoKey)
    .unwrap();
  let subscriber = participant_b.create_subscriber(&qos).unwrap();
  let reader = subscriber
    .create_datareader_no_key_cdr::<Ping>(&topic_b, None)
    .unwrap();

  // Each side reports the other one, with the change since its last read.
  // Nothing here reads the status events.
  let on_writer = wait_for(|| {
    let status = writer.get_publication_matched_status();
    (status.current.count() > 0).then_some(status)
  })
  .expect("the writer never reported a matched reader");
  assert_eq!(
    (on_writer.total.count(), on_writer.total.count_change()),
    (1, 1)
  );
  assert_eq!(
    (on_writer.current.count(), on_writer.current.count_change()),
    (1, 1)
  );

  let on_reader = wait_for(|| {
    let status = reader.get_subscription_matched_status();
    (status.current.count() > 0).then_some(status)
  })
  .expect("the reader never reported a matched writer");
  assert_eq!(
    (on_reader.total.count(), on_reader.total.count_change()),
    (1, 1)
  );
  assert_eq!(
    (on_reader.current.count(), on_reader.current.count_change()),
    (1, 1)
  );

  // The reader goes away: the writer is matched with nobody, and has matched
  // one reader ever.
  drop(reader);
  let after_loss = wait_for(|| {
    let status = writer.get_publication_matched_status();
    (status.current.count() == 0).then_some(status)
  })
  .expect("the writer never reported the reader as lost");
  assert_eq!(after_loss.current.count_change(), -1);
  assert_eq!(
    (after_loss.total.count(), after_loss.total.count_change()),
    (1, 0)
  );
}
