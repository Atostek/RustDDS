//! A DataWriter or DataReader created with the USER_DATA QoS policy announces
//! it in its discovery data, so a remote participant sees it. ROS 2 puts the
//! type hash of each endpoint there (REP-2016).
use std::time::{Duration, Instant};

use rustdds::{
  policy, DomainParticipant, DomainParticipantStatusEvent, QosPolicies, QosPolicyBuilder,
  StatusEvented, TopicKind,
};
use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Ping {
  seq: u32,
}

const TOPIC: &str = "user_data_test_topic";

fn user_data(value: &str) -> policy::UserData {
  policy::UserData {
    value: value.as_bytes().to_vec(),
  }
}

#[test]
fn remote_endpoints_carry_their_user_data() {
  let domain_id = common::test_domain_id(30);
  let announcing = DomainParticipant::new(domain_id).unwrap();
  let observing = DomainParticipant::new(domain_id).unwrap();
  let status = observing.status_listener();

  let qos = QosPolicyBuilder::new().build();
  let topic = announcing
    .create_topic(
      TOPIC.to_string(),
      "Ping".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .unwrap();
  let writer_qos: QosPolicies = qos.clone().with_user_data(user_data("typehash=writer;"));
  let reader_qos: QosPolicies = qos.clone().with_user_data(user_data("typehash=reader;"));
  let _writer = announcing
    .create_publisher(&qos)
    .unwrap()
    .create_datawriter_no_key_cdr::<Ping>(&topic, Some(writer_qos))
    .unwrap();
  let _reader = announcing
    .create_subscriber(&qos)
    .unwrap()
    .create_datareader_no_key_cdr::<Ping>(&topic, Some(reader_qos))
    .unwrap();

  let mut writer_user_data = None;
  let mut reader_user_data = None;
  let deadline = Instant::now() + Duration::from_secs(10);
  while (writer_user_data.is_none() || reader_user_data.is_none()) && Instant::now() < deadline {
    match status.try_recv_status() {
      Some(DomainParticipantStatusEvent::WriterDetected { writer })
        if writer.topic_name == TOPIC =>
      {
        writer_user_data = Some((writer.qos.user_data(), writer.user_data));
      }
      Some(DomainParticipantStatusEvent::ReaderDetected { reader })
        if reader.topic_name == TOPIC =>
      {
        reader_user_data = Some((reader.qos.user_data(), reader.user_data));
      }
      Some(_) => {}
      None => std::thread::sleep(Duration::from_millis(20)),
    }
  }

  let (writer_qos, writer_raw) = writer_user_data.expect("the remote writer was not detected");
  assert_eq!(writer_qos, Some(user_data("typehash=writer;")));
  // The raw parameter: the CDR sequence length, then the bytes.
  assert!(writer_raw
    .get(4..)
    .is_some_and(|bytes| bytes.starts_with(b"typehash=writer;")));
  let (reader_qos, reader_raw) = reader_user_data.expect("the remote reader was not detected");
  assert_eq!(reader_qos, Some(user_data("typehash=reader;")));
  assert!(reader_raw
    .get(4..)
    .is_some_and(|bytes| bytes.starts_with(b"typehash=reader;")));
}
