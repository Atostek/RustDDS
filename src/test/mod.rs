pub(crate) mod random_data;
pub(crate) mod shape_type;
pub(crate) mod test_data;
pub(crate) mod test_domain;
pub(crate) mod test_properties;

use std::{env::VarError, thread, time::Duration};

use anyhow::Result;

use self::test_domain::{test_domain_base, test_domain_id};
use crate::{
  policy::{Durability, History, Reliability},
  DomainParticipant, QosPolicyBuilder, Timestamp, TopicKind,
};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct TestType;

#[test]
fn timestamp() -> Result<()> {
  let participant = DomainParticipant::new(test_domain_id(0))?;
  let qos = QosPolicyBuilder::new()
    .history(History::KeepAll)
    .reliability(Reliability::Reliable {
      max_blocking_time: Duration::from_secs(0).into(),
    })
    .durability(Durability::TransientLocal)
    .build();
  let p1_topic = participant.create_topic(
    "test".to_string(),
    "TestType".to_string(),
    &qos,
    TopicKind::NoKey,
  )?;
  let writer = participant
    .create_publisher(&qos)?
    .create_datawriter_no_key_cdr::<TestType>(&p1_topic, None)?;
  let participant2 = DomainParticipant::new(test_domain_id(0))?;
  let p2_topic = participant2.create_topic(
    "test".to_string(),
    "TestType".to_string(),
    &qos,
    TopicKind::NoKey,
  )?;
  let mut reader = participant2
    .create_subscriber(&qos)?
    .create_datareader_no_key_cdr::<TestType>(&p2_topic, None)?;
  let timestamp = Timestamp::now();
  writer.write(TestType, Some(timestamp))?;
  thread::sleep(Duration::from_secs(3));
  loop {
    if let Ok(Some(sample)) = reader.take_next_sample() {
      assert_eq!(timestamp, sample.sample_info().source_timestamp().unwrap());
      break;
    }
    thread::sleep(Duration::from_millis(100));
  }
  Ok(())
}

#[test]
fn test_domain_base_parsing() {
  assert_eq!(test_domain_base(Err(VarError::NotPresent)), Ok(0));
  assert_eq!(test_domain_base(Ok(String::new())), Ok(0));
  assert_eq!(test_domain_base(Ok("42".to_string())), Ok(42));
  // 147 + MAX_TEST_DOMAIN_OFFSET = 199, just below the lock-order test's domains.
  assert_eq!(test_domain_base(Ok("147".to_string())), Ok(147));
  for bad in ["148", "180", "65535", "65536", "-1", " 42", "forty-two"] {
    assert!(test_domain_base(Ok(bad.to_string())).is_err(), "{bad:?}");
  }
  assert!(test_domain_base(Err(VarError::NotUnicode("x".into()))).is_err());
}
