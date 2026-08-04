#![cfg(feature = "mio_08")]
use log::error;
use mio_08::{Events, Interest, Poll, Token};
use serde::{Deserialize, Serialize};

// Publish-subscribe test using polling from mio-0.8.x
// This is based on code from @garamgim (https://github.com/garamgim)

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RustDDSMessage {
  pub id: u8,
  pub value: Vec<u8>,
}

#[test]
fn mio_08_pub_sub_test_main() {
  use rustdds::*;

  const READER_READY: Token = Token(1);
  const READER_STATUS_READY: Token = Token(2);
  const WRITER_STATUS_READY: Token = Token(3);

  let domain_participant = DomainParticipant::new(11).unwrap();

  let qos = QosPolicyBuilder::new()
    .reliability(policy::Reliability::BestEffort)
    .durability(policy::Durability::Volatile)
    .history(policy::History::KeepAll)
    .liveliness(policy::Liveliness::ManualByTopic {
      lease_duration: (rustdds::Duration::INFINITE),
    })
    .build();

  let publisher = domain_participant.create_publisher(&qos).unwrap();
  let subscriber = domain_participant.create_subscriber(&qos).unwrap();

  let some_topic = domain_participant
    .create_topic(
      "Ping".to_string(),
      "RustDDSMessage".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .unwrap();

  let mut poll = Poll::new().unwrap();
  let mut events = Events::with_capacity(4);

  let mut writer = publisher
    .create_datawriter_no_key_cdr::<RustDDSMessage>(&some_topic, Some(qos.clone()))
    .unwrap();

  poll
    .registry()
    .register(
      writer.as_status_source(),
      WRITER_STATUS_READY,
      Interest::READABLE,
    )
    .unwrap();

  let mut reader = subscriber
    .create_datareader_no_key_cdr::<RustDDSMessage>(&some_topic, Some(qos.clone()))
    .unwrap();

  poll
    .registry()
    .register(&mut reader, READER_READY, Interest::READABLE)
    .unwrap();

  poll
    .registry()
    .register(
      reader.as_status_source(),
      READER_STATUS_READY,
      Interest::READABLE,
    )
    .unwrap();

  let mut receive_count = 0;

  'polling_loop: loop {
    if let Err(e) = poll.poll(&mut events, None) {
      println!("Poll error {e}",);
      return;
    }

    for event in &events {
      println!("Token: {:?}", event.token());
      match event.token() {
        READER_READY => loop {
          if let Ok(Some(sample)) = reader.take_next_sample() {
            receive_count += 1;
            println!("Received message {}", sample.value().clone().id);
            // completely arbitrary limit here
            if receive_count > 5 {
              println!("Received enough");
              break 'polling_loop;
            }
          }
        },
        READER_STATUS_READY => {
          if let Some(status) = reader.try_recv_status() {
            println!("DataReader status: {status:?}");
          } else {
            error!("Where is my reader?");
          }
        }

        WRITER_STATUS_READY => {
          if let Some(status) = writer.try_recv_status() {
            println!("DataWriter status: {status:?}");
            for i in 1..=10 {
              writer
                .write(
                  RustDDSMessage {
                    id: i,
                    value: vec![7; 10],
                  },
                  None,
                )
                .unwrap();
            }
          } else {
            error!("Where is my writer?");
          }
        }
        other_token => {
          println!("Polled event is {other_token:?}. WTF?");
        }
      }
    }
  }
}
