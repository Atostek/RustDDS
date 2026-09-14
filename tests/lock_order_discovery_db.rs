//! Regression test for an ABBA lock-order inversion between the two
//! per-participant locks of a single `DomainParticipant`:
//!
//! * `dpi`          : `Arc<Mutex<DomainParticipantDisc>>`
//! * `discovery_db` : `Arc<RwLock<DiscoveryDB>>`
//!
//! Before the fix, endpoint creation took `discovery_db` -> `dpi`:
//! `InnerPublisher::create_datawriter` held `discovery_db.write()` across the
//! `DiscoveredWriterData::new(..)` call, which reached back into the
//! participant (`domain_id()`, `participant_id()`, `only_networks()`,
//! `guid()`) and therefore locked `dpi`. `InnerSubscriber::create_datareader`
//! had the same shape via `DiscoveryDB::update_local_topic_reader`.
//!
//! The discovery accessors took them in the opposite order, `dpi` ->
//! `discovery_db`: `DomainParticipant::discovered_readers()` (and
//! `discovered_writers`, `discovered_topics`, `find_topic`) locked `dpi` for
//! the whole statement and took `discovery_db.read()` inside it.
//!
//! Two threads on the *same* participant - one creating endpoints, one polling
//! the discovery accessors - could therefore deadlock: the creating thread held
//! `discovery_db.write()` and waited for `dpi`, while the polling thread held
//! `dpi` and waited for `discovery_db.read()`.
//!
//! Endpoint creation and the three snapshot accessors now avoid holding the
//! two locks together.
//!
//! Because a deadlocked pair would wedge the libtest harness forever, the
//! racing threads run in a *child process*: this same test binary re-executed
//! with `RUSTDDS_LOCK_ORDER_CHILD=1`. The parent waits with a deadline, kills
//! the child if it hangs, and fails instead of hanging.

use std::{
  env,
  process::{Command, Stdio},
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
  },
  thread,
  time::{Duration, Instant},
};

use rustdds::{policy, DomainParticipant, QosPolicies, QosPolicyBuilder, TopicKind};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Sample {
  seq: u32,
}

/// Set in the re-executed child process that actually runs the racing threads.
const CHILD_ENV: &str = "RUSTDDS_LOCK_ORDER_CHILD";
/// Domain id handed down to the child, so parent and child agree on it.
const DOMAIN_ENV: &str = "RUSTDDS_LOCK_ORDER_DOMAIN";
/// Must match the name of the `#[test]` function below.
const RACE_TEST_NAME: &str = "endpoint_creation_races_discovery_accessors";
/// Proves the child completed the race; libtest also exits with 0 if no test
/// matched.
const CHILD_SUCCESS_EXIT_CODE: i32 = 42;

/// How long the parent waits for the child before declaring a deadlock.
const CHILD_DEADLINE: Duration = Duration::from_secs(30);
/// Threads that create endpoints.
const CREATOR_THREADS: usize = 2;
/// Threads that poll discovery snapshots.
const ACCESSOR_THREADS: usize = 2;
/// Writer + reader pairs created per creator thread.
const ENDPOINTS_PER_CREATOR: usize = 150;

fn test_qos() -> QosPolicies {
  QosPolicyBuilder::new()
    .reliability(policy::Reliability::BestEffort)
    .durability(policy::Durability::Volatile)
    .history(policy::History::KeepLast { depth: 1 })
    .build()
}

/// Pick a domain id that is unlikely to collide with other DDS traffic on this
/// machine, while staying inside the range where the RTPS port arithmetic
/// (`7400 + 250 * domain_id + ..`) still fits in a `u16`.
fn scratch_domain_id(base: u16, span: u32) -> u16 {
  base + (std::process::id() % span) as u16
}

/// The body that runs in the child process: `CREATOR_THREADS` threads creating
/// endpoints and `ACCESSOR_THREADS` threads polling the discovery accessors,
/// all on one shared `DomainParticipant`.
fn run_race() {
  let domain_id: u16 = env::var(DOMAIN_ENV)
    .expect("child started without a domain id")
    .parse()
    .expect("domain id is not a u16");

  let qos = test_qos();
  let dp = DomainParticipant::new(domain_id).expect("failed to create DomainParticipant");

  // Complete fallible setup before starting any barrier workers. A setup panic
  // must fail the child instead of stranding another worker at the barrier.
  let creators: Vec<_> = (0..CREATOR_THREADS)
    .map(|_| {
      let publisher = dp
        .create_publisher(&qos)
        .expect("failed to create Publisher");
      let subscriber = dp
        .create_subscriber(&qos)
        .expect("failed to create Subscriber");
      (publisher, subscriber)
    })
    .collect();

  // Counts the creator threads that have finished, so the accessor threads know
  // when to stop. Start only after all workers have completed their setup;
  // the lock interleavings themselves are still determined by the scheduler.
  let creators_done = Arc::new(AtomicUsize::new(0));
  let start = Arc::new(Barrier::new(CREATOR_THREADS + ACCESSOR_THREADS));
  let mut threads = Vec::with_capacity(CREATOR_THREADS + ACCESSOR_THREADS);

  for (creator, (publisher, subscriber)) in creators.into_iter().enumerate() {
    let dp = dp.clone();
    let qos = qos.clone();
    let creators_done = Arc::clone(&creators_done);
    let start = Arc::clone(&start);
    threads.push(thread::spawn(move || {
      start.wait();
      for i in 0..ENDPOINTS_PER_CREATOR {
        let topic = dp
          .create_topic(
            format!("lock_order_topic_{creator}_{i}"),
            "Sample".to_string(),
            &qos,
            TopicKind::NoKey,
          )
          .expect("failed to create Topic");

        // Both constructors used to hold `discovery_db.write()` while taking
        // `dpi`. Drop the endpoints at each iteration to bound resource use.
        let _writer = publisher
          .create_datawriter_no_key_cdr::<Sample>(&topic, None)
          .expect("failed to create DataWriter");
        let _reader = subscriber
          .create_datareader_no_key_cdr::<Sample>(&topic, None)
          .expect("failed to create DataReader");
      }

      creators_done.fetch_add(1, Ordering::SeqCst);
    }));
  }

  for _ in 0..ACCESSOR_THREADS {
    let dp = dp.clone();
    let creators_done = Arc::clone(&creators_done);
    let start = Arc::clone(&start);
    threads.push(thread::spawn(move || {
      start.wait();
      // Exercise every accessor at least once, even if this thread is delayed
      // until after the creators finish.
      loop {
        std::hint::black_box(dp.discovered_readers());
        std::hint::black_box(dp.discovered_writers());
        std::hint::black_box(dp.discovered_topics());

        if creators_done.load(Ordering::SeqCst) == CREATOR_THREADS {
          break;
        }
      }
    }));
  }

  for t in threads {
    t.join().expect("a racing thread panicked");
  }
}

/// Reproduces the lock-order inversion in a child process and fails - rather
/// than hanging - if the child deadlocks.
#[test]
fn endpoint_creation_races_discovery_accessors() {
  if env::var_os(CHILD_ENV).is_some() {
    run_race();
    std::process::exit(CHILD_SUCCESS_EXIT_CODE);
  }

  let exe = env::current_exe().expect("cannot locate the test executable");
  let domain_id = scratch_domain_id(200, 20);

  let started = Instant::now();
  let mut child = Command::new(&exe)
    .arg(RACE_TEST_NAME)
    .arg("--exact")
    .arg("--nocapture")
    .arg("--test-threads=1")
    .env(CHILD_ENV, "1")
    .env(DOMAIN_ENV, domain_id.to_string())
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .spawn()
    .expect("cannot re-execute the test binary");

  let deadline = started + CHILD_DEADLINE;
  let status = loop {
    match child.try_wait().expect("cannot poll the child process") {
      Some(status) => break Some(status),
      None if Instant::now() >= deadline => break None,
      // Polling interval only; it does not affect whether the test passes,
      // just how promptly the parent notices that the child is done.
      None => thread::sleep(Duration::from_millis(20)),
    }
  };

  match status {
    Some(status) if status.code() == Some(CHILD_SUCCESS_EXIT_CODE) => (),
    Some(status) => panic!(
      "the child did not report race completion (domain {domain_id}, exit {status}, expected exit \
       code {CHILD_SUCCESS_EXIT_CODE}) after {:.1} s",
      started.elapsed().as_secs_f64()
    ),
    None => {
      let _ = child.kill();
      let _ = child.wait();
      panic!(
        "The racing child process (domain {domain_id}) did not finish within {} s and was killed. \
         Possible lock-order deadlock between endpoint creation and discovery snapshots.",
        CHILD_DEADLINE.as_secs()
      )
    }
  }
}

/// Single-threaded sanity check that documents the API exercised above. It
/// passes both before and after the fix, because one thread can never hold both
/// locks in conflicting orders.
#[test]
fn single_thread_create_writer_then_query_discovery() {
  let qos = test_qos();
  let dp =
    DomainParticipant::new(scratch_domain_id(220, 10)).expect("failed to create participant");

  let topic = dp
    .create_topic(
      "lock_order_sanity_topic".to_string(),
      "Sample".to_string(),
      &qos,
      TopicKind::NoKey,
    )
    .expect("failed to create Topic");
  let publisher = dp
    .create_publisher(&qos)
    .expect("failed to create Publisher");
  let _writer = publisher
    .create_datawriter_no_key_cdr::<Sample>(&topic, None)
    .expect("failed to create DataWriter");

  // The accessors must return without deadlocking or panicking. The remote
  // sets may legitimately be empty - nothing else is required to be on the
  // network - so only the local topic is asserted on.
  let _ = dp.discovered_readers();
  let _ = dp.discovered_writers();
  let topics = dp.discovered_topics();
  assert!(
    topics
      .iter()
      .any(|t| t.topic_data.name == "lock_order_sanity_topic"),
    "the locally created topic should show up in discovered_topics(), got {:?}",
    topics
      .iter()
      .map(|t| t.topic_data.name.clone())
      .collect::<Vec<_>>()
  );
}
