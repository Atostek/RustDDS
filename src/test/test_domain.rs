//! Domain ids for tests that create `DomainParticipant`s.
//!
//! Tests run on domain 0 unless `RUSTDDS_TEST_DOMAIN_ID` is set. Moving them
//! keeps a test run on a shared network away from the domain 0 of other DDS
//! systems.
//!
//! The integration tests in `tests/` include this file too (see
//! `tests/common/mod.rs`), so it uses only `std`.

use std::env::{self, VarError};

/// Environment variable holding the base domain id of the tests.
pub const TEST_DOMAIN_ENV: &str = "RUSTDDS_TEST_DOMAIN_ID";

/// Largest offset that a test passes to [`test_domain_id`].
pub const MAX_TEST_DOMAIN_OFFSET: u16 = 52;

/// First of the scratch domains (200-229) that `tests/lock_order_discovery_db.rs`
/// picks for itself. The shifted test domains stay below it.
pub const LOCK_ORDER_FIRST_DOMAIN: u16 = 200;

/// Largest accepted value of `RUSTDDS_TEST_DOMAIN_ID`, 147: the highest test
/// domain, base + `MAX_TEST_DOMAIN_OFFSET`, is then 199.
pub const MAX_TEST_DOMAIN_BASE: u16 = LOCK_ORDER_FIRST_DOMAIN - 1 - MAX_TEST_DOMAIN_OFFSET;

/// Domain id for a test: the base from `RUSTDDS_TEST_DOMAIN_ID` (0 if unset
/// or empty) plus `offset`, which keeps tests that must not discover each
/// other on different domains. Tests that send raw packets to the ports of a
/// domain take that domain from here too, so they move with the participants.
///
/// Panics if the variable is not a usable domain id.
pub fn test_domain_id(offset: u16) -> u16 {
  assert!(
    offset <= MAX_TEST_DOMAIN_OFFSET,
    "test domain offset {offset} is larger than MAX_TEST_DOMAIN_OFFSET"
  );
  match test_domain_base(env::var(TEST_DOMAIN_ENV)) {
    Ok(base) => base + offset,
    Err(e) => panic!("{e}"),
  }
}

/// Parses the value of `RUSTDDS_TEST_DOMAIN_ID`, as returned by `env::var`.
pub fn test_domain_base(value: Result<String, VarError>) -> Result<u16, String> {
  let base = match value {
    Err(VarError::NotPresent) => return Ok(0),
    Err(e) => return Err(format!("{TEST_DOMAIN_ENV}: {e}")),
    Ok(v) if v.is_empty() => return Ok(0),
    Ok(v) => v
      .parse::<u16>()
      .map_err(|e| format!("{TEST_DOMAIN_ENV}={v:?} is not a domain id: {e}"))?,
  };
  // Below the lock-order domains, the RTPS ports (7400 + 250 * 199 + ...) also
  // fit in a u16.
  if base > MAX_TEST_DOMAIN_BASE {
    let highest = u32::from(base) + u32::from(MAX_TEST_DOMAIN_OFFSET);
    return Err(format!(
      "{TEST_DOMAIN_ENV}={base} is too large: tests would use domain ids up to {highest}, which \
       must stay below {LOCK_ORDER_FIRST_DOMAIN}, where the lock-order test's scratch domains \
       start. Use at most {MAX_TEST_DOMAIN_BASE}."
    ));
  }
  Ok(base)
}
