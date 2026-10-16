//! Helpers shared by the integration tests.

// The same file is `crate::test::test_domain` for the unit tests.
#[path = "../../src/test/test_domain.rs"]
mod test_domain;

pub use test_domain::test_domain_id;
