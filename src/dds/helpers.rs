use crate::structure::duration::Duration;

// By default, give background thread 20 ms to react
pub const TIMEOUT_FALLBACK: Duration = Duration::from_nanos(20_000_000); // 20 ms
