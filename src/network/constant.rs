// These constants are from RTPS spec Section 9.6.2.3 Default Port Numbers
const PB: u16 = 7400;
const DG: u16 = 250;
const PG: u16 = 2;

const D0: u16 = 0;
const D1: u16 = 10;
const D2: u16 = 1;
const D3: u16 = 11;

pub const fn spdp_well_known_multicast_port(domain_id: u16) -> u16 {
  PB + DG * domain_id + D0
}

pub const fn spdp_well_known_unicast_port(domain_id: u16, participant_id: u16) -> u16 {
  PB + DG * domain_id + D1 + PG * participant_id
}

pub const fn user_traffic_multicast_port(domain_id: u16) -> u16 {
  PB + DG * domain_id + D2
}

pub const fn user_traffic_unicast_port(domain_id: u16, participant_id: u16) -> u16 {
  PB + DG * domain_id + D3 + PG * participant_id
}

/// Default number of same-host participant slots probed by unicast SPDP
/// "localhost peers". SPDP is additionally sent to
/// `127.0.0.1:spdp_well_known_unicast_port(domain, pid)` for
/// `pid in 0..SPDP_LOCALHOST_PEER_COUNT`, so that two participants on the same
/// host discover each other even when there is no external network and
/// multicast on the loopback interface is unavailable (the Linux default for
/// `lo`). See `src/rtps/loopback_same_host_design.md`.
pub const SPDP_LOCALHOST_PEER_COUNT: u16 = 12;
