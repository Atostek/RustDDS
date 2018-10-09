extern crate rtps;
extern crate time;

use self::rtps::common::time::{Time_t, TIME_ZERO, TIME_INVALID, TIME_INFINITE};
use self::time::at_utc;

assert_ser_de!({
    time_zero,
    TIME_ZERO,
    le = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    be = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
});

assert_ser_de!({
    time_invalid,
    TIME_INVALID,
    le = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
    be = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
});

assert_ser_de!({
    time_infinite,
    TIME_INFINITE,
    le = [0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF],
    be = [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
});

assert_ser_de!({
    time_current_empty_fraction,
    Time_t { seconds: 1537045491, fraction: 0 },
    le = [0xF3, 0x73, 0x9D, 0x5B, 0x00, 0x00, 0x00, 0x00],
    be = [0x5B, 0x9D, 0x73, 0xF3, 0x00, 0x00, 0x00, 0x00]
});

assert_ser_de!({
    time_from_wireshark,
    Time_t { seconds: 1519152760, fraction: 1328210046 },
    le = [0x78, 0x6E, 0x8C, 0x5A, 0x7E, 0xE0, 0x2A, 0x4F],
    be = [0x5A, 0x8C, 0x6E, 0x78, 0x4F, 0x2A, 0xE0, 0x7E]
});

#[test]
fn convert_from_timespec() {
    let timespec = time::Timespec { sec: 1519152760, nsec: 1328210046 };
    let time: Time_t = timespec.into();

    assert_eq!(time, Time_t { seconds: 1519152760, fraction: 1328210046 });
}

#[test]
fn convert_to_timespec() {
    let time = Time_t { seconds: 1519152760, fraction: 1328210046 };
    let timespec: time::Timespec = time.into();

    assert_eq!(timespec, time::Timespec { sec: 1519152760, nsec: 1328210046 });
}