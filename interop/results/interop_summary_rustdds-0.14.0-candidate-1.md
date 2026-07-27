# Interoperability summary: rustdds-0.14.0-candidate-1

- Chosen implementation: **rustdds-0.14.0-candidate-1**
- dds-rtps test suite version: **v2.1.2026 (recorded at run time)**
- Test platform: `Linux 7.0.0-27-generic #27-Ubuntu SMP PREEMPT_DYNAMIC Thu Jun 18 19:13:49 UTC 2026 x86_64 GNU/Linux`
- Generated: 2026-07-08 19:13:34

Each cell shows **passed / unsupported / failed** test cases. *Unsupported* means the program under test reported the feature as unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other mismatch. Both test directions (chosen vendor as Publisher and as Subscriber) are summed per peer.

> The self row (chosen vendor vs itself) has about half as many test cases as the other rows: reversing the publisher and subscriber roles when both endpoints are the same implementation is redundant, so that pairing is run in one direction only.

| Peer | Data Repr. | Domain | Reliability | Topic | Color | Durability | History | Ownership | Total |
|---|---|---|---|---|---|---|---|---|---|
| `connext_dds-7.7.0` | 2 / 0 / 0 | 8 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 12 / 0 / 0 | 20 / 0 / 0 | 8 / 0 / 0 | 10 / 0 / 4 | **72 / 0 / 4** |
| `dust_dds-0.15.0` | 0 / 0 / 2 | 4 / 0 / 4 | 0 / 0 / 6 | 2 / 0 / 4 | 0 / 0 / 12 | 0 / 0 / 20 | 0 / 0 / 8 | 0 / 0 / 14 | **6 / 0 / 70** |
| `eclipse_cyclone-11.0.1` | 2 / 0 / 0 | 8 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 12 / 0 / 0 | 18 / 0 / 2 | 8 / 0 / 0 | 10 / 0 / 4 | **70 / 0 / 6** |
| `eprosima_fast_dds_2.13.2` | 1 / 0 / 1 | 6 / 0 / 2 | 3 / 0 / 3 | 4 / 0 / 2 | 6 / 0 / 6 | 10 / 2 / 8 | 4 / 0 / 4 | 6 / 0 / 8 | **40 / 2 / 34** |
| `eprosima_fastdds-3.6.1` | 2 / 0 / 0 | 8 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 12 / 0 / 0 | 20 / 0 / 0 | 8 / 0 / 0 | 10 / 0 / 4 | **72 / 0 / 4** |
| `hdds-1.3.0` | 2 / 0 / 0 | 8 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 12 / 0 / 0 | 20 / 0 / 0 | 8 / 0 / 0 | 13 / 0 / 1 | **75 / 0 / 1** |
| `intercom_dds-3.15.2` | 1 / 0 / 1 | 6 / 0 / 2 | 3 / 0 / 3 | 4 / 0 / 2 | 6 / 0 / 6 | 15 / 0 / 5 | 4 / 0 / 4 | 6 / 0 / 8 | **45 / 0 / 31** |
| `intercom_dds-4.3.1` | 2 / 0 / 0 | 8 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 12 / 0 / 0 | 20 / 0 / 0 | 8 / 0 / 0 | 10 / 0 / 4 | **72 / 0 / 4** |
| `opendds-3.35.0-dev` | 2 / 0 / 0 | 8 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 12 / 0 / 0 | 19 / 0 / 1 | 8 / 0 / 0 | 10 / 0 / 4 | **71 / 0 / 5** |
| `rti_connext_dds-6.1.2` | 1 / 0 / 1 | 6 / 0 / 2 | 3 / 0 / 3 | 4 / 0 / 2 | 6 / 0 / 6 | 15 / 0 / 5 | 4 / 0 / 4 | 6 / 0 / 8 | **45 / 0 / 31** |
| `toc_coredx_dds-6.15.0` | 2 / 0 / 0 | 8 / 0 / 0 | 6 / 0 / 0 | 6 / 0 / 0 | 12 / 0 / 0 | 20 / 0 / 0 | 8 / 0 / 0 | 10 / 0 / 4 | **72 / 0 / 4** |
| `zzdds-0.1.1` | 1 / 0 / 1 | 6 / 0 / 2 | 3 / 0 / 3 | 4 / 0 / 2 | 6 / 0 / 6 | 10 / 0 / 10 | 4 / 0 / 4 | 6 / 0 / 8 | **40 / 0 / 36** |
| **Total** | **18 / 0 / 6** | **84 / 0 / 12** | **54 / 0 / 18** | **60 / 0 / 12** | **108 / 0 / 36** | **187 / 2 / 51** | **72 / 0 / 24** | **97 / 0 / 71** | **680 / 2 / 230** |

Legend: `passed / unsupported / failed`.
