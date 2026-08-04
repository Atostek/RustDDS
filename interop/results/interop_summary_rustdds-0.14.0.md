# Interoperability summary: rustdds-0.14.0

- Chosen implementation: **rustdds-0.14.0**
- dds-rtps test suite version: **v0.1-alpha-146-ga4c7fed (recorded at run time)**
- Test platform: `Linux 6.8.0-117-generic #117-Ubuntu SMP PREEMPT_DYNAMIC Tue May  5 19:26:24 UTC 2026 x86_64 GNU/Linux`
- Generated: 2026-08-03 17:48:29

Each cell shows **passed / unsupported / failed** test cases. *Unsupported* means the program under test reported the feature as unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other mismatch. Both test directions (chosen vendor as Publisher and as Subscriber) are summed per peer.

> The self row (chosen vendor vs itself) has about half as many test cases as the other rows: reversing the publisher and subscriber roles when both endpoints are the same implementation is redundant, so that pairing is run in one direction only.

| Peer | Domain | Data Repr. | Reliability | History | Ownership | Deadline | Topic | CFT | Partition | Durability | TimeBasedFilter | FinalInstState | LargeData | Lifespan | OrderedAccess | CoherentSets | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `rustdds-0.14.0` | 3 / 0 / 0 | 1 / 3 / 0 | 6 / 0 / 0 | 2 / 0 / 0 | 6 / 0 / 1 | 4 / 0 / 0 | 2 / 0 / 0 | 0 / 1 / 1 | 0 / 3 / 0 | 18 / 0 / 0 | 0 / 2 / 0 | 0 / 0 / 3 | 1 / 0 / 0 | 8 / 0 / 0 | 0 / 11 / 0 | 0 / 13 / 0 | **51 / 33 / 5** |
| `connext_dds-7.7.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 14 / 0 / 2 | 0 / 22 / 0 | 0 / 26 / 0 | **107 / 61 / 10** |
| `dust_dds-0.15.0` | 1 / 0 / 2 | 0 / 2 / 2 | 0 / 0 / 6 | 0 / 0 / 2 | 0 / 0 / 7 | 0 / 0 / 4 | 1 / 0 / 1 | 0 / 1 / 1 | 0 / 3 / 0 | 0 / 0 / 18 | 0 / 2 / 0 | 0 / 0 / 3 | 0 / 0 / 1 | 0 / 0 / 8 | 0 / 11 / 0 | 0 / 13 / 0 | **2 / 32 / 55** |
| `eclipse_cyclone-11.0.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 2 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 2 / 0 / 4 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **105 / 62 / 11** |
| `eprosima_fastdds-3.6.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 4 / 0 | 0 / 0 / 6 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **104 / 63 / 11** |
| `hdds-1.3.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 13 / 0 / 1 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **112 / 61 / 5** |
| `intercom_dds-4.3.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 9 / 0 / 5 | 8 / 0 / 0 | 4 / 0 / 0 | 3 / 1 / 0 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 2 / 0 / 4 | 2 / 0 / 0 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **90 / 61 / 27** |
| `opendds-3.35.0-dev` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 35 / 0 / 1 | 1 / 2 / 1 | 2 / 0 / 4 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **105 / 61 / 12** |
| `toc_coredx_dds-6.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 2 / 0 / 4 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **108 / 61 / 9** |
| `zzdds-0.1.1` | 4 / 0 / 2 | 1 / 4 / 3 | 6 / 0 / 6 | 2 / 0 / 2 | 6 / 0 / 8 | 3 / 0 / 5 | 3 / 0 / 1 | 0 / 1 / 3 | 0 / 6 / 0 | 14 / 0 / 22 | 0 / 2 / 2 | 0 / 0 / 6 | 1 / 0 / 1 | 7 / 0 / 9 | 0 / 22 / 0 | 0 / 26 / 0 | **47 / 61 / 70** |
| **Total** | **50 / 0 / 4** | **30 / 37 / 5** | **96 / 0 / 12** | **31 / 0 / 5** | **84 / 0 / 42** | **63 / 0 / 9** | **34 / 0 / 2** | **14 / 11 / 11** | **0 / 54 / 0** | **283 / 0 / 41** | **7 / 22 / 7** | **14 / 0 / 40** | **16 / 0 / 2** | **109 / 0 / 35** | **0 / 198 / 0** | **0 / 234 / 0** | **831 / 556 / 215** |

Legend: `passed / unsupported / failed`.
