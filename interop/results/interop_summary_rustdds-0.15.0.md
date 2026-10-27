# Interoperability summary: rustdds-0.15.0

- Chosen implementation: **rustdds-0.15.0**
- dds-rtps test suite version: **v0.1-alpha-147-gee81bfb (recorded at run time)**
- Test platform: `Linux 6.8.0-117-generic #117-Ubuntu SMP PREEMPT_DYNAMIC Tue May  5 19:26:24 UTC 2026 x86_64 GNU/Linux`
- Generated: 2026-10-26 20:12:07

Each cell shows **passed / unsupported / failed** test cases. *Unsupported* means the program under test reported the feature as unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other mismatch. Both test directions (chosen vendor as Publisher and as Subscriber) are summed per peer.

> The self row (chosen vendor vs itself) has about half as many test cases as the other rows: reversing the publisher and subscriber roles when both endpoints are the same implementation is redundant, so that pairing is run in one direction only.

| Peer | Domain | Data Repr. | Reliability | History | Ownership | Deadline | Topic | CFT | Partition | Durability | TimeBasedFilter | FinalInstState | LargeData | Lifespan | OrderedAccess | CoherentSets | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `rustdds-0.15.0` | 3 / 0 / 0 | 1 / 3 / 0 | 6 / 0 / 0 | 0 / 0 / 2 | 6 / 0 / 1 | 4 / 0 / 0 | 2 / 0 / 0 | 0 / 1 / 1 | 0 / 3 / 0 | 18 / 0 / 0 | 0 / 2 / 0 | 0 / 0 / 3 | 1 / 0 / 0 | 8 / 0 / 0 | 0 / 11 / 0 | 0 / 13 / 0 | **49 / 33 / 7** |
| `connext_dds-7.7.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 14 / 0 / 2 | 0 / 22 / 0 | 0 / 26 / 0 | **107 / 61 / 10** |
| `dust_dds-0.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 1 / 2 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 0 / 0 / 6 | 0 / 0 / 2 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **100 / 61 / 17** |
| `eclipse_cyclone-11.0.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 2 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **106 / 62 / 10** |
| `eprosima_fastdds-3.6.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 4 / 0 | 2 / 0 / 4 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **106 / 63 / 9** |
| `hdds-1.3.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 13 / 0 / 1 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 1 / 0 / 1 | 14 / 0 / 2 | 0 / 22 / 0 | 0 / 26 / 0 | **108 / 61 / 9** |
| `intercom_dds-4.3.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 2 / 0 / 4 | 2 / 0 / 0 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **90 / 61 / 27** |
| `opendds-3.35.0-dev` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 35 / 0 / 1 | 0 / 2 / 2 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **106 / 61 / 11** |
| `toc_coredx_dds-6.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 2 / 0 / 4 | 2 / 0 / 0 | 15 / 0 / 1 | 0 / 22 / 0 | 0 / 26 / 0 | **107 / 61 / 10** |
| `zzdds-0.1.1` | 4 / 0 / 2 | 2 / 4 / 2 | 6 / 0 / 6 | 2 / 0 / 2 | 4 / 0 / 10 | 4 / 0 / 4 | 3 / 0 / 1 | 0 / 1 / 3 | 0 / 6 / 0 | 15 / 0 / 21 | 0 / 2 / 2 | 0 / 0 / 6 | 0 / 0 / 2 | 6 / 0 / 10 | 0 / 22 / 0 | 0 / 26 / 0 | **46 / 61 / 71** |
| **Total** | **55 / 0 / 2** | **35 / 39 / 2** | **108 / 0 / 6** | **32 / 0 / 6** | **93 / 0 / 40** | **72 / 0 / 4** | **37 / 0 / 1** | **14 / 11 / 13** | **0 / 57 / 0** | **320 / 0 / 22** | **6 / 22 / 10** | **18 / 0 / 39** | **14 / 0 / 5** | **121 / 0 / 31** | **0 / 209 / 0** | **0 / 247 / 0** | **925 / 585 / 181** |

Legend: `passed / unsupported / failed`.
