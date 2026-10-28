# Interoperability summary: rustdds-0.15.1

- Chosen implementation: **rustdds-0.15.1**
- dds-rtps test suite version: **v0.1-alpha-147-gee81bfb (recorded at run time)**
- Test platform: `Linux 6.8.0-117-generic #117-Ubuntu SMP PREEMPT_DYNAMIC Tue May  5 19:26:24 UTC 2026 x86_64 GNU/Linux`
- Generated: 2026-10-27 19:11:12

Each cell shows **passed / unsupported / failed** test cases. *Unsupported* means the program under test reported the feature as unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other mismatch. Both test directions (chosen vendor as Publisher and as Subscriber) are summed per peer.

> The self row (chosen vendor vs itself) has about half as many test cases as the other rows: reversing the publisher and subscriber roles when both endpoints are the same implementation is redundant, so that pairing is run in one direction only.

| Peer | Domain | Data Repr. | Reliability | History | Ownership | Deadline | Topic | CFT | Partition | Durability | TimeBasedFilter | FinalInstState | LargeData | Lifespan | OrderedAccess | CoherentSets | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `rustdds-0.15.1` | 3 / 0 / 0 | 1 / 3 / 0 | 6 / 0 / 0 | 2 / 0 / 0 | 6 / 0 / 1 | 4 / 0 / 0 | 2 / 0 / 0 | 0 / 1 / 1 | 0 / 3 / 0 | 18 / 0 / 0 | 0 / 2 / 0 | 0 / 0 / 3 | 1 / 0 / 0 | 8 / 0 / 0 | 0 / 11 / 0 | 0 / 13 / 0 | **51 / 33 / 5** |
| `connext_dds-7.7.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 2 / 0 / 4 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **108 / 61 / 9** |
| `dust_dds-0.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 1 / 2 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 0 / 0 / 6 | 0 / 0 / 2 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **100 / 61 / 17** |
| `eclipse_cyclone-11.0.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 2 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **106 / 62 / 10** |
| `eprosima_fastdds-3.6.1` | 6 / 0 / 0 | 4 / 4 / 0 | 10 / 0 / 2 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 4 / 0 | 2 / 0 / 4 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **104 / 63 / 11** |
| `hdds-1.3.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 13 / 0 / 1 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 1 / 0 / 1 | 14 / 0 / 2 | 0 / 22 / 0 | 0 / 26 / 0 | **108 / 61 / 9** |
| `intercom_dds-4.3.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 2 / 0 / 4 | 2 / 0 / 0 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **90 / 61 / 27** |
| `opendds-3.35.0-dev` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 1 / 2 / 1 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **108 / 61 / 9** |
| `toc_coredx_dds-6.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **109 / 61 / 8** |
| `zzdds-0.1.1` | 6 / 0 / 0 | 4 / 4 / 0 | 10 / 0 / 2 | 3 / 0 / 1 | 9 / 0 / 5 | 8 / 0 / 0 | 3 / 0 / 1 | 2 / 1 / 1 | 0 / 6 / 0 | 34 / 0 / 2 | 2 / 2 / 0 | 0 / 0 / 6 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **99 / 61 / 18** |
| **Total** | **57 / 0 / 0** | **37 / 39 / 0** | **110 / 0 / 4** | **35 / 0 / 3** | **98 / 0 / 35** | **76 / 0 / 0** | **37 / 0 / 1** | **16 / 11 / 11** | **0 / 57 / 0** | **340 / 0 / 2** | **9 / 22 / 7** | **18 / 0 / 39** | **16 / 0 / 3** | **134 / 0 / 18** | **0 / 209 / 0** | **0 / 247 / 0** | **983 / 585 / 123** |

Legend: `passed / unsupported / failed`.
