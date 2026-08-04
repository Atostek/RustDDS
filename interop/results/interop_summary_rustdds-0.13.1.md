# Interoperability summary: rustdds-0.13.1

- Chosen implementation: **rustdds-0.13.1**
- dds-rtps test suite version: **v0.1-alpha-146-ga4c7fed (recorded at run time)**
- Test platform: `Linux 6.8.0-117-generic #117-Ubuntu SMP PREEMPT_DYNAMIC Tue May  5 19:26:24 UTC 2026 x86_64 GNU/Linux`
- Generated: 2026-07-02 23:08:06

Each cell shows **passed / unsupported / failed** test cases. *Unsupported* means the program under test reported the feature as unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other mismatch. Both test directions (chosen vendor as Publisher and as Subscriber) are summed per peer.

> The self row (chosen vendor vs itself) has about half as many test cases as the other rows: reversing the publisher and subscriber roles when both endpoints are the same implementation is redundant, so that pairing is run in one direction only.

| Peer | Domain | Data Repr. | Reliability | History | Ownership | Deadline | Topic | CFT | Partition | Durability | TimeBasedFilter | FinalInstState | LargeData | Lifespan | OrderedAccess | CoherentSets | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `rustdds-0.13.1` | 3 / 0 / 0 | 1 / 3 / 0 | 6 / 0 / 0 | 2 / 0 / 0 | 6 / 0 / 1 | 4 / 0 / 0 | 2 / 0 / 0 | 0 / 1 / 1 | 0 / 3 / 0 | 18 / 0 / 0 | 0 / 2 / 0 | 0 / 0 / 3 | 1 / 0 / 0 | 8 / 0 / 0 | 0 / 11 / 0 | 0 / 13 / 0 | **51 / 33 / 5** |
| `connext_dds-7.7.0` | 6 / 0 / 0 | 4 / 4 / 0 | 11 / 0 / 1 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 1 / 0 / 5 | 2 / 0 / 0 | 12 / 0 / 4 | 0 / 22 / 0 | 0 / 26 / 0 | **102 / 61 / 15** |
| `eclipse_cyclone-11.0.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 2 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 1 / 0 / 5 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **104 / 62 / 12** |
| `eprosima_fastdds-3.6.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 2 / 0 / 2 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 4 / 0 | 1 / 0 / 5 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **103 / 63 / 12** |
| `hdds-1.3.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 13 / 0 / 1 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **111 / 61 / 6** |
| `intercom_dds-4.3.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 0 / 0 / 6 | 2 / 0 / 0 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **87 / 61 / 30** |
| `opendds-3.35.0-dev` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 35 / 0 / 1 | 0 / 2 / 2 | 0 / 0 / 6 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **102 / 61 / 15** |
| `toc_coredx_dds-6.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 13 / 0 / 3 | 0 / 22 / 0 | 0 / 26 / 0 | **106 / 61 / 11** |
| `zzdds-0.1.1` | 2 / 0 / 4 | 0 / 4 / 4 | 0 / 0 / 12 | 0 / 0 / 4 | 0 / 0 / 14 | 0 / 0 / 8 | 2 / 0 / 2 | 0 / 1 / 3 | 0 / 6 / 0 | 0 / 0 / 36 | 0 / 2 / 2 | 0 / 0 / 6 | 0 / 0 / 2 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **4 / 61 / 113** |
| `dust_dds-0.15.0` | _not run (rc=124)_ |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | _not run (rc=124)_ |
| **Total** | **47 / 0 / 4** | **29 / 35 / 4** | **89 / 0 / 13** | **25 / 0 / 9** | **79 / 0 / 40** | **60 / 0 / 8** | **32 / 0 / 2** | **13 / 10 / 11** | **0 / 51 / 0** | **269 / 0 / 37** | **6 / 20 / 8** | **9 / 0 / 42** | **15 / 0 / 2** | **97 / 0 / 39** | **0 / 187 / 0** | **0 / 221 / 0** | **770 / 524 / 219** |

Legend: `passed / unsupported / failed`.
