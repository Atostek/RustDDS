# Interoperability summary: rustdds-0.12.0

- Chosen implementation: **rustdds-0.12.0**
- dds-rtps test suite version: **v0.1-alpha-146-ga4c7fed (detected from current checkout)**
- Test platform: `Linux 6.8.0-117-generic #117-Ubuntu SMP PREEMPT_DYNAMIC Tue May  5 19:26:24 UTC 2026 x86_64 GNU/Linux`
- Generated: 2026-07-02 11:50:34

Each cell shows **passed / unsupported / failed** test cases. *Unsupported* means the program under test reported the feature as unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other mismatch. Both test directions (chosen vendor as Publisher and as Subscriber) are summed per peer.

> The self row (chosen vendor vs itself) has about half as many test cases as the other rows: reversing the publisher and subscriber roles when both endpoints are the same implementation is redundant, so that pairing is run in one direction only.

| Peer | Domain | Data Repr. | Reliability | History | Ownership | Deadline | Topic | CFT | Partition | Durability | TimeBasedFilter | FinalInstState | LargeData | Lifespan | OrderedAccess | CoherentSets | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `rustdds-0.12.0` | 3 / 0 / 0 | 1 / 3 / 0 | 6 / 0 / 0 | 2 / 0 / 0 | 5 / 0 / 2 | 4 / 0 / 0 | 2 / 0 / 0 | 0 / 1 / 1 | 0 / 3 / 0 | 18 / 0 / 0 | 0 / 2 / 0 | 0 / 0 / 3 | 1 / 0 / 0 | 8 / 0 / 0 | 0 / 11 / 0 | 0 / 13 / 0 | **50 / 33 / 6** |
| `connext_dds-7.7.0` | 6 / 0 / 0 | 2 / 4 / 2 | 12 / 0 / 0 | 4 / 0 / 0 | 9 / 0 / 5 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 1 / 0 / 5 | 1 / 0 / 1 | 12 / 0 / 4 | 0 / 22 / 0 | 0 / 26 / 0 | **99 / 61 / 18** |
| `eclipse_cyclone-11.0.1` | 6 / 0 / 0 | 2 / 4 / 2 | 12 / 0 / 0 | 4 / 0 / 0 | 9 / 0 / 5 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 2 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 1 / 0 / 5 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **101 / 62 / 15** |
| `eprosima_fastdds-3.6.1` | 6 / 0 / 0 | 2 / 4 / 2 | 12 / 0 / 0 | 3 / 0 / 1 | 9 / 0 / 5 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 4 / 0 | 2 / 0 / 4 | 1 / 0 / 1 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **101 / 63 / 14** |
| `hdds-1.3.0` | 6 / 0 / 0 | 2 / 4 / 2 | 12 / 0 / 0 | 3 / 0 / 1 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 15 / 0 / 1 | 0 / 22 / 0 | 0 / 26 / 0 | **105 / 61 / 12** |
| `intercom_dds-4.3.1` | 6 / 0 / 0 | 2 / 4 / 2 | 12 / 0 / 0 | 3 / 0 / 1 | 9 / 0 / 5 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 0 / 0 / 6 | 2 / 0 / 0 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **84 / 61 / 33** |
| `opendds-3.35.0-dev` | 6 / 0 / 0 | 2 / 4 / 2 | 12 / 0 / 0 | 3 / 0 / 1 | 9 / 0 / 5 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 35 / 0 / 1 | 0 / 2 / 2 | 1 / 0 / 5 | 1 / 0 / 1 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **99 / 61 / 18** |
| `toc_coredx_dds-6.15.0` | 6 / 0 / 0 | 2 / 4 / 2 | 12 / 0 / 0 | 4 / 0 / 0 | 9 / 0 / 5 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 12 / 0 / 4 | 0 / 22 / 0 | 0 / 26 / 0 | **102 / 61 / 15** |
| `zzdds-0.1.1` | 2 / 0 / 4 | 0 / 4 / 4 | 0 / 0 / 12 | 0 / 0 / 4 | 0 / 0 / 14 | 0 / 0 / 8 | 2 / 0 / 2 | 0 / 1 / 3 | 0 / 6 / 0 | 0 / 0 / 36 | 0 / 2 / 2 | 0 / 0 / 6 | 0 / 0 / 2 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **4 / 61 / 113** |
| `dust_dds-0.15.0` | _not run (rc=124)_ |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | _not run (rc=124)_ |
| **Total** | **47 / 0 / 4** | **15 / 35 / 18** | **90 / 0 / 12** | **26 / 0 / 8** | **69 / 0 / 50** | **60 / 0 / 8** | **32 / 0 / 2** | **13 / 10 / 11** | **0 / 51 / 0** | **269 / 0 / 37** | **6 / 20 / 8** | **11 / 0 / 40** | **12 / 0 / 5** | **95 / 0 / 41** | **0 / 187 / 0** | **0 / 221 / 0** | **745 / 524 / 244** |

Legend: `passed / unsupported / failed`.
