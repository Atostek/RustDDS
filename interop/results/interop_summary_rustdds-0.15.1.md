# Interoperability summary: rustdds-0.15.1

- Chosen implementation: **rustdds-0.15.1**
- dds-rtps test suite version: **v0.1-alpha-147-gee81bfb (recorded at run time)**
- Test platform: `Linux 6.8.0-117-generic #117-Ubuntu SMP PREEMPT_DYNAMIC Tue May  5 19:26:24 UTC 2026 x86_64 GNU/Linux`
- Generated: 2026-10-28 18:56:37

Each cell shows **passed / unsupported / failed** test cases. *Unsupported* means the program under test reported the feature as unsupported (`PUB_/SUB_UNSUPPORTED_FEATURE`); *failed* is any other mismatch. Both test directions (chosen vendor as Publisher and as Subscriber) are summed per peer.

> The self row (chosen vendor vs itself) has about half as many test cases as the other rows: reversing the publisher and subscriber roles when both endpoints are the same implementation is redundant, so that pairing is run in one direction only.

| Peer | Domain | Data Repr. | Reliability | History | Ownership | Deadline | Topic | CFT | Partition | Durability | TimeBasedFilter | FinalInstState | LargeData | Lifespan | OrderedAccess | CoherentSets | Total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `rustdds-0.15.1` | 3 / 0 / 0 | 1 / 3 / 0 | 6 / 0 / 0 | 2 / 0 / 0 | 6 / 0 / 1 | 4 / 0 / 0 | 2 / 0 / 0 | 0 / 1 / 1 | 0 / 3 / 0 | 18 / 0 / 0 | 0 / 2 / 0 | 0 / 0 / 3 | 1 / 0 / 0 | 8 / 0 / 0 | 0 / 11 / 0 | 0 / 13 / 0 | **51 / 33 / 5** |
| `connext_dds-7.7.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **109 / 61 / 8** |
| `dust_dds-0.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 1 / 2 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 0 / 0 / 6 | 0 / 0 / 2 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **100 / 61 / 17** |
| `eclipse_cyclone-11.0.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 1 / 2 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **106 / 62 / 10** |
| `eprosima_fastdds-3.6.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 4 / 0 | 2 / 0 / 4 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **106 / 63 / 9** |
| `hdds-1.3.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 3 / 0 / 1 | 13 / 0 / 1 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 14 / 0 / 2 | 0 / 22 / 0 | 0 / 26 / 0 | **109 / 61 / 8** |
| `intercom_dds-4.3.1` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 0 / 2 / 2 | 2 / 0 / 4 | 2 / 0 / 0 | 0 / 0 / 16 | 0 / 22 / 0 | 0 / 26 / 0 | **90 / 61 / 27** |
| `opendds-3.35.0-dev` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 1 / 2 / 1 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **108 / 61 / 9** |
| `toc_coredx_dds-6.15.0` | 6 / 0 / 0 | 4 / 4 / 0 | 12 / 0 / 0 | 4 / 0 / 0 | 10 / 0 / 4 | 8 / 0 / 0 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 36 / 0 / 0 | 2 / 2 / 0 | 3 / 0 / 3 | 2 / 0 / 0 | 16 / 0 / 0 | 0 / 22 / 0 | 0 / 26 / 0 | **109 / 61 / 8** |
| `zzdds-0.1.1` | 5 / 0 / 1 | 3 / 4 / 1 | 11 / 0 / 1 | 2 / 0 / 2 | 10 / 0 / 4 | 6 / 0 / 2 | 4 / 0 / 0 | 2 / 1 / 1 | 0 / 6 / 0 | 33 / 0 / 3 | 2 / 2 / 0 | 0 / 0 / 6 | 2 / 0 / 0 | 15 / 0 / 1 | 0 / 22 / 0 | 0 / 26 / 0 | **95 / 61 / 22** |
| **Total** | **56 / 0 / 1** | **36 / 39 / 1** | **113 / 0 / 1** | **34 / 0 / 4** | **99 / 0 / 34** | **74 / 0 / 2** | **38 / 0 / 0** | **16 / 11 / 11** | **0 / 57 / 0** | **339 / 0 / 3** | **9 / 22 / 7** | **19 / 0 / 38** | **17 / 0 / 2** | **133 / 0 / 19** | **0 / 209 / 0** | **0 / 247 / 0** | **983 / 585 / 123** |

Legend: `passed / unsupported / failed`.
