# Missing cache on reused DataFrame: baseline vs. tuned

## Findings before tuning
- **[MEDIUM] missing_cache**: 3 separate SQL executions in this application independently scan a parquet source, and none of them show an InMemoryTableScan -- no caching is happening anywhere in the app. If these executions share an upstream derived DataFrame (a join, aggregation, or expensive column derivation reused across multiple actions), it is being recomputed from scratch each time.

## Findings after tuning
- none detected

## Metric comparison
| metric | baseline | tuned | change |
|---|---|---|---|
| tasks | 29 | 15 | ↓48% |
| shuffle read | 0.0B | 0.0B | n/a |
| shuffle write | 36.7MB | 23.5MB | ↓36% |
| spill | 0.0B | 0.0B | n/a |
| executor run time (sum across tasks) | 2887 | 2840 | ↓2% |
| longest single task (straggler) | 427 | 462 | ↑8% |

## What fixed it
- **Cache the shared upstream DataFrame once**: Spark's DAG is lazy: every action against a derived DataFrame re-triggers the full upstream computation unless something persists it. If a join/aggregation/expensive-column-derivation feeds more than one downstream action, .cache() it once and materialize with a cheap action (e.g. .count()) before the real downstream work runs, then .unpersist() when done with it.