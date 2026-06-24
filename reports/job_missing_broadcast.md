# Missing broadcast join: baseline vs. tuned

## Findings before tuning
- **[HIGH] missing_broadcast**: SQL execution 0: plan uses SortMergeJoin with no BroadcastHashJoin anywhere in the query. If either join side is small enough to fit in executor memory (roughly under spark.sql.autoBroadcastJoinThreshold, default 10MB), this join is paying for a full shuffle it doesn't need.

## Findings after tuning
- none detected

## Metric comparison
| metric | baseline | tuned | change |
|---|---|---|---|
| tasks | 15 | 11 | ↓27% |
| shuffle read | 50.0MB | 1.2KB | ↓100% |
| shuffle write | 50.0MB | 1.2KB | ↓100% |
| spill | 0.0B | 0.0B | n/a |
| executor run time (sum across tasks) | 5598 | 2513 | ↓55% |
| longest single task (straggler) | 734 | 378 | ↓49% |
| worst shuffle-read skew ratio | 1.1x | n/a | ↓100% |

## What fixed it
- **Force a broadcast hint on the small join side**: Spark's automatic broadcast decision relies on statistics it can size at plan time; that estimate is frequently wrong after several DataFrame transformations (filters, projections, prior joins). An explicit broadcast() hint removes the guesswork. Confirm the small side's actual in-memory size first -- broadcasting something larger than spark.sql.autoBroadcastJoinThreshold defeats the purpose and can OOM the driver collecting it.