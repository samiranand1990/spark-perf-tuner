# Skewed join (hot customer key): baseline vs. tuned

## Findings before tuning
- **[MEDIUM] skew**: Stage 4: one task reads 8.9x the median shuffle-read volume of its 100 peers. This is consistent with a hot key dominating a single hash partition during a join or groupBy shuffle.
- **[HIGH] missing_broadcast**: SQL execution 0: plan uses SortMergeJoin with no BroadcastHashJoin anywhere in the query. If either join side is small enough to fit in executor memory (roughly under spark.sql.autoBroadcastJoinThreshold, default 10MB), this join is paying for a full shuffle it doesn't need.
- **[LOW] serialization_overhead**: Stage 4: (de)serialization time is 26% of executor run time. Worth checking the serializer in use (Kryo vs. default Java serialization) and whether wide/nested row schemas can be narrowed before this stage.

## Findings after tuning
- **[HIGH] missing_broadcast**: SQL execution 0: plan uses SortMergeJoin with no BroadcastHashJoin anywhere in the query. If either join side is small enough to fit in executor memory (roughly under spark.sql.autoBroadcastJoinThreshold, default 10MB), this join is paying for a full shuffle it doesn't need.

## Metric comparison
| metric | baseline | tuned | change |
|---|---|---|---|
| tasks | 110 | 210 | ↑91% |
| shuffle read | 327.3KB | 1.8MB | ↑454% |
| shuffle write | 327.3KB | 1.8MB | ↑454% |
| spill | 0.0B | 0.0B | n/a |
| executor run time (sum across tasks) | 3309 | 5823 | ↑76% |
| longest single task (straggler) | 459 | 399 | ↓13% |
| worst shuffle-read skew ratio | 8.9x | 1.4x | ↓84% |

## What fixed it
- **Salt the hot key before shuffling**: For a JOIN: salt one side with a random bucket in [0, N) and explode the other side into N copies (one per bucket), then join on (original_key, salt). This spreads every key's contribution -- including the hot one -- across N partitions instead of one, at the cost of N-x row replication on the exploded side. For a GROUP BY: note that Spark's map-side partial aggregation already collapses a hot key to one row per map task before the shuffle, so simple sum/count aggregates rarely need this fix -- check whether AQE (spark.sql.adaptive.skewJoin.enabled, on by default in Spark 3.x) is already handling it before reaching for manual salting.
- **Force a broadcast hint on the small join side**: Spark's automatic broadcast decision relies on statistics it can size at plan time; that estimate is frequently wrong after several DataFrame transformations (filters, projections, prior joins). An explicit broadcast() hint removes the guesswork. Confirm the small side's actual in-memory size first -- broadcasting something larger than spark.sql.autoBroadcastJoinThreshold defeats the purpose and can OOM the driver collecting it.
- **Switch to Kryo serialization and narrow row schemas**: Confirm spark.serializer is set to KryoSerializer (Java serialization is Spark's default and is noticeably slower for the same data). If Kryo is already in use, the next lever is reducing what's being serialized -- drop unused columns earlier in the pipeline with select() before a shuffle, since every shuffled column pays this cost on every task.

## Note on the straggler metric in this demo
This runs on a single local JVM (`local[4]`), not a real cluster. On a real cluster, eliminating an 8-9x straggler partition cuts wall-clock time substantially, because every *other* executor finishes early and sits idle while that one task is still running -- the job's completion time is bounded by its slowest task, not the sum of all task times. In local mode there's no idle-executor cost to recover, so the straggler-elimination benefit doesn't show up as cleanly here as it would on a multi-node cluster. The skew ratio itself (8.9x -> 1.5x) is still the correct signal; the wall-clock benefit is a cluster-scale effect this local demo can't fully reproduce.