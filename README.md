# Spark Job Performance Analyzer & Auto-Tuner

A small platform-engineering tool that diagnoses common Spark
performance anti-patterns from event logs and recommends concrete
fixes -- the same kind of problem Unravel Data's product solves, built
from scratch against open-source Spark to demonstrate Spark internals
(shuffle, partitioning, caching, serialization, DAG execution) rather
than against any vendor platform.

## What it does

1. **`datagen/`** generates synthetic datasets with deliberately
   engineered anti-patterns (skewed join key, too many small files,
   a join that should broadcast but doesn't, a derived DataFrame
   that's recomputed instead of cached).
2. **`jobs/`** contains 4 paired Spark jobs (`--mode baseline` /
   `--mode tuned`) that reproduce each anti-pattern and its fix.
   Every job run writes a standard Spark JSON event log.
3. **`analyzer/`** parses those event logs (no live listener
   required -- this works equally well against a history-server log
   from yesterday's failed job) and:
   - extracts per-stage/per-task metrics (`event_log_parser.py`)
   - runs rule-based anti-pattern detectors (`detectors.py`)
   - maps findings to concrete Spark conf / code recommendations
     (`tuner.py`)
   - renders before/after Markdown reports (`report.py`, `cli.py`)

## Quick start

```bash
pip install pyspark==3.5.4

# 1. generate the synthetic datasets (one-time)
python -m datagen.generate

# 2. run each baseline/tuned job pair (produces event logs)
for job in job_skewed_join job_small_files job_missing_broadcast job_missing_cache; do
  python -m jobs.$job --mode baseline
  python -m jobs.$job --mode tuned
done

# 3. analyze a single run
python -m analyzer.cli analyze --app-name job_skewed_join_baseline

# 4. compare a baseline/tuned pair
python -m analyzer.cli compare \
  --baseline job_skewed_join_baseline --tuned job_skewed_join_tuned \
  --label "Skewed join"

# 5. or just run the whole demo suite and get all 4 reports at once
python -m analyzer.cli demo
```

Generated `data/` and `event_logs/` aren't checked in (run the steps
above to regenerate); `reports/` holds example output from the last
run for quick reference.

## Anti-patterns covered

| Job | Anti-pattern | Fix | Key Spark concept |
|---|---|---|---|
| `job_skewed_join` | Hot key dominates one shuffle partition in a JOIN | Key salting (generic, no AQE dependency) | shuffle, partitioning, DAG |
| `job_small_files` | Many tiny files inflate per-task overhead | `coalesce()` after read | file scan planning, task scheduling |
| `job_missing_broadcast` | Small dim table joined via shuffle instead of broadcast | explicit `broadcast()` hint | join strategy selection |
| `job_missing_cache` | Expensive derived DataFrame recomputed on every action | `.cache()` once, reuse | DAG laziness, caching |

## Design notes and things that didn't work on the first try

Worth knowing before extending this, because each of these changed how
a job or detector is built:

- **A skewed `groupBy(sum/count)` barely shows shuffle skew.** Spark's
  map-side partial aggregation (`HashAggregate`) collapses a hot key
  down to one pre-aggregated row per map task *before* the shuffle.
  `job_skewed_join` therefore demonstrates skew on an actual **JOIN**
  (no equivalent mitigation exists there), not a `groupBy`.
- **AQE's `coalescePartitions` quietly merges away the skew signal**
  even with `skewJoin` handling off, because it also adaptively
  combines small post-shuffle partitions. Both baseline and tuned runs
  disable AQE entirely so the comparison isolates "no skew handling"
  vs. "explicit salting."
- **A large cold-customer pool dilutes skew.** With many cold
  customers sharing the hot key's hash bucket, their combined volume
  can mask the hot key's dominance in the bytes-per-partition signal.
  The join-skew dataset deliberately uses a *small* cold-customer pool
  (2,000 ids) so the hot key's ~17x bucket dominance is unambiguous.
- **Spark's file-scan auto-coalescing neutralizes the naive small-file
  demo.** Reading 2,000 tiny files with default settings collapses to
  ~60 partitions automatically via `openCostInBytes` bin-packing.
  `job_small_files` instead tunes `maxPartitionBytes` to sit just
  above `openCostInBytes`, which flips the bin-packer to "one file per
  partition" -- reproducing the actual failure mode seen on storage
  backends where `openCostInBytes` underestimates true per-file cost.
- **Caching a high-cardinality string column can cost more than the
  recompute it's meant to save.** An earlier version of
  `job_missing_cache` derived a `sha2` hex-string hash column; caching
  5M rows of that made the "tuned" run *20x slower* than the naive
  baseline, because Spark's columnar cache encoder handles
  high-entropy strings very poorly. Swapped to `xxhash64` (a single
  numeric column) -- same "expensive to compute" property, none of the
  cache-encoding cliff. Left as a documented lesson rather than hidden,
  because it's a genuinely useful real-world caveat: caching isn't
  free, and wide/string-heavy frames are exactly where it can backfire.
- **Local single-JVM mode doesn't show the wall-clock benefit of
  fixing skew as cleanly as a real cluster would.** Eliminating a
  straggler partition cuts wall-clock time on a cluster because every
  *other* executor would otherwise sit idle waiting on it; in
  `local[4]` mode there's no idle-executor cost to recover, so total
  summed executor time can even look slightly worse after salting
  (replication overhead is real CPU cost) while the skew *ratio*
  itself is still the correct, and dramatically improved, signal. The
  comparison report says this explicitly rather than leaving the
  numbers to look contradictory.

## Extending this

- New anti-pattern: add a `jobs/job_<name>.py` with `--mode
  baseline/tuned`, a detector function in `analyzer/detectors.py`
  (append to `ALL_DETECTORS`), and a `Recommendation` entry in
  `analyzer/tuner.py`. Nothing else needs to change -- `report.py` and
  `cli.py` are generic over whatever `run_all_detectors()` returns.
- Different metrics source: `analyzer/event_log_parser.py` only reads
  the JSON event log schema, which is identical whether it comes from
  a live `SparkListener` or a Spark History Server log directory (S3,
  HDFS, or local) -- so this is a relatively short step from "analyze
  one job I just ran locally" to "analyze any job in our history
  server."
