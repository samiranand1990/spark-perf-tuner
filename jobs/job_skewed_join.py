"""
jobs/job_skewed_join.py
------------------------
Anti-pattern: an inner equi-JOIN on a skewed key, with Adaptive Query
Execution disabled.

This is deliberately a JOIN, not a skewed GROUP BY aggregation (an
earlier version of this file did the latter, and the difference matters
enough to call out explicitly): Spark's HashAggregate does map-side
partial aggregation before the shuffle, which collapses a hot key down
to one pre-aggregated row per map task *before* anything is shuffled.
That means a skewed groupBy(sum/count) barely shows up as shuffle skew
at all -- the combiner-style optimization neutralizes it. A JOIN has no
equivalent mitigation: every matching row pair on both sides must be
shuffled and materialized, so a hot join key produces a genuinely
oversized shuffle partition and a single straggler task. That's the
real-world "one reducer runs for an hour while the rest finish in
seconds" failure mode.

We disable AQE entirely (not just skewJoin) for the baseline so the
naive case is fully exposed; the tuned run uses explicit key salting,
which is the general-purpose fix and works even when AQE/skew-join
detection is unavailable or the skew is below AQE's detection
threshold.

Run standalone:
    python -m jobs.job_skewed_join --mode baseline
    python -m jobs.job_skewed_join --mode tuned
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sparksession import get_spark
from pyspark.sql import functions as F

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def _load(spark):
    orders = spark.read.parquet(os.path.join(DATA_DIR, "orders_join_fact.parquet"))
    events = spark.read.parquet(os.path.join(DATA_DIR, "events_join_fact.parquet"))
    return orders, events


def run_baseline(spark):
    """
    Plain sort-merge join on customer_id. Broadcast is disabled via
    spark.sql.autoBroadcastJoinThreshold=-1 (set in main()) so both
    sides shuffle by hash(customer_id) -- the hot key's ~3,000 order
    rows and ~1,000 event rows all land in the same single shuffle
    partition, producing ~3,000,000 output rows in that one task while
    a typical cold-key partition produces a few dozen.
    """
    orders, events = _load(spark)
    joined = orders.join(events, on="customer_id", how="inner")
    result = joined.groupBy("customer_id").agg(
        F.count("*").alias("matched_pairs"),
        F.sum("order_amount").alias("total_amount"),
    )
    result.collect()


def run_tuned(spark):
    """
    Tuned: salted join. We salt the orders side with a random bucket
    in [0, N) and explode the events side into N copies (one per
    bucket), then join on (customer_id, salt). This spreads every
    key's contribution -- including the hot one -- across N separate
    shuffle partitions instead of one, at the cost of N-x replication
    on the exploded (smaller) side. This is the standard general-
    purpose skew-join fix: it doesn't require knowing which key is hot
    ahead of time, unlike single-key isolation/broadcast tricks.
    """
    orders, events = _load(spark)
    salt_buckets = 20

    orders_salted = orders.withColumn("salt", (F.rand(seed=55) * salt_buckets).cast("int"))
    events_salted = events.withColumn(
        "salt", F.explode(F.array([F.lit(i) for i in range(salt_buckets)]))
    )

    joined = orders_salted.join(events_salted, on=["customer_id", "salt"], how="inner")
    result = joined.groupBy("customer_id").agg(
        F.count("*").alias("matched_pairs"),
        F.sum("order_amount").alias("total_amount"),
    )
    result.collect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "tuned"], default="baseline")
    args = parser.parse_args()

    app_name = f"job_skewed_join_{args.mode}"
    # AQE's coalescePartitions feature adaptively merges small post-shuffle
    # partitions, which -- as a side effect -- also blurs single-partition
    # skew. Disabling AQE entirely for both runs keeps the comparison to
    # "no skew handling" vs. "explicit salting" as the only variable.
    spark = get_spark(
        app_name,
        shuffle_partitions=100,
        extra_conf={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
        },
    )
    if args.mode == "baseline":
        run_baseline(spark)
    else:
        run_tuned(spark)
    spark.stop()
    print(f"{app_name} complete.")


if __name__ == "__main__":
    main()
