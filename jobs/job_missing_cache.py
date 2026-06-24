"""
jobs/job_missing_cache.py
---------------------------
Anti-pattern: an expensive, reused DataFrame is recomputed from scratch
on every action because it was never cached. This is one of the most
common real-world Spark cost issues -- a derived DataFrame (heavy
join/aggregation) gets referenced in three or four downstream actions,
and because Spark's DAG is lazy, each `.collect()`/`.write()`
re-triggers the entire upstream computation unless something persists
it.

We simulate "expensive" with a deliberately heavy column expression
(nested string ops + a join) so the recomputation cost is visible in
stage durations, then run three downstream actions against the same
logical DataFrame.

Run standalone:
    python -m jobs.job_missing_cache --mode baseline
    python -m jobs.job_missing_cache --mode tuned
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sparksession import get_spark
from pyspark.sql import functions as F

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def build_expensive_frame(spark):
    # .limit(300_000): the full 5M-row orders table made cache
    # materialization itself slow enough (tens of seconds, in this
    # sandboxed environment) to muddy the before/after comparison this
    # job exists to demonstrate. The recompute-vs-cache concept doesn't
    # need 5M rows to be visible -- 300K is enough to make 3 repeated
    # full recomputations clearly more expensive than computing once
    # and reusing, without the run time being dominated by constant
    # cache-encoding overhead unrelated to the anti-pattern itself.
    orders = spark.read.parquet(os.path.join(DATA_DIR, "orders.parquet")).limit(300_000)
    customers = spark.read.parquet(os.path.join(DATA_DIR, "customers.parquet"))
    enriched = (
        orders.join(F.broadcast(customers), on="customer_id", how="inner")
        .withColumn("name_upper", F.upper(F.col("customer_name")))
        # xxhash64 (not sha2): both are "expensive to compute" for this demo's
        # purposes, but sha2 produces a 64-char hex *string* per row, which
        # hit Spark's columnar cache encoder pathologically hard when tried
        # at full scale (118s for a stage that took 7s uncached -- verified
        # empirically). xxhash64 returns a single 8-byte long instead. Real
        # lesson either way: caching wide/high-cardinality string columns can
        # cost more than the recompute it's meant to save.
        .withColumn("name_hash", F.xxhash64(F.concat_ws("-", "name_upper", "order_id")))
        .withColumn("amount_bucket", (F.col("order_amount") / 100).cast("int"))
    )
    return enriched


def run_baseline(spark):
    """No cache: each downstream action re-triggers the full upstream DAG."""
    enriched = build_expensive_frame(spark)

    by_region = enriched.groupBy("region").agg(F.sum("order_amount").alias("total"))
    by_region.collect()

    by_bucket = enriched.groupBy("amount_bucket").count()
    by_bucket.collect()

    by_signup = enriched.groupBy("signup_region").agg(F.avg("order_amount").alias("avg_amount"))
    by_signup.collect()


def run_tuned(spark):
    """
    Tuned: persist the expensive frame once after it's built, so the
    join + UDF-like column derivation only happens a single time and
    all three downstream actions reuse the cached result.
    """
    enriched = build_expensive_frame(spark).cache()
    enriched.count()  # materialize the cache deterministically

    by_region = enriched.groupBy("region").agg(F.sum("order_amount").alias("total"))
    by_region.collect()

    by_bucket = enriched.groupBy("amount_bucket").count()
    by_bucket.collect()

    by_signup = enriched.groupBy("signup_region").agg(F.avg("order_amount").alias("avg_amount"))
    by_signup.collect()

    enriched.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "tuned"], default="baseline")
    args = parser.parse_args()

    app_name = f"job_missing_cache_{args.mode}"
    spark = get_spark(app_name, shuffle_partitions=64)
    if args.mode == "baseline":
        run_baseline(spark)
    else:
        run_tuned(spark)
    spark.stop()
    print(f"{app_name} complete.")


if __name__ == "__main__":
    main()
