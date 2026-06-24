"""
jobs/job_missing_broadcast.py
-------------------------------
Anti-pattern: joining a large fact table against a small dimension
table using a full shuffle (sort-merge) join, when the small table is
well within broadcast range. We force this by setting
spark.sql.autoBroadcastJoinThreshold to -1, which disables Spark's
automatic broadcast decision entirely -- reproducing what actually
happens in production when someone's join input is wrapped in a
DataFrame transformation Spark's planner can't size statically (a
common real cause of "why isn't this broadcasting" tickets).

Run standalone:
    python -m jobs.job_missing_broadcast --mode baseline
    python -m jobs.job_missing_broadcast --mode tuned
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sparksession import get_spark
from pyspark.sql import functions as F

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def run_baseline(spark):
    """Shuffle (sort-merge) join: broadcast threshold disabled."""
    orders = spark.read.parquet(os.path.join(DATA_DIR, "orders.parquet"))
    customers = spark.read.parquet(os.path.join(DATA_DIR, "customers.parquet"))
    joined = orders.join(customers, on="customer_id", how="inner")
    result = joined.groupBy("signup_region").agg(F.sum("order_amount").alias("total"))
    result.collect()


def run_tuned(spark):
    """
    Tuned: explicit broadcast hint on the small side. customers.parquet
    is ~200K rows / a few MB -- comfortably under the default 10MB-100MB
    broadcast range -- so this avoids a shuffle on the large orders
    table entirely and turns the join into a single-stage map-side join.
    """
    orders = spark.read.parquet(os.path.join(DATA_DIR, "orders.parquet"))
    customers = spark.read.parquet(os.path.join(DATA_DIR, "customers.parquet"))
    joined = orders.join(F.broadcast(customers), on="customer_id", how="inner")
    result = joined.groupBy("signup_region").agg(F.sum("order_amount").alias("total"))
    result.collect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "tuned"], default="baseline")
    args = parser.parse_args()

    app_name = f"job_missing_broadcast_{args.mode}"
    extra_conf = {"spark.sql.autoBroadcastJoinThreshold": "-1"} if args.mode == "baseline" else {}
    spark = get_spark(app_name, shuffle_partitions=200, extra_conf=extra_conf)
    if args.mode == "baseline":
        run_baseline(spark)
    else:
        run_tuned(spark)
    spark.stop()
    print(f"{app_name} complete.")


if __name__ == "__main__":
    main()
