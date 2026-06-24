"""
datagen/generate.py
--------------------
Generates synthetic datasets with deliberately engineered performance
anti-patterns, so the analyzer has something real to detect:

1. orders.parquet   -- 5M rows, customer_id is heavily skewed (one "hot"
                        customer owns 35% of rows) -> reproduces join/
                        group-by skew.
2. orders_small_files/ -- the same data written with a huge number of
                        partitions -> reproduces the "small file problem".
3. customers.parquet -- 50K rows, small enough to broadcast, used to
                        demonstrate a *missed* broadcast join when joined
                        naively against orders.

This is run once via `python -m datagen.generate` and the parquet files
are reused by every job, so all jobs are analyzing the same underlying
data shape -- only the query/job logic changes.
"""
import os
import random

from pyspark.sql import functions as F

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sparksession import get_spark

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def generate_skewed_orders(spark, n_rows=5_000_000, n_customers=200_000, hot_customer_share=0.35):
    """
    customer_id distribution: one hot customer gets `hot_customer_share`
    of all rows, the remaining rows are spread uniformly across the rest.
    This is the classic real-world skew pattern (a single tenant / VIP
    account / bot account dominating a table) that causes one Spark task
    to process far more data than its peers during a shuffle.
    """
    hot_rows = int(n_rows * hot_customer_share)
    remaining_rows = n_rows - hot_rows

    hot_df = spark.range(hot_rows).withColumn("customer_id", F.lit(1))
    cold_df = (
        spark.range(remaining_rows)
        .withColumn("customer_id", (F.rand(seed=42) * (n_customers - 2) + 2).cast("long"))
    )
    orders = hot_df.unionByName(cold_df)

    orders = (
        orders.withColumnRenamed("id", "order_id")
        .withColumn("order_amount", F.round(F.rand(seed=7) * 1000, 2))
        .withColumn("order_ts", F.current_timestamp())
        .withColumn("region", (F.rand(seed=3) * 5).cast("int"))
    )
    return orders


def generate_customers(spark, n_customers=200_000):
    return (
        spark.range(n_customers)
        .withColumnRenamed("id", "customer_id")
        .withColumn("customer_name", F.concat(F.lit("customer_"), F.col("customer_id")))
        .withColumn("signup_region", (F.rand(seed=11) * 5).cast("int"))
    )


def generate_join_skew_tables(spark):
    """
    Dedicated, deliberately small dataset for the join-skew demo --
    NOT the same as orders.parquet/customers.parquet used elsewhere.

    Why separate: an equi-join's output size for a given key is
    (matches on side A) x (matches on side B). If we reused the main
    5M-row orders dataset (1.75M rows for the hot key) and joined it
    against another similarly-sized fact table, the hot key alone
    would produce well over a billion output rows -- a real
    consequence of join skew in production, but not something that
    finishes in a reasonable time on a single local JVM for a demo.

    Sizing here keeps the hot key's matched-pair count in the low
    millions (visibly dominant vs. cold keys, which only produce a
    handful of pairs each) while the whole job still finishes in
    seconds:
      - orders_join_fact: ~23K rows, hot customer_id=1 owns 3,000 of them
      - events_join_fact: ~6K rows, hot customer_id=1 owns 1,000 of them
      - hot key matched pairs: 3,000 x 1,000 = 3,000,000
      - an average cold key: ~10 orders x ~2.5 events = ~25 pairs

    Cold customer pool deliberately kept small (2,000 ids, not 20,000+):
    with 100 shuffle partitions, a large cold pool spreads ~200 cold
    customers into the *same* bucket as the hot key, and their combined
    volume dilutes the skew ratio enough to mask it (verified this
    empirically -- a 20K-customer pool produced only ~2x skew despite
    the hot key being 1000x any single cold key). A 2,000-customer pool
    means each bucket holds ~20 cold customers, whose combined volume
    stays well below the hot key's, so the hot bucket's true dominance
    actually shows up in shuffle-read bytes.
    """
    n_customers = 2_002  # ids 2..2001 are "cold", id 1 is hot

    hot_orders = spark.range(3_000).withColumn("customer_id", F.lit(1))
    cold_orders = (
        spark.range(20_000)
        .withColumn("customer_id", (F.rand(seed=21) * (n_customers - 2) + 2).cast("long"))
    )
    orders_join_fact = (
        hot_orders.unionByName(cold_orders)
        .withColumnRenamed("id", "order_id")
        .withColumn("order_amount", F.round(F.rand(seed=22) * 1000, 2))
    )

    hot_events = spark.range(1_000).withColumn("customer_id", F.lit(1))
    cold_events = (
        spark.range(5_000)
        .withColumn("customer_id", (F.rand(seed=23) * (n_customers - 2) + 2).cast("long"))
    )
    events_join_fact = (
        hot_events.unionByName(cold_events)
        .withColumnRenamed("id", "event_id")
        .withColumn("event_type", (F.rand(seed=24) * 4).cast("int"))
    )

    orders_path = os.path.join(DATA_DIR, "orders_join_fact.parquet")
    events_path = os.path.join(DATA_DIR, "events_join_fact.parquet")
    orders_join_fact.repartition(8).write.mode("overwrite").parquet(orders_path)
    events_join_fact.repartition(8).write.mode("overwrite").parquet(events_path)
    print(f"  wrote {orders_path}")
    print(f"  wrote {events_path}")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    spark = get_spark("datagen", shuffle_partitions=64)

    print("Generating skewed orders dataset...")
    orders = generate_skewed_orders(spark)

    # Write 1: normal partition count -> used by most jobs
    orders_path = os.path.join(DATA_DIR, "orders.parquet")
    orders.repartition(64).write.mode("overwrite").parquet(orders_path)
    print(f"  wrote {orders_path}")

    # Write 2: a smaller, separate slice written with a huge number of
    # partitions -> small-file anti-pattern. Deliberately a smaller row
    # count than the main orders dataset: this job forces one Spark
    # task per file to make the overhead visible, and at 5M rows /
    # 2000 files that means 2000 serialized task launches, which is
    # unnecessarily slow for a demo without adding any insight. 300
    # files over 300K rows reproduces the same pattern (many tiny
    # files, large task-count-to-data-ratio) in a fraction of the time.
    small_files_rows = generate_skewed_orders(spark, n_rows=300_000, n_customers=20_000)
    small_files_path = os.path.join(DATA_DIR, "orders_small_files.parquet")
    small_files_rows.repartition(300).write.mode("overwrite").parquet(small_files_path)
    print(f"  wrote {small_files_path} (small-file anti-pattern, 300 files)")

    print("Generating customers dataset...")
    customers = generate_customers(spark)
    customers_path = os.path.join(DATA_DIR, "customers.parquet")
    customers.repartition(8).write.mode("overwrite").parquet(customers_path)
    print(f"  wrote {customers_path}")

    print("Generating join-skew demo tables...")
    generate_join_skew_tables(spark)

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
