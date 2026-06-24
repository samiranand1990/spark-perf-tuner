"""
sparksession.py
----------------
Central factory for SparkSessions used across all demo jobs.

Why this exists:
Every job in this repo must emit a Spark event log, because the analyzer
package consumes those logs (not live listener state) to compute metrics.
Centralizing the builder means every job gets identical, comparable
instrumentation -- same log dir, same serializer, same UI retention --
which matters when we diff a "baseline" run against a "tuned" run.

Usage:
    from sparksession import get_spark
    spark = get_spark(app_name="skewed_join_baseline", shuffle_partitions=200)
"""
import os
from pyspark.sql import SparkSession

EVENT_LOG_DIR = os.path.join(os.path.dirname(__file__), "event_logs")
os.makedirs(EVENT_LOG_DIR, exist_ok=True)


def get_spark(app_name: str, shuffle_partitions: int = 200, extra_conf: dict | None = None) -> SparkSession:
    """
    Build a local SparkSession with event logging turned on so the run
    can be analyzed offline by analyzer/event_log_parser.py.

    Args:
        app_name: becomes part of the event log filename -> ties a log
                   file back to the job that produced it.
        shuffle_partitions: spark.sql.shuffle.partitions. Deliberately
                   exposed as a parameter (not hardcoded) because the
                   auto-tuner's whole job is to recommend a better value
                   for this and re-run with it.
        extra_conf: any additional spark.conf overrides a job needs
                   (e.g. disabling AQE to deliberately reproduce skew
                   for the demo, or setting broadcast threshold to -1
                   to force a shuffle join).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{EVENT_LOG_DIR}")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.enabled", "false")
    )
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def latest_event_log_for(app_name_prefix: str) -> str:
    """
    Spark names event log files after the application ID, not the app
    name, so jobs call this helper right after spark.stop() to find the
    log they just produced (matched by mtime + the app name recorded
    inside the log's first line).
    """
    import glob
    candidates = []
    for path in glob.glob(os.path.join(EVENT_LOG_DIR, "*")):
        if os.path.isdir(path):
            continue
        candidates.append(path)
    candidates.sort(key=os.path.getmtime, reverse=True)
    for path in candidates:
        with open(path, "r") as f:
            head = "".join(f.readline() for _ in range(10))
            if f'"App Name":"{app_name_prefix}"' in head or app_name_prefix in head:
                return path
    raise FileNotFoundError(f"No event log found for app prefix '{app_name_prefix}'")
