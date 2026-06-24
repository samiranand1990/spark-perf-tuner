"""
analyzer/tuner.py
-------------------
Maps Finding categories to concrete recommendations: a short title, the
specific Spark conf or code change, and the rationale. This is kept
separate from detectors.py on purpose -- detection ("is this happening")
and prescription ("what do you do about it") are different concerns
with different failure modes. A detector that's slightly miscalibrated
just produces a false positive; a recommendation that's wrong gives
someone bad advice they might actually run in production. Keeping them
separate also means the same finding can map to more than one viable
recommendation (e.g. skew can be fixed by salting OR by enabling AQE
skew-join, and which one applies depends on whether it's a join or an
aggregation -- a distinction this module makes explicit rather than
papering over).
"""
from __future__ import annotations

from dataclasses import dataclass

from analyzer.detectors import Finding


@dataclass
class Recommendation:
    category: str
    title: str
    detail: str
    spark_conf: dict[str, str] | None = None
    code_change: str | None = None


_RECOMMENDATIONS: dict[str, Recommendation] = {
    "skew": Recommendation(
        category="skew",
        title="Salt the hot key before shuffling",
        detail=(
            "For a JOIN: salt one side with a random bucket in [0, N) and explode "
            "the other side into N copies (one per bucket), then join on "
            "(original_key, salt). This spreads every key's contribution -- "
            "including the hot one -- across N partitions instead of one, at the "
            "cost of N-x row replication on the exploded side. For a GROUP BY: "
            "note that Spark's map-side partial aggregation already collapses a "
            "hot key to one row per map task before the shuffle, so simple "
            "sum/count aggregates rarely need this fix -- check whether AQE "
            "(spark.sql.adaptive.skewJoin.enabled, on by default in Spark 3.x) is "
            "already handling it before reaching for manual salting."
        ),
        spark_conf={"spark.sql.adaptive.skewJoin.enabled": "true"},
        code_change=(
            "df_a.withColumn('salt', (rand()*N).cast('int'))\n"
            "  .join(df_b.withColumn('salt', explode(array([lit(i) for i in range(N)]))),\n"
            "        on=['key', 'salt'])"
        ),
    ),
    "small_files": Recommendation(
        category="small_files",
        title="Coalesce small input partitions after read",
        detail=(
            "coalesce(N) immediately after read merges narrow partitions without "
            "a shuffle (it only reduces partition count, unlike repartition()). "
            "Target N using the standard rule of thumb: total data size / 128MB. "
            "If this dataset is written by an upstream job you control, fix it at "
            "the source too -- writing with .coalesce(N) before .write() prevents "
            "every downstream consumer from hitting the same problem."
        ),
        spark_conf={"spark.sql.files.maxPartitionBytes": "134217728"},
        code_change="df = spark.read.parquet(path).coalesce(target_partitions)",
    ),
    "missing_broadcast": Recommendation(
        category="missing_broadcast",
        title="Force a broadcast hint on the small join side",
        detail=(
            "Spark's automatic broadcast decision relies on statistics it can "
            "size at plan time; that estimate is frequently wrong after several "
            "DataFrame transformations (filters, projections, prior joins). An "
            "explicit broadcast() hint removes the guesswork. Confirm the small "
            "side's actual in-memory size first -- broadcasting something larger "
            "than spark.sql.autoBroadcastJoinThreshold defeats the purpose and can "
            "OOM the driver collecting it."
        ),
        spark_conf={"spark.sql.autoBroadcastJoinThreshold": "104857600"},
        code_change="big_df.join(F.broadcast(small_df), on='key')",
    ),
    "missing_cache": Recommendation(
        category="missing_cache",
        title="Cache the shared upstream DataFrame once",
        detail=(
            "Spark's DAG is lazy: every action against a derived DataFrame "
            "re-triggers the full upstream computation unless something "
            "persists it. If a join/aggregation/expensive-column-derivation feeds "
            "more than one downstream action, .cache() it once and materialize "
            "with a cheap action (e.g. .count()) before the real downstream work "
            "runs, then .unpersist() when done with it."
        ),
        spark_conf=None,
        code_change="df = expensive_transform(...).cache()\ndf.count()  # materialize\n# ... reuse df in multiple actions ...\ndf.unpersist()",
    ),
    "spill": Recommendation(
        category="spill",
        title="Increase execution memory or reduce partition size",
        detail=(
            "Spill means a task's working set exceeded its share of execution "
            "memory and Spark fell back to disk. Two independent levers: "
            "increase spark.executor.memory (or the execution/storage memory "
            "fraction), or reduce the amount of data each task handles by "
            "increasing spark.sql.shuffle.partitions so each partition is "
            "smaller. Check which lever applies by looking at whether the spill "
            "is concentrated in a skewed partition (fix the skew first) or spread "
            "evenly (genuinely under-provisioned memory)."
        ),
        spark_conf={"spark.sql.shuffle.partitions": "<increase from current value>"},
        code_change=None,
    ),
    "serialization_overhead": Recommendation(
        category="serialization_overhead",
        title="Switch to Kryo serialization and narrow row schemas",
        detail=(
            "Confirm spark.serializer is set to KryoSerializer (Java serialization "
            "is Spark's default and is noticeably slower for the same data). If "
            "Kryo is already in use, the next lever is reducing what's being "
            "serialized -- drop unused columns earlier in the pipeline with "
            "select() before a shuffle, since every shuffled column pays this "
            "cost on every task."
        ),
        spark_conf={"spark.serializer": "org.apache.spark.serializer.KryoSerializer"},
        code_change=None,
    ),
}


def recommend_for(finding: Finding) -> Recommendation:
    return _RECOMMENDATIONS[finding.category]


def recommendations_for_findings(findings: list[Finding]) -> list[Recommendation]:
    """Deduplicated, in first-seen order -- a stage with 5 skew findings still gets 1 recommendation."""
    seen = set()
    out = []
    for f in findings:
        if f.category in seen:
            continue
        seen.add(f.category)
        out.append(recommend_for(f))
    return out
