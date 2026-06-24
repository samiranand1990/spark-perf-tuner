"""
analyzer/detectors.py
-----------------------
Rule-based detectors that turn a parsed AppRun into a list of Finding
objects. Each detector is independent and stateless -- it only reads
AppRun/StageRecord data, so adding a new detector means adding one
function and registering it in ALL_DETECTORS, without touching parsing
or reporting code.

Every threshold is a named constant with a one-line justification next
to it (not because the numbers are sacred, but because in a real
review the first question is always "why this number," and "default
Spark config docs" or "empirically observed in our demo data" is a
better answer than silence).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from analyzer.event_log_parser import AppRun

# Thresholds
SKEW_RATIO_THRESHOLD = 3.0          # a task doing 3x the median work is a real straggler risk
SKEW_MIN_TASKS = 8                  # below this, "skew" is just normal task-time noise
SMALL_FILE_AVG_BYTES_THRESHOLD = 5 * 1024 * 1024   # Spark's own default openCostInBytes is 4MB
SMALL_FILE_MIN_TASKS = 20
SPILL_BYTES_THRESHOLD = 1            # any spill at all is worth surfacing; severity scales with size
SERIALIZATION_OVERHEAD_RATIO_THRESHOLD = 0.25  # >25% of run time in (de)serialization is high


@dataclass
class Finding:
    category: str            # e.g. "skew", "missing_broadcast", "small_files", "missing_cache", "spill"
    severity: str             # "high" | "medium" | "low"
    stage_id: Optional[int]
    message: str
    evidence: dict = field(default_factory=dict)


def detect_skew(run: AppRun) -> list[Finding]:
    findings = []
    for sid, stage in sorted(run.stages.items()):
        if stage.num_tasks < SKEW_MIN_TASKS:
            continue
        ratio = stage.shuffle_read_skew_ratio()
        if ratio is None or ratio < SKEW_RATIO_THRESHOLD:
            continue
        severity = "high" if ratio >= 10 else "medium"
        findings.append(
            Finding(
                category="skew",
                severity=severity,
                stage_id=sid,
                message=(
                    f"Stage {sid}: one task reads {ratio:.1f}x the median shuffle-read "
                    f"volume of its {stage.num_tasks} peers. This is consistent with a "
                    f"hot key dominating a single hash partition during a join or "
                    f"groupBy shuffle."
                ),
                evidence={
                    "skew_ratio": round(ratio, 2),
                    "num_tasks": stage.num_tasks,
                    "total_shuffle_read_bytes": stage.total_shuffle_read_bytes,
                },
            )
        )
    return findings


def detect_small_files(run: AppRun) -> list[Finding]:
    findings = []
    for sid, stage in sorted(run.stages.items()):
        if stage.num_tasks < SMALL_FILE_MIN_TASKS:
            continue
        if stage.total_input_bytes <= 0:
            continue
        avg_bytes = stage.total_input_bytes / stage.num_tasks
        if avg_bytes >= SMALL_FILE_AVG_BYTES_THRESHOLD:
            continue
        findings.append(
            Finding(
                category="small_files",
                severity="medium",
                stage_id=sid,
                message=(
                    f"Stage {sid}: {stage.num_tasks} tasks averaging "
                    f"{avg_bytes / 1024:.1f} KB of input each. Per-task overhead "
                    f"(scheduling, file open/footer reads) is likely dominating actual "
                    f"compute time -- classic small-file problem."
                ),
                evidence={
                    "num_tasks": stage.num_tasks,
                    "avg_input_bytes_per_task": round(avg_bytes, 1),
                    "total_input_bytes": stage.total_input_bytes,
                },
            )
        )
    return findings


def detect_missing_broadcast(run: AppRun) -> list[Finding]:
    findings = []
    for execu in run.sql_executions:
        plan = execu.physical_plan
        if "SortMergeJoin" in plan and "BroadcastHashJoin" not in plan:
            findings.append(
                Finding(
                    category="missing_broadcast",
                    severity="high",
                    stage_id=None,
                    message=(
                        f"SQL execution {execu.execution_id}: plan uses SortMergeJoin "
                        f"with no BroadcastHashJoin anywhere in the query. If either "
                        f"join side is small enough to fit in executor memory "
                        f"(roughly under spark.sql.autoBroadcastJoinThreshold, default "
                        f"10MB), this join is paying for a full shuffle it doesn't need."
                    ),
                    evidence={"execution_id": execu.execution_id},
                )
            )
    return findings


def detect_missing_cache(run: AppRun) -> list[Finding]:
    """
    Heuristic: if 2+ SQL executions in the same application independently
    scan a parquet source, and NONE of the app's executions show an
    InMemoryTableScan/InMemoryRelation anywhere, that's consistent with
    the same upstream DataFrame being recomputed from scratch on every
    action -- i.e. a derived frame that should have been .cache()'d but
    wasn't. This is necessarily a heuristic (the parser can't see the
    Python-level DataFrame variable that's being reused), but the
    physical-plan signal is strong: real caching always leaves an
    InMemoryTableScan node in the plan of every execution that reuses it.
    """
    scanning_executions = [e for e in run.sql_executions if "Scan parquet" in e.physical_plan]
    any_cached = any(
        "InMemoryTableScan" in e.physical_plan or "InMemoryRelation" in e.physical_plan
        for e in run.sql_executions
    )
    if len(scanning_executions) >= 2 and not any_cached:
        return [
            Finding(
                category="missing_cache",
                severity="medium",
                stage_id=None,
                message=(
                    f"{len(scanning_executions)} separate SQL executions in this "
                    f"application independently scan a parquet source, and none of "
                    f"them show an InMemoryTableScan -- no caching is happening "
                    f"anywhere in the app. If these executions share an upstream "
                    f"derived DataFrame (a join, aggregation, or expensive column "
                    f"derivation reused across multiple actions), it is being "
                    f"recomputed from scratch each time."
                ),
                evidence={"num_scanning_executions": len(scanning_executions)},
            )
        ]
    return []


def detect_spill(run: AppRun) -> list[Finding]:
    findings = []
    for sid, stage in sorted(run.stages.items()):
        spill = stage.total_memory_spilled + stage.total_disk_spilled
        if spill <= SPILL_BYTES_THRESHOLD:
            continue
        severity = "high" if stage.total_disk_spilled > 0 else "medium"
        findings.append(
            Finding(
                category="spill",
                severity=severity,
                stage_id=sid,
                message=(
                    f"Stage {sid}: {spill / 1e6:.1f} MB spilled "
                    f"({stage.total_memory_spilled / 1e6:.1f} MB memory, "
                    f"{stage.total_disk_spilled / 1e6:.1f} MB disk). Task working set "
                    f"exceeded available execution memory; data was written to disk "
                    f"mid-shuffle/aggregation."
                ),
                evidence={
                    "memory_spilled_bytes": stage.total_memory_spilled,
                    "disk_spilled_bytes": stage.total_disk_spilled,
                },
            )
        )
    return findings


def detect_serialization_overhead(run: AppRun) -> list[Finding]:
    findings = []
    for sid, stage in sorted(run.stages.items()):
        if stage.num_tasks < SKEW_MIN_TASKS:
            continue
        run_time = stage.total_executor_run_time_ms
        if run_time <= 0:
            continue
        overhead = sum(
            t.executor_deserialize_time_ms + t.result_serialization_time_ms for t in stage.tasks
        )
        ratio = overhead / run_time
        if ratio < SERIALIZATION_OVERHEAD_RATIO_THRESHOLD:
            continue
        findings.append(
            Finding(
                category="serialization_overhead",
                severity="low",
                stage_id=sid,
                message=(
                    f"Stage {sid}: (de)serialization time is {ratio * 100:.0f}% of "
                    f"executor run time. Worth checking the serializer in use "
                    f"(Kryo vs. default Java serialization) and whether wide/nested "
                    f"row schemas can be narrowed before this stage."
                ),
                evidence={"overhead_ratio": round(ratio, 3)},
            )
        )
    return findings


ALL_DETECTORS = [
    detect_skew,
    detect_small_files,
    detect_missing_broadcast,
    detect_missing_cache,
    detect_spill,
    detect_serialization_overhead,
]


def run_all_detectors(run: AppRun) -> list[Finding]:
    findings: list[Finding] = []
    for detector in ALL_DETECTORS:
        findings.extend(detector(run))
    return findings
