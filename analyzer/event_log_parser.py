"""
analyzer/event_log_parser.py
------------------------------
Parses a Spark JSON event log (the file Spark writes when
spark.eventLog.enabled=true) into a structured AppRun object.

This deliberately works from event logs rather than a live
SparkListener for one practical reason: it decouples diagnosis from
the run itself. A platform team's tuning tool needs to analyze
yesterday's failed/slow job from history-server logs just as often as
a live one, and any logic written against the log format works in
both cases (live listener events and on-disk event logs are the same
JSON schema). Building on the listener API directly would only cover
the live case.

We do NOT regex the file. Every line is one JSON object; we parse it
with json.loads and pull fields by name, because event schemas vary
slightly across Spark versions and minor-version field additions are
extremely common -- robustness here means tolerating missing keys
(via .get with defaults), not pattern-matching the file as text.
"""
from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TaskRecord:
    stage_id: int
    task_id: int
    duration_ms: int
    executor_run_time_ms: int
    executor_deserialize_time_ms: int
    result_serialization_time_ms: int
    gc_time_ms: int
    memory_bytes_spilled: int
    disk_bytes_spilled: int
    shuffle_read_bytes: int
    shuffle_read_records: int
    shuffle_write_bytes: int
    input_bytes: int
    failed: bool


@dataclass
class StageRecord:
    stage_id: int
    name: str
    num_tasks: int = 0
    submission_time_ms: Optional[int] = None
    completion_time_ms: Optional[int] = None
    tasks: list[TaskRecord] = field(default_factory=list)

    @property
    def duration_ms(self) -> Optional[int]:
        if self.submission_time_ms is None or self.completion_time_ms is None:
            return None
        return self.completion_time_ms - self.submission_time_ms

    @property
    def total_shuffle_read_bytes(self) -> int:
        return sum(t.shuffle_read_bytes for t in self.tasks)

    @property
    def total_shuffle_write_bytes(self) -> int:
        return sum(t.shuffle_write_bytes for t in self.tasks)

    @property
    def total_memory_spilled(self) -> int:
        return sum(t.memory_bytes_spilled for t in self.tasks)

    @property
    def total_disk_spilled(self) -> int:
        return sum(t.disk_bytes_spilled for t in self.tasks)

    @property
    def total_gc_time_ms(self) -> int:
        return sum(t.gc_time_ms for t in self.tasks)

    @property
    def total_executor_run_time_ms(self) -> int:
        return sum(t.executor_run_time_ms for t in self.tasks)

    @property
    def max_task_run_time_ms(self) -> int:
        return max((t.executor_run_time_ms for t in self.tasks), default=0)

    @property
    def total_input_bytes(self) -> int:
        return sum(t.input_bytes for t in self.tasks)

    def shuffle_read_skew_ratio(self) -> Optional[float]:
        """
        max(per-task shuffle read bytes) / median(per-task shuffle read
        bytes) across tasks that actually read shuffle data. This is
        the primary skew signal: a hot partition key shows up as one
        task reading dramatically more shuffle bytes than its peers,
        regardless of cluster size (it manifests even in local mode,
        since hash partitioning still routes the hot key's rows into a
        single shuffle partition).
        """
        reads = [t.shuffle_read_bytes for t in self.tasks if t.shuffle_read_bytes > 0]
        if len(reads) < 2:
            return None
        median = statistics.median(reads)
        if median == 0:
            return None
        return max(reads) / median

    def task_duration_skew_ratio(self) -> Optional[float]:
        durations = [t.executor_run_time_ms for t in self.tasks if t.executor_run_time_ms > 0]
        if len(durations) < 2:
            return None
        median = statistics.median(durations)
        if median == 0:
            return None
        return max(durations) / median


@dataclass
class SqlExecution:
    execution_id: int
    description: str
    physical_plan: str
    duration_ms: Optional[int] = None


@dataclass
class AppRun:
    app_name: str
    app_id: str
    log_path: str
    stages: dict[int, StageRecord] = field(default_factory=dict)
    sql_executions: list[SqlExecution] = field(default_factory=list)

    @property
    def total_duration_ms(self) -> int:
        return sum(s.duration_ms or 0 for s in self.stages.values())

    @property
    def total_shuffle_read_bytes(self) -> int:
        return sum(s.total_shuffle_read_bytes for s in self.stages.values())

    @property
    def total_shuffle_write_bytes(self) -> int:
        return sum(s.total_shuffle_write_bytes for s in self.stages.values())

    @property
    def total_memory_spilled(self) -> int:
        return sum(s.total_memory_spilled for s in self.stages.values())

    @property
    def total_disk_spilled(self) -> int:
        return sum(s.total_disk_spilled for s in self.stages.values())

    @property
    def total_tasks(self) -> int:
        return sum(s.num_tasks for s in self.stages.values())

    @property
    def total_gc_time_ms(self) -> int:
        return sum(s.total_gc_time_ms for s in self.stages.values())

    @property
    def total_executor_run_time_ms(self) -> int:
        return sum(s.total_executor_run_time_ms for s in self.stages.values())

    @property
    def max_task_run_time_ms(self) -> int:
        """
        The single longest-running task across the whole app -- i.e. the
        straggler. This is the metric that actually reflects wall-clock
        impact for a skew fix: salting can *increase* total summed
        executor time across all tasks (replication overhead means more
        total work happens), while still *decreasing* wall-clock job
        time, because the job's critical path is bounded by its single
        slowest task, not the sum of all task durations. Reporting only
        the sum without this would make a correct skew fix look like a
        regression.
        """
        return max((s.max_task_run_time_ms for s in self.stages.values()), default=0)

    def worst_skew_ratio(self) -> Optional[float]:
        ratios = [s.shuffle_read_skew_ratio() for s in self.stages.values()]
        ratios = [r for r in ratios if r is not None]
        return max(ratios) if ratios else None

    def combined_physical_plans(self) -> str:
        return "\n---\n".join(e.physical_plan for e in self.sql_executions)


def _get_metric(task_metrics: dict, *path, default=0):
    node = task_metrics
    for key in path:
        if not isinstance(node, dict):
            return default
        node = node.get(key, default)
    return node if node is not None else default


def parse_event_log(path: str) -> AppRun:
    """
    Stream the event log line by line (these files can be large; we
    never load the whole thing as one JSON blob) and build up an
    AppRun. Unknown event types are ignored by design -- we only react
    to the handful of event types we need, so the parser doesn't break
    when Spark adds new event types in future versions.
    """
    app_name = None
    app_id = None
    stages: dict[int, StageRecord] = {}
    sql_executions: list[SqlExecution] = []
    sql_start_times: dict[int, int] = {}

    with open(path, "r") as f:
        for raw_line in f:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                event = json.loads(raw_line)
            except json.JSONDecodeError:
                continue  # tolerate a truncated last line in .inprogress logs

            etype = event.get("Event")

            if etype == "SparkListenerApplicationStart":
                app_name = event.get("App Name")
                app_id = event.get("App ID")

            elif etype == "SparkListenerStageSubmitted":
                info = event.get("Stage Info", {})
                sid = info.get("Stage ID")
                if sid is not None:
                    rec = stages.setdefault(sid, StageRecord(stage_id=sid, name=info.get("Stage Name", "")))
                    rec.submission_time_ms = info.get("Submission Time")

            elif etype == "SparkListenerStageCompleted":
                info = event.get("Stage Info", {})
                sid = info.get("Stage ID")
                if sid is not None:
                    rec = stages.setdefault(sid, StageRecord(stage_id=sid, name=info.get("Stage Name", "")))
                    rec.completion_time_ms = info.get("Completion Time")
                    if rec.submission_time_ms is None:
                        rec.submission_time_ms = info.get("Submission Time")

            elif etype == "SparkListenerTaskEnd":
                sid = event.get("Stage ID")
                if sid is None:
                    continue
                rec = stages.setdefault(sid, StageRecord(stage_id=sid, name=""))
                tm = event.get("Task Metrics", {}) or {}
                info = event.get("Task Info", {}) or {}
                reason = event.get("Task End Reason", {}) or {}

                launch = info.get("Launch Time", 0)
                finish = info.get("Finish Time", 0)
                task = TaskRecord(
                    stage_id=sid,
                    task_id=info.get("Task ID", -1),
                    duration_ms=max(finish - launch, 0),
                    executor_run_time_ms=_get_metric(tm, "Executor Run Time"),
                    executor_deserialize_time_ms=_get_metric(tm, "Executor Deserialize Time"),
                    result_serialization_time_ms=_get_metric(tm, "Result Serialization Time"),
                    gc_time_ms=_get_metric(tm, "JVM GC Time"),
                    memory_bytes_spilled=_get_metric(tm, "Memory Bytes Spilled"),
                    disk_bytes_spilled=_get_metric(tm, "Disk Bytes Spilled"),
                    shuffle_read_bytes=(
                        _get_metric(tm, "Shuffle Read Metrics", "Remote Bytes Read")
                        + _get_metric(tm, "Shuffle Read Metrics", "Local Bytes Read")
                    ),
                    shuffle_read_records=_get_metric(tm, "Shuffle Read Metrics", "Total Records Read"),
                    shuffle_write_bytes=_get_metric(tm, "Shuffle Write Metrics", "Shuffle Bytes Written"),
                    input_bytes=_get_metric(tm, "Input Metrics", "Bytes Read"),
                    failed=reason.get("Reason") != "Success",
                )
                rec.tasks.append(task)
                rec.num_tasks += 1

            elif etype == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                exec_id = event.get("executionId")
                sql_start_times[exec_id] = event.get("time")
                sql_executions.append(
                    SqlExecution(
                        execution_id=exec_id,
                        description=event.get("description", ""),
                        physical_plan=event.get("physicalPlanDescription", ""),
                    )
                )

            elif etype == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                exec_id = event.get("executionId")
                start = sql_start_times.get(exec_id)
                end = event.get("time")
                if start is not None and end is not None:
                    for e in sql_executions:
                        if e.execution_id == exec_id:
                            e.duration_ms = end - start
                            break

    return AppRun(
        app_name=app_name or "unknown",
        app_id=app_id or "unknown",
        log_path=path,
        stages=stages,
        sql_executions=sql_executions,
    )


def find_run_by_app_name(event_log_dir: str, app_name: str) -> AppRun:
    """Scan a directory of event logs and return the AppRun matching app_name exactly."""
    import glob
    import os

    for path in sorted(glob.glob(os.path.join(event_log_dir, "*"))):
        if os.path.isdir(path) or path.endswith(".inprogress"):
            continue
        with open(path, "r") as f:
            # SparkListenerApplicationStart usually isn't the first line
            # (LogStart, ResourceProfileAdded etc. precede it) -- scan a
            # small window rather than assuming line 1.
            head = "".join(f.readline() for _ in range(10))
        if f'"App Name":"{app_name}"' in head:
            return parse_event_log(path)
    raise FileNotFoundError(f"No event log found with app name '{app_name}' in {event_log_dir}")
