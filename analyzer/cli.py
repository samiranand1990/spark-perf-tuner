"""
analyzer/cli.py
-----------------
Command-line entry point for the analyzer.

Usage:
    # Analyze one job's event log
    python -m analyzer.cli analyze --app-name job_skewed_join_baseline

    # Compare a baseline/tuned pair and write a markdown report
    python -m analyzer.cli compare --baseline job_skewed_join_baseline \\
        --tuned job_skewed_join_tuned --label "Join skew" \\
        --out reports/skewed_join.md

    # Run the full demo suite (all 4 job pairs) and write all reports
    python -m analyzer.cli demo
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyzer.event_log_parser import find_run_by_app_name
from analyzer.report import render_comparison_report, render_single_run_report

EVENT_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "event_logs")
REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")

JOB_PAIRS = [
    ("job_skewed_join_baseline", "job_skewed_join_tuned", "Skewed join (hot customer key)"),
    ("job_small_files_baseline", "job_small_files_tuned", "Small-file problem"),
    ("job_missing_broadcast_baseline", "job_missing_broadcast_tuned", "Missing broadcast join"),
    ("job_missing_cache_baseline", "job_missing_cache_tuned", "Missing cache on reused DataFrame"),
]


def cmd_analyze(args):
    run = find_run_by_app_name(EVENT_LOG_DIR, args.app_name)
    report = render_single_run_report(run)
    print(report)
    if args.out:
        with open(args.out, "w") as f:
            f.write(report)
        print(f"\n[written to {args.out}]", file=sys.stderr)


def cmd_compare(args):
    baseline = find_run_by_app_name(EVENT_LOG_DIR, args.baseline)
    tuned = find_run_by_app_name(EVENT_LOG_DIR, args.tuned)
    report = render_comparison_report(baseline, tuned, args.label or args.baseline)
    print(report)
    if args.out:
        with open(args.out, "w") as f:
            f.write(report)
        print(f"\n[written to {args.out}]", file=sys.stderr)


def cmd_demo(args):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    summary_lines = ["# Spark Job Performance Analyzer — demo suite results", ""]
    for baseline_name, tuned_name, label in JOB_PAIRS:
        try:
            baseline = find_run_by_app_name(EVENT_LOG_DIR, baseline_name)
            tuned = find_run_by_app_name(EVENT_LOG_DIR, tuned_name)
        except FileNotFoundError as e:
            summary_lines.append(f"## {label}\n_skipped: {e}_\n")
            continue
        report = render_comparison_report(baseline, tuned, label)
        out_path = os.path.join(REPORTS_DIR, f"{baseline_name.replace('_baseline', '')}.md")
        with open(out_path, "w") as f:
            f.write(report)
        print(f"wrote {out_path}")
        summary_lines.append(f"## {label}")
        summary_lines.append(f"See `{os.path.basename(out_path)}`")
        summary_lines.append("")

    summary_path = os.path.join(REPORTS_DIR, "SUMMARY.md")
    with open(summary_path, "w") as f:
        f.write("\n".join(summary_lines))
    print(f"wrote {summary_path}")


def main():
    parser = argparse.ArgumentParser(description="Spark Job Performance Analyzer")
    sub = parser.add_subparsers(dest="command", required=True)

    p_analyze = sub.add_parser("analyze", help="Analyze a single job's event log")
    p_analyze.add_argument("--app-name", required=True)
    p_analyze.add_argument("--out", default=None)
    p_analyze.set_defaults(func=cmd_analyze)

    p_compare = sub.add_parser("compare", help="Compare a baseline/tuned pair")
    p_compare.add_argument("--baseline", required=True)
    p_compare.add_argument("--tuned", required=True)
    p_compare.add_argument("--label", default=None)
    p_compare.add_argument("--out", default=None)
    p_compare.set_defaults(func=cmd_compare)

    p_demo = sub.add_parser("demo", help="Run the full demo suite of job pairs")
    p_demo.set_defaults(func=cmd_demo)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
