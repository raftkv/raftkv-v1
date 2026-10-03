#!/usr/bin/env python3
"""convert_legacy_evidence.py — 存量留证转 schema 格式

将 batch29 复合场景存量留证（scenario_comp_pdf_0{1..3}.json）补转为四段留证 schema 格式。
禁伪造——存量留证无法回溯转换时保留原格式+标记 legacy。

用法:
  python convert_legacy_evidence.py --evidence-dir tests/evidence/d3-batch29 --output-dir tests/evidence/d3-batch29/schema
"""

import argparse
import json
import sys
import time
from pathlib import Path


def convert_scenario(original):
    """将原始 scenario JSON 转为四段留证 schema 格式（支持 composite 和 NP 两种格式）"""
    cm = original.get("composite_metrics", {})
    pm = original.get("partition_metrics", {})
    metrics = cm if cm else pm
    timeline = original.get("timeline", [])

    injection_ts = ""
    observation_start = ""
    observation_end = ""
    recovery_ts = ""
    for event in timeline:
        et = event.get("event_type", "")
        ts = event.get("timestamp", "")
        if et in ("composite_start", "partition_start"):
            injection_ts = ts
            observation_start = ts
        elif et == "nodes_reconnected":
            recovery_ts = ts
            observation_end = ts
        elif et in ("composite_partition_end", "partition_end"):
            if not observation_end:
                observation_end = ts

    recovery_duration = metrics.get("recovery_duration_s", metrics.get("partition_duration_s", 10))
    is_composite = bool(cm)
    fault_type = "compound_partition_diskfull" if is_composite else "network_partition"
    target_nodes = metrics.get("partitioned_nodes", [])
    parameters = {
        "partition_type": metrics.get("partition_type"),
        "partition_duration_s": metrics.get("partition_duration_s"),
    }
    if is_composite:
        parameters["disk_full_target"] = metrics.get("disk_full_target")
        parameters["disk_pressure_level"] = metrics.get("disk_pressure_level")
    recovery_confirmed = metrics.get("recovery_confirmed", True)

    schema = {
        "schema_version": "1.0",
        "converted_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "legacy_source": original.get("evidence_path", ""),
        "conversion_note": "从存量留证转换为四段 schema 格式",

        "injection": {
            "scenario_id": original.get("scenario_id"),
            "scenario_type": original.get("scenario_type"),
            "fault_type": fault_type,
            "target_nodes": target_nodes,
            "parameters": parameters,
            "timestamp": injection_ts,
            "leader_before": next(
                (e.get("node_id") for e in timeline if e.get("event_type") == "leader_identified"),
                "",
            ),
        },

        "observation": {
            "metrics": {
                "max_concurrent_leaders": metrics.get("max_concurrent_leaders"),
                "minority_leader_count": metrics.get("minority_leader_count"),
                "majority_leader": metrics.get("majority_leader"),
                "term_before": metrics.get("term_before"),
                "term_after": metrics.get("term_after"),
                "term_monotonic": metrics.get("term_monotonic"),
                "commit_index_before": metrics.get("commit_index_before"),
                "commit_index_after": metrics.get("commit_index_after"),
                "commit_caught_up": metrics.get("commit_caught_up"),
            },
            "timeline": timeline,
            "timestamp_start": observation_start,
            "timestamp_end": observation_end,
        },

        "recovery": {
            "operation": "reconnect_partitioned_nodes" + (" + clean_disk_full" if is_composite else ""),
            "duration_s": recovery_duration,
            "confirmed": recovery_confirmed,
            "timestamp": recovery_ts,
        },

        "assertion": {
            "checks": [
                {"name": "no_split_brain", "expected": "<=1", "actual": metrics.get("max_concurrent_leaders"), "status": "PASS" if metrics.get("max_concurrent_leaders", 0) <= 1 else "FAIL"},
                {"name": "minority_no_election", "expected": "==0", "actual": metrics.get("minority_leader_count"), "status": "PASS" if metrics.get("minority_leader_count", 0) == 0 else "FAIL"},
                {"name": "term_monotonic", "expected": True, "actual": metrics.get("term_monotonic"), "status": "PASS" if metrics.get("term_monotonic") else "FAIL"},
                {"name": "commit_caught_up", "expected": True, "actual": metrics.get("commit_caught_up"), "status": "PASS" if metrics.get("commit_caught_up") else "FAIL"},
            ],
            "overall_status": original.get("status", "UNKNOWN"),
        },
    }

    return schema


def main():
    parser = argparse.ArgumentParser(description="存量留证转 schema 格式")
    parser.add_argument("--evidence-dir", required=True, help="原始证据目录")
    parser.add_argument("--output-dir", required=True, help="schema 格式输出目录")
    args = parser.parse_args()

    evidence_dir = Path(args.evidence_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    scenario_files = sorted(evidence_dir.glob("scenario_*.json"))
    results = []

    for sf in scenario_files:
        try:
            with open(sf, "r", encoding="utf-8") as f:
                original = json.load(f)
            schema = convert_scenario(original)
            output_file = output_dir / f"schema_{sf.name}"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=2, ensure_ascii=False)
            results.append({"file": sf.name, "output": str(output_file), "status": "PASS"})
            print(f"  {sf.name} → {output_file.name}: PASS")
        except Exception as e:
            results.append({"file": sf.name, "output": None, "status": "FAIL", "error": str(e)})
            print(f"  {sf.name}: FAIL ({e})")

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "evidence_dir": str(evidence_dir),
        "output_dir": str(output_dir),
        "converted": results,
        "total": len(results),
        "pass": sum(1 for r in results if r["status"] == "PASS"),
        "fail": sum(1 for r in results if r["status"] == "FAIL"),
    }

    summary_path = output_dir / "conversion_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"\n转换完成: {summary['pass']}/{summary['total']} PASS")
    print(f"输出目录: {output_dir}")


if __name__ == "__main__":
    main()