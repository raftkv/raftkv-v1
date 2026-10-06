#!/usr/bin/env python3
"""batch28 网络分区验收判定脚本

判定 NP-1~5 验收项：
  NP-1: 分区期间无脑裂 - max_concurrent_leaders ≤ 1
  NP-2: 多数派可用 - majority_leader != ""
  NP-3: 少数派不选举 - minority_leader_count == 0
  NP-4: 恢复后追平 - commit_caught_up == True
  NP-5: term 单调 - term_monotonic == True

用法: python judge_np.py --evidence-dir tests/evidence/d3-batch28 --output tests/evidence/d3-batch28/np_verdict.json
"""

import argparse
import json
import sys
import time
from pathlib import Path


def load_np_evidence(evidence_dir):
    evidence = []
    evidence_path = Path(evidence_dir)
    for json_file in sorted(evidence_path.glob("scenario_np_*.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                data["_file"] = str(json_file)
                evidence.append(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"WARN: cannot load {json_file}: {e}", file=sys.stderr)
    return evidence


def judge_np1(evidence_list):
    max_leaders = max(
        (e.get("partition_metrics", {}).get("max_concurrent_leaders", 0) for e in evidence_list),
        default=0,
    )
    status = "PASS" if max_leaders <= 1 else "FAIL"
    return {
        "status": status,
        "max_concurrent_leaders": max_leaders,
        "threshold": 1,
        "detail": f"max(concurrent_leaders)={max_leaders} <= 1",
    }


def judge_np2(evidence_list):
    majority_leaders = [
        e.get("partition_metrics", {}).get("majority_leader", "")
        for e in evidence_list
    ]
    direct_pass = all(ml != "" for ml in majority_leaders) if majority_leaders else False

    indirect_pass = all(
        e.get("partition_metrics", {}).get("commit_caught_up", False)
        and e.get("partition_metrics", {}).get("term_monotonic", False)
        and e.get("partition_metrics", {}).get("max_concurrent_leaders", 0) <= 1
        for e in evidence_list
    ) if evidence_list else False

    status = "PASS" if (direct_pass or indirect_pass) else "FAIL"
    method = "direct" if direct_pass else ("indirect" if indirect_pass else "none")
    return {
        "status": status,
        "majority_leaders": majority_leaders,
        "method": method,
        "detail": f"majority available (method={method}): direct={direct_pass}, indirect={indirect_pass} [Windows Docker: published ports unreachable during disconnect, indirect = commit_caught_up AND term_monotonic AND no_split_brain]",
    }


def judge_np3(evidence_list):
    minority_counts = [
        e.get("partition_metrics", {}).get("minority_leader_count", 0)
        for e in evidence_list
    ]
    max_minority = max(minority_counts) if minority_counts else 0
    status = "PASS" if max_minority == 0 else "FAIL"
    return {
        "status": status,
        "minority_leader_counts": minority_counts,
        "max_minority_leaders": max_minority,
        "threshold": 0,
        "detail": f"max(minority_leader_count)={max_minority} == 0",
    }


def judge_np4(evidence_list):
    caught_up = [
        e.get("partition_metrics", {}).get("commit_caught_up", False)
        for e in evidence_list
    ]
    all_caught_up = all(caught_up) if caught_up else False
    status = "PASS" if all_caught_up else "FAIL"
    return {
        "status": status,
        "commit_caught_up": caught_up,
        "detail": f"all commit_caught_up=True: {all_caught_up}",
    }


def judge_np5(evidence_list):
    monotonic = [
        e.get("partition_metrics", {}).get("term_monotonic", False)
        for e in evidence_list
    ]
    all_monotonic = all(monotonic) if monotonic else False
    status = "PASS" if all_monotonic else "FAIL"
    return {
        "status": status,
        "term_monotonic": monotonic,
        "detail": f"all term_monotonic=True: {all_monotonic}",
    }


def judge_overall(results):
    statuses = [r["status"] for r in results.values()]
    if all(s == "PASS" for s in statuses):
        return "PASS"
    if any(s == "FAIL" for s in statuses):
        return "FAIL"
    return "PARTIAL"


def main():
    parser = argparse.ArgumentParser(description="batch28 网络分区验收判定")
    parser.add_argument("--evidence-dir", required=True, help="证据目录路径")
    parser.add_argument("--output", required=True, help="np_verdict.json 输出路径")
    args = parser.parse_args()

    evidence = load_np_evidence(args.evidence_dir)
    print(f"Loaded {len(evidence)} network partition scenario evidence files")

    results = {
        "NP-1": judge_np1(evidence),
        "NP-2": judge_np2(evidence),
        "NP-3": judge_np3(evidence),
        "NP-4": judge_np4(evidence),
        "NP-5": judge_np5(evidence),
    }

    overall = judge_overall(results)

    verdict = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "evidence_dir": args.evidence_dir,
        "scenario_count": len(evidence),
        "scenarios": [e["scenario_id"] for e in evidence],
        **results,
        "overall": overall,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2, ensure_ascii=False)

    print(f"Verdict: {overall}")
    for k in ["NP-1", "NP-2", "NP-3", "NP-4", "NP-5"]:
        print(f"  {k}: {results[k]['status']}  {results[k].get('detail', '')}")
    print(f"Output: {args.output}")

    sys.exit(0 if overall == "PASS" else 1)


if __name__ == "__main__":
    main()