#!/usr/bin/env python3
"""batch22 判定脚本 — 逐字段对照证据 JSON vs YAML 阈值

用法: python judge_fault2.py --contract tests/contracts/fault_injection_2.yaml --evidence-dir tests/evidence/d3-batch22 --output tests/evidence/d3-batch22/verdict.json

退出码:
  0 = 判定完成（PASS 或 FAIL）
  2 = YAML 格式错误或参数异常
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(2)


def load_contract(contract_path):
    with open(contract_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_evidence(evidence_dir):
    evidence = []
    evidence_path = Path(evidence_dir)
    for json_file in sorted(evidence_path.glob("scenario_*.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                data["_file"] = str(json_file)
                evidence.append(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"WARN: cannot load {json_file}: {e}", file=sys.stderr)
    return evidence


def steady_elections(evidence_list):
    steady = [e for e in evidence_list if e.get("scenario_type") == "steady_kill_leader"]
    return [e.get("election_metrics", {}).get("completion_duration") for e in steady if e.get("election_metrics", {}).get("completion_duration") is not None]


def judge_f1(evidence_list, threshold):
    medians = steady_elections(evidence_list)
    if not medians:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no steady data", "value": None, "threshold": threshold}
    max_median = max(medians)
    status = "PASS" if max_median <= threshold else "FAIL"
    return {"status": status, "value": round(max_median, 4), "threshold": threshold, "unit": "seconds", "operator": "<=", "detail": f"max(medians)={max_median:.4f}s vs threshold={threshold}s"}


def judge_f2(evidence_list, during_max, after_threshold):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}
    during_rates = [e.get("reject_metrics", {}).get("reject_rate", 0) for e in evidence_list]
    after_rates = [e.get("reject_metrics", {}).get("post_recovery_reject_rate", 0) for e in evidence_list]
    max_during = max(during_rates) if during_rates else 0
    pass_during = max_during <= during_max
    pass_after = all(r == after_threshold for r in after_rates) if after_rates else True
    status = "PASS" if pass_during and pass_after else "FAIL"
    return {"status": status, "during_max_reject_rate": max_during, "during_threshold": during_max, "after_rates": after_rates, "after_threshold": after_threshold, "detail": f"during max={max_during:.2f}% <= {during_max}%: {pass_during}; after all=={after_threshold}: {pass_after}"}


def judge_f3(evidence_list, expected_rate):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}
    survival_rates = [e.get("survival_metrics", {}).get("survival_rate", 0) for e in evidence_list]
    mismatched = []
    for e in evidence_list:
        m = e.get("survival_metrics", {}).get("mismatched_entries", [])
        if m:
            mismatched.extend(m)
    min_survival = min(survival_rates) if survival_rates else 0
    status = "PASS" if min_survival >= expected_rate else "FAIL"
    return {"status": status, "min_survival_rate": min_survival, "threshold": expected_rate, "mismatched_count": len(mismatched), "detail": f"min(survival)={min_survival:.2f}% vs expected={expected_rate}%"}


def judge_f4(evidence_list, max_leaders):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}
    max_observed_list = [e.get("split_brain_metrics", {}).get("max_concurrent_leaders", 1) for e in evidence_list]
    max_observed = max(max_observed_list) if max_observed_list else 1
    status = "PASS" if max_observed <= max_leaders else "FAIL"
    return {"status": status, "max_concurrent_leaders": max_observed, "threshold": max_leaders, "detail": f"max(concurrent_leaders)={max_observed} <= {max_leaders}"}


def judge_f5(evidence_list):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}
    all_complete = all(e.get("log_metrics", {}).get("timeline_complete", False) for e in evidence_list)
    all_replayable = all(e.get("log_metrics", {}).get("replayable", False) for e in evidence_list)
    status = "PASS" if all_complete and all_replayable else "FAIL"
    return {"status": status, "all_timeline_complete": all_complete, "all_replayable": all_replayable, "detail": f"timeline_complete={all_complete}, replayable={all_replayable}"}


def judge_e1(evidence_list, threshold):
    """E1: 稳态选举 3 次取中位 ≤ 2.0s"""
    medians = steady_elections(evidence_list)
    if not medians:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no steady data", "value": None, "threshold": threshold}
    sorted_m = sorted(medians)
    mid3 = sorted_m[len(sorted_m)//2 - 1 : len(sorted_m)//2 + 2] if len(sorted_m) >= 3 else sorted_m
    median_of_mid3 = sorted(mid3)[len(mid3)//2]
    status = "PASS" if median_of_mid3 <= threshold else "FAIL"
    return {"status": status, "value": round(median_of_mid3, 4), "threshold": threshold, "unit": "seconds", "operator": "<=", "detail": f"3-median={median_of_mid3:.4f}s vs threshold={threshold}s", "all_steady": [round(m, 4) for m in sorted_m]}


def judge_e2(evidence_list, max_leaders):
    """E2: 脑裂检测 max_concurrent_leaders ≤ 1"""
    return judge_f4(evidence_list, max_leaders)


def judge_e3(evidence_list, threshold):
    """E3: 压测下拒载率 ≤ 30%"""
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}
    rates = [e.get("reject_metrics", {}).get("reject_rate", 0) for e in evidence_list]
    max_rate = max(rates) if rates else 0
    status = "PASS" if max_rate <= threshold else "FAIL"
    return {"status": status, "value": round(max_rate, 2), "threshold": threshold, "unit": "percent", "operator": "<=", "detail": f"max(reject_rate)={max_rate:.2f}% <= {threshold}%"}


def judge_s1(evidence_list, expected_rate):
    """S1: 存活率 = 100%"""
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}
    survival_rates = [e.get("survival_metrics", {}).get("survival_rate", 0) for e in evidence_list]
    sampled_counts = [e.get("survival_metrics", {}).get("sampled_entries_count", 0) for e in evidence_list]
    min_survival = min(survival_rates) if survival_rates else 0
    min_sampled = min(sampled_counts) if sampled_counts else 0
    status = "PASS" if min_survival >= expected_rate and min_sampled > 0 else "FAIL"
    return {"status": status, "min_survival_rate": min_survival, "threshold": expected_rate, "min_sampled": min_sampled, "detail": f"min(survival)={min_survival:.2f}% >= {expected_rate}% and min(sampled)={min_sampled} > 0"}


def judge_s2(evidence_list, threshold):
    """S2: /raft/entry 端点可用"""
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}
    sampled_counts = [e.get("survival_metrics", {}).get("sampled_entries_count", 0) for e in evidence_list]
    min_sampled = min(sampled_counts) if sampled_counts else 0
    status = "PASS" if min_sampled >= threshold else "FAIL"
    return {"status": status, "min_sampled": min_sampled, "threshold": threshold, "detail": f"min(sampled_entries_count)={min_sampled} >= {threshold}"}


def judge_overall(results):
    statuses = [r["status"] for r in results.values()]
    if all(s == "PASS" for s in statuses):
        return "PASS"
    if any(s == "FAIL" for s in statuses):
        return "FAIL"
    return "PARTIAL"


def main():
    parser = argparse.ArgumentParser(description="batch22 判定脚本")
    parser.add_argument("--contract", required=True, help="验收契约 YAML 路径")
    parser.add_argument("--evidence-dir", required=True, help="证据目录路径")
    parser.add_argument("--output", required=True, help="verdict.json 输出路径")
    args = parser.parse_args()

    try:
        contract = load_contract(args.contract)
    except Exception as e:
        print(f"ERROR: cannot load contract YAML: {e}", file=sys.stderr)
        sys.exit(2)

    evidence = load_evidence(args.evidence_dir)
    print(f"Loaded {len(evidence)} scenario evidence files")

    criteria = contract.get("acceptance_criteria", {})

    f1 = judge_f1(evidence, criteria.get("F1", {}).get("threshold", 5.0))
    f2_cfg = criteria.get("F2", {})
    f2 = judge_f2(evidence, f2_cfg.get("during_injection", {}).get("threshold", 30.0), f2_cfg.get("after_recovery", {}).get("threshold", 0))
    f3 = judge_f3(evidence, criteria.get("F3", {}).get("threshold", 100.0))
    f4 = judge_f4(evidence, criteria.get("F4", {}).get("threshold", 1))
    f5 = judge_f5(evidence)

    e1 = judge_e1(evidence, criteria.get("E1", {}).get("threshold", 2.0))
    e2 = judge_e2(evidence, criteria.get("E2", {}).get("threshold", 1))
    e3 = judge_e3(evidence, criteria.get("E3", {}).get("threshold", 30.0))
    s1 = judge_s1(evidence, criteria.get("S1", {}).get("threshold", 100.0))
    s2 = judge_s2(evidence, criteria.get("S2", {}).get("threshold", 1))

    results = {"F1": f1, "F2": f2, "F3": f3, "F4": f4, "F5": f5, "E1": e1, "E2": e2, "E3": e3, "S1": s1, "S2": s2}
    overall = judge_overall(results)

    verdict = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "contract": args.contract,
        "evidence_dir": args.evidence_dir,
        "scenario_count": len(evidence),
        **results,
        "overall": overall,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2, ensure_ascii=False)

    print(f"Verdict: {overall}")
    for k in ["F1", "F2", "F3", "F4", "F5", "E1", "E2", "E3", "S1", "S2"]:
        print(f"  {k}: {results[k]['status']}  {results[k].get('detail', '')}")
    print(f"Output: {args.output}")

    sys.exit(0)


if __name__ == "__main__":
    main()