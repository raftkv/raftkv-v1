#!/usr/bin/env python3
"""batch21 判定脚本 — 逐字段对照证据 JSON vs YAML 阈值

用法: python judge_batch21.py --contract tests/contracts/batch21.yaml --evidence-dir tests/evidence/d3-batch21 --output tests/evidence/d3-batch21/verdict.json

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


def median_of_3(values):
    if len(values) < 1:
        return None
    if len(values) >= 3:
        sorted_v = sorted(values[:3])
        return sorted_v[1]
    return sorted(values)[len(values) // 2]


def judge_f1(evidence_list, threshold):
    steady = [e for e in evidence_list if e.get("scenario_type") == "steady_kill_leader"]
    if not steady:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no steady_kill_leader scenarios", "value": None, "threshold": threshold}

    medians = []
    for e in steady:
        em = e.get("election_metrics", {})
        dur = em.get("completion_duration")
        if dur is not None:
            medians.append(dur)

    if not medians:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no completion_duration data", "value": None, "threshold": threshold}

    max_median = max(medians)
    status = "PASS" if max_median <= threshold else "FAIL"
    return {"status": status, "value": max_median, "threshold": threshold, "unit": "seconds", "operator": "<=", "detail": f"max(medians)={max_median:.3f}s vs threshold={threshold}s"}


def judge_f2(evidence_list, during_max, after_threshold):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}

    during_rates = []
    after_rates = []
    for e in evidence_list:
        rm = e.get("reject_metrics", {})
        rr = rm.get("reject_rate")
        if rr is not None:
            during_rates.append(rr)
        prr = rm.get("post_recovery_reject_rate")
        if prr is not None:
            after_rates.append(prr)

    if not during_rates and not after_rates:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no reject_rate data", "value": None}

    max_during = max(during_rates) if during_rates else 0
    pass_during = max_during <= during_max
    pass_after = all(r == after_threshold for r in after_rates) if after_rates else True
    status = "PASS" if pass_during and pass_after else "FAIL"
    return {
        "status": status,
        "during_max_reject_rate": max_during,
        "during_threshold": during_max,
        "after_rates": after_rates,
        "after_threshold": after_threshold,
        "detail": f"during max={max_during:.2f}% <= {during_max}%: {pass_during}; after all=={after_threshold}: {pass_after}",
    }


def judge_f3(evidence_list, expected_rate):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}

    survival_rates = []
    mismatched = []
    for e in evidence_list:
        sm = e.get("survival_metrics", {})
        sr = sm.get("survival_rate")
        if sr is not None:
            survival_rates.append(sr)
        mismatches = sm.get("mismatched_entries", [])
        if mismatches:
            mismatched.extend(mismatches)

    if not survival_rates:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no survival_rate data", "value": None}

    min_survival = min(survival_rates)
    status = "PASS" if min_survival >= expected_rate else "FAIL"
    return {
        "status": status,
        "min_survival_rate": min_survival,
        "threshold": expected_rate,
        "mismatched_count": len(mismatched),
        "mismatched_entries": mismatched[:10],
        "detail": f"min(survival)={min_survival:.2f}% vs expected={expected_rate}%",
    }


def judge_f4(evidence_list, max_leaders):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}

    max_observed_list = []
    for e in evidence_list:
        sbm = e.get("split_brain_metrics", {})
        ml = sbm.get("max_concurrent_leaders")
        if ml is not None:
            max_observed_list.append(ml)

    if not max_observed_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no max_concurrent_leaders data", "value": None}

    max_observed = max(max_observed_list)
    status = "PASS" if max_observed <= max_leaders else "FAIL"
    return {
        "status": status,
        "max_concurrent_leaders": max_observed,
        "threshold": max_leaders,
        "detail": f"max(concurrent_leaders)={max_observed} <= {max_leaders}",
    }


def judge_f5(evidence_list):
    if not evidence_list:
        return {"status": "INSUFFICIENT_EVIDENCE", "detail": "no evidence", "value": None}

    all_complete = all(e.get("log_metrics", {}).get("timeline_complete", False) for e in evidence_list)
    all_replayable = all(e.get("log_metrics", {}).get("replayable", False) for e in evidence_list)
    status = "PASS" if all_complete and all_replayable else "FAIL"
    return {
        "status": status,
        "all_timeline_complete": all_complete,
        "all_replayable": all_replayable,
        "detail": f"timeline_complete={all_complete}, replayable={all_replayable}",
    }


def judge_overall(f1, f2, f3, f4, f5):
    statuses = [f1["status"], f2["status"], f3["status"], f4["status"], f5["status"]]
    if all(s == "PASS" for s in statuses):
        return "PASS"
    if any(s == "FAIL" for s in statuses):
        return "FAIL"
    return "PARTIAL"


def main():
    parser = argparse.ArgumentParser(description="batch21 判定脚本")
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
    f2 = judge_f2(
        evidence,
        f2_cfg.get("during_injection", {}).get("threshold", 30.0),
        f2_cfg.get("after_recovery", {}).get("threshold", 0),
    )
    f3 = judge_f3(evidence, criteria.get("F3", {}).get("threshold", 100.0))
    f4 = judge_f4(evidence, criteria.get("F4", {}).get("threshold", 1))
    f5 = judge_f5(evidence)
    overall = judge_overall(f1, f2, f3, f4, f5)

    verdict = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "contract": args.contract,
        "evidence_dir": args.evidence_dir,
        "scenario_count": len(evidence),
        "F1": f1,
        "F2": f2,
        "F3": f3,
        "F4": f4,
        "F5": f5,
        "overall": overall,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2, ensure_ascii=False)

    print(f"Verdict: {overall}")
    print(f"  F1 (election):     {f1['status']}")
    print(f"  F2 (reject_rate):   {f2['status']}")
    print(f"  F3 (survival):      {f3['status']}")
    print(f"  F4 (no_split_brain): {f4['status']}")
    print(f"  F5 (log_complete):  {f5['status']}")
    print(f"Output: {args.output}")

    sys.exit(0)


if __name__ == "__main__":
    main()