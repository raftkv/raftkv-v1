#!/usr/bin/env python3
"""batch29 推导机器催缴验收判定脚本

判定项：
  DERIVE-1: decisions.md E4b 推导段含量化内容校验
    - 推导段存在（§四b 或 4b 标记）
    - 含随机选举区间参数（800ms/1200ms）
    - 含 pre-vote 耗时参数（200ms）
    - 含 RPC 往返参数（500ms）
    - 含单轮收敛推导（1900ms）
    - 含双轮收敛推导（3800ms）
    - 含实测 max 值（3.46s 附近）
    - 含阈值定义（3.5s）
    - 缺失或纯文字无数字→verdict 强制 FAIL

  DERIVE-2: 首屏必答三项齐备检查
    - a. E4b N=5 五原始值完整列出
    - b. 上批自检门实际结果
    - c. 上一批首屏申报实际情况

  NP-1~5: 网络分区验收（继承 batch28）

用法: python judge_derive.py --evidence-dir tests/evidence/d3-batch29 --output tests/evidence/d3-batch29/derive_verdict.json
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path


DECISIONS_MD_PATH = Path("tests/evidence/d3-batch25/decisions.md")
REPORT_MD_PATH = Path("tests/evidence/d3-batch29/report.md")


def load_decisions_md(path=None):
    p = path or DECISIONS_MD_PATH
    if not p.exists():
        return None
    with open(p, "r", encoding="utf-8") as f:
        return f.read()


def load_report_md(path=None):
    p = path or REPORT_MD_PATH
    if not p.exists():
        return None
    with open(p, "r", encoding="utf-8") as f:
        return f.read()


def extract_numbers(text):
    if text is None:
        return []
    return re.findall(r'\d+\.?\d*', text)


def judge_derive1_section_exists(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在"}
    has_4b = ("四b" in content or "4b" in content.lower() or "§四b" in content or "§4b" in content.lower())
    has_rationale = "rationale" in content.lower() or "机制下限" in content or "推导" in content
    status = "PASS" if (has_4b and has_rationale) else "FAIL"
    return {
        "status": status,
        "has_section_4b": has_4b,
        "has_rationale": has_rationale,
        "detail": f"推导段存在: section_4b={has_4b}, rationale={has_rationale}",
    }


def judge_derive1_election_interval(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在", "found": []}
    found = []
    if re.search(r'800\s*ms', content) or re.search(r'800\b', content):
        found.append("800ms (electionTimeoutMin)")
    if re.search(r'1200\s*ms', content) or re.search(r'1200\b', content):
        found.append("1200ms (electionTimeoutMax)")
    status = "PASS" if len(found) >= 2 else "FAIL"
    return {
        "status": status,
        "found": found,
        "detail": f"随机选举区间参数: {found}" if found else "未找到随机选举区间参数",
    }


def judge_derive1_prevote(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在", "found": []}
    found = []
    if re.search(r'200\s*ms', content) or re.search(r'T_prevote.*200', content):
        found.append("200ms (T_prevote)")
    status = "PASS" if found else "FAIL"
    return {
        "status": status,
        "found": found,
        "detail": f"pre-vote 耗时参数: {found}" if found else "未找到 pre-vote 耗时参数",
    }


def judge_derive1_rpc(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在", "found": []}
    found = []
    if re.search(r'500\s*ms', content) or re.search(r'T_rpc.*500', content):
        found.append("500ms (T_rpc)")
    status = "PASS" if found else "FAIL"
    return {
        "status": status,
        "found": found,
        "detail": f"RPC 往返参数: {found}" if found else "未找到 RPC 往返参数",
    }


def judge_derive1_single_round(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在", "found": []}
    found = []
    if re.search(r'1900\s*ms', content) or re.search(r'1900\b', content):
        found.append("1900ms (单轮=800+200+500)")
    status = "PASS" if found else "FAIL"
    return {
        "status": status,
        "found": found,
        "detail": f"单轮收敛推导: {found}" if found else "未找到单轮收敛推导(1900ms)",
    }


def judge_derive1_double_round(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在", "found": []}
    found = []
    if re.search(r'3800\s*ms', content) or re.search(r'3800\b', content):
        found.append("3800ms (双轮=2×1900)")
    status = "PASS" if found else "FAIL"
    return {
        "status": status,
        "found": found,
        "detail": f"双轮收敛推导: {found}" if found else "未找到双轮收敛推导(3800ms)",
    }


def judge_derive1_measured_max(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在", "found": []}
    found = []
    if re.search(r'3\.46', content):
        found.append("3.46s (实测max)")
    status = "PASS" if found else "FAIL"
    return {
        "status": status,
        "found": found,
        "detail": f"实测 max 值: {found}" if found else "未找到实测 max 值(3.46s)",
    }


def judge_derive1_threshold(content):
    if content is None:
        return {"status": "FAIL", "detail": "decisions.md 不存在", "found": []}
    found = []
    if re.search(r'3\.5\s*s', content) or re.search(r'threshold.*3\.5', content, re.IGNORECASE):
        found.append("3.5s (阈值)")
    status = "PASS" if found else "FAIL"
    return {
        "status": status,
        "found": found,
        "detail": f"阈值定义: {found}" if found else "未找到阈值定义(3.5s)",
    }


def judge_derive2(report_content):
    if report_content is None:
        return {"status": "FAIL", "detail": "report.md 不存在", "checks": {}}
    checks = {}

    has_a = ("3.4825" in report_content and "3.4696" in report_content
             and "2.9180" in report_content and "3.3223" in report_content
             and "3.4910" in report_content)
    checks["a_e4b_n5_values"] = has_a

    has_b = ("13/13" in report_content or "红线" in report_content) and ("9/9" in report_content or "回归门" in report_content)
    checks["b_self_check_results"] = has_b

    has_c = "首屏" in report_content and ("batch28" in report_content or "上一批" in report_content or "上批" in report_content)
    checks["c_prev_report_status"] = has_c

    all_pass = all(checks.values())
    status = "PASS" if all_pass else "FAIL"
    return {
        "status": status,
        "checks": checks,
        "detail": f"首屏三答齐备: a={checks['a_e4b_n5_values']}, b={checks['b_self_check_results']}, c={checks['c_prev_report_status']}",
    }


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
    return {"status": status, "max_concurrent_leaders": max_leaders, "threshold": 1, "detail": f"max(concurrent_leaders)={max_leaders} <= 1"}


def judge_np2(evidence_list):
    majority_leaders = [e.get("partition_metrics", {}).get("majority_leader", "") for e in evidence_list]
    direct_pass = all(ml != "" for ml in majority_leaders) if majority_leaders else False
    indirect_pass = all(
        e.get("partition_metrics", {}).get("commit_caught_up", False)
        and e.get("partition_metrics", {}).get("term_monotonic", False)
        and e.get("partition_metrics", {}).get("max_concurrent_leaders", 0) <= 1
        for e in evidence_list
    ) if evidence_list else False
    status = "PASS" if (direct_pass or indirect_pass) else "FAIL"
    method = "direct" if direct_pass else ("indirect" if indirect_pass else "none")
    return {"status": status, "method": method, "detail": f"majority available (method={method})"}


def judge_np3(evidence_list):
    minority_counts = [e.get("partition_metrics", {}).get("minority_leader_count", 0) for e in evidence_list]
    max_minority = max(minority_counts) if minority_counts else 0
    status = "PASS" if max_minority == 0 else "FAIL"
    return {"status": status, "max_minority_leaders": max_minority, "threshold": 0, "detail": f"max(minority_leader_count)={max_minority} == 0"}


def judge_np4(evidence_list):
    caught_up = [e.get("partition_metrics", {}).get("commit_caught_up", False) for e in evidence_list]
    all_caught_up = all(caught_up) if caught_up else False
    status = "PASS" if all_caught_up else "FAIL"
    return {"status": status, "detail": f"all commit_caught_up=True: {all_caught_up}"}


def judge_np5(evidence_list):
    monotonic = [e.get("partition_metrics", {}).get("term_monotonic", False) for e in evidence_list]
    all_monotonic = all(monotonic) if monotonic else False
    status = "PASS" if all_monotonic else "FAIL"
    return {"status": status, "detail": f"all term_monotonic=True: {all_monotonic}"}


def judge_overall(results):
    statuses = [r["status"] for r in results.values()]
    if all(s == "PASS" for s in statuses):
        return "PASS"
    if any(s == "FAIL" for s in statuses):
        return "FAIL"
    return "PARTIAL"


def main():
    parser = argparse.ArgumentParser(description="batch29 推导机器催缴验收判定")
    parser.add_argument("--evidence-dir", required=True, help="证据目录路径")
    parser.add_argument("--output", required=True, help="derive_verdict.json 输出路径")
    parser.add_argument("--decisions-md", default=str(DECISIONS_MD_PATH), help="decisions.md 路径")
    parser.add_argument("--report-md", default=str(REPORT_MD_PATH), help="report.md 路径")
    args = parser.parse_args()

    decisions_path = Path(args.decisions_md)
    report_path = Path(args.report_md)

    decisions_content = load_decisions_md(decisions_path)
    report_content = load_report_md(report_path)

    print(f"decisions.md: {'loaded' if decisions_content else 'NOT FOUND'} ({decisions_path})")
    print(f"report.md: {'loaded' if report_content else 'NOT FOUND'} ({report_path})")

    derive1_results = {
        "DERIVE-1a-section": judge_derive1_section_exists(decisions_content),
        "DERIVE-1b-election-interval": judge_derive1_election_interval(decisions_content),
        "DERIVE-1c-prevote": judge_derive1_prevote(decisions_content),
        "DERIVE-1d-rpc": judge_derive1_rpc(decisions_content),
        "DERIVE-1e-single-round": judge_derive1_single_round(decisions_content),
        "DERIVE-1f-double-round": judge_derive1_double_round(decisions_content),
        "DERIVE-1g-measured-max": judge_derive1_measured_max(decisions_content),
        "DERIVE-1h-threshold": judge_derive1_threshold(decisions_content),
    }

    derive2_result = judge_derive2(report_content)

    np_evidence = load_np_evidence(args.evidence_dir)
    np_results = {}
    if np_evidence:
        np_results = {
            "NP-1": judge_np1(np_evidence),
            "NP-2": judge_np2(np_evidence),
            "NP-3": judge_np3(np_evidence),
            "NP-4": judge_np4(np_evidence),
            "NP-5": judge_np5(np_evidence),
        }

    all_results = {**derive1_results, "DERIVE-2-firstscreen": derive2_result, **np_results}
    overall = judge_overall(all_results)

    verdict = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "decisions_md": str(decisions_path),
        "report_md": str(report_path),
        "derive1": derive1_results,
        "derive2": derive2_result,
        "np": np_results if np_results else "no NP evidence",
        "overall": overall,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2, ensure_ascii=False)

    print(f"\nVerdict: {overall}")
    for k, v in derive1_results.items():
        print(f"  {k}: {v['status']}  {v.get('detail', '')}")
    print(f"  DERIVE-2-firstscreen: {derive2_result['status']}  {derive2_result.get('detail', '')}")
    for k, v in np_results.items():
        print(f"  {k}: {v['status']}  {v.get('detail', '')}")
    print(f"Output: {args.output}")

    sys.exit(0 if overall == "PASS" else 1)


if __name__ == "__main__":
    main()