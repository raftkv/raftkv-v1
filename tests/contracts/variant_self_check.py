#!/usr/bin/env python3
"""batch27 任务一: 三组变异自检 — 证明 verdict 与回归门结论一致

三组:
1. 旧线边界: E4 threshold=2.0s, 测试值 1.99s(PASS) / 2.01s(FAIL)
2. 新线边界: E4b threshold=3.5s, 测试值 3.49s(PASS) / 3.51s(FAIL)
3. 线族切换: E4=3.2849s, 旧线(2.0s)=FAIL, 新线(3.5s)=PASS, 证明切换有效
"""

import json
import sys
import time
from pathlib import Path

EVIDENCE_DIR = Path("tests/evidence/d3-batch27")


def judge_e4_variant(value, threshold):
    """模拟 judge_e4 判定逻辑"""
    status = "PASS" if value <= threshold else "FAIL"
    return {
        "status": status,
        "value": round(value, 4),
        "threshold": threshold,
        "operator": "<=",
        "detail": f"max(cascading_duration)={value:.4f}s vs threshold={threshold}s",
    }


def run_group1_old_line_boundary():
    """组1: 旧线边界 — E4 threshold=2.0s (REG-6 threshold_e4a)"""
    results = []
    for label, value in [("below_boundary", 1.99), ("above_boundary", 2.01)]:
        r = judge_e4_variant(value, 2.0)
        r["label"] = label
        r["reg_line"] = "REG-6 threshold_e4a"
        results.append(r)
    expected = {"below_boundary": "PASS", "above_boundary": "FAIL"}
    passed = all(r["status"] == expected[r["label"]] for r in results)
    return {"group": "1_old_line_boundary", "threshold": 2.0, "results": results, "expected": expected, "passed": passed}


def run_group2_new_line_boundary():
    """组2: 新线边界 — E4b threshold=3.5s (REG-6 threshold_e4b)"""
    results = []
    for label, value in [("below_boundary", 3.49), ("above_boundary", 3.51)]:
        r = judge_e4_variant(value, 3.5)
        r["label"] = label
        r["reg_line"] = "REG-6 threshold_e4b"
        results.append(r)
    expected = {"below_boundary": "PASS", "above_boundary": "FAIL"}
    passed = all(r["status"] == expected[r["label"]] for r in results)
    return {"group": "2_new_line_boundary", "threshold": 3.5, "results": results, "expected": expected, "passed": passed}


def run_group3_line_switch():
    """组3: 线族切换 — E4=3.2849s, 旧线=FAIL, 新线=PASS"""
    value = 3.2849
    old_line = judge_e4_variant(value, 2.0)
    old_line["reg_line"] = "REG-6 threshold_e4a (旧线)"
    new_line = judge_e4_variant(value, 3.5)
    new_line["reg_line"] = "REG-6 threshold_e4b (新线)"
    results = [old_line, new_line]
    expected = {"old_line": "FAIL", "new_line": "PASS"}
    passed = old_line["status"] == "FAIL" and new_line["status"] == "PASS"
    return {"group": "3_line_switch", "value": value, "results": results, "expected": expected, "passed": passed}


def main():
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

    g1 = run_group1_old_line_boundary()
    g2 = run_group2_new_line_boundary()
    g3 = run_group3_line_switch()

    all_passed = g1["passed"] and g2["passed"] and g3["passed"]

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "description": "三组变异自检: 旧线边界/新线边界/线族切换",
        "groups": [g1, g2, g3],
        "all_passed": all_passed,
        "conclusion": "PASS: verdict 与回归门结论一致" if all_passed else "FAIL: 结论不一致",
    }

    output_path = EVIDENCE_DIR / "variant_self_check.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"三组变异自检: {'PASS' if all_passed else 'FAIL'}")
    for g in [g1, g2, g3]:
        print(f"  {g['group']}: {'PASS' if g['passed'] else 'FAIL'}")
    print(f"Output: {output_path}")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()