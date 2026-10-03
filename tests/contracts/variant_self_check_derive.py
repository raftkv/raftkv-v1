#!/usr/bin/env python3
"""batch29 推导机器催缴变异自检

构造两组变异用例验证 judge_batch29 的校验逻辑生效：
  变异1: decisions.md 删除 §四b 推导段 → DERIVE-1 应 FAIL
  变异2: report.md 删除首屏三答 → DERIVE-2 应 FAIL

用法: python variant_self_check_derive.py --output tests/evidence/d3-batch29/variant_self_check_derive.json
"""

import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path


DECISIONS_MD_PATH = Path("tests/evidence/d3-batch25/decisions.md")
REPORT_MD_PATH = Path("tests/evidence/d3-batch29/report.md")
JUDGE_SCRIPT = Path("tests/contracts/judge_batch29.py")


def run_judge(decisions_path, report_path, output_path):
    result = subprocess.run(
        ["python", str(JUDGE_SCRIPT),
         "--evidence-dir", "tests/evidence/d3-batch29",
         "--output", str(output_path),
         "--decisions-md", str(decisions_path),
         "--report-md", str(report_path)],
        capture_output=True, text=True, cwd="."
    )
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            verdict = json.load(f)
    except (json.JSONDecodeError, IOError):
        verdict = {"overall": "ERROR", "stdout": result.stdout, "stderr": result.stderr}
    return verdict


def variant1_derive_section_removed():
    with open(DECISIONS_MD_PATH, "r", encoding="utf-8") as f:
        original = f.read()

    removed = re.sub(r'### 4b\..*?(?=### 4c\.|\Z)', '', original, flags=re.DOTALL)
    if removed == original:
        removed = original.replace("### 4b. regression.yaml 提案 diff", "### 4b. [REMOVED FOR VARIANT TEST]")
        removed = re.sub(r'### 4b\. \[REMOVED FOR VARIANT TEST\].*?(?=### 4c\.|\Z)', '### 4b. [REMOVED FOR VARIANT TEST]\n推导段已删除\n\n', removed, flags=re.DOTALL)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False, encoding='utf-8') as f:
        f.write(removed)
        temp_decisions = f.name

    temp_output = tempfile.NamedTemporaryFile(suffix='.json', delete=False).name

    try:
        verdict = run_judge(temp_decisions, str(REPORT_MD_PATH), temp_output)
        derive1_statuses = {k: v.get("status") for k, v in verdict.get("derive1", {}).items()}
        any_fail = any(s == "FAIL" for s in derive1_statuses.values())
        return {
            "status": "PASS" if any_fail else "FAIL",
            "derive1_statuses": derive1_statuses,
            "detail": f"变异1(删除推导段): derive1 有 FAIL={any_fail}, 期望=True",
        }
    finally:
        Path(temp_decisions).unlink(missing_ok=True)
        Path(temp_output).unlink(missing_ok=True)


def variant2_firstscreen_removed():
    with open(REPORT_MD_PATH, "r", encoding="utf-8") as f:
        original = f.read()

    removed = original.replace("3.4825", "X").replace("3.4696", "X").replace("2.9180", "X").replace("3.3223", "X").replace("3.4910", "X")
    removed = removed.replace("13/13", "X").replace("9/9", "X")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False, encoding='utf-8') as f:
        f.write(removed)
        temp_report = f.name

    temp_output = tempfile.NamedTemporaryFile(suffix='.json', delete=False).name

    try:
        verdict = run_judge(str(DECISIONS_MD_PATH), temp_report, temp_output)
        derive2_status = verdict.get("derive2", {}).get("status", "UNKNOWN")
        is_fail = derive2_status == "FAIL"
        return {
            "status": "PASS" if is_fail else "FAIL",
            "derive2_status": derive2_status,
            "detail": f"变异2(删除首屏三答): derive2 status={derive2_status}, 期望=FAIL",
        }
    finally:
        Path(temp_report).unlink(missing_ok=True)
        Path(temp_output).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="batch29 推导机器催缴变异自检")
    parser.add_argument("--output", required=True, help="输出路径")
    args = parser.parse_args()

    print("变异自检1: 删除 decisions.md 推导段 → DERIVE-1 应 FAIL")
    v1 = variant1_derive_section_removed()
    print(f"  结果: {v1['status']}  {v1['detail']}")

    print("变异自检2: 删除 report.md 首屏三答 → DERIVE-2 应 FAIL")
    v2 = variant2_firstscreen_removed()
    print(f"  结果: {v2['status']}  {v2['detail']}")

    all_pass = v1["status"] == "PASS" and v2["status"] == "PASS"
    overall = "PASS" if all_pass else "FAIL"

    result = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "variant1_derive_removed": v1,
        "variant2_firstscreen_removed": v2,
        "overall": overall,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\nOverall: {overall}")
    print(f"Output: {args.output}")
    sys.exit(0 if overall == "PASS" else 1)


if __name__ == "__main__":
    main()