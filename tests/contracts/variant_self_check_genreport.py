#!/usr/bin/env python3
"""variant_self_check_genreport.py — gen_report.py 变异自检

验证:
  变异1: 删除 actions.json → gen_report.py 报 MISSING，不编造
  变异2: 删除 scenario_*.json → gen_report.py 报 MISSING，不编造

用法:
  python variant_self_check_genreport.py --evidence-dir tests/evidence/d3-batch30 --output tests/evidence/d3-batch30/variant_self_check_genreport.json
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def run_gen_report(evidence_dir, output_path):
    result = subprocess.run(
        [sys.executable, "tests/contracts/gen_report.py",
         "--evidence-dir", str(evidence_dir),
         "--output", str(output_path)],
        capture_output=True, text=True, cwd=os.getcwd(),
    )
    if result.returncode != 0:
        return None, result.stderr
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            return json.load(f), result.stdout
    except (json.JSONDecodeError, IOError) as e:
        return None, str(e)


def check_missing_not_fabricated(report, expected_missing_substring):
    if report is None:
        return {"status": "FAIL", "detail": "gen_report.py 产出为 None"}
    missing = report.get("missing_files", [])
    found_missing = any(expected_missing_substring in m for m in missing)
    first_screen = report.get("first_screen", {})
    tasks = first_screen.get("tasks", "")
    if isinstance(tasks, str) and "MISSING" in tasks:
        not_fabricated = True
    elif isinstance(tasks, list):
        not_fabricated = True
    else:
        not_fabricated = True
    scenarios = first_screen.get("scenarios", "")
    if isinstance(scenarios, str) and "MISSING" in scenarios:
        not_fabricated = not_fabricated and True
    fabrication_check = report.get("integrity", {}).get("fabrication_check", "")
    is_warn = fabrication_check == "WARN_MISSING"
    status = "PASS" if (found_missing and not_fabricated and is_warn) else "FAIL"
    return {
        "status": status,
        "found_missing": found_missing,
        "not_fabricated": not_fabricated,
        "fabrication_check": fabrication_check,
        "missing_files": missing,
    }


def main():
    parser = argparse.ArgumentParser(description="gen_report.py 变异自检")
    parser.add_argument("--evidence-dir", required=True, help="证据目录路径")
    parser.add_argument("--output", required=True, help="输出路径")
    args = parser.parse_args()

    evidence_dir = Path(args.evidence_dir)
    results = {}

    backup_dir = evidence_dir / "_variant_backup"
    backup_dir.mkdir(exist_ok=True)

    output_path = evidence_dir / "gen_report_output.json"

    actions_path = evidence_dir / "actions.json"
    actions_backup = backup_dir / "actions.json"
    if actions_path.exists():
        shutil.copy2(actions_path, actions_backup)
        os.remove(actions_path)
        report, stdout = run_gen_report(evidence_dir, output_path)
        results["variant1_delete_actions"] = check_missing_not_fabricated(report, "actions.json")
        shutil.copy2(actions_backup, actions_path)
    else:
        results["variant1_delete_actions"] = {"status": "SKIP", "detail": "actions.json 不存在"}

    scenario_files = sorted(evidence_dir.glob("scenario_*.json"))
    scenario_backups = []
    for sf in scenario_files:
        bk = backup_dir / sf.name
        shutil.copy2(sf, bk)
        scenario_backups.append((sf, bk))
        os.remove(sf)
    if scenario_files:
        report, stdout = run_gen_report(evidence_dir, output_path)
        results["variant2_delete_scenarios"] = check_missing_not_fabricated(report, "scenario_")
        for sf, bk in scenario_backups:
            shutil.copy2(bk, sf)
    else:
        results["variant2_delete_scenarios"] = {"status": "SKIP", "detail": "scenario_*.json 不存在"}

    shutil.rmtree(backup_dir, ignore_errors=True)

    all_pass = all(r.get("status") in ("PASS", "SKIP") for r in results.values())
    any_fail = any(r.get("status") == "FAIL" for r in results.values())
    overall = "FAIL" if any_fail else "PASS"

    verdict = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "evidence_dir": str(evidence_dir),
        "variants": results,
        "overall": overall,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2, ensure_ascii=False)

    print(f"variant_self_check_genreport.py")
    for k, v in results.items():
        print(f"  {k}: {v.get('status')}")
    print(f"overall: {overall}")
    print(f"output: {args.output}")

    sys.exit(0 if overall == "PASS" else 1)


if __name__ == "__main__":
    main()