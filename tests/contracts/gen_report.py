#!/usr/bin/env python3
"""gen_report.py — 申报即产物（红线级）

扫描 evidence 产物自动生成晨报首屏，禁止手写（除机制级归因）。
缺失产物报 MISSING，禁止编造。

用法:
  python gen_report.py --evidence-dir tests/evidence/d3-batch30 --output tests/evidence/d3-batch30/gen_report_output.json
"""

import argparse
import json
import sys
import time
from pathlib import Path


class ProductScanner:
    """扫描 evidence 产物，缺失则报 MISSING，禁止编造"""

    def __init__(self, evidence_dir):
        self.evidence_dir = Path(evidence_dir)
        self.missing = []
        self.loaded = {}

    def load_json(self, filename):
        path = self.evidence_dir / filename
        if not path.exists():
            self.missing.append(filename)
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.loaded[filename] = True
                return data
        except (json.JSONDecodeError, IOError) as e:
            self.missing.append(f"{filename} (load error: {e})")
            return None

    def load_text(self, filename):
        path = self.evidence_dir / filename
        if not path.exists():
            self.missing.append(filename)
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = f.read()
                self.loaded[filename] = True
                return data
        except IOError as e:
            self.missing.append(f"{filename} (load error: {e})")
            return None

    def glob_json(self, pattern):
        files = sorted(self.evidence_dir.glob(pattern))
        results = []
        for f in files:
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                    data["_file"] = f.name
                    results.append(data)
                    self.loaded[f.name] = True
            except (json.JSONDecodeError, IOError) as e:
                self.missing.append(f"{f.name} (load error: {e})")
        return results

    def glob_text(self, pattern):
        files = sorted(self.evidence_dir.glob(pattern))
        results = []
        for f in files:
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    results.append({"_file": f.name, "content": fh.read()})
                    self.loaded[f.name] = True
            except IOError as e:
                self.missing.append(f"{f.name} (load error: {e})")
        return results


def generate_first_screen(scanner):
    """从产物文件提取首屏数据，缺失报 MISSING"""
    fs = {}

    actions = scanner.load_json("actions.json")
    if actions:
        if isinstance(actions, dict):
            fs["batch"] = actions.get("batch")
            fs["chain"] = actions.get("chain")
            fs["tasks"] = []
            for t in actions.get("tasks", []):
                fs["tasks"].append({
                    "task_id": t.get("task_id"),
                    "name": t.get("name"),
                    "status": t.get("status"),
                })
        elif isinstance(actions, list):
            fs["batch"] = "unknown (list format)"
            fs["chain"] = None
            fs["tasks"] = []
            for entry in actions:
                if isinstance(entry, dict):
                    fs["tasks"].append({
                        "task_id": entry.get("task", ""),
                        "name": entry.get("reason", ""),
                        "status": entry.get("type", ""),
                    })
    else:
        fs["tasks"] = "MISSING: actions.json"

    verdict = scanner.load_json("verdict.json")
    if verdict:
        fs["verdict"] = verdict.get("overall", "UNKNOWN")
        fs["redlines"] = verdict.get("redlines", {})
        fs["regression_gate"] = verdict.get("regression_gate", {})
    else:
        fs["verdict"] = "MISSING: verdict.json"

    scenarios = scanner.glob_json("scenario_*.json")
    if scenarios:
        fs["scenarios"] = []
        for s in scenarios:
            fs["scenarios"].append({
                "scenario_id": s.get("scenario_id"),
                "status": s.get("status"),
                "composite_metrics": s.get("composite_metrics", {}),
                "partition_metrics": s.get("partition_metrics", {}),
            })
    else:
        scanner.missing.append("scenario_*.json")
        fs["scenarios"] = "MISSING: scenario_*.json"

    reg_texts = scanner.glob_text("regression_gate_*.txt")
    if reg_texts:
        fs["regression_gate_text"] = reg_texts[0]["content"]
    else:
        scanner.missing.append("regression_gate_*.txt")
        fs["regression_gate_text"] = "MISSING"

    vsc_files = sorted(scanner.evidence_dir.glob("variant_self_check_*.json"))
    if vsc_files:
        vsc_data = scanner.load_json(vsc_files[0].name)
        if vsc_data:
            fs["variant_self_check"] = vsc_data
    else:
        scanner.missing.append("variant_self_check_*.json")
        fs["variant_self_check"] = "MISSING"

    qb = scanner.load_json("quorumbench_retry.json")
    if qb:
        fs["quorumbench_retry"] = {
            "final_result": qb.get("final_result"),
            "attempts": qb.get("attempts", []),
        }

    return fs


def main():
    parser = argparse.ArgumentParser(description="gen_report.py 申报即产物")
    parser.add_argument("--evidence-dir", required=True, help="证据目录路径")
    parser.add_argument("--output", required=True, help="输出路径")
    args = parser.parse_args()

    scanner = ProductScanner(args.evidence_dir)
    first_screen = generate_first_screen(scanner)

    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "evidence_dir": str(args.evidence_dir),
        "first_screen": first_screen,
        "loaded_files": list(scanner.loaded.keys()),
        "missing_files": scanner.missing,
        "handwritten_fields": {
            "note": "仅机制级归因可手写，以下字段为手写",
            "mechanism_level_attribution": None,
        },
        "integrity": {
            "fabrication_check": "PASS" if not scanner.missing else "WARN_MISSING",
            "rule": "缺失产物报 MISSING，禁止编造",
        },
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"gen_report.py — 申报即产物")
    print(f"evidence_dir: {args.evidence_dir}")
    print(f"loaded: {len(scanner.loaded)} files")
    print(f"missing: {len(scanner.missing)} files")
    for m in scanner.missing:
        print(f"  MISSING: {m}")
    print(f"integrity: {report['integrity']['fabrication_check']}")
    print(f"output: {args.output}")


if __name__ == "__main__":
    main()