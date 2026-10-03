#!/usr/bin/env python3
"""closure_integration.py — 闭案流程集成脚本

集成 gen_report.py + meter_diff.py + judge_schema.py 到闭案流程。
每批闭案时自动调用，生成首屏+计量对账+schema校验。

用法:
  python closure_integration.py --batch-id batch30 --evidence-dir tests/evidence/d3-batch30 --output-dir tests/evidence/d3-batch30
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path


def run_script(script, args, cwd=None):
    cmd = [sys.executable, f"tests/contracts/{script}"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd or str(Path.cwd()))
    return result.returncode, result.stdout, result.stderr


def main():
    parser = argparse.ArgumentParser(description="闭案流程集成")
    parser.add_argument("--batch-id", required=True, help="批次 ID")
    parser.add_argument("--evidence-dir", required=True, help="证据目录")
    parser.add_argument("--output-dir", required=True, help="输出目录")
    parser.add_argument("--self-report-tokens", type=int, default=0, help="自报 token 数")
    parser.add_argument("--ide-tokens", type=int, default=None, help="IDE 计量 token 数")
    args = parser.parse_args()

    results = {}

    rc, out, err = run_script("gen_report.py", [
        "--evidence-dir", args.evidence_dir,
        "--output", f"{args.output_dir}/gen_report_output.json",
    ])
    results["gen_report"] = {"rc": rc, "status": "PASS" if rc == 0 else "FAIL", "stdout": out.strip()}

    if args.self_report_tokens > 0:
        meter_args = ["--self-report-tokens", str(args.self_report_tokens)]
        if args.ide_tokens is not None:
            meter_args += ["--ide-tokens", str(args.ide_tokens)]
        else:
            meter_args += ["--ide-metrics-unavailable", "--barrier-detail", "IDE 计量不可程序化读取"]
        meter_args += ["--output", f"{args.output_dir}/meter_diff.json"]
        rc, out, err = run_script("meter_diff.py", meter_args)
        results["meter_diff"] = {"rc": rc, "status": "PASS" if rc == 0 else "FAIL", "stdout": out.strip()}
    else:
        results["meter_diff"] = {"status": "SKIP", "reason": "未提供 self-report-tokens"}

    schema_dir = Path(args.evidence_dir) / "schema"
    if schema_dir.exists():
        rc, out, err = run_script("judge_schema.py", [
            "--schema-dir", str(schema_dir),
            "--output", f"{args.output_dir}/schema_judge_result.json",
        ])
        results["judge_schema"] = {"rc": rc, "status": "PASS" if rc == 0 else "FAIL", "stdout": out.strip()}
    else:
        results["judge_schema"] = {"status": "SKIP", "reason": "无 schema 目录"}

    all_pass = all(r.get("status") in ("PASS", "SKIP") for r in results.values())
    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "batch_id": args.batch_id,
        "steps": results,
        "overall": "PASS" if all_pass else "FAIL",
    }

    output_path = f"{args.output_dir}/closure_integration.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"closure_integration.py — 闭案流程集成")
    for k, v in results.items():
        print(f"  {k}: {v.get('status')}")
    print(f"overall: {summary['overall']}")
    print(f"output: {output_path}")


if __name__ == "__main__":
    main()