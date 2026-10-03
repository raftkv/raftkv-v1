#!/usr/bin/env python3
"""judge_schema.py — evidence schema 四段留证字段完整性校验

校验四段（injection/observation/recovery/assertion）必填字段完整性。
recovery.duration_s 为必填字段。

用法:
  python judge_schema.py --schema-dir tests/evidence/d3-batch29/schema --output tests/evidence/d3-batch30/schema_judge_result.json
"""

import argparse
import json
import sys
import time
from pathlib import Path


REQUIRED_FIELDS = {
    "injection": ["scenario_id", "scenario_type", "fault_type", "target_nodes", "parameters", "timestamp"],
    "observation": ["metrics", "timeline", "timestamp_start", "timestamp_end"],
    "recovery": ["operation", "duration_s", "confirmed", "timestamp"],
    "assertion": ["checks", "overall_status"],
}


def validate_section(schema, section_name):
    section = schema.get(section_name, {})
    required = REQUIRED_FIELDS.get(section_name, [])
    missing = [f for f in required if f not in section or section[f] is None]
    return {
        "section": section_name,
        "missing_fields": missing,
        "status": "PASS" if not missing else "FAIL",
    }


def validate_schema(schema):
    results = {}
    for section in REQUIRED_FIELDS:
        results[section] = validate_section(schema, section)

    recovery = schema.get("recovery", {})
    duration = recovery.get("duration_s")
    results["recovery_duration_s_required"] = {
        "field": "recovery.duration_s",
        "value": duration,
        "status": "PASS" if duration is not None and isinstance(duration, (int, float)) else "FAIL",
    }

    all_pass = all(r["status"] == "PASS" for r in results.values())
    return {"checks": results, "overall": "PASS" if all_pass else "FAIL"}


def main():
    parser = argparse.ArgumentParser(description="evidence schema 四段留证字段完整性校验")
    parser.add_argument("--schema-dir", required=True, help="schema 文件目录")
    parser.add_argument("--output", required=True, help="输出路径")
    args = parser.parse_args()

    schema_dir = Path(args.schema_dir)
    schema_files = sorted(schema_dir.glob("schema_scenario_*.json"))
    results = []

    for sf in schema_files:
        try:
            with open(sf, "r", encoding="utf-8") as f:
                schema = json.load(f)
            validation = validate_schema(schema)
            results.append({"file": sf.name, "overall": validation["overall"], "checks": validation["checks"]})
            print(f"  {sf.name}: {validation['overall']}")
        except Exception as e:
            results.append({"file": sf.name, "overall": "FAIL", "error": str(e)})
            print(f"  {sf.name}: FAIL ({e})")

    all_pass = all(r["overall"] == "PASS" for r in results)
    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "schema_dir": str(schema_dir),
        "files_checked": len(results),
        "results": results,
        "overall": "PASS" if all_pass else "FAIL",
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"\n校验完成: {sum(1 for r in results if r['overall'] == 'PASS')}/{len(results)} PASS")
    print(f"overall: {summary['overall']}")
    print(f"output: {args.output}")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()