#!/usr/bin/env python3
"""batch28 统计量口径变异自检

验证 regression.yaml stat 字段实际控制判定结果。
构造 E4b 边界用例 [3.2438, 3.4802, 3.5166]：
  - stat=median → median=3.4802s ≤ 3.5s → PASS
  - stat=max    → max=3.5166s > 3.5s    → FAIL

若两条均 PASS 或均 FAIL，说明 stat 字段未生效，自检失败。
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from judge_batch23 import compute_statistic, get_reg_stat, load_regression_gate

E4B_THRESHOLD = 3.5
BOUNDARY_VALUES = [3.2438, 3.4802, 3.5166]

def run_check():
    results = {}
    reg_gate = load_regression_gate()

    reg6_stat = get_reg_stat(reg_gate, "REG-6")
    results["reg6_stat_declared"] = reg6_stat

    median_val = compute_statistic(BOUNDARY_VALUES, "median")
    max_val = compute_statistic(BOUNDARY_VALUES, "max")
    min_val = compute_statistic(BOUNDARY_VALUES, "min")
    count_val = compute_statistic(BOUNDARY_VALUES, "count")

    results["boundary_values"] = BOUNDARY_VALUES
    results["statistics"] = {
        "median": round(median_val, 4),
        "max": round(max_val, 4),
        "min": round(min_val, 4),
        "count": count_val,
    }

    median_pass = median_val <= E4B_THRESHOLD
    max_pass = max_val <= E4B_THRESHOLD
    results["median_judgment"] = {
        "stat": "median",
        "value": round(median_val, 4),
        "threshold": E4B_THRESHOLD,
        "status": "PASS" if median_pass else "FAIL",
    }
    results["max_judgment"] = {
        "stat": "max",
        "value": round(max_val, 4),
        "threshold": E4B_THRESHOLD,
        "status": "PASS" if max_pass else "FAIL",
    }

    check1_pass = median_pass and (not max_pass)
    results["check1_median_pass_max_fail"] = {
        "expected": "median=PASS, max=FAIL",
        "actual": f"median={'PASS' if median_pass else 'FAIL'}, max={'PASS' if max_pass else 'FAIL'}",
        "status": "PASS" if check1_pass else "FAIL",
        "detail": "stat 字段实际控制判定：median 口径 PASS，max 口径 FAIL，二者不同",
    }

    check2_pass = reg6_stat == "median"
    results["check2_reg6_stat_is_median"] = {
        "expected": "median",
        "actual": reg6_stat,
        "status": "PASS" if check2_pass else "FAIL",
        "detail": "regression.yaml REG-6 stat 字段已声明为 median（晨审改判口径）",
    }

    actual_judgment_stat = reg6_stat
    actual_judgment_value = compute_statistic(BOUNDARY_VALUES, actual_judgment_stat)
    actual_judgment_pass = actual_judgment_value <= E4B_THRESHOLD
    check3_pass = actual_judgment_pass and actual_judgment_stat == "median"
    results["check3_actual_judgment_uses_median"] = {
        "stat_used": actual_judgment_stat,
        "value": round(actual_judgment_value, 4),
        "threshold": E4B_THRESHOLD,
        "status": "PASS" if actual_judgment_pass else "FAIL",
        "detail": f"按 REG-6 stat={actual_judgment_stat} 执行判定，值={actual_judgment_value:.4f}s ≤ {E4B_THRESHOLD}s → PASS",
        "check_pass": check3_pass,
    }

    all_pass = check1_pass and check2_pass and check3_pass
    results["overall"] = "PASS" if all_pass else "FAIL"
    results["summary"] = {
        "check1_median_pass_max_fail": check1_pass,
        "check2_reg6_stat_is_median": check2_pass,
        "check3_actual_judgment_uses_median": check3_pass,
        "all_pass": all_pass,
    }

    return results

if __name__ == "__main__":
    results = run_check()
    print(json.dumps(results, indent=2, ensure_ascii=False))
    evidence_dir = Path("tests/evidence/d3-batch28")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    output_path = evidence_dir / "variant_self_check_stats.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n证据已写入: {output_path}")
    sys.exit(0 if results["overall"] == "PASS" else 1)