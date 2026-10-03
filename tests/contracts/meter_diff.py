#!/usr/bin/env python3
"""meter_diff.py — 计量对账（红线级）

读 IDE/平台会话计量（含 T 计数）与助手自报 token，diff>10% → FAIL 入 MISBEHAVIOR。
若平台计量不可程序化读取，申报具体障碍，改为晨报首屏双数字申报（IDE 计数+自报值），人工 diff。

用法:
  python meter_diff.py --self-report-tokens 36000 --ide-tokens 35000 --output tests/evidence/d3-batch30/meter_diff.json
  python meter_diff.py --self-report-tokens 36000 --ide-metrics-unavailable --output tests/evidence/d3-batch30/meter_diff.json
"""

import argparse
import json
import sys
import time
from pathlib import Path


def compute_diff(ide_tokens, self_report_tokens):
    if self_report_tokens == 0:
        return 0.0
    return abs(ide_tokens - self_report_tokens) / self_report_tokens * 100.0


def main():
    parser = argparse.ArgumentParser(description="meter_diff.py 计量对账")
    parser.add_argument("--self-report-tokens", type=int, required=True, help="助手自报 token 数")
    parser.add_argument("--ide-tokens", type=int, default=None, help="IDE 计量 token 数")
    parser.add_argument("--ide-metrics-unavailable", action="store_true", help="IDE 计量不可程序化读取")
    parser.add_argument("--barrier-detail", default="", help="IDE 计量不可读的具体障碍说明")
    parser.add_argument("--threshold-percent", type=float, default=10.0, help="diff 阈值（百分比，默认 10%%）")
    parser.add_argument("--equation-check", action="store_true", help="等式校验模式: 截图差值=自报之和")
    parser.add_argument("--screenshot-diff", type=float, default=None, help="IDE 截图差值（等式校验模式）")
    parser.add_argument("--prior-batch-tokens", type=float, default=0, help="前批自报 token（等式校验模式）")
    parser.add_argument("--output", required=True, help="输出路径")
    args = parser.parse_args()

    result = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "self_report_tokens": args.self_report_tokens,
        "threshold_percent": args.threshold_percent,
    }

    if args.equation_check and args.screenshot_diff is not None:
        sum_self_report = args.prior_batch_tokens + args.self_report_tokens
        diff_abs = abs(args.screenshot_diff - sum_self_report)
        precision = 0.1
        result["mode"] = "equation_check"
        result["screenshot_diff"] = args.screenshot_diff
        result["prior_batch_tokens"] = args.prior_batch_tokens
        result["sum_self_report"] = sum_self_report
        result["diff_abs"] = round(diff_abs, 1)
        result["precision"] = precision
        if diff_abs <= precision:
            result["status"] = "PASS"
            result["misbehavior"] = None
            result["detail"] = f"截图差值={args.screenshot_diff} == 自报之和={sum_self_report}（精度{precision}K）"
        else:
            result["status"] = "FAIL"
            result["misbehavior"] = "MB-006: 等式校验不通过"
            result["detail"] = f"截图差值={args.screenshot_diff} != 自报之和={sum_self_report}（差{diff_abs}K）"
    elif args.ide_metrics_unavailable:
        result["mode"] = "manual_dual_number"
        result["ide_tokens"] = None
        result["diff_percent"] = None
        result["status"] = "MANUAL"
        result["barrier"] = args.barrier_detail or "IDE 计量不可程序化读取"
        result["action"] = "改为晨报首屏双数字申报（IDE 计数+自报值），人工 diff"
        result["misbehavior"] = None
    elif args.ide_tokens is not None:
        diff = compute_diff(args.ide_tokens, args.self_report_tokens)
        result["mode"] = "automated"
        result["ide_tokens"] = args.ide_tokens
        result["diff_percent"] = round(diff, 2)
        if diff > args.threshold_percent:
            result["status"] = "FAIL"
            result["misbehavior"] = "MB-006: token 计量对账 diff>{threshold}%，入 MISBEHAVIOR"
        else:
            result["status"] = "PASS"
            result["misbehavior"] = None
    else:
        result["mode"] = "no_ide_tokens"
        result["ide_tokens"] = None
        result["diff_percent"] = None
        result["status"] = "MANUAL"
        result["barrier"] = "未提供 --ide-tokens，也未声明 --ide-metrics-unavailable"
        result["action"] = "请在晨报首屏双数字申报（IDE 计数+自报值），人工 diff"
        result["misbehavior"] = None

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"meter_diff.py — 计量对账")
    print(f"mode: {result['mode']}")
    print(f"self_report: {result['self_report_tokens']} tokens")
    if result.get("ide_tokens") is not None:
        print(f"ide: {result['ide_tokens']} tokens")
    if result.get("diff_percent") is not None:
        print(f"diff: {result['diff_percent']}% (threshold: {result['threshold_percent']}%)")
    print(f"status: {result['status']}")
    if result.get("barrier"):
        print(f"barrier: {result['barrier']}")
    if result.get("misbehavior"):
        print(f"misbehavior: {result['misbehavior']}")
    print(f"output: {args.output}")


if __name__ == "__main__":
    main()