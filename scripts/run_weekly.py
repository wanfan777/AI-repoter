#!/usr/bin/env python3
"""Run the weekly report pipeline."""
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
import subprocess
import sys
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run weekly report pipeline")
    parser.add_argument("--since_days", type=int, default=7)
    parser.add_argument("--topics", default="LLM,Agent")
    parser.add_argument("--lang", default="zh")
    parser.add_argument("--max_items", type=int, default=12)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--max_per_source", type=int, default=30)
    return parser.parse_args()


def run_step(step_name: str, command: Iterable[str]) -> bool:
    print(f"\n==> {step_name}: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        if result.stdout:
            print(result.stdout)
        return True

    print(f"[ERROR] Step failed: {step_name}")
    print(f"[ERROR] Exit code: {result.returncode}")
    if result.stdout:
        print("[ERROR] stdout:")
        print(result.stdout)
    if result.stderr:
        print("[ERROR] stderr:")
        print(result.stderr)
    return False


def main() -> int:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent
    out_dir = Path(args.out_dir) if args.out_dir else Path("runs") / dt.date.today().isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    steps = [
        (
            "collect_rss",
            base_dir / "collect_rss.py",
            [
                "--since_days",
                str(args.since_days),
                "--topics",
                args.topics,
                "--max_items",
                str(args.max_items),
                "--out_dir",
                str(out_dir),
                "--max_per_source",
                str(args.max_per_source),
            ],
        ),
        (
            "normalize",
            base_dir / "normalize.py",
            ["--out_dir", str(out_dir)],
        ),
        (
            "dedupe_rank",
            base_dir / "dedupe_rank.py",
            ["--out_dir", str(out_dir), "--max_items", str(args.max_items)],
        ),
        (
            "render_report",
            base_dir / "render_report.py",
            [
                "--out_dir",
                str(out_dir),
                "--topics",
                args.topics,
                "--lang",
                args.lang,
            ],
        ),
    ]

    failures = []
    for step_name, script_path, step_args in steps:
        if not script_path.exists():
            print(f"[ERROR] Missing script: {script_path}")
            failures.append(step_name)
            continue
        command = [sys.executable, str(script_path), *step_args]
        if not run_step(step_name, command):
            failures.append(step_name)

    report_path = out_dir / "report.md"
    if not report_path.exists():
        print("[ERROR] report.md not generated. Failing run.")
        return 1

    if failures:
        print("[WARN] Some steps failed but report.md exists:")
        for step in failures:
            print(f"  - {step}")
    else:
        print("[INFO] All steps completed successfully.")

    print(f"[INFO] Report generated: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
