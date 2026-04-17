#!/usr/bin/env python3
from __future__ import annotations

import argparse

from app.config import get_settings
from app.stress.drills import SCENARIO_NAMES, run_named_drill


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a non-production paper burn-in stress drill.")
    parser.add_argument("--scenario", choices=SCENARIO_NAMES, required=True)
    parser.add_argument("--log-dir", default="logs/burnin")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run_named_drill(
        args.scenario,
        settings=get_settings(),
        log_dir=args.log_dir,
    )
    print(f"scenario={result.scenario}")
    print(f"severity={result.severity}")
    print(f"reason={result.reason}")
    print(f"expected={result.expected_behavior}")
    print(f"observed={result.observed_behavior}")
    print(f"log_path={result.log_path}")
    print("follow_up_commands:")
    for command in result.follow_up_commands:
        print(f"- {command}")


if __name__ == "__main__":
    main()
