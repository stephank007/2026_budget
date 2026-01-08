#!/usr/bin/env python3
"""
run_pipeline.py

Pipeline order:

1) data_fixer       -> src/core/bank-fixer.py
2) merge_statements -> src/core/merge_statements.py
3) income_run_total -> data/raw_transactions/income_run_total.py
4) app.py           -> Dash app at project root

Usage (from project root):

    python run_pipeline.py
    python run_pipeline.py --no-app
    python run_pipeline.py --from merge
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Sequence


# ---------------------------------------------------------------------------
# Path setup (make imports reliable)
# ---------------------------------------------------------------------------

def _detect_project_root() -> Path:
    """
    Best-effort project root detection:
    - Prefer: folder containing this file
    - Must contain: 'src' directory
    """
    here = Path(__file__).resolve()
    root = here.parent
    if (root / "src").exists():
        return root
    # Fallback: try parents
    for p in here.parents:
        if (p / "src").exists():
            return p
    # If all fails, assume current working directory
    cwd = Path.cwd().resolve()
    if (cwd / "src").exists():
        return cwd
    raise RuntimeError(
        "Could not detect project root (expected a folder containing 'src/'). "
        "Run this script from the project root."
    )


PROJECT_ROOT_GUESS = _detect_project_root()
SRC_DIR = PROJECT_ROOT_GUESS / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


# ---------------------------------------------------------------------------
# Canonical paths (from common_paths)
# ---------------------------------------------------------------------------

try:
    # common_paths.py is expected to live inside src/
    from common_paths import PROJECT_ROOT, DATA_RAW_TRANSACTIONS  # type: ignore
except Exception as exc:
    raise RuntimeError(
        "Could not import 'common_paths'.\n"
        "Expected: <project_root>/src/common_paths.py\n"
        f"Detected project root guess: {PROJECT_ROOT_GUESS}\n"
        f"sys.path[0]: {sys.path[0] if sys.path else '<empty>'}\n"
    ) from exc

# Scripts
DATA_FIXER_SCRIPT = PROJECT_ROOT / "src" / "core" / "bank-fixer.py"
MERGE_STATEMENTS_SCRIPT = PROJECT_ROOT / "src" / "core" / "merge_statements.py"
INCOME_RUN_TOTAL_SCRIPT = DATA_RAW_TRANSACTIONS / "income_run_total.py"
APP_SCRIPT = PROJECT_ROOT / "app.py"

STEPS = ["fixer", "merge", "income", "app"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_step(
    *,
    name: str,
    script: Path,
    cwd: Path,
    extra_args: Sequence[str] | None = None,
) -> None:
    """
    Run a single Python script as a subprocess with logging & error handling.
    """
    extra_args = list(extra_args or [])
    
    if not script.exists():
        raise SystemExit(f"[{name}] Script not found: {script}")
    
    cmd = [sys.executable, str(script), *extra_args]
    
    print("\n" + "=" * 78)
    print(f"[{name}] CWD : {cwd}")
    print(f"[{name}] CMD : {' '.join(cmd)}")
    print("=" * 78 + "\n", flush=True)
    
    try:
        result = subprocess.run(cmd, cwd=str(cwd), check=False)
    except FileNotFoundError as e:
        raise SystemExit(f"[{name}] Failed to start process: {e}") from e
    
    if result.returncode != 0:
        raise SystemExit(f"[{name}] failed with exit code {result.returncode}")
    
    print(f"\n[{name}] finished successfully ✅\n", flush=True)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run banking pipeline: fixer -> merge -> income -> app"
    )
    
    parser.add_argument(
        "--no-app",
        action="store_true",
        help="Run only data-prep steps (skip launching app.py).",
    )
    
    parser.add_argument(
        "--from",
        dest="from_step",
        choices=STEPS,
        default="fixer",
        help="Start pipeline from a specific step (runs that step and everything after it).",
    )
    
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    
    start_i = STEPS.index(args.from_step)
    
    # Determine what to run
    run_fixer = start_i <= STEPS.index("fixer")
    run_merge = start_i <= STEPS.index("merge")
    run_income = start_i <= STEPS.index("income")
    run_app = (not args.no_app) and (start_i <= STEPS.index("app"))
    
    # 1) bank-fixer (expects PROJECT_ROOT as cwd)
    if run_fixer:
        run_step(name="data_fixer", script=DATA_FIXER_SCRIPT, cwd=PROJECT_ROOT)
    
    # 2) merge_statements (expects PROJECT_ROOT as cwd)
    if run_merge:
        run_step(name="merge_statements", script=MERGE_STATEMENTS_SCRIPT, cwd=PROJECT_ROOT)
    
    # 3) income_run_total (expects DATA_RAW_TRANSACTIONS as cwd)
    if run_income:
        run_step(name="income_run_total", script=INCOME_RUN_TOTAL_SCRIPT, cwd=DATA_RAW_TRANSACTIONS)
    
    # 4) app.py (expects PROJECT_ROOT as cwd)
    if run_app:
        print("Launching Dash app (app.py). Press Ctrl+C to stop.\n", flush=True)
        run_step(name="app", script=APP_SCRIPT, cwd=PROJECT_ROOT)
    
    print("Pipeline completed. ✨")


if __name__ == "__main__":
    main()
