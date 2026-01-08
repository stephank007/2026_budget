"""
Pipeline runner:

2. Run pdf-extract-tables         -> CSV files in data/csv
3. Run pdf-merge-csv-tables      -> merged_tables.xlsx
4. Run merge_fix                  -> 9016_mm_YYYY.xlsx (in project root)
5. Delete leftover tables.xlsx and merged_tables.xlsx

Run from project root:
    python run_pipeline.py

Note:
    All Python tools executed by this script are expected to live under:
        <project_root>/src/bank
"""

from pathlib import Path
import shutil
import subprocess
import sys

from common_paths import DATA_CSV


# ---------------- Utility helpers ---------------- #

def cleanup_seed_csv(seed_csv_dir: Path) -> None:
    """Delete everything inside data/seed/csv (if it exists)."""
    if not seed_csv_dir.exists():
        print(f"[INFO] Creating seed CSV directory: {seed_csv_dir}")
        seed_csv_dir.mkdir(parents=True, exist_ok=True)
        return
    
    print(f"[INFO] Cleaning seed CSV directory: {seed_csv_dir}")
    for entry in seed_csv_dir.iterdir():
        if entry.is_file() or entry.is_symlink():
            entry.unlink()
        elif entry.is_dir():
            shutil.rmtree(entry)
    print("[OK] Cleanup completed.\n")


def run_tool(stem: str, project_root: Path) -> None:
    """
    Run a tool that lives under src/bank (preferred) or in the project root.

    Search order (first match wins):
      1) <project_root>/src/bank/<stem>.py via Python
      2) <project_root>/src/bank/<stem>   as executable script
      3) <project_root>/<stem>.py         via Python  (legacy fallback)
      4) <project_root>/<stem>            as executable script (legacy fallback)
    """
    preprocess_dir = project_root / "src" / "pre-process"
    
    # Prefer Python scripts in src/pre-process
    script_py_candidates = [
        preprocess_dir / f"{stem}.py",
        project_root / f"{stem}.py",  # fallback to project root
    ]
    script_exe_candidates = [
        preprocess_dir / stem,
        project_root / stem,  # fallback to project root
    ]
    
    script_py = next((p for p in script_py_candidates if p.exists()), None)
    script_exe = next((p for p in script_exe_candidates if p.exists()), None)
    
    if script_py is not None:
        cmd = [sys.executable, str(script_py)]
        display = script_py
    elif script_exe is not None:
        cmd = [str(script_exe)]
        display = script_exe
    else:
        search_paths = "\n  - ".join(
            [str(p) for p in script_py_candidates + script_exe_candidates]
        )
        raise SystemExit(
            f"[ERROR] Could not find script '{stem}'.\n"
            f"Tried:\n  - {search_paths}\n"
            f"Make sure one of these exists."
        )
    
    print(f"[STEP] Running {display} ...")
    # Keep working directory at project_root so relative paths inside tools still work
    subprocess.run(cmd, cwd=str(project_root), check=True)
    print(f"[OK] {display} finished.\n")


def delete_file_if_exists(path: Path, label: str) -> None:
    """Delete a given file if it exists."""
    if path.exists():
        print(f"[INFO] Deleting leftover file: {label}")
        path.unlink()
        print(f"[OK] {label} deleted.\n")
    else:
        print(f"[INFO] {label} not found — nothing to delete.\n")


# ---------------- Main runner ---------------- #

def main() -> None:
    project_root = Path(__file__).resolve().parent
    print(f"[INFO] Project root: {project_root}\n")
    
    # 2. Run pdf-extract-tables -> CSV files in data/csv
    run_tool("pdf-extract-tables", project_root)
    
    # 3. Run pdf-merge-csv-tables -> merged_tables.xlsx
    run_tool("pdf-merge-csv-tables", project_root)
    
    # 4. Run merge_fix -> 9016_mm_YYYY.xlsx (in project root)
    run_tool("pdf-merge-fix", project_root)
    
    # 5. Delete temporary Excel files in project root
    # delete_file_if_exists(DATA_INTERIM / "tables.xlsx", "tables.xlsx")
    # delete_file_if_exists(MERGED_TABLES_XLSX, "merged_tables.xlsx")
    print("[DONE] Full pipeline completed.\n")


if __name__ == "__main__":
    main()
