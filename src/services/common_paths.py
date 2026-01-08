from pathlib import Path

# This module centralizes all the important filesystem paths for the project.
# It assumes the following layout:
#
# project_root/
#   app.py
#   run_pipeline.py
#   config/
#   data/
#     raw_transactions/
#     processed/
#     csv/          (created by the PDF extraction pipeline)
#     interim/      (optional, used for temporary Excel files)
#   src/
#     common_paths.py
#     core/
#
# If you change the folder layout, you should only need to update this file.

# Path to the project root (the directory that contains `src/`, `data/`, `config/`, etc.)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
print(PROJECT_ROOT)

# Config directory (Excel + JSON config files)
CONFIG_DIR = PROJECT_ROOT.joinpath("config")

# Data directories
DATA_DIR = PROJECT_ROOT.joinpath("data")
DATA_9016 = DATA_DIR.joinpath("9016")
DATA_RAW_TRANSACTIONS = DATA_DIR.joinpath("raw_transactions")
DATA_PROCESSED = DATA_DIR.joinpath("processed")

# Intermediate data directories (created if needed at runtime)
# DATA_CSV = DATA_DIR.joinpath("csv")          # for Camelot CSV exports
# DATA_INTERIM = DATA_DIR.joinpath("interim")  # for temporary Excel files, e.g. tables.xlsx / merged_tables.xlsx

# Concrete files
xls_files = list(DATA_9016.glob("*.xlsx"))
#
if len(xls_files) == 0:
    raise FileNotFoundError(f"No  files found in {DATA_9016}")

if len(xls_files) > 1:
    raise RuntimeError(
        f"Expected exactly one AccountActivity.xlsx in {DATA_9016}, found {len(xls_files)}: "
        f"{[p.name for p in xls_files]}"
    )

TNUOT_9016 = xls_files[0]

PAYEE_LOOKUP_XLSX = CONFIG_DIR.joinpath("payee_lookup.xlsx")
PAYEE_RULES_XLSX  = CONFIG_DIR.joinpath("payee_rules.xlsx")
CONFIG_JSON_PATH  = CONFIG_DIR.joinpath("config.json")

# Outputs of the Excel-merge pipeline
MERGED_ONE_SHEET_XLSX = DATA_PROCESSED.joinpath("merged_one_sheet.xlsx")
