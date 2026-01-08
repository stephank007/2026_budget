import re
import pandas as pd
from common_paths import DATA_CSV, DATA_INTERIM, MERGED_TABLES_XLSX

# =======================
# CONFIG
# =======================
# Folder containing the Camelot-generated CSVs
FOLDER = DATA_CSV
CSV_GLOB = "tables-page-*-table-*.csv"

# Output Excel file with all tables merged
OUT_EXCEL = MERGED_TABLES_XLSX

# Make sure required folders exist
DATA_CSV.mkdir(parents=True, exist_ok=True)
DATA_INTERIM.mkdir(parents=True, exist_ok=True)


N_COLS = 5
DATA_COLS = [f"col_{i}" for i in range(1, N_COLS + 1)]

NUM_COL = "col_3"     # signed numeric
HEBREW_COL = "col_4"  # Hebrew text (reverse)
DATE_COL = "col_5"    # dd/mm/yyyy, ffill+bfill per file

# =======================
# HELPERS
# =======================
def parse_page_table(name: str):
    m = re.search(r"page-(\d+)-table-(\d+)", name)
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)

def reverse_text(x):
    return x[::-1] if isinstance(x, str) else x

def is_row_empty(row):
    return all(str(v).strip() == "" for v in row)

def parse_signed_number(x):
    if x is None:
        return pd.NA
    s = str(x).strip()
    if s == "" or s == "-":
        return pd.NA

    s = (s.replace("\u2212", "-")
         .replace("\u2013", "-")
         .replace("\u2014", "-")
         .replace("\xa0", " "))

    s = re.sub(r"\s+", "", s).replace(",", "")

    if re.fullmatch(r"\(\d*\.?\d+\)", s):
        s = "-" + s.strip("()")

    if re.fullmatch(r"\d*\.?\d+-", s):
        s = "-" + s[:-1]

    if not re.fullmatch(r"[-+]?\d*\.?\d+", s):
        return pd.NA

    return pd.to_numeric(s, errors="coerce")

def parse_date_ddmmyyyy(x):
    if x is None:
        return pd.NaT
    s = str(x).strip()
    if s == "":
        return pd.NaT
    return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")

# =======================
# MERGE
# =======================
csv_files = sorted(FOLDER.glob(CSV_GLOB))
if not csv_files:
    raise FileNotFoundError(f"No files matching {CSV_GLOB} in {FOLDER.resolve()}")

dfs = []

for f in csv_files:
    df = pd.read_csv(f, header=None, dtype=str, keep_default_na=False)

    # Force exactly N_COLS columns
    if df.shape[1] < N_COLS:
        for _ in range(N_COLS - df.shape[1]):
            df[df.shape[1]] = ""
    elif df.shape[1] > N_COLS:
        df = df.iloc[:, :N_COLS]

    df.columns = DATA_COLS

    # Convert columns
    df[NUM_COL] = df[NUM_COL].apply(parse_signed_number)
    df[HEBREW_COL] = df[HEBREW_COL].apply(reverse_text)
    df[DATE_COL] = df[DATE_COL].apply(parse_date_ddmmyyyy)

    # ✅ Fill dates per-file: forward-fill then back-fill
    df[DATE_COL] = df[DATE_COL].ffill().bfill()

    # Drop fully empty rows
    df = df[~df[DATA_COLS].apply(is_row_empty, axis=1)]

    # Provenance columns
    page, table = parse_page_table(f.name)
    df.insert(0, "source_file", f.name)
    df.insert(1, "page", page)
    df.insert(2, "table", table)

    dfs.append(df)

merged = pd.concat(dfs, ignore_index=True)

# =======================
# WRITE EXCEL
# =======================
with pd.ExcelWriter(OUT_EXCEL, engine="openpyxl") as writer:
    merged.to_excel(writer, sheet_name="merged", index=False)

print(f"✅ Merged {len(csv_files)} CSVs into {OUT_EXCEL}")

# =======================
# CLEANUP
# =======================
for f in csv_files:
    try:
        f.unlink()
    except Exception as e:
        print(f"⚠️ Could not delete {f.name}: {e}")

print("🧹 CSV cleanup completed")
