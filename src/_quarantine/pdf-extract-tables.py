import re
from pathlib import Path
import camelot
import pandas as pd
from common_paths import DATA_CSV, DATA_INTERIM, TNUOT_PDF

def pad_page_number_in_name(name):
    return re.sub(
        r"page-(\d+)-",
        lambda m: f"page-{int(m.group(1)):02}-",
        name
    )
root = Path(__file__).parent.parent
src  = root.joinpath("raw_transactions")
pdf_path = src.joinpath("tnuot.pdf").resolve()

# Ensure the output folders exist
DATA_CSV.mkdir(parents=True, exist_ok=True)
DATA_INTERIM.mkdir(parents=True, exist_ok=True)

# PDF input file
pdf_path = TNUOT_PDF.resolve()

# Extract tables
tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
if tables.n == 0:
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream")

print("tables found:", tables.n)

# Export CSVs (Camelot-controlled naming) into data/csv
tables.export(str(DATA_CSV.joinpath("tables.csv")), f="csv")

# ✅ Rename generated CSV files so page numbers are zero-padded
for csv_file in DATA_CSV.glob("tables-page-*-table-*.csv"):
    new_name = pad_page_number_in_name(csv_file.name)
    if new_name != csv_file.name:
        csv_file.rename(csv_file.with_name(new_name))

# Optional Excel export (for inspection) into data/interim
tables_xlsx = DATA_INTERIM.joinpath("tables.xlsx")
with pd.ExcelWriter(tables_xlsx) as writer:
    for i, t in enumerate(tables):
        t.df.to_excel(writer, sheet_name=f"table_{i+1:02}", index=False)