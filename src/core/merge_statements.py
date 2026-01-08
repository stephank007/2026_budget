from pathlib import Path
from typing import List
import warnings

import pandas as pd

from common_paths import DATA_RAW_TRANSACTIONS, MERGED_ONE_SHEET_XLSX
from excel_formatter import format_workbook_default
from services.payee_service import (
    load_payee_resources,
    apply_payee_rules_and_categories,
)
from utils import (
    FIXED_RATES_TO_ILS,
    FOUR_DIGITS_PREFIX,
    normalize_header_text,
    normalize_payee,
    convert_to_ils_fixed_rate,
    split_filename,
    detect_sheet_currency,
    parse_amount_and_currency,
    find_header_row,
    guess_columns,
)

warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style",
)
warnings.filterwarnings(
    "ignore",
    message="Could not infer format, so each element will be parsed individually, falling back to `dateutil`.",
)

# Build a dedupe key:
def norm_payee_for_dedupe(p):
    if not isinstance(p, str):
        return ""
    p = normalize_payee(p)
    return " ".join(p.upper().split())

# -------------------------------------------------------------------
#  Main merge function
# -------------------------------------------------------------------
def merge_to_one_sheet_keep_dates(
    include_subfolders: bool = False   ,
    sheet_name        : str  = "Merged",
):
    """
    Merge all Excel files from {root_dir}/data/raw_transactions (by default) into a single sheet:
    - Dynamically detects header row and Hebrew/English column names.
    - Only rows with a valid parsed date in the chosen date column are kept.
    - Supports multiple date formats (dd.mm.yy, dd/mm/yyyy, yyyy-mm-dd, Excel datetime values, etc.).
    - Detects the amount column by headers starting with 'סכום' or 'amount'.
    - Tries to extract currency from the amount or a dedicated currency column,
      or falls back to the sheet-level currency (scanned from all text).
    - Converts all amounts to ILS using FIXED_RATES_TO_ILS.
    - Normalizes payee names using payee_rules.xlsx.
    - Categorizes payees using payee_lookup.xlsx.
    - Writes the merged result to MERGED_ONE_SHEET_XLSX.

    Files and layout expectations (relative to root_dir):
        data/raw_transactions/*.xlsx - raw bank Excel files
        config/payee_rules.xlsx      - rules for payee normalization
        config/payee_lookup.xlsx     - lookup table with categories
    """
    
    input_dir = DATA_RAW_TRANSACTIONS
    output_path = MERGED_ONE_SHEET_XLSX
    
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    
    # Load rules and lookup once
    rules_df, lookup_df = load_payee_resources()
    
    # Collect all Excel files
    glob_pattern = "**/*.xls*" if include_subfolders else "*.xls*"
    candidates = sorted(input_dir.glob(glob_pattern))
    
    files = [
        p
        for p in candidates
        if p.is_file()
           and not p.name.startswith("~$")  # ignore Excel temp/lock files
           and FOUR_DIGITS_PREFIX.match(p.stem)  # starts with 4 digits
    ]
    
    if not files:
        raise RuntimeError(
            f"No Excel files starting with 4 digits found in: {input_dir}",
        )
    
    all_frames: List[pd.DataFrame] = []
    
    # Process each file
    for f in files:
        try:
            df_raw: pd.DataFrame = pd.read_excel(f, sheet_name=0, dtype=object)
            
            # Detect header row inside the sheet
            header_idx = find_header_row(df_raw)
            
            if header_idx is None:
                # Fallback: assume the file already has a proper header row
                # (like 9016_12_2025.xlsx with columns: date, payee, amount)
                header = [normalize_header_text(c) for c in df_raw.columns]
                df_sheet = df_raw.copy()
                df_sheet.columns = header
                df_sheet = df_sheet.dropna(how="all")
            else:
                # Classic bank-export case: header is a row in the sheet body
                header = [normalize_header_text(v) for v in df_raw.iloc[header_idx]]
                df_sheet = df_raw.iloc[header_idx + 1:].copy()
                df_sheet.columns = header
                df_sheet = df_sheet.dropna(how="all")
            
            # Guess important columns
            date_col, payee_col, expense_col, currency_col = guess_columns(
                list(df_sheet.columns),
            )
            
            if not date_col or not payee_col or not expense_col:
                raise RuntimeError(
                    f"Could not detect required columns (date/payee/amount). "
                    f"Detected: date={date_col}, payee={payee_col}, "
                    f"expense={expense_col}, currency={currency_col}",
                )
            
            # Detect sheet-level currency from any text (header, notes, etc.)
            sheet_currency = detect_sheet_currency(df_raw)
            
            # Build normalized frame
            df_norm = pd.DataFrame()
            df_norm["date" ] = df_sheet[date_col ]
            df_norm["payee"] = df_sheet[payee_col]
            
            # Parse expense & currency per row
            expenses  : List[float] = []
            currencies: List[str  ] = []
            
            if currency_col:
                cur_series = df_sheet[currency_col]
                for a, c_val in zip(df_sheet[expense_col], cur_series):
                    amt, cur = parse_amount_and_currency(
                        a,
                        default_currency=sheet_currency,
                    )
                    # If currency column has explicit info, override
                    if isinstance(c_val, str):
                        for token in FIXED_RATES_TO_ILS.keys():
                            if token and token in c_val:
                                cur = token
                                break
                    expenses.append(amt)
                    currencies.append(cur)
            else:
                # No explicit currency column: try to parse from amount text;
                # otherwise fall back to sheet-level currency.
                for a in df_sheet[expense_col]:
                    amt, cur = parse_amount_and_currency(
                        a,
                        default_currency=sheet_currency,
                    )
                    expenses.append(amt)
                    currencies.append(cur)
            
            df_norm["expense"] = expenses
            df_norm["currency"] = currencies
            
            # Parse the date column FLEXIBLY
            df_norm["run_date"] = pd.to_datetime(
                df_norm["date"],
                dayfirst=True,
                errors="coerce",
            )
            
            # Drop rows where we couldn't parse a date
            df_norm = df_norm[~df_norm["run_date"].isna()].copy()
            if df_norm.empty:
                raise RuntimeError("All rows had invalid/unparseable dates.")
            
            # Convert amounts to ILS
            df_norm = convert_to_ils_fixed_rate(
                df_norm,
                amount_col="expense",
                currency_col="currency",
            )
            
            # Parse account from filename (ignore filename month)
            acc, _ = split_filename(f.name)
            df_norm["account"] = acc
            
            # run_month must come from the transaction date only
            df_norm["run_month"] = df_norm["run_date"].dt.strftime("%Y-%m")
            
            # Basic payee cleanup
            df_norm["payee"] = df_norm["payee"].astype(str).str.strip()
            
            # ---------------------------
            # Normalize payee via rules
            # ---------------------------
            df_norm = apply_payee_rules_and_categories(
                df_norm,
                rules_df=rules_df,
                lookup_df=lookup_df,
                payee_col="payee",
            )
            
            if not df_norm.empty:
                all_frames.append(df_norm)
            
            log_month = df_norm["run_month"].iloc[0]
            print(
                f"{f.name} - {acc}-{log_month}: "
                f"read {len(df_sheet)} rows, kept {len(df_norm)}",
            )
        
        except Exception as e:
            print(f"Skipped {f.name} (error: {e})")
    
    if not all_frames:
        raise RuntimeError(
            "No transactions found: none of the Excel files in data/seed "
            "contained recognizable dates + amounts with the expected structure.",
        )
    
    # Final merge
    merged = pd.concat(all_frames, ignore_index=True)
    merged["dedupe_date"] = pd.to_datetime(
        merged["run_date"],
        dayfirst=True,
        errors="coerce"
    ).dt.date
    
    merged["dedupe_payee"] = merged["payee"].apply(norm_payee_for_dedupe)
    merged["dedupe_amount"] = pd.to_numeric(merged["expense"], errors="coerce")
    
    # Drop duplicates, keeping the first occurrence
    merged = merged.drop_duplicates(
        subset=["account", "dedupe_date", "dedupe_payee", "dedupe_amount"],
        keep="first"
    ).reset_index(drop=True)
    
    # -----------------------------------------------
    # Standardize ALL date formats -> dd.mm.yy
    # -----------------------------------------------
    for col in ["run_date", "date"]:
        if col in merged.columns:
            merged[col] = pd.to_datetime(
                merged[col],
                dayfirst=True,
                errors="coerce",
            ).dt.strftime("%d.%m.%y")
    
    # Clean up expense values
    merged["expense"] = pd.to_numeric(merged["expense"], errors="coerce").round(2)
    merged["expense"] = merged["expense"].apply(lambda x: f"{x:.2f}")
    merged["expense"] = pd.to_numeric(merged["expense"], errors="coerce")
    
    # Final payee normalization
    merged["payee"] = merged["payee"].apply(normalize_payee)
    
    # -----------------------------------------------
    # Sort unresolved payees first ("לא מזוהה")
    # -----------------------------------------------
    if "לא מזוהה" in merged.columns:
        merged = merged.sort_values(
            by="לא מזוהה",
            ascending=False,  # unresolved first
            kind="stable",
        ).reset_index(drop=True)
        
    # Write to Excel + formatting
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        merged.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        
        wb = writer.book
        ws = wb[sheet_name[:31]]
        
        # Apply default Excel formatting to all sheets:
        # - header style (bold, centered, light blue)
        # - auto column widths
        # - auto-filter used range
        # - freeze top row (A2)
        # - detect & format amount-like columns
        format_workbook_default(wb)
        
        # Script-specific: ensure EXPENSE column is numeric with thousands + 2 decimals
        expense_col_idx = None
        for i, cell in enumerate(ws[1], start=1):
            if str(cell.value).lower() == "expense":
                expense_col_idx = i
                break
        
        if expense_col_idx:
            for row in range(2, ws.max_row + 1):
                cell = ws.cell(row=row, column=expense_col_idx)
                cell.number_format = "#,##0.00"
    
    print(f"\nDone. Wrote {len(merged)} rows to: {output_path.resolve()}")
    return output_path
    
    print(f"\nDone. Wrote {len(merged)} rows to: {output_path.resolve()}")
    return output_path


# -------------------------------------------------------------------
#  CLI entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    merge_to_one_sheet_keep_dates()
