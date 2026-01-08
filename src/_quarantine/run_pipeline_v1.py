# src/core/run_pipeline_v1.py

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from typing import List

import pandas as pd

from common_paths import PROJECT_ROOT, DATA_RAW_TRANSACTIONS
from db import (
    get_client,
    get_db,
    get_transactions_collection,
    ensure_transactions_indexes,
    insert_transactions_df,
)
from payee_service import (
    load_payee_resources,
    apply_payee_rules_and_categories,
)
from utils import (
    FIXED_RATES_TO_ILS,
    normalize_header_text,
    normalize_payee as base_normalize_payee,
    convert_to_ils_fixed_rate,
    split_filename,
    detect_sheet_currency,
    parse_amount_and_currency,
    find_header_row,
    guess_columns,
)


# -------------------------------------------------------------------
#  Normalization helpers for dedupe
# -------------------------------------------------------------------

def dedupe_normalize_payee(p: str) -> str:
    """
    Stronger normalization for deduplication:
    - use existing project-level normalize_payee
    - uppercase and collapse whitespace
    """
    p = base_normalize_payee(p)
    if not isinstance(p, str):
        return ""
    return " ".join(p.upper().split())


def normalize_amount(amount) -> float:
    """Normalize amount as a float with 2 decimal places."""
    return round(float(amount), 2)


def normalize_date_iso(d) -> str:
    """Normalize date to ISO 'YYYY-MM-DD' using pandas parsing."""
    return pd.to_datetime(d).date().isoformat()


def add_dedupe_fields(df: pd.DataFrame,
                      account_last4: str,
                      source_file: str) -> pd.DataFrame:
    """
    Add account, source_file and the deduplication hash column 'txn_hash'.
    Logical key: account | norm_date | norm_payee | norm_amount
    """
    df = df.copy()
    
    df["account"] = account_last4
    df["source_file"] = source_file
    
    df["norm_date"] = df["run_date"].apply(normalize_date_iso)
    df["norm_payee"] = df["payee"].apply(dedupe_normalize_payee)
    df["norm_amount"] = df["expense"].apply(normalize_amount)
    
    concat = (
            df["account"] + "|" +
            df["norm_date"] + "|" +
            df["norm_payee"] + "|" +
            df["norm_amount"].astype(str)
    )
    
    df["txn_hash"] = concat.apply(
        lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest()
    )
    
    return df


# -------------------------------------------------------------------
#  Excel loading + mapping (mirror merge_statements logic)
# -------------------------------------------------------------------

def load_and_normalize_excel(
    path: Path,
    account_last4: str,
    rules_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Read a single Excel statement using the same logic as merge_to_one_sheet_keep_dates:
      - detect internal header row
      - normalize headers
      - guess date/payee/amount/currency columns
      - parse amounts & currency
      - parse date flexibly (Hebrew/English, multiple formats)
      - convert all amounts to ILS
      - apply payee rules + categories

    Returns a DataFrame with at least:
      ["date", "payee", "expense", "currency", "run_date", "run_month", "account",
       "txn_hash", ...]
    """
    print(f"  [INFO] Reading Excel: {path}")
    # read as objects so header detection & parsing work like in merge_statements
    df_raw = pd.read_excel(path, sheet_name=0, dtype=object)
    
    # 1) detect header row
    header_idx = find_header_row(df_raw)
    
    if header_idx is None:
        # fallback: assume first row is header
        header = [normalize_header_text(c) for c in df_raw.columns]
        df_sheet = df_raw.copy()
        df_sheet.columns = header
        df_sheet = df_sheet.dropna(how="all")
    else:
        # header row is somewhere in the body
        header = [normalize_header_text(v) for v in df_raw.iloc[header_idx]]
        df_sheet = df_raw.iloc[header_idx + 1 :].copy()
        df_sheet.columns = header
        df_sheet = df_sheet.dropna(how="all")
    
    # 2) guess columns
    date_col, payee_col, expense_col, currency_col = guess_columns(
        list(df_sheet.columns),
    )
    
    if not date_col or not payee_col or not expense_col:
        raise RuntimeError(
            f"Could not detect required columns (date/payee/amount) in {path.name}. "
            f"Detected: date={date_col}, payee={payee_col}, "
            f"expense={expense_col}, currency={currency_col}",
        )
    
    # 3) detect sheet-level currency from any text (header, notes, etc.)
    sheet_currency = detect_sheet_currency(df_raw)
    
    # 4) build normalized frame
    df_norm = pd.DataFrame()
    df_norm["date"] = df_sheet[date_col]
    df_norm["payee"] = df_sheet[payee_col]
    
    expenses: List[float] = []
    currencies: List[str] = []
    
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
        # No explicit currency column: try to parse from amount text,
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
    
    # 5) parse date flexibly, day-first, like merge_statements
    df_norm["run_date"] = pd.to_datetime(
        df_norm["date"],
        dayfirst=True,
        errors="coerce",
        format="mixed"
    )
    
    # drop rows with invalid / unparsable dates
    df_norm = df_norm[~df_norm["run_date"].isna()].copy()
    if df_norm.empty:
        raise RuntimeError(f"All rows had invalid/unparseable dates in {path.name}.")
    
    # 6) convert all amounts to ILS (amount column = 'expense')
    df_norm = convert_to_ils_fixed_rate(
        df_norm,
        amount_col="expense",
        currency_col="currency",
    )
    
    # 7) account + run_month
    df_norm["account"] = account_last4
    df_norm["run_month"] = df_norm["run_date"].dt.strftime("%Y-%m")
    
    # basic payee cleanup
    df_norm["payee"] = df_norm["payee"].astype(str).str.strip()
    
    # 8) apply payee rules + categories (same as merge_statements)
    df_norm = apply_payee_rules_and_categories(
        df_norm,
        rules_df=rules_df,
        lookup_df=lookup_df,
        payee_col="payee",
    )
    
    # 9) add dedupe fields + txn_hash
    df_norm = add_dedupe_fields(
        df_norm,
        account_last4=account_last4,
        source_file=path.name,
    )
    
    return df_norm


# -------------------------------------------------------------------
#  File processing
# -------------------------------------------------------------------

def process_file(path: Path,
                 rules_df: pd.DataFrame,
                 lookup_df: pd.DataFrame,
                 coll) -> None:
    """
    Process a single Excel file:
      - Parse filename (account_last4, run_month) via split_filename
      - Skip account 9016 (reserved for pipeline v2)
      - Load + normalize Excel
      - Insert into MongoDB with dedupe
    """
    rel_name = path.name
    
    try:
        account_last4, run_month = split_filename(rel_name)
    except ValueError as e:
        print(f"[SKIP] {rel_name}: {e}")
        return
    
    # Skip the special account 9016 (handled by pipeline v2)
    if account_last4 == "9016":
        print(f"[SKIP] {rel_name}: account 9016 handled by pipeline v2.")
        return
    
    print(f"[FILE] {rel_name} -> account {account_last4}, run_month={run_month}")
    
    df = load_and_normalize_excel(
        path,
        account_last4=account_last4,
        rules_df=rules_df,
        lookup_df=lookup_df,
    )
    
    insert_transactions_df(df, coll)


# -------------------------------------------------------------------
#  Main entry point
# -------------------------------------------------------------------

def main() -> None:
    project_root = PROJECT_ROOT
    raw_dir = DATA_RAW_TRANSACTIONS
    
    print(f"[INFO] Project root:     {project_root}")
    print(f"[INFO] Raw XLSX folder:  {raw_dir}\n")
    
    if not raw_dir.exists():
        print(f"[ERROR] Raw directory does not exist: {raw_dir}")
        sys.exit(1)
    
    # Connect to Mongo and ensure indexes once
    client = get_client()
    db = get_db(client)
    coll = get_transactions_collection(db)
    ensure_transactions_indexes(coll)
    
    # Load payee rules / lookup once (same as merge_statements)
    rules_df, lookup_df = load_payee_resources()
    
    # Collect Excel files (only in raw_transactions, no subfolders for now)
    xlsx_files = sorted(raw_dir.glob("*.xls*"))
    
    if not xlsx_files:
        print("[INFO] No .xls* files found. Nothing to do.")
        return
    
    for path in xlsx_files:
        try:
            process_file(path, rules_df=rules_df, lookup_df=lookup_df, coll=coll)
        except Exception as e:
            print(f"[ERROR] Failed processing {path.name}: {e}")
    
    print("[DONE] Pipeline v1 completed.\n")


if __name__ == "__main__":
    main()
