# src/core/run_pipeline_v2_9016.py

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import List

import pandas as pd

from common_paths import PROJECT_ROOT, DATA_9016, TNUOT_9016
from _quarantine.db import (
    get_client,
    get_db,
    get_transactions_collection,
    ensure_transactions_indexes,
    insert_transactions_df,
)
from services.payee_service import (
    load_payee_resources,
    apply_payee_rules_and_categories,
)
from utils import (
    FIXED_RATES_TO_ILS,
    normalize_header_text,
    normalize_payee as base_normalize_payee,
    convert_to_ils_fixed_rate,
    detect_sheet_currency,
    parse_amount_and_currency,
    find_header_row,
    guess_columns,
)


ACCOUNT_9016 = "9016"


# -------------------------------------------------------------------
#  Dedup helpers (same logic as v1)
# -------------------------------------------------------------------

def dedupe_normalize_payee(p: str) -> str:
    p = base_normalize_payee(p)
    if not isinstance(p, str):
        return ""
    return " ".join(p.upper().split())


def normalize_amount(amount) -> float:
    return round(float(amount), 2)


def normalize_date_iso(d) -> str:
    return pd.to_datetime(d).date().isoformat()


def add_dedupe_fields(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Add account, source_file and the deduplication hash column 'txn_hash'.
    Logical key: account | norm_date | norm_payee | norm_amount
    """
    df = df.copy()
    
    df["account"] = ACCOUNT_9016
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
#  Load the inspected 9016 Excel and normalize like merge_statements
# -------------------------------------------------------------------

def load_and_normalize_9016(path: Path,
                            rules_df: pd.DataFrame,
                            lookup_df: pd.DataFrame) -> pd.DataFrame:
    """
    Read the inspected 9016 statement (output of bank-fixer) and normalize it.

    This mirrors the logic in merge_to_one_sheet_keep_dates:
      - detect internal header row
      - normalize headers (Hebrew/English)
      - guess date/payee/amount/currency columns
      - parse amounts & currency
      - parse date flexibly, day-first
      - convert all amounts to ILS
      - apply payee rules + categories

    Assumes you have already inspected this file for correctness
    BEFORE running this pipeline.
    """
    print(f"[INFO] Loading inspected 9016 file: {path}")
    
    # Read as objects so header detection & flexible parsing work well
    df_raw = pd.read_excel(path, sheet_name=0, dtype=object)
    
    # 1) Detect header row
    header_idx = find_header_row(df_raw)
    
    if header_idx is None:
        header = [normalize_header_text(c) for c in df_raw.columns]
        df_sheet = df_raw.copy()
        df_sheet.columns = header
        df_sheet = df_sheet.dropna(how="all")
    else:
        header = [normalize_header_text(v) for v in df_raw.iloc[header_idx]]
        df_sheet = df_raw.iloc[header_idx + 1:].copy()
        df_sheet.columns = header
        df_sheet = df_sheet.dropna(how="all")
    
    # 2) Guess columns (date, payee, amount, currency)
    date_col, payee_col, expense_col, currency_col = guess_columns(
        list(df_sheet.columns),
    )
    
    if not date_col or not payee_col or not expense_col:
        raise RuntimeError(
            f"Could not detect required columns (date/payee/amount) in {path.name}. "
            f"Detected: date={date_col}, payee={payee_col}, "
            f"expense={expense_col}, currency={currency_col}",
        )
    
    # 3) Detect sheet-level currency
    sheet_currency = detect_sheet_currency(df_raw)
    
    # 4) Build normalized frame
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
            if isinstance(c_val, str):
                for token in FIXED_RATES_TO_ILS.keys():
                    if token and token in c_val:
                        cur = token
                        break
            expenses.append(amt)
            currencies.append(cur)
    else:
        for a in df_sheet[expense_col]:
            amt, cur = parse_amount_and_currency(
                a,
                default_currency=sheet_currency,
            )
            expenses.append(amt)
            currencies.append(cur)
    
    df_norm["expense"] = expenses
    df_norm["currency"] = currencies
    
    # 5) Flexible date parsing (multiple formats)
    df_norm["run_date"] = pd.to_datetime(
        df_norm["date"],
        dayfirst=True,
        errors="coerce",
        format="mixed",  # allow mixed formats without warnings
    )
    
    df_norm = df_norm[~df_norm["run_date"].isna()].copy()
    if df_norm.empty:
        raise RuntimeError(f"All rows had invalid/unparseable dates in {path.name}.")
    
    # 6) Convert amounts to ILS
    df_norm = convert_to_ils_fixed_rate(
        df_norm,
        amount_col="expense",
        currency_col="currency",
    )
    
    # 7) Account + run_month
    df_norm["account"] = ACCOUNT_9016
    df_norm["run_month"] = df_norm["run_date"].dt.strftime("%Y-%m")
    
    # Basic payee cleanup
    df_norm["payee"] = df_norm["payee"].astype(str).str.strip()
    
    # 8) Apply payee rules + categories
    df_norm = apply_payee_rules_and_categories(
        df_norm,
        rules_df=rules_df,
        lookup_df=lookup_df,
        payee_col="payee",
    )
    
    # 9) Add dedupe fields + txn_hash
    df_norm = add_dedupe_fields(df_norm, source_file=path.name)
    
    return df_norm


# -------------------------------------------------------------------
#  Main
# -------------------------------------------------------------------

def main() -> None:
    print(f"[INFO] Project root: {PROJECT_ROOT}")
    print(f"[INFO] 9016 data dir: {DATA_9016}")
    print(f"[INFO] Using inspected file: {TNUOT_9016}")
    
    if not DATA_9016.exists():
        raise FileNotFoundError(f"9016 data directory not found: {DATA_9016}")
    
    if not TNUOT_9016.exists():
        raise FileNotFoundError(f"Inspected 9016 file not found: {TNUOT_9016}")
    
    # Connect to Mongo
    client = get_client()
    db = get_db(client)
    coll = get_transactions_collection(db)
    ensure_transactions_indexes(coll)
    
    # Load payee rules / lookup (same as merge_statements)
    rules_df, lookup_df = load_payee_resources()
    
    # Load & normalize the inspected 9016 file
    df_9016 = load_and_normalize_9016(
        TNUOT_9016,
        rules_df=rules_df,
        lookup_df=lookup_df,
    )
    
    print(f"[INFO] Prepared {len(df_9016)} 9016 transactions for insert.")
    insert_transactions_df(df_9016, coll)
    
    print("[DONE] 9016 pipeline v2 completed.\n")


if __name__ == "__main__":
    main()
