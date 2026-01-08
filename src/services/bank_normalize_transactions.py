from __future__ import annotations

from typing import Tuple

import pandas as pd

from common_paths import CONFIG_JSON_PATH
from utils import (
    load_config,
    normalize_columns,
    apply_payee_renames,
    apply_amount_rules,
    extract_payees,
)


def process_transactions(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Normalize raw 9016 transactions and split them into two dataframes.

    Parameters
    ----------
    df : pd.DataFrame
        Expecting at least columns: 'date', 'payee', 'amount'
        (as produced by load_and_clean_9016_xls in bank-fixer.py).

    Returns
    -------
    remainder_df : pd.DataFrame
        Main transactions dataframe for the 'Parsed' sheet:
        - Columns normalized (via utils.normalize_columns).
        - Payees renamed (config['payee_renames']).
        - Amount rules applied (config['amount_rules']).
        - Rows for extract_payees removed to extracted_df.
        - Expense amounts set negative.
        - Guarantees a 'bank_normal' column exists (used later).

    extracted_df : pd.DataFrame
        Rows that were extracted according to config['extract_payees']
        (e.g. salary / income), usually written to 'Income' sheet.
    """
    
    # Always work on a copy so caller's df is unchanged
    df = df.copy()
    
    # 1) Load config.json
    config = load_config(CONFIG_JSON_PATH) or {}
    
    payee_renames = config.get("payee_renames", {}) or {}
    amount_rules = config.get("amount_rules", []) or []
    extract_payees_list = config.get("extract_payees", []) or []
    
    # 2) Normalize column names / types
    df = normalize_columns(df)
    
    # 3) Rename payees
    df = apply_payee_renames(df, payee_renames)
    
    # 4) Apply amount rules (rounding / fixes)
    df = apply_amount_rules(df, amount_rules)
    
    # 5) Extract special payees (e.g. income) into a separate df
    #    extract_payees returns: extracted_df, remainder_df
    extracted_df, remainder_df = extract_payees(df, extract_payees_list)
    
    # 6) Make expenses negative
    if "amount" in remainder_df.columns:
        remainder_df["amount"] = remainder_df["amount"] * -1
    
    # 7) Ensure 'bank_normal' exists – this is what Parsed uses
    if "bank_normal" not in remainder_df.columns:
        if "payee" in remainder_df.columns:
            remainder_df["bank_normal"] = remainder_df["payee"]
        else:
            remainder_df["bank_normal"] = ""
    
    return remainder_df, extracted_df
