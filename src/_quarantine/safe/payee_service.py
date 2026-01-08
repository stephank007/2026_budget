from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd

from common_paths import PAYEE_RULES_XLSX, PAYEE_LOOKUP_XLSX


def norm_payee_series(s: pd.Series) -> pd.Series:
    """
    Normalize payee strings for matching:
    - cast to str
    - collapse whitespace
    - strip
    """
    return (
        s.astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def normalize_payee_name_from_rule(payee: str, rules_df: pd.DataFrame) -> str:
    """
    Apply normalization rules from payee_rules.xlsx:
    if `match_string` is contained in payee, replace with `normalized_payee`.
    """
    if not isinstance(payee, str):
        return ""
    
    for _, row in rules_df.iterrows():
        if row["match_string"] in payee:
            return row["normalized_payee"]
    return payee


def load_payee_resources(
    rules_path: str | Path = PAYEE_RULES_XLSX,
    lookup_path: str | Path = PAYEE_LOOKUP_XLSX,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load payee rules and lookup table, and prepare lookup for joins.
    """
    rules_df = pd.read_excel(rules_path)
    
    lookup_df = pd.read_excel(lookup_path, sheet_name="payee_lookup")
    lookup_df["payee_norm"] = norm_payee_series(lookup_df["payee"])
    lookup_df = lookup_df.drop_duplicates(subset=["payee_norm"])
    
    return rules_df, lookup_df


def apply_payee_rules_and_categories(
    df: pd.DataFrame,
    rules_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    payee_col: str = "payee",
) -> pd.DataFrame:
    """
    Given a DataFrame with a payee column:
      1) apply payee_rules normalization
      2) join categories from payee_lookup
      3) mark unknown payees
    Returns a new DataFrame with columns:
      - payee (normalized)
      - payee_norm
      - category
      - לא מזוהה  (True/False for unknown)
    """
    out = df.copy()
    
    # Basic cleanup
    out[payee_col] = out[payee_col].astype(str).str.strip()
    
    # Rules-based normalization
    out[payee_col] = out[payee_col].apply(
        lambda p: normalize_payee_name_from_rule(p, rules_df),
    )
    
    # Build normalized key and join categories
    out["payee_norm"] = norm_payee_series(out[payee_col])
    
    out = out.merge(
        lookup_df[["payee_norm", "category"]],
        on="payee_norm",
        how="left",
    )
    
    out["לא מזוהה"] = out["category"].isna()
    out["category"] = out["category"].fillna("לא מזוהה")
    
    return out
