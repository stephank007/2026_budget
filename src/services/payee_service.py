from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from common_paths import PAYEE_RULES_XLSX, PAYEE_LOOKUP_XLSX

# We intentionally import utils lazily in functions to avoid circular imports in some pipelines.


def _norm_payee_text(x) -> str:
    """Normalize a single payee string for joining (case-insensitive, whitespace-collapsed)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    s = " ".join(s.split())
    return s.strip().lower()


def norm_payee_series(s: pd.Series) -> pd.Series:
    """Vectorized version of _norm_payee_text."""
    return s.apply(_norm_payee_text)


def load_payee_resources(
    *,
    rules_path: Path = PAYEE_RULES_XLSX,
    lookup_path: Path = PAYEE_LOOKUP_XLSX,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load payee_rules.xlsx and payee_lookup.xlsx.

    Returns:
        rules_df: raw rules sheet (no normalization; utils.resolve_payee_name reads it as-is)
        lookup_df: lookup table with an added 'payee_norm' column used for merging categories
    """
    rules_df = pd.read_excel(rules_path).dropna(how="all")
    
    lookup_df = pd.read_excel(lookup_path).dropna(how="all")
    # Expected columns in lookup: payee, category (case-insensitive). We add payee_norm for joining.
    cols_lower = {str(c).strip().lower(): c for c in lookup_df.columns}
    payee_col = cols_lower.get("payee") or cols_lower.get("merchant") or cols_lower.get("payee_name")
    cat_col = cols_lower.get("category") or cols_lower.get("קטגוריה")
    if payee_col is None or cat_col is None:
        raise ValueError(
            f"payee_lookup.xlsx must contain columns like 'payee' and 'category'. Found: {list(lookup_df.columns)}"
        )
    lookup_df = lookup_df.rename(columns={payee_col: "payee", cat_col: "category"})
    lookup_df["payee_norm"] = norm_payee_series(lookup_df["payee"])
    
    # Keep only needed columns (plus original 'payee' for inspection)
    lookup_df = lookup_df[["payee", "payee_norm", "category"]].copy()
    return rules_df, lookup_df


def apply_payee_rules_and_categories(
    df: pd.DataFrame,
    *,
    rules_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    payee_col: str = "payee",
    date_col: str = "date",
    account_col: str = "account",
    # expense_col is what rules expect; if not present we will try common alternatives.
    expense_col: Optional[str] = None,
    out_payee_col: Optional[str] = None,
    amount_tol: float = 0.01,
) -> pd.DataFrame:
    """
    Apply payee normalization (rules) + category lookup.

    Cautious behavior:
    - If utils.resolve_payee_name exists, use it (supports BOTH legacy + transaction-specific rules).
    - Otherwise, falls back to legacy substring rules using the provided rules_df.
    - For matching the numeric 'expense' rule, uses ABS(value) so negative expenses still match.
    - Category join is done on normalized payee text via 'payee_norm'.

    Returns a copy of df with:
      - payee normalized (written to out_payee_col if provided, else payee_col)
      - category column (defaults to "לא מזוהה" if missing)
      - flag column "לא מזוהה" indicating missing category (kept for backward compatibility)
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    
    out = df.copy()
    
    target_payee_col = out_payee_col or payee_col
    if target_payee_col not in out.columns:
        # create target from source payee, or blank
        out[target_payee_col] = out[payee_col] if payee_col in out.columns else ""
    
    # Decide which numeric column to use for expense matching
    if expense_col is None:
        if "expense" in out.columns:
            expense_col = "expense"
        elif "amount" in out.columns:
            expense_col = "amount"
        else:
            expense_col = None
    
    # --- Apply rules ---
    resolved_col = target_payee_col
    
    # Prefer utils.resolve_payee_name if available
    try:
        import utils  # type: ignore
        
        if hasattr(utils, "resolve_payee_name"):
            # Build an ABS expense column for matching if we have any numeric column at all
            match_exp_col = None
            if expense_col and expense_col in out.columns:
                out["_expense_match"] = pd.to_numeric(out[expense_col], errors="coerce").abs()
                match_exp_col = "_expense_match"
            
            out = utils.resolve_payee_name(
                out,
                rules_xlsx_path=None,  # allow utils to use common_paths.PAYEE_RULES_XLSX if desired
                account_col=account_col,
                date_col=date_col,
                payee_col=payee_col,
                expense_col=match_exp_col or (expense_col or "expense"),
                out_col=resolved_col,
                amount_tol=amount_tol,
            )
    except Exception:
        # Fallback: legacy substring matching using rules_df
        if rules_df is not None and not rules_df.empty:
            cols = {str(c).strip().lower(): c for c in rules_df.columns}
            ms_col = cols.get("match_string")
            np_col = cols.get("normalized_payee")
            if ms_col and np_col and payee_col in out.columns:
                payee_lower = out[resolved_col].astype(str).str.lower()
                resolved = out[resolved_col].astype(str).copy()
                already_set = pd.Series(False, index=out.index)
                
                for _, rule in rules_df[[ms_col, np_col]].dropna().iterrows():
                    ms = str(rule[ms_col]).strip().lower()
                    if not ms:
                        continue
                    mask = ~already_set & payee_lower.str.contains(ms, na=False)
                    if mask.any():
                        resolved.loc[mask] = str(rule[np_col]).strip()
                        already_set.loc[mask] = True
                
                out[resolved_col] = resolved
    
    # Cleanup temp column if created
    if "_expense_match" in out.columns:
        out = out.drop(columns=["_expense_match"])
    
    # --- Category lookup ---
    # Build normalized key and join categories
    out["payee_norm"] = norm_payee_series(out[resolved_col])
    
    out = out.merge(
        lookup_df[["payee_norm", "category"]],
        on="payee_norm",
        how="left",
    )
    
    # Backward-compatible outputs
    out["לא מזוהה"] = out["category"].isna()
    out["category"] = out["category"].fillna("לא מזוהה")
    
    return out
