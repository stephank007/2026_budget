from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# -------------------------------------------------------------------
#  Currency conversion + filename helpers (from read_banking_statements)
# -------------------------------------------------------------------
# :contentReference[oaicite:0]{index=0}

FIXED_RATES_TO_ILS: Dict[str, float] = {
    # ILS
    "₪"  : 1.0,
    "ILS": 1.0,
    
    # USD
    "$"  : 3.3,
    "USD": 3.3,
    
    # EUR
    "€"  : 3.9,
    "EUR": 3.9,
    
    # GBP
    "£"  : 4.7,
    "GBP": 4.7,
    
    # JPY
    "¥"  : 0.021,
    "JPY": 0.021,
}

FOUR_DIGITS_PREFIX = re.compile(r"^\d{4}")
FILENAME_PATTERN = re.compile(
    r"^(\d{4})_(0[1-9]|1[0-2])_(\d{4})\.xlsx$",
    re.IGNORECASE,
)

# -------------------------------------------------------------------
#  Date / number helpers (from 9016-fixer)
# -------------------------------------------------------------------
# :contentReference[oaicite:1]{index=1}

DATE_TOKEN_RE = re.compile(r"\d{2}/\d{2}/\d{2}")
DATE_FULL_RE = re.compile(r"^(\d{2})/(\d{2})/(\d{2})$")


def normalize_header_text(val) -> str:
    """Normalize header cell text (both Hebrew and English)."""
    if pd.isna(val):
        return ""
    s = str(val)
    s = s.replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_payee(payee: str) -> str:
    """
    Final payee string cleanup:
    - remove quotes
    - strip whitespace
    - remove ONLY suffixes of the form " - something",
      but DO NOT remove internal dashes (like ח-ן).
    """
    if not isinstance(payee, str):
        return ""
    
    payee = payee.replace('"', "").strip()
    
    # remove: space-dash-space + trailing text
    payee = re.sub(r"\s+-\s+.*$", "", payee)
    
    return payee.strip()


# -------------------------------------------------------------------
#  Payee resolution from payee_rules.xlsx (supports legacy + specific rules)
# -------------------------------------------------------------------

def _coerce_rule_date(val) -> Optional[str]:
    """Coerce a rule 'date' cell into dd.mm.yy if possible, else return stripped string."""
    if pd.isna(val):
        return None
    try:
        dt = pd.to_datetime(val, dayfirst=True, errors="coerce")
    except Exception:
        dt = pd.NaT
    if not pd.isna(dt):
        return dt.strftime("%d.%m.%y")
    s = str(val).strip()
    return s or None


def _coerce_rule_amount(val) -> Optional[float]:
    if pd.isna(val):
        return None
    try:
        return float(val)
    except Exception:
        try:
            return float(str(val).replace(",", "").strip())
        except Exception:
            return None


def _coerce_rule_account(val) -> Optional[str]:
    """Coerce account numbers to a stable string (e.g. 9016.0 -> '9016')."""
    if pd.isna(val):
        return None
    try:
        f = float(val)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    s = str(val).strip()
    s = re.sub(r"\.0$", "", s)
    return s or None


def resolve_payee_name(
    df: pd.DataFrame,
    *,
    rules_xlsx_path: Optional[Path] = None,
    account_col: str = "account",
    date_col: str = "date",
    payee_col: str = "payee",
    expense_col: str = "expense",
    out_col: Optional[str] = None,
    amount_tol: float = 0.01,
) -> pd.DataFrame:
    """
    Resolve/normalize payee names using an *extended* payee_rules.xlsx.

    Supports two rule styles in the same sheet:
      1) Legacy rules: columns [match_string, normalized_payee]
         - case-insensitive substring match on the current payee.
      2) Specific transaction rules: columns
         [account, date, payee, expense, normalized_payee]
         - each provided field acts as a filter (blank = wildcard)
         - date is compared in dd.mm.yy format (best-effort coercion)
         - expense is compared numerically with `amount_tol`

    If rules_xlsx_path is None, this function will try to import
    common_paths.PAYEE_RULES_XLSX (keeps older packages working).
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    
    if rules_xlsx_path is None:
        try:
            from common_paths import PAYEE_RULES_XLSX  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "rules_xlsx_path not provided and common_paths.PAYEE_RULES_XLSX not available"
            ) from e
        rules_xlsx_path = PAYEE_RULES_XLSX
    
    rules_df = pd.read_excel(rules_xlsx_path).dropna(how="all")
    if rules_df.empty:
        return df.copy()
    
    df_out = df.copy()
    target_col = out_col or payee_col
    if target_col not in df_out.columns:
        df_out[target_col] = df_out[payee_col] if payee_col in df_out.columns else ""
    
    # ----------------
    # Specific rules
    # ----------------
    specific_cols = {"account", "date", "payee", "expense", "normalized_payee"}
    has_specific = specific_cols.issubset({str(c).strip() for c in rules_df.columns})
    
    if has_specific:
        r = rules_df.copy()
        r.columns = [str(c).strip() for c in r.columns]
        
        r["account"] = r["account"].apply(_coerce_rule_account)
        r["date"] = r["date"].apply(_coerce_rule_date)
        r["payee"] = r["payee"].apply(lambda x: None if pd.isna(x) else str(x).strip() or None)
        r["expense"] = r["expense"].apply(_coerce_rule_amount)
        r["normalized_payee"] = r["normalized_payee"].apply(lambda x: None if pd.isna(x) else str(x).strip() or None)
        
        r = r.dropna(subset=["normalized_payee"])
        r = r[r[["account", "date", "payee", "expense"]].notna().any(axis=1)].copy()
        
        if not r.empty:
            acc_series = (
                df_out[account_col].apply(_coerce_rule_account).fillna("").astype(str)
                if account_col in df_out.columns else pd.Series("", index=df_out.index)
            )
            
            date_series = (
                df_out[date_col].apply(_coerce_rule_date).astype(str)
                if date_col in df_out.columns else pd.Series("", index=df_out.index)
            )
            
            payee_series = (
                df_out[payee_col].astype(str).str.strip()
                if payee_col in df_out.columns else pd.Series("", index=df_out.index)
            )
            payee_lower = payee_series.str.lower()
            
            exp_series = (
                pd.to_numeric(df_out[expense_col], errors="coerce")
                if expense_col in df_out.columns else pd.Series(np.nan, index=df_out.index)
            )
            
            resolved = df_out[target_col].astype(str)
            already_set = pd.Series(False, index=df_out.index)
            
            for _, rule in r.iterrows():
                mask = ~already_set
                
                if rule.get("account") is not None and account_col in df_out.columns:
                    mask &= (acc_series == str(rule["account"]))
                
                if rule.get("date") is not None and date_col in df_out.columns:
                    mask &= (date_series == str(rule["date"]))
                
                if rule.get("payee") is not None and payee_col in df_out.columns:
                    rp = str(rule["payee"]).strip().lower()
                    mask &= (payee_lower == rp)
                
                if rule.get("expense") is not None and expense_col in df_out.columns:
                    mask &= (exp_series - float(rule["expense"])).abs() <= amount_tol
                
                if mask.any():
                    resolved.loc[mask] = str(rule["normalized_payee"])
                    already_set.loc[mask] = True
            
            df_out[target_col] = resolved
    
    # -------------
    # Legacy rules
    # -------------
    if {"match_string", "normalized_payee"}.issubset({str(c).strip() for c in rules_df.columns}):
        r2 = rules_df.copy()
        r2.columns = [str(c).strip() for c in r2.columns]
        legacy = r2[["match_string", "normalized_payee"]].dropna(subset=["match_string", "normalized_payee"])
        if not legacy.empty:
            payee_series = df_out[target_col].astype(str)
            payee_lower = payee_series.str.lower()
            
            resolved = payee_series.copy()
            already_set = pd.Series(False, index=df_out.index)
            
            for _, rule in legacy.iterrows():
                ms = str(rule["match_string"]).strip().lower()
                if not ms:
                    continue
                mask = ~already_set & payee_lower.str.contains(re.escape(ms), na=False)
                if mask.any():
                    resolved.loc[mask] = str(rule["normalized_payee"]).strip()
                    already_set.loc[mask] = True
            
            df_out[target_col] = resolved
    
    return df_out


def convert_to_ils_fixed_rate(
    df: pd.DataFrame, amount_col: str = "amount", currency_col: str = "currency", ) -> pd.DataFrame:
    """
    Convert `amount_col` to ILS using FIXED_RATES_TO_ILS and set `currency_col` to `₪`.
    """
    out = df.copy()
    
    out["_rate"] = out[currency_col].map(FIXED_RATES_TO_ILS)
    out[amount_col] = pd.to_numeric(out[amount_col], errors="coerce")
    out[amount_col] = out[amount_col] * out["_rate"]
    
    out[currency_col] = "₪"
    return out.drop(columns="_rate")


def split_filename(value: str) -> Tuple[str, str]:
    """
    Convert 'xxxx_mm_yyyy.xlsx' -> ('xxxx', 'yyyy-mm')

    Example:
        '1234_01_2025.xlsx' -> ('1234', '2025-01')
    """
    match = FILENAME_PATTERN.match(value)
    if not match:
        raise ValueError(f"Invalid filename format: {value}")
    
    xxxx, mm, yyyy = match.groups()
    return xxxx, f"{yyyy}-{mm}"


def detect_sheet_currency(df_raw: pd.DataFrame) -> str:
    """
    Try to detect a currency symbol or code anywhere in the sheet text.
    Falls back to ₪ if nothing is detected.
    """
    symbols = list(FIXED_RATES_TO_ILS.keys())
    for v in df_raw.select_dtypes(include="object").to_numpy().ravel():
        if pd.isna(v):
            continue
        s = str(v)
        for sym in symbols:
            if sym and sym in s:
                return sym
    return "₪"


def parse_amount_and_currency(v, default_currency: str = "₪", ) -> Tuple[float, str]:
    """
    Parse a mixed amount+currency cell into (amount, currency).
    Examples:
        279.42          -> (279.42, '₪')    if default is ₪
        '279.42 ₪'      -> (279.42, '₪')
        '1,234.50-$'    -> (1234.5, '$')
        '10.5 USD'      -> (10.5, 'USD')
    """
    if pd.isna(v):
        return (np.nan, default_currency)
    
    if isinstance(v, (int, float, np.number)):
        return (float(v), default_currency)
    
    s = str(v).strip()
    currency = None
    
    # Detect explicit currency tokens
    for token in FIXED_RATES_TO_ILS.keys():
        if token and token in s:
            currency = token
            s = s.replace(token, "")
    
    if currency is None:
        currency = default_currency
    
    # Remove commas (thousands separators)
    s2 = s.replace(",", "")
    
    # Extract first numeric token (with optional sign and decimal)
    m = re.search(r"[-+]?\d*\.?\d+", s2)
    if not m:
        return (np.nan, currency)
    
    num_str = m.group(0)
    try:
        amt = float(num_str)
    except ValueError:
        amt = np.nan
    
    return (amt, currency)


def find_header_row(df_raw: pd.DataFrame) -> Optional[int]:
    """
    Heuristic to find the header row.
    Looks for a row that contains something like 'תאריך' (Hebrew 'date')
    or 'date' in any cell, ignoring whitespace and case.
    """
    for i, row in df_raw.iterrows():
        for v in row:
            if pd.isna(v):
                continue
            t = str(v)
            t_norm = normalize_header_text(t).lower()
            if "תאריך" in t_norm or "date" in t_norm:
                return i
    return None


def guess_columns(cols: List[str], ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Guess which columns are date, payee, expense, currency by translating /
    normalizing Hebrew/English headers.
    """
    norm_cols = {c: normalize_header_text(c) for c in cols}
    
    date_col = None
    payee_col = None
    amount_candidates: List[str] = []
    currency_col = None
    
    for c, n in norm_cols.items():
        # date
        if any(key in n for key in ["תאריך", "date"]):
            if date_col is None:
                date_col = c
        
        # payee / merchant
        if any(
                key in n
                for key in [
                    "שם בית",
                    "שם ספק",
                    "שם לקוח",
                    "merchant",
                    "payee",
                    "שם עסקה",
                ]
        ):
            if payee_col is None:
                payee_col = c
        
        # amount columns (start with 'סכום' or contain 'amount')
        if n.startswith("סכום") or "amount" in n.lower():
            amount_candidates.append(c)
        
        # currency column
        if any(key in n for key in ["מטבע", "currency"]):
            currency_col = c
    
    # Choose expense column
    expense_col = None
    if amount_candidates:
        # Prefer the one that looks like "charge"/"חיוב"
        for c in amount_candidates:
            n = norm_cols[c]
            if any(key in n for key in ["חיוב", "debit", "charge"]):
                expense_col = c
                break
        if expense_col is None:
            expense_col = amount_candidates[0]
    
    return date_col, payee_col, expense_col, currency_col


def fix_date(s: str) -> Optional[str]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    
    try:
        # Handle ISO-style timestamps explicitly
        if re.match(r"\d{4}-\d{2}-\d{2}", s):
            dt = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        else:
            dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    except Exception:
        dt = pd.NaT
    
    if not pd.isna(dt):
        return dt.strftime("%d.%m.%y")
    
    m = DATE_FULL_RE.match(s)
    if not m:
        return s
    a, b, c = m.groups()
    return f"{c}.{b[::-1]}.{a[::-1]}"


def contains_hebrew(s: str) -> bool:
    return any("\u0590" <= ch <= "\u05FF" for ch in s)


def is_number_like(s: str) -> bool:
    s = s.strip()
    return bool(s and re.fullmatch(r"-?[0-9.,]+-?", s))


def normalize_number(s: str) -> Optional[float]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    if s.endswith("-") and not s.startswith("-"):
        s = "-" + s[:-1]
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def load_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"date", "payee", "amount"}
    cols_lower = {c.lower(): c for c in df.columns}
    
    if not required_cols.issubset(cols_lower.keys()):
        raise ValueError(
            f"Input file must contain columns: {required_cols}. "
            f"Found: {list(df.columns)}",
        )
    
    df = df.rename(
        columns={
            cols_lower["date"]  : "date",
            cols_lower["payee"] : "payee",
            cols_lower["amount"]: "amount",
        },
    )
    
    return df


def apply_payee_renames(df: pd.DataFrame, payee_renames: dict) -> pd.DataFrame:
    if not payee_renames:
        return df
    
    ci_map = {k.lower(): v for k, v in payee_renames.items()}
    
    def rename_payee(p):
        if isinstance(p, str):
            return ci_map.get(p.lower(), p)
        return p
    
    df = df.copy()
    df["payee"] = df["payee"].apply(rename_payee)
    return df


def apply_amount_rules(df: pd.DataFrame, amount_rules: list) -> pd.DataFrame:
    if not amount_rules:
        return df
    
    df = df.copy()
    
    for rule in amount_rules:
        approx_amount = rule.get("approx_amount")
        new_payee = rule.get("new_payee")
        tolerance = rule.get("tolerance", 50)
        
        if approx_amount is None or new_payee is None:
            continue
        
        lower = approx_amount - tolerance
        upper = approx_amount + tolerance
        
        mask = df["amount"].between(lower, upper)
        df.loc[mask, "payee"] = new_payee
    
    return df


def extract_payees(df: pd.DataFrame, extract_list: list, ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not extract_list:
        return pd.DataFrame(columns=df.columns), df.copy()
    
    df = df.copy()
    df["_pl"] = df["payee"].astype(str).str.lower()
    
    extract_set = {p.lower() for p in extract_list}
    
    mask = df["_pl"].isin(extract_set)
    
    extracted_df = df[mask].drop(columns=["_pl"])
    remainder_df = df[~mask].drop(columns=["_pl"])
    
    return extracted_df, remainder_df


def fmt_currency(amount: float) -> str:
    """Format as ₪ X,XXX (no decimals)."""
    if pd.isna(amount):
        return "₪ 0"
    return f"₪ {amount:,.0f}"


def fmt_currency_2(amount: float) -> str:
    """Format as ₪ X,XXX.XX (with decimals)."""
    if pd.isna(amount):
        return "₪ 0.00"
    return f"₪ {amount:,.2f}"
