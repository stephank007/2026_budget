import copy  # NEW: for cloning column definitions

from dash import Dash, html
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import pandas as pd

from common_paths import MERGED_ONE_SHEET_XLSX
from utils import fmt_currency, fmt_currency_2

# =========================
# Load data
# =========================
f = MERGED_ONE_SHEET_XLSX
# Read once for metrics (proper dtypes)
df_raw = pd.read_excel(f, sheet_name=0)

# Keep the columns we care about
df_raw = df_raw[["account", "run_date", "payee", "expense", "category", "run_month"]]

# Ensure expense is numeric
df_raw["expense"] = pd.to_numeric(df_raw["expense"], errors="coerce")
df_raw = df_raw.dropna(subset=["expense"])

# =========================
# Metrics for KPI cards
# =========================
total_expense = df_raw["expense"].sum()
txn_count = len(df_raw)

# Monthly totals (based on run_month, which is derived from transaction date)
month_totals = (
    df_raw.groupby("run_month")["expense"]
    .sum()
    .sort_index()
)

# 1. Monthly burn rate (average monthly spend)
if not month_totals.empty:
    monthly_burn = month_totals.mean()
else:
    monthly_burn = 0.0

# 2. Median transaction size (more robust than mean)
median_txn = df_raw["expense"].median() if txn_count else 0.0

# Identify the latest month & month-over-month change
if not month_totals.empty:
    current_month = month_totals.index[-1]
    current_month_total = month_totals.iloc[-1]
    if len(month_totals) > 1:
        prev_month_total = month_totals.iloc[-2]
    else:
        prev_month_total = 0.0
else:
    current_month = "N/A"
    current_month_total = 0.0
    prev_month_total = 0.0

if prev_month_total and prev_month_total != 0:
    month_change = current_month_total - prev_month_total
    month_change_pct = (month_change / prev_month_total) * 100
else:
    month_change = None
    month_change_pct = None

# Decide trend icon/color for "This Month"
if month_change is None:
    month_trend_text = "No previous month to compare"
    month_trend_class = "text-muted"
else:
    if month_change > 0:
        # Spending increased (bad) -> red
        month_trend_icon = "▲"
        month_trend_class = "text-danger"
    elif month_change < 0:
        # Spending decreased (good) -> green
        month_trend_icon = "▼"
        month_trend_class = "text-success"
    else:
        month_trend_icon = "■"
        month_trend_class = "text-muted"
    
    sign = "+" if month_change > 0 else ""
    month_trend_text = (
        f"{month_trend_icon} {sign}{month_change:,.0f} "
        f"({sign}{month_change_pct:.1f}%) vs previous month"
    )

# Top category (by total expense)
if "category" in df_raw.columns and not df_raw["category"].isna().all():
    cat_totals = (
        df_raw.groupby("category")["expense"]
        .sum()
        .sort_values(ascending=False)
    )
    top_category_name = cat_totals.index[0]
    top_category_value = cat_totals.iloc[0]
else:
    top_category_name = "N/A"
    top_category_value = 0.0

# =========================
# Prepare data for grids (store expense in cents)
# =========================

df = df_raw.copy()
df["expense"] = df["expense"].apply(lambda x: int(round(x * 100)))
df["txn_id"] = df.index.astype(str)

grand_total_cents = int(df["expense"].sum())

# =========================
# Dash app + layout
# =========================

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)

# =========================
# Column definitions (base) + per-grid variants
# =========================

base_columnDefs = [
    # Hidden hierarchy helpers
    {"field": "run_date", "hide": True},
    {"field": "category", "hide": True},
    {"field": "account", "hide": True},
    
    # keep run_month hidden but sortable
    {"field": "run_month", "hide": True, "sortable": True},
    
    # Show run_date as Date column (visible)
    {"field": "run_date", "headerName": "Date", "minWidth": 120},
    
    {
        "field"     : "payee",
        "headerName": "Payee",
        "minWidth"  : 180,
        "cellStyle" : {"function": "totalLabelStyle(params)"},
    },
    
    {
        "field"         : "expense",
        "headerName"    : "Expense",
        "type"          : "numericColumn",
        "aggFunc"       : "sum",  # group rows show SUM
        "sortable"      : True,
        "valueFormatter": {"function": "formatCents(params.value)"},
        "cellStyle"     : {"function": "moneyCellStyle(params)"},
        "minWidth"      : 120,
    },
]

# Grid 1: Category → Payee → Month
# Sort by expense DESC
columnDefs_grid = copy.deepcopy(base_columnDefs)
for col in columnDefs_grid:
    if col.get("field") == "expense":
        col["sort"] = "desc"
        col["sortIndex"] = 0

# Grid 2: Month → Category → Payee
# Sort by run_month ASC, then expense DESC
columnDefs_payee = copy.deepcopy(base_columnDefs)
for col in columnDefs_payee:
    if col.get("field") == "run_month":
        col["sort"] = "asc"
        col["sortIndex"] = 0
    elif col.get("field") == "expense":
        col["sort"] = "desc"
        col["sortIndex"] = 1

# =========================
# KPI cards (top row)
# =========================
trend_color = (
    "#ff6b6b" if month_change and month_change > 0 else
    "#3cf2a3" if month_change and month_change < 0 else
    "#dcdcdc"
)

kpi_row = dbc.Row(
    [
        # Total Expenses
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H6("Total Expenses", className="text-white-50 text-center"),
                        html.H2(fmt_currency(total_expense), className="text-white text-center"),
                        html.Div(
                            "Across all loaded transactions",
                            className="text-white-50 text-center",
                        ),
                    ],
                ),
                color="danger",
                inverse=True,
                className="shadow-sm h-100 rounded-3",
            ),
            md=3,
        ),
        
        # This Month's Spend + trend
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H6("This Month's Spend", className="text-white-50 text-center"),
                        html.H2(fmt_currency(current_month_total), className="text-white text-center"),
                        html.Div(current_month, className="text-white-50 text-center mb-1"),
                        html.Div(
                            month_trend_text,
                            className=f"text-center fw-semibold",
                            style={"color": trend_color},
                        ),
                    ],
                ),
                color="primary",
                inverse=True,
                className="shadow-sm h-100 rounded-3",
            ),
            md=3,
        ),
        
        # Monthly burn rate + median transaction
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H6(
                            "Monthly Burn Rate",
                            className="text-black-50 text-center",
                        ),
                        html.H2(
                            fmt_currency(monthly_burn),
                            className="text-white text-center",
                        ),
                        html.Div(
                            f"Median txn: {fmt_currency_2(median_txn)}",
                            className="text-black-50 text-center",
                        ),
                    ],
                ),
                color="warning",  # stands out as “attention”
                inverse=True,
                className="shadow-sm h-100 rounded-3",
            ),
            md=3,
        ),
        
        # Top category
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H6("Top Category", className="text-white-50 text-center"),
                        html.H2(top_category_name, className="text-white text-center"),
                        html.Div(
                            f"Total: {fmt_currency(top_category_value)}",
                            className="text-white-50 text-center",
                        ),
                    ],
                ),
                color="secondary",
                inverse=True,
                className="shadow-sm h-100 rounded-3",
            ),
            md=3,
        ),
    ],
    className="mb-4",
)

# =========================
# Grids
# =========================

# Category → Payee → Month tree
grid = dag.AgGrid(
    id="expenses-tree",
    className="ag-theme-alpine",
    rowData=df.to_dict("records"),
    columnDefs=columnDefs_grid,
    defaultColDef={"resizable": True, "sortable": True, "filter": True, "flex": 1},
    enableEnterpriseModules=True,
    dangerously_allow_code=True,
    dashGridOptions={
        "treeData"            : True,
        "animateRows"         : True,
        "getDataPath"         : {
            "function": "getDataPath(params)",  # defined in assets/dashAgGridFunctions.js
        },
        "groupDefaultExpanded": 0,
        "icons"               : {
            "groupExpanded"  : '<span class="group-icon expanded">−</span>',
            "groupContracted": '<span class="group-icon contracted">+</span>',
        },
        "pinnedBottomRowData" : [
            {
                "payee"    : "Grand Total",
                "expense"  : grand_total_cents,
                "run_date" : "",
                "category" : "",
                "account"  : "",
                "run_month": "",
            },
        ],
        "autoGroupColumnDef"  : {
            "headerName"        : "Hierarchy",
            "minWidth"          : 320,
            "cellRendererParams": {"suppressCount": True},
        },
    },
    style={"height": "720px", "width": "100%"},
)

# Month → Category → Payee tree
payee_grid = dag.AgGrid(
    id="payee-tree",
    className="ag-theme-alpine",
    rowData=df.to_dict("records"),
    columnDefs=columnDefs_payee,
    defaultColDef={"resizable": True, "sortable": True, "filter": True, "flex": 1},
    enableEnterpriseModules=True,
    dangerously_allow_code=True,
    dashGridOptions={
        "treeData"            : True,
        "animateRows"         : True,
        "getDataPath"         : {
            "function": "getDataPathPayee(params)",  # defined in assets/dashAgGridFunctions.js
        },
        "groupDefaultExpanded": 0,
        # "icons"               : {
        #     "groupExpanded"  : '<span style="font-weight:700;">−</span>',
        #    "groupContracted": '<span style="font-weight:700;">+</span>',
        # },
        "icons"               : {
            "groupExpanded"  : '<span class="group-icon expanded">−</span>',
            "groupContracted": '<span class="group-icon contracted">+</span>',
        },
        "pinnedBottomRowData" : [
            {
                "payee"    : "Grand Total",
                "expense"  : grand_total_cents,
                "run_date" : "",
                "category" : "",
                "account"  : "",
                "run_month": "",
            },
        ],
        "autoGroupColumnDef"  : {
            "headerName"        : "Hierarchy",
            "minWidth"          : 320,
            "cellRendererParams": {"suppressCount": True},
        },
        # important so group order follows sorted children
        "groupMaintainOrder"  : True,
    },
    style={"height": "720px", "width": "100%"},
)

# =========================
# Layout
# =========================

grids_row = dbc.Row(
    [
        dbc.Col(
            [
                html.H4("Expenses Tree (Category → Payee → Month)", className="text-center"),
                html.Div(
                    className="text-muted text-center mb-2",
                ),
                grid,
            ],
            md=6,
        ),
        dbc.Col(
            [
                html.H4("Expenses Tree (Month → Category → Payee)", className="text-center"),
                payee_grid,
            ],
            md=6,
        ),
    ],
    className="mb-4",
)

app.layout = dbc.Container(
    [
        html.H2("Expenses Dashboard", className="text-center"),
        html.Hr(),
        kpi_row,
        html.Div(
            className="text-muted mb-2",
        ),
        grids_row,
        html.Br(),
    ],
    fluid=True,
)

if __name__ == "__main__":
    app.run(debug=True)
