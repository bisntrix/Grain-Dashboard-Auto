# app.py
from __future__ import annotations

import io
import time
from typing import List, Dict, Any

import pandas as pd
import streamlit as st

# Local helpers you already have / added
from resilient_fetch import fetch_coop_table
from patch_duplicate_columns import patch_duplicate_columns
from debug_shim import display_dataframe_safe  # safe wrapper around st.dataframe (your existing helper)

st.set_page_config(page_title="Grain Marketing Dashboard", layout="wide")

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

# TODO: Replace each `url` with your real pages
COOPS: List[Dict[str, str]] = [
    {"name": "ADM Cedar Rapids",            "url": "https://<ADM BIDS PAGE>",                   "location": "ADM Cedar Rapids"},
    {"name": "Cargill Cedar Rapids (Soy)",  "url": "https://<CARGILL SOY BIDS PAGE>",           "location": "Cargill Cedar Rapids (Soy)"},
    {"name": "Dunkerton Coop",              "url": "https://<DUNKERTON BIDS PAGE>",             "location": "Dunkerton Coop"},
    {"name": "Heartland (Washburn)",        "url": "https://<HEARTLAND WASHBURN BIDS>",         "location": "Heartland Coop Washburn"},
    {"name": "Mid-Iowa (La Porte City)",    "url": "https://<MID IOWA LPC BIDS>",               "location": "Mid-Iowa Coop La Porte City"},
    {"name": "Shell Rock Soy Processing",   "url": "https://<SRSP BIDS PAGE>",                  "location": "Shell Rock Soy Processing"},
    {"name": "POET Fairbank",               "url": "https://<POET FAIRBANK BIDS>",              "location": "POET Fairbank"},
    {"name": "POET Shell Rock",             "url": "https://<POET SHELL ROCK BIDS>",            "location": "POET Shell Rock"},
]

FUTURE_INPUT_DEFAULTS = {
    "Corn (ZC) nearby": 4.50,
    "Soybeans (ZS) nearby": 11.50,
}

# Optional: if you keep a Google Sheet (or CSV) with manual rows, put the URL in
# Streamlit Secrets as st.secrets["MANUAL_FEED_URL"] (must be a direct csv export)
MANUAL_FEED_URL = st.secrets.get("MANUAL_FEED_URL", "")

# ──────────────────────────────────────────────────────────────────────────────
# FETCH + NORMALIZE
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=10 * 60, show_spinner=True)
def collect_all() -> tuple[pd.DataFrame, List[str], List[Dict[str, Any]]]:
    """Try to fetch each co-op; return combined df, issues (strings), and raw debug info."""
    frames: List[pd.DataFrame] = []
    issues: List[str] = []
    debug_rows: List[Dict[str, Any]] = []

    for c in COOPS:
        res = fetch_coop_table(c["url"], c["location"])
        debug_rows.append(res)
        if res.get("ok"):
            frames.append(res["data"])
        else:
            issues.append(f'{c["name"]}: {res.get("error")}')

    table = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return table, issues, debug_rows


def load_manual_feed() -> pd.DataFrame:
    """Optional manual feed (CSV/Sheet) you can paste into st.secrets."""
    if not MANUAL_FEED_URL:
        return pd.DataFrame()

    try:
        df = pd.read_csv(MANUAL_FEED_URL)
        # Expect columns roughly like: commodity, delivery, cash, basis, futures, location
        # We'll be forgiving on column names; patch_duplicate_columns will help too.
        return df
    except Exception:
        return pd.DataFrame()


def coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def recompute_basis_if_requested(df: pd.DataFrame, futures_overrides: Dict[str, float]) -> pd.DataFrame:
    """If user enters current futures, we can recompute/override basis."""
    if df.empty:
        return df

    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]

    # Allow flexible 'commodity' labels
    if "commodity" not in out.columns:
        # try to infer: if there's a column with 'crop' or 'product'
        for c in list(out.columns):
            if any(k in c for k in ["commodity", "product", "crop"]):
                out = out.rename(columns={c: "commodity"})
                break

    # If we have cash and commodity and user provided a futures override, set futures and basis
    if "cash" in out.columns:
        out = coerce_numeric(out, ["cash", "futures", "basis"])
        for key, fut_val in futures_overrides.items():
            # key like "Corn (ZC) nearby"; map using simple contains
            is_corn = "corn" in key.lower()
            is_soy  = "soy" in key.lower()
            mask = pd.Series([False] * len(out))
            if "commodity" in out.columns:
                com = out["commodity"].astype(str).str.lower()
                if is_corn:
                    mask = mask | com.str.contains("corn", na=False)
                if is_soy:
                    mask = mask | com.str.contains("soy", na=False) | com.str.contains("bean", na=False)
            # If futures column exists, override for the masked rows; else create it
            if "futures" in out.columns:
                out.loc[mask, "futures"] = fut_val
            else:
                out["futures"] = pd.NA
                out.loc[mask, "futures"] = fut_val

        # If futures now present, compute basis = cash - futures (but don't erase existing real basis)
        if "futures" in out.columns:
            if "basis" not in out.columns:
                out["basis"] = pd.NA
            # Only fill basis where missing
            missing_basis = out["basis"].isna()
            out.loc[missing_basis, "basis"] = out.loc[missing_basis, "cash"] - out.loc[missing_basis, "futures"]

    return out


def format_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    cols_order = [c for c in ["commodity", "delivery", "cash", "basis", "futures", "location", "last_refresh_epoch"] if c in df.columns]
    rest = [c for c in df.columns if c not in cols_order]
    df = df[cols_order + rest]
    return df


# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────

st.title("Auto-fetched Cash Bids (beta)")

with st.sidebar:
    st.header("Options")
    st.caption("Set fallback futures (optional) to compute basis when sites don’t provide it.")

    futures_inputs = {}
    for label, default_val in FUTURE_INPUT_DEFAULTS.items():
        futures_inputs[label] = st.number_input(label, value=float(default_val), step=0.01, format="%.4f")

    st.divider()
    st.caption("Manual feed (optional)")
    if MANUAL_FEED_URL:
        st.success("Manual feed URL detected in secrets (MANUAL_FEED_URL).")
    else:
        st.info("Add MANUAL_FEED_URL to Streamlit secrets to merge a CSV/Sheet with custom rows.")

# Try all live co-op pages
table, issues, debug_rows = collect_all()

# Merge optional manual feed
manual_df = load_manual_feed()
if not manual_df.empty:
    table = pd.concat([table, manual_df], ignore_index=True)

# If nothing live came back, show diagnostics and demo rows (so the app still feels alive)
if table.empty:
    st.warning("No live tables were collected. Showing diagnostics and fallback demo rows.")

    if issues:
        with st.expander("Fetch issues (per site)"):
            for i in issues:
                st.write("•", i)

    # Always keep the app usable with a tiny demo table:
    demo = pd.DataFrame(
        {
            "commodity": ["Corn", "Soybeans"],
            "delivery": ["Nearby", "Nearby"],
            "cash": [4.20, 10.85],
            "basis": [-0.35, -0.90],
            "location": ["Demo Location A", "Demo Location B"],
            "last_refresh_epoch": [int(time.time())] * 2,
        }
    )
    demo = patch_duplicate_columns(demo)
    display_dataframe_safe(demo, use_container_width=True, height=420)

    st.stop()

# We have at least some data — patch dupes, allow futures override, and format
table = patch_duplicate_columns(table)
table = recompute_basis_if_requested(table, futures_inputs)
table = format_for_display(table)

# Summary header
num_rows = len(table)
num_locs = table["location"].nunique() if "location" in table.columns else None
msg = f"Collected **{num_rows}** rows"
if num_locs:
    msg += f" from **{num_locs}** location(s)"
st.success(msg)

# Main grid
display_dataframe_safe(table, use_container_width=True, height=520)

# Diagnostics panel
with st.expander("Diagnostics"):
    st.write("Last refresh:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if issues:
        st.write("**Fetch issues:**")
        for i in issues:
            st.write("•", i)
    else:
        st.write("No fetch issues reported.")
    st.write("Raw results sample:")
    st.json(debug_rows[:3])

# ──────────────────────────────────────────────────────────────────────────────
# EXPORTS
# ──────────────────────────────────────────────────────────────────────────────

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="bids")
        # Autofit columns (roughly)
        for idx, col in enumerate(df.columns):
            width = min(40, max(10, int(df[col].astype(str).str.len().mean() + 5)))
            writer.sheets["bids"].set_column(idx, idx, width)
    return output.getvalue()

col1, col2 = st.columns(2)
with col1:
    csv_bytes = table.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download CSV",
        data=csv_bytes,
        file_name="cash_bids.csv",
        mime="text/csv"
    )
with col2:
    xlsx_bytes = to_excel_bytes(table)
    st.download_button(
        "⬇️ Download Excel",
        data=xlsx_bytes,
        file_name="cash_bids.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

st.caption("Tip: If a co-op switches to a JavaScript-rendered table, point `resilient_fetch.py` to a CSV/JSON endpoint if available, or add a manual row via the optional Sheet/CSV feed.")
