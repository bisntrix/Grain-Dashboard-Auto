from __future__ import annotations
import io, time, re, requests
from typing import List, Dict, Any

import pandas as pd
import streamlit as st

from resilient_fetch import fetch_coop_table
from patch_duplicate_columns import patch_duplicate_columns
from debug_shim import display_dataframe_safe

st.set_page_config(page_title="Grain Marketing Dashboard", layout="wide")

COOPS: List[Dict[str, str]] = [
    {"name": "Dunkerton Co-op (Source)", "url": "https://www.dunkertoncoop.com/CashBids", "location": "Dunkerton Co-op"},
]

FUTURE_INPUT_DEFAULTS = {"Corn (ZC) nearby": 4.50, "Soybeans (ZS) nearby": 11.50}
MANUAL_FEED_URL = st.secrets.get("MANUAL_FEED_URL", "")

PROCESSOR_PATTERNS = [
    {"name": "ADM Cedar Rapids", "patterns": [r"\badm\b\s*[-–—:]*\s*cedar\s*rapids", r"cedar\s*rapids\s*[-–—:]*\s*\badm\b", r"\badm\b.*\bcr\b", r"\bcr\b.*\badm\b"]},
    {"name": "Cargill Cedar Rapids (Soy)", "patterns": [r"\bcargill\b\s*[-–—:]*\s*cedar\s*rapids", r"cedar\s*rapids\s*[-–—:]*\s*\bcargill\b", r"\bcargill\b.*\bcr\b", r"\bcr\b.*\bcargill\b", r"cargill.*soy", r"soy.*cargill"]},
    {"name": "Shell Rock Soy Processing", "patterns": [r"shell\s*rock\s*soy", r"\bsrsp\b"]},
]

def _series_or_first_col(x):
    # If selecting a label with duplicate columns produced a DataFrame, take first col as Series
    return x.iloc[:,0] if isinstance(x, pd.DataFrame) else x

def route_rows_to_processors(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    cols = {c.lower(): c for c in df.columns}
    delivery_col = cols.get("delivery") or next((c for c in df.columns if "deliv" in c.lower() or "month" in c.lower() or "period" in c.lower()), None)
    location_col = cols.get("location")
    work = df.copy()
    if delivery_col:
        text = _series_or_first_col(work[delivery_col].astype(str))
    elif location_col:
        text = _series_or_first_col(work[location_col].astype(str))
    else:
        text = work.select_dtypes(include=["object"]).astype(str).apply(lambda r: " | ".join(r.values), axis=1)
    text_norm = (text.str.lower().str.replace(r"[\u2013\u2014–—]+", "-", regex=True).str.replace(r"\s+", " ", regex=True).str.strip())
    keep_mask = pd.Series(False, index=work.index)
    resolved_location = pd.Series(pd.NA, index=work.index)
    for proc in PROCESSOR_PATTERNS:
        combined = re.compile("|".join(proc["patterns"]), flags=re.I)
        hit = text_norm.str.contains(combined, na=False)
        resolved_location.loc[hit] = proc["name"]
        keep_mask = keep_mask | hit
    out = work[keep_mask].copy()
    if out.empty: return out
    if "location" in out.columns:
        out["location"] = resolved_location.loc[out.index].fillna(out["location"])
    else:
        out["location"] = resolved_location.loc[out.index]
    if "source_site" not in out.columns:
        out["source_site"] = "Dunkerton"
    return out.reset_index(drop=True)

@st.cache_data(ttl=10 * 60, show_spinner=True)
def collect_all() -> tuple[pd.DataFrame, List[str], List[Dict[str, Any]]]:
    frames: List[pd.DataFrame] = []; issues: List[str] = []; debug_rows: List[Dict[str, Any]] = []
    for c in COOPS:
        res = fetch_coop_table(c["url"], c["location"])
        debug_rows.append(res)
        if res.get("ok"): frames.append(res["data"])
        else: issues.append(f'{c["name"]}: {res.get("error")} (status={res.get("status_code")}, has_table={res.get("has_table_tag")}, len={res.get("content_len")})')
    table = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return table, issues, debug_rows

def load_manual_feed() -> pd.DataFrame:
    if not MANUAL_FEED_URL: return pd.DataFrame()
    try: return pd.read_csv(MANUAL_FEED_URL)
    except Exception: return pd.DataFrame()

def coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns: out[col] = pd.to_numeric(out[col], errors="coerce")
    return out

def recompute_basis_if_requested(df: pd.DataFrame, futures_overrides: Dict[str, float]) -> pd.DataFrame:
    if df.empty: return df
    out = df.copy(); out.columns = [str(c).strip().lower() for c in out.columns]
    if "commodity" not in out.columns:
        for c in list(out.columns):
            if any(k in c for k in ["commodity","product","crop"]): out = out.rename(columns={c: "commodity"}); break
    if "cash" in out.columns:
        out = coerce_numeric(out, ["cash","futures","basis"])
        for key, fut_val in futures_overrides.items():
            is_corn = "corn" in key.lower(); is_soy = "soy" in key.lower()
            mask = pd.Series([False]*len(out))
            if "commodity" in out.columns:
                com = out["commodity"].astype(str).str.lower()
                if is_corn: mask = mask | com.str.contains("corn", na=False)
                if is_soy:  mask = mask | com.str.contains("soy|bean|soybean", regex=True, na=False)
            if "futures" in out.columns: out.loc[mask, "futures"] = fut_val
            else: out["futures"] = pd.NA; out.loc[mask, "futures"] = fut_val
        if "futures" in out.columns:
            if "basis" not in out.columns: out["basis"] = pd.NA
            missing = out["basis"].isna()
            out.loc[missing, "basis"] = out.loc[missing, "cash"] - out.loc[missing, "futures"]
    return out

def format_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    cols_order = [c for c in ["commodity","delivery","cash","basis","futures","location","last_refresh_epoch"] if c in df.columns]
    rest = [c for c in df.columns if c not in cols_order]
    return df[cols_order + rest]

st.title("Auto-fetched Cash Bids (beta)")

with st.sidebar:
    st.header("Options")
    futures_inputs = {label: st.number_input(label, value=float(default), step=0.01, format="%.4f") for label, default in FUTURE_INPUT_DEFAULTS.items()}
    st.divider()
    st.caption("Manual feed (optional)")
    st.info("Add MANUAL_FEED_URL to Streamlit secrets to merge a CSV/Sheet with custom rows.")

table, issues, debug_rows = collect_all()

manual_df = load_manual_feed()
if not manual_df.empty: table = pd.concat([table, manual_df], ignore_index=True)

table = patch_duplicate_columns(table)

routed = route_rows_to_processors(table)
if not routed.empty: table = routed

with st.expander("Live Fetch Debug"):
    st.write("Issues:", issues or "None")
    try:
        r = requests.get(COOPS[0]["url"], headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        st.write({"status_code": r.status_code, "len": len(r.text), "has_<table>": "<table" in r.text.lower()})
        st.code(r.text[:1500], language="html")
    except Exception as e:
        st.error(f"Probe error: {e}")

if table.empty:
    st.warning("No live tables were collected. Showing diagnostics and fallback demo rows.")
    demo = pd.DataFrame({"commodity":["Corn","Soybeans"],"delivery":["Nearby","Nearby"],"cash":[4.20,10.85],"basis":[-0.35,-0.90],"location":["Demo A","Demo B"],"last_refresh_epoch":[int(time.time())]*2})
    display_dataframe_safe(demo, use_container_width=True, height=420)
    st.stop()

table = recompute_basis_if_requested(table, futures_inputs)
table = format_for_display(table)

st.success(f"Collected **{len(table)}** rows")
display_dataframe_safe(table, use_container_width=True, height=520)

with st.expander("Diagnostics (raw results sample)"):
    st.json(debug_rows[:3])

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="bids")
        for idx, col in enumerate(df.columns):
            try: width = min(40, max(10, int(df[col].astype(str).str.len().mean() + 5)))
            except Exception: width = 20
            writer.sheets["bids"].set_column(idx, idx, width)
    return output.getvalue()

c1, c2 = st.columns(2)
with c1:
    st.download_button("⬇️ Download CSV", data=table.to_csv(index=False).encode("utf-8"), file_name="cash_bids.csv", mime="text/csv")
with c2:
    st.download_button("⬇️ Download Excel", data=to_excel_bytes(table), file_name="cash_bids.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
