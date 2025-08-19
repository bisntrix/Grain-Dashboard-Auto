# app.py
# Grain Marketing Dashboard – Auto-Fetch Edition
# Pulls cash bids from public co-op pages where available and normalizes to a common table.
# Sources included (public pages):
# - Dunkerton Co-op cash grid (includes ADM Cedar Rapids Corn & Cargill Cedar Rapids Soy) 
# - Heartland Co-op (Washburn location page)
# - Mid Iowa Co-op (cash bids page)
# - POET (Fairbank & Shell Rock) via Gradable public market pages (best-effort)
#
# Notes:
# - Some pages are powered by Barchart and render simple HTML tables → parsed via pandas.read_html.
# - Some pages require login (ADM FarmView, Cargill portals) → not scraped; we use alternate public mirrors when possible.
# - If a scrape fails, the app skips that source and shows what it could fetch.
#
# Deploy on Streamlit Cloud with app.py + requirements.txt

import io
import math
from datetime import datetime, date
from typing import List, Dict

import pandas as pd
import numpy as np
import yfinance as yf
import streamlit as st

st.set_page_config(page_title="Grain Marketing Dashboard (Auto)", layout="wide")

# -----------------------
# Futures
# -----------------------
@st.cache_data(ttl=90)
def get_futures_quote(symbol: str) -> dict:
    try:
        t = yf.Ticker(symbol)
        info = getattr(t, "fast_info", None)
        price, prev = None, None
        if info:
            price = info.get("last_price")
            prev = info.get("previous_close")
        if price is None or prev is None:
            hist = t.history(period="2d", auto_adjust=False)
            if not hist.empty:
                price = float(hist["Close"].iloc[-1])
                prev = float(hist["Close"].iloc[-2]) if len(hist) > 1 else price
        chg = None if (price is None or prev is None) else price - prev
        chg_pct = None if (price is None or prev in (None, 0)) else (100 * chg / prev)
        return {"symbol": symbol, "price": price, "prev": prev, "chg": chg, "chg_pct": chg_pct}
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

def fmt(x, nd=2):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "-"
    return f"{x:,.{nd}f}"

# -----------------------
# Scrapers
# -----------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_dunkerton_grid() -> pd.DataFrame:
    # Main grid (all locations) + specific embedded pages that expose ADM/Cargill lines.
    urls = [
        "https://www.dunkertoncoop.com/markets/cashgrid.php?commodity_filter=",
        "https://www.dunkertoncoop.com/markets/cash.php?location_filter=35558&print=true",  # ADM Cedar Rapids Corn
        "https://www.dunkertoncoop.com/markets/cash.php?location_filter=82661",            # Cargill Cedar Rapids - Soybeans
    ]
    frames = []
    for url in urls:
        try:
            tables = pd.read_html(url, flavor="lxml")
            for t in tables:
                # Heuristic: keep tables that have 'Cash Price' or 'Cash' and 'Basis'
                cols = [c.lower() for c in t.columns.astype(str)]
                if any("cash" in c for c in cols) and any("basis" in c for c in cols):
                    t["source"] = "Dunkerton Co-op"
                    t["source_url"] = url
                    frames.append(t)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True).dropna(how="all")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def fetch_heartland_washburn() -> pd.DataFrame:
    url = "https://www.heartlandcoop.com/markets/cash.php?location_filter=19834"
    try:
        tables = pd.read_html(url, flavor="lxml")
        frames = []
        for t in tables:
            cols = [c.lower() for c in t.columns.astype(str)]
            if any("cash" in c for c in cols) and any("basis" in c for c in cols):
                t["source"] = "Heartland Coop – Washburn"
                t["source_url"] = url
                frames.append(t)
        if frames:
            return pd.concat(frames, ignore_index=True).dropna(how="all")
    except Exception:
        pass
    return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_mid_iowa() -> pd.DataFrame:
    url = "https://www.midiowacoop.com/grains/cash-bids/"
    try:
        tables = pd.read_html(url, flavor="lxml")
        frames = []
        for t in tables:
            cols = [c.lower() for c in t.columns.astype(str)]
            if any("cash" in c for c in cols) and any("basis" in c for c in cols):
                t["source"] = "Mid Iowa Coop – La Porte City"
                t["source_url"] = url
                frames.append(t)
        if frames:
            return pd.concat(frames, ignore_index=True).dropna(how="all")
    except Exception:
        pass
    return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_poet(term: str) -> pd.DataFrame:
    # term: "Fairbank" or "Shell Rock"
    # Gradable pages may be JS-rendered; pandas may or may not capture a table.
    url_map = {
        "Fairbank": "https://poet.gradable.com/market/Fairbank--IA",
        "Shell Rock": "https://poet.gradable.com/market/Shell-Rock--IA",
    }
    url = url_map.get(term)
    try:
        tables = pd.read_html(url, flavor="lxml")
        frames = []
        for t in tables:
            # Look for 'Corn' / bids columns heuristically
            cols = [c.lower() for c in t.columns.astype(str)]
            if any("cash" in c for c in cols) or any("bid" in c for c in cols):
                t["source"] = f"POET – {term}"
                t["source_url"] = url
                frames.append(t)
        if frames:
            return pd.concat(frames, ignore_index=True).dropna(how="all")
    except Exception:
        pass
    return pd.DataFrame()

def _normalize_df(raw: pd.DataFrame, commodity_hint: str | None = None, location_hint: str | None = None) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # Try to identify common columns
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if "delivery" in cl and "end" not in cl and "start" not in cl and "month" not in cl:
            rename[c] = "Delivery"
        if cl.startswith("basis") and "month" not in cl:
            rename[c] = "Basis"
        if ("cash" in cl and "price" in cl) or cl == "cash":
            rename[c] = "Cash"
        if "futures" in cl and "price" in cl:
            rename[c] = "Futures"
        if "month" in cl and "basis" in cl:
            rename[c] = "Basis Month"
        if cl == "name":
            rename[c] = "Name"
    df = df.rename(columns=rename)
    # Keep relevant columns if present
    keep = [c for c in ["Name", "Delivery", "Basis Month", "Futures", "Basis", "Cash"] if c in df.columns]
    if not keep:
        return pd.DataFrame()
    df = df[keep]
    # Add hints
    if "Name" not in df.columns and location_hint:
        df["Name"] = location_hint
    if commodity_hint:
        df["Commodity"] = commodity_hint
    # Clean numeric
    for col in ["Futures", "Basis", "Cash"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.replace("¢", "", regex=False)
                .str.extract(r"([-+]?\d*\.?\d+)")[0]
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

@st.cache_data(ttl=300, show_spinner=False)
def aggregate_bids() -> pd.DataFrame:
    parts = []
    # Dunkerton (often includes ADM/Cargill lines)
    d = fetch_dunkerton_grid()
    if not d.empty:
        parts.append(_normalize_df(d))
    # Heartland – Washburn
    h = fetch_heartland_washburn()
    if not h.empty:
        parts.append(_normalize_df(h, location_hint="Heartland – Washburn"))
    # Mid Iowa – La Porte City
    m = fetch_mid_iowa()
    if not m.empty:
        parts.append(_normalize_df(m, location_hint="Mid Iowa – La Porte City"))
    # POET plants (best-effort)
    pf = fetch_poet("Fairbank")
    if not pf.empty:
        parts.append(_normalize_df(pf, commodity_hint="Corn", location_hint="POET – Fairbank"))
    ps = fetch_poet("Shell Rock")
    if not ps.empty:
        parts.append(_normalize_df(ps, commodity_hint="Corn", location_hint="POET – Shell Rock"))
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    # Remove all-empty rows, drop dup columns
    df = df.dropna(how="all")
    # Create basis cents when Futures available
    if "Futures" in df.columns and "Cash" in df.columns:
        df["Basis (¢/bu)"] = (df["Cash"] - df["Futures"]) * 100.0
    # Minor tidy
    if "Basis" in df.columns and "Basis (¢/bu)" not in df.columns:
        df["Basis (¢/bu)"] = df["Basis"]
    # Add timestamp
    df["Fetched"] = pd.Timestamp.utcnow().tz_localize(None)
    return df

# -----------------------
# UI
# -----------------------
with st.sidebar:
    st.title("⚙️ Settings")
    data_mode = st.radio("Data source", ["Auto-fetch (web)", "Upload CSV"], index=0)
    st.caption("Auto mode scrapes public cash-bid pages. If a site requires login, use CSV upload.")
    st.write("---")
    st.markdown("**Futures symbols**  \nCorn: ZC=F  |  Soybeans: ZS=F")
    st.write("---")
    export_btn = st.empty()

st.title("🌽 Grain Marketing Dashboard — Auto")
st.caption("Experimental auto-fetch of public cash-bid pages. For planning only.")

# Futures header
corn = get_futures_quote("ZC=F")
soy = get_futures_quote("ZS=F")
f_left, f_right = st.columns(2)
with f_left:
    st.subheader("Corn Futures (ZC=F)")
    c1, c2, c3 = st.columns(3)
    c1.metric("Price ($/bu)", fmt(corn.get("price")))
    c2.metric("Change ($)", fmt(corn.get("chg")))
    c3.metric("Change (%)", "-" if corn.get("chg_pct") is None else f"{corn['chg_pct']:.2f}%")
with f_right:
    st.subheader("Soybean Futures (ZS=F)")
    s1, s2, s3 = st.columns(3)
    s1.metric("Price ($/bu)", fmt(soy.get("price")))
    s2.metric("Change ($)", fmt(soy.get("chg")))
    s3.metric("Change (%)", "-" if soy.get("chg_pct") is None else f"{soy['chg_pct']:.2f}%")

st.write("---")

if data_mode == "Upload CSV":
    file = st.file_uploader("Upload cash bids CSV", type=["csv"])
    if file is None:
        st.info("Upload a CSV to continue, or switch to Auto-fetch.")
        st.stop()
    try:
        bids = pd.read_csv(file)
        st.subheader("Uploaded Bids")
        st.dataframe(bids, use_container_width=True, height=350)
    except Exception as e:
        st.error(f"Could not read CSV: {e}")
        st.stop()
else:
    st.subheader("Auto-fetched Cash Bids (beta)")
    agg = aggregate_bids()
    if agg.empty:
        st.warning("No public tables fetched. Some sources may require login. Try again later or use CSV upload.")
    else:
        # Sort and display
        display_cols = [c for c in ["Name", "Commodity", "Basis Month", "Futures", "Basis (¢/bu)", "Cash", "source"] if c in agg.columns]
        table = agg[display_cols + ["source_url"]].copy()
        st.dataframe(table, use_container_width=True, height=400)
        # Export
        csv_buf = io.StringIO()
        table.to_csv(csv_buf, index=False)
        export_btn.download_button("⬇️ Download fetched bids (CSV)", data=csv_buf.getvalue(), file_name="fetched_bids.csv", mime="text/csv")

st.caption("Sources vary in availability. Public pages can change without notice. If a site blocks scraping or requires login, upload a CSV instead.")
