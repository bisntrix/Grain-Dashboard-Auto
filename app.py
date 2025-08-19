# app.py (patched to avoid KeyError on missing columns)
import io, math
from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf
import streamlit as st

st.set_page_config(page_title="Grain Marketing Dashboard (Auto)", layout="wide")

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

@st.cache_data(ttl=300, show_spinner=False)
def fetch_table(url, source_label):
    try:
        frames = []
        for t in pd.read_html(url, flavor="lxml"):
            cols = [c.lower() for c in t.columns.astype(str)]
            if any("cash" in c for c in cols) or any("basis" in c for c in cols) or any("bid" in c for c in cols):
                t["source"] = source_label
                t["source_url"] = url
                frames.append(t)
        if frames:
            return pd.concat(frames, ignore_index=True).dropna(how="all")
    except Exception:
        pass
    return pd.DataFrame()

@st.cache_data(ttl=300, show_spinner=False)
def aggregate_bids():
    sources = [
        ("https://www.dunkertoncoop.com/markets/cashgrid.php?commodity_filter=", "Dunkerton Co-op"),
        ("https://www.heartlandcoop.com/markets/cash.php?location_filter=19834", "Heartland – Washburn"),
        ("https://www.midiowacoop.com/grains/cash-bids/", "Mid Iowa Coop – La Porte City"),
        ("https://poet.gradable.com/market/Fairbank--IA", "POET – Fairbank"),
        ("https://poet.gradable.com/market/Shell-Rock--IA", "POET – Shell Rock"),
    ]
    parts = []
    for url, label in sources:
        df = fetch_table(url, label)
        if not df.empty:
            parts.append(df)
    if not parts:
        return pd.DataFrame()
    raw = pd.concat(parts, ignore_index=True).dropna(how="all")
    # Normalize loose columns
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if ("cash" in cl and "price" in cl) or cl == "cash": rename[c] = "Cash"
        if cl.startswith("basis") and "month" not in cl: rename[c] = "Basis"
        if "futures" in cl: rename[c] = "Futures"
        if "month" in cl and "basis" in cl: rename[c] = "Basis Month"
        if c == "Name": rename[c] = "Name"
    df = df.rename(columns=rename)
    # Clean numerics
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
    # Create Basis (¢/bu) if possible
    if "Futures" in df.columns and "Cash" in df.columns:
        df["Basis (¢/bu)"] = (df["Cash"] - df["Futures"]) * 100.0
    elif "Basis" in df.columns:
        df["Basis (¢/bu)"] = df["Basis"]
    # Ensure source columns exist
    for c in ["source", "source_url"]:
        if c not in df.columns:
            df[c] = ""
    return df

with st.sidebar:
    st.title("⚙️ Settings")
    st.markdown("**Futures symbols**  \nCorn: `ZC=F`  |  Soybeans: `ZS=F`")  
Corn: ZC=F  |  Soybeans: ZS=F")
    export_btn = st.empty()

st.title("🌽 Grain Marketing Dashboard — Auto (patched)")
st.caption("Auto-fetches public cash-bid tables when available.")

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
st.subheader("Auto-fetched Cash Bids (beta)")

agg = aggregate_bids()
if agg.empty:
    st.warning("No public tables fetched right now. Some sources may require login or block scraping. Try again later or upload CSV in the manual app.")
else:
    desired = ["Name", "Commodity", "Basis Month", "Futures", "Basis (¢/bu)", "Cash", "source", "source_url"]
    have = [c for c in desired if c in agg.columns]
    # Avoid KeyError: select only existing columns
    table = agg.loc[:, have].copy()
    st.dataframe(table, use_container_width=True, height=420)
    # Export
    csv_buf = io.StringIO()
    table.to_csv(csv_buf, index=False)
    export_btn.download_button("⬇️ Download fetched bids (CSV)", data=csv_buf.getvalue(), file_name="fetched_bids.csv", mime="text/csv")
