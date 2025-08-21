# ===== TOP HEADER =====
import streamlit as st
import pandas as pd

from debug_shim import boot_banner, safe_render_df
from patch_duplicate_columns import display_dataframe_safe

# Show banner so you know the app started
boot_banner()
import requests
from bs4 import BeautifulSoup  # only used if we need a fallback selector
import pandas as pd
import streamlit as st

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; GrainDashboard/1.0)"}

def _read_html_first_table(url: str) -> pd.DataFrame:
    # Many Agricharts/Barchart pages expose an HTML <table> we can read directly
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    # read_html returns a list; pick the widest table which usually is the grid
    tables = pd.read_html(res.text)
    if not tables:
        raise ValueError(f"No HTML tables found at {url}")
    df = max(tables, key=lambda t: t.shape[1])
    df["__source_url__"] = url
    return df

@st.cache_data(ttl=300, show_spinner=False)
def fetch_adm_cedar_rapids() -> pd.DataFrame:
    # ADM Cedar Rapids via Agricharts proxy
    url = "https://agriwaypartners.com/markets/cash.php?location_filter=18601"
    return _read_html_first_table(url).assign(Location="ADM Cedar Rapids")

@st.cache_data(ttl=300, show_spinner=False)
def fetch_cargill_cr_soy() -> pd.DataFrame:
    # Cargill Cedar Rapids (Soy) via Agricharts location id
    url = "https://www.dunkertoncoop.com/markets/cash.php?location_filter=82661"
    return _read_html_first_table(url).assign(Location="Cargill Cedar Rapids (Soy)")

@st.cache_data(ttl=300, show_spinner=False)
def fetch_dunkerton() -> pd.DataFrame:
    url = "https://dunkertoncoop.agricharts.com/markets/cash.php?location_filter=35525"
    return _read_html_first_table(url).assign(Location="Dunkerton Coop")

@st.cache_data(ttl=300, show_spinner=False)
def fetch_heartland_washburn() -> pd.DataFrame:
    url = "https://www.heartlandcoop.com/markets/cash.php?location_filter=19834"
    return _read_html_first_table(url).assign(Location="Heartland Coop (Washburn)")

@st.cache_data(ttl=300, show_spinner=False)
def fetch_mid_iowa_la_porte_city() -> pd.DataFrame:
    # Mid Iowa’s site has multiple views; this one lists a combined grid incl. SRSP row
    url = "https://www.midiowacoop.com/grains/cash-bids/"
    return _read_html_first_table(url).assign(Location="Mid Iowa Coop (La Porte City)")

@st.cache_data(ttl=300, show_spinner=False)
def fetch_poet_fairbank() -> pd.DataFrame:
    url = "https://www.farmerswin.com/markets/cash.php?location_filter=79179"
    return _read_html_first_table(url).assign(Location="POET Fairbank (Corn)")

@st.cache_data(ttl=300, show_spinner=False)
def fetch_poet_shell_rock() -> pd.DataFrame:
    url = "https://www.farmerswin.com/markets/cash.php?location_filter=79180"
    return _read_html_first_table(url).assign(Location="POET Shell Rock (Corn)")

@st.cache_data(ttl=300, show_spinner=False)
def fetch_srsp_via_mid_iowa() -> pd.DataFrame:
    # SRSP’s own page asks you to call for bids. We’ll surface the SRSP line from Mid-Iowa’s grid (when present).
    # If SRSP rows are present, we tag them specifically.
    url = "https://www.midiowacoop.com/grains/cash-bids/"
    df = _read_html_first_table(url)
    # Try to filter rows that mention Shell Rock Soy Processing (SRSP)
    mask = df.apply(lambda s: s.astype(str).str.contains("Shell Rock Soy", case=False, na=False)).any(axis=1)
    df = df.loc[mask].copy()
    if df.empty:
        return pd.DataFrame()
    return df.assign(Location="Shell Rock Soy Processing (via Mid Iowa)")

def _normalize(df: pd.DataFrame, location_label: str = None) -> pd.DataFrame:
    """
    Normalize various vendor grids to a common schema:
    ['Location','Commodity','DeliveryStart','DeliveryEnd','FuturesMonth','Futures','Basis','Cash']
    Columns on vendor pages commonly include: Name, Delivery, Delivery End, Futures Month, Futures Price, Change, Basis, $ Price
    """
    d = df.copy()
    # Keep only string column names
    d.columns = [str(c).strip() for c in d.columns]
    # Try to detect columns
    def _first_match(cands):  # return first existing column
        for c in cands:
            if c in d.columns:
                return c
        return None

    col_name = _first_match(["Name","Commodity","Product"])
    col_deliv = _first_match(["Delivery","Deliv Start","Delivery Start"])
    col_deliv_end = _first_match(["Delivery End","Deliv End","End"])
    col_fut_mo = _first_match(["Futures Month","Month","Option Month"])
    col_fut_px = _first_match(["Futures Price","Futures","Board","Board Price"])
    col_basis   = _first_match(["Basis","Basis (USD/bu)","Basis $"])
    col_cash    = _first_match(["$ Price","Cash Price","Cash","Cash (USD/bu)"])

    out = pd.DataFrame()
    out["Location"] = location_label or d.get("Location", location_label)
    out["Commodity"] = d[col_name] if col_name in d else None
    out["DeliveryStart"] = d[col_deliv] if col_deliv in d else None
    out["DeliveryEnd"] = d[col_deliv_end] if col_deliv_end in d else None
    out["FuturesMonth"] = d[col_fut_mo] if col_fut_mo in d else None
    out["Futures"] = pd.to_numeric(d[col_fut_px], errors="coerce") if col_fut_px in d else None
    out["Basis"] = pd.to_numeric(d[col_basis], errors="coerce") if col_basis in d else None
    out["Cash"]  = pd.to_numeric(d[col_cash], errors="coerce") if col_cash in d else None

    # Some tables include both CORN & SOY rows in "Name" or similar; keep just the useful rows
    # If no cash values, drop row
    if "Cash" in out:
        out = out.dropna(subset=["Cash"], how="all")
    return out

with st.status("Fetching live cash bids…", expanded=False):
    frames = []
    for fetcher in [
        fetch_adm_cedar_rapids,
        fetch_cargill_cr_soy,
        fetch_dunkerton,
        fetch_heartland_washburn,
        fetch_mid_iowa_la_porte_city,
        fetch_srsp_via_mid_iowa,   # may be empty if Mid-Iowa isn’t listing SRSP today
        fetch_poet_fairbank,
        fetch_poet_shell_rock,
    ]:
        try:
            raw = fetcher()
            # location label already assigned by fetcher()
            loc = raw.get("Location", [None])[0] if not isinstance(raw.get("Location"), str) else raw.get("Location")
            frames.append(_normalize(raw, location_label=loc if isinstance(loc, str) else None))
        except Exception as e:
            st.warning(f"Skipped {fetcher.__name__}: {e}")

    bids_table = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True)

# Fallback demo rows if still empty:
if bids_table.empty:
    bids_table = pd.DataFrame([
        {"Location":"ADM Cedar Rapids","Commodity":"CORN","FuturesMonth":"Dec","Futures":4.60,"Basis":-0.12,"Cash":4.48},
        {"Location":"Cargill Cedar Rapids (Soy)","Commodity":"SOYBEANS","FuturesMonth":"Nov","Futures":11.50,"Basis":-0.08,"Cash":11.42},
    ])

# ✅ Final table for render
table = bids_table[["Location","Commodity","FuturesMonth","Futures","Basis","Cash"]]
# ===== END LIVE FETCH =====
safe_render_df(table, use_container_width=True, height=420)
# ===== END RENDER =====
