# resilient_fetch.py
from __future__ import annotations
import re
import time
from typing import Optional, List, Dict, Any
import requests
import pandas as pd
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

def http_get(url: str, timeout: int = 20) -> requests.Response:
    resp = requests.get(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp

def read_tables_any(resp_text: str) -> List[pd.DataFrame]:
    """Try multiple parsers to maximize odds of getting tables."""
    out = []
    # 1) pandas read_html (lxml)
    for flavor in ["lxml", "html5lib"]:
        try:
            dfs = pd.read_html(resp_text, flavor=flavor)
            out.extend(dfs)
        except Exception:
            pass
    # 2) manual soup → to pandas
    try:
        soup = BeautifulSoup(resp_text, "html.parser")
        tables = soup.find_all("table")
        for t in tables:
            html = str(t)
            try:
                dfs = pd.read_html(html)
                out.extend(dfs)
            except Exception:
                pass
    except Exception:
        pass
    # De-dup by shape+columns signature
    dedup = []
    seen = set()
    for df in out:
        sig = (tuple(df.columns.astype(str)), df.shape)
        if sig not in seen:
            dedup.append(df)
            seen.add(sig)
    return dedup

def normalize_bid_table(df: pd.DataFrame, location: str) -> pd.DataFrame:
    """Make lots of column-name variations work; keep Corn/Soy, create basis if missing."""
    original_cols = [str(c).strip() for c in df.columns]
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]

    # unify column names
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if "comm" in lc or "product" in lc or lc in {"commodity", "crop"}:
            col_map[c] = "commodity"
        elif "deliv" in lc or "period" in lc or "month" in lc or "delivery" in lc:
            col_map[c] = "delivery"
        elif any(k in lc for k in ["cash", "bid", "price", "$/bu", "$ per bu", "$/ bu"]):
            # prefer first occurrence as cash
            col_map.setdefault(c, "cash")
        elif "basis" in lc:
            col_map[c] = "basis"
        elif "fut" in lc or "cbot" in lc:
            col_map[c] = "futures"
        elif "loc" in lc:
            col_map[c] = "location"
        else:
            # keep others as-is
            col_map[c] = c

    df = df.rename(columns=col_map)

    # keep useful columns if present
    keep = [c for c in ["commodity", "delivery", "cash", "basis", "futures", "location"] if c in df.columns]
    # include any numeric-looking columns if cash/basis were missed
    if "cash" not in keep:
        num_candidates = [c for c in df.columns if c not in keep and pd.api.types.is_numeric_dtype(df[c])]
        keep += num_candidates[:1]  # pick one
    df = df[keep].copy()

    # clean currency-like text
    for col in ["cash", "basis", "futures"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^0-9\.\-\+]", "", regex=True)
                .replace({"": None})
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # filter to corn/soy only if a commodity col exists
    if "commodity" in df.columns:
        df["commodity_norm"] = df["commodity"].astype(str).str.lower()
        mask = df["commodity_norm"].str.contains("corn|soy|bean|soybean", regex=True, na=False)
        # If filtering kills everything, don’t filter
        if mask.any():
            df = df[mask].copy()
        df.drop(columns=["commodity_norm"], inplace=True)

    # stamp location
    if "location" not in df.columns:
        df["location"] = location

    # create basis if missing and we have cash+futures
    if "basis" not in df.columns and all(c in df.columns for c in ["cash", "futures"]):
        df["basis"] = df["cash"] - df["futures"]

    # keep reasonable rows only (non-null cash or basis)
    if "cash" in df.columns or "basis" in df.columns:
        df = df[(df.get("cash").notna() | df.get("basis").notna())]

    # add a last_refresh
    df["last_refresh_epoch"] = int(time.time())
    return df.reset_index(drop=True)

def fetch_coop_table(url: str, location: str) -> Dict[str, Any]:
    meta = {"url": url, "location": location}
    try:
        resp = http_get(url)
    except Exception as e:
        return {"ok": False, "error": f"http_error: {e}", **meta}

    try:
        tables = read_tables_any(resp.text)
        if not tables:
            return {"ok": False, "error": "no_tables_found", **meta, "content_len": len(resp.text)}
        # pick the biggest table by area
        tables.sort(key=lambda dfi: dfi.shape[0] * dfi.shape[1], reverse=True)
        df = normalize_bid_table(tables[0], location=location)
        if df.empty:
            return {"ok": False, "error": "normalized_empty", **meta, "table_shape": tables[0].shape}
        return {"ok": True, "data": df, **meta}
    except Exception as e:
        return {"ok": False, "error": f"parse_error: {e}", **meta}
