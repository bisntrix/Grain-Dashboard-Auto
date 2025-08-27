from __future__ import annotations
import re, time, requests, pandas as pd
from typing import List, Dict, Any
from bs4 import BeautifulSoup

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

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

def _make_unique(cols):
    seen = {}
    out = []
    for c in map(str, cols):
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out

def read_tables_any(resp_text: str) -> List[pd.DataFrame]:
    out = []
    for flavor in ["lxml", "html5lib"]:
        try:
            out.extend(pd.read_html(resp_text, flavor=flavor))
        except Exception:
            pass
    try:
        soup = BeautifulSoup(resp_text, "html.parser")
        for t in soup.find_all("table"):
            try:
                out.extend(pd.read_html(str(t)))
            except Exception:
                pass
    except Exception:
        pass
    # de-dup exact frames
    dedup, seen = [], set()
    for df in out:
        df = df.copy()
        df.columns = _make_unique(df.columns)
        sig = (tuple(map(str, df.columns)), df.shape)
        if sig not in seen:
            dedup.append(df)
            seen.add(sig)
    return dedup

def normalize_bid_table(df: pd.DataFrame, location: str) -> pd.DataFrame:
    df = df.copy()
    # ensure unique column names before any Series ops
    df.columns = _make_unique(df.columns)
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]

    # map to standard names
    cmap = {}
    for c in df.columns:
        lc = c.lower()
        if "comm" in lc or lc in {"commodity","crop","product"}: cmap[c]="commodity"
        elif any(k in lc for k in ["deliv","month","period","delivery"]): cmap[c]="delivery"
        elif "basis" in lc: cmap[c]="basis"
        elif "fut" in lc or "cbot" in lc: cmap[c]="futures"
        elif any(k in lc for k in ["cash","bid","price","$/bu","$ per bu","$/ bu"]): cmap.setdefault(c,"cash")
        elif "loc" in lc: cmap[c]="location"
        else: cmap[c]=c
    df = df.rename(columns=cmap)

    keep = [c for c in ["commodity","delivery","cash","basis","futures","location"] if c in df.columns]
    if "cash" not in keep:
        nums = [c for c in df.columns if c not in keep and pd.api.types.is_numeric_dtype(df[c])]
        keep += nums[:1]
    df = df[keep].copy() if keep else df

    # clean numeric-like columns safely even if duplicates existed
    for col in ["cash","basis","futures"]:
        if col in df.columns:
            ser = df[col]
            if isinstance(ser, pd.DataFrame):
                ser = ser.iloc[:,0]
            ser = ser.astype(str).str.replace(r"[^0-9.\-+]", "", regex=True).replace({"": None})
            df[col] = pd.to_numeric(ser, errors="coerce")

    if "commodity" in df.columns:
        ser = df["commodity"]
        if isinstance(ser, pd.DataFrame):
            ser = ser.iloc[:,0]
        m = ser.astype(str).str.lower().str.contains("corn|soy|bean|soybean", regex=True, na=False)
        if m.any(): df = df[m].copy()

    if "location" not in df.columns: df["location"] = location
    if "basis" not in df.columns and all(c in df.columns for c in ["cash","futures"]):
        df["basis"] = df["cash"] - df["futures"]
    if "cash" in df.columns or "basis" in df.columns:
        df = df[(df.get("cash").notna() | df.get("basis").notna())]
    df["last_refresh_epoch"] = int(time.time())
    return df.reset_index(drop=True)

def fetch_coop_table(url: str, location: str) -> Dict[str, Any]:
    meta = {"url": url, "location": location}
    try:
        resp = http_get(url)
    except Exception as e:
        return {"ok": False, "error": f"http_error: {e}", **meta}
    try:
        html = resp.text
        tables = read_tables_any(html)
        if not tables:
            return {"ok": False, "error": "no_tables_found", **meta, "status_code": resp.status_code, "content_len": len(html), "has_table_tag": "<table" in html.lower()}
        tables.sort(key=lambda d: d.shape[0]*d.shape[1], reverse=True)
        df = normalize_bid_table(tables[0], location)
        if df.empty:
            return {"ok": False, "error": "normalized_empty", **meta, "table_shape": tables[0].shape}
        return {"ok": True, "data": df, **meta}
    except Exception as e:
        return {"ok": False, "error": f"parse_error: {e}", **meta}
