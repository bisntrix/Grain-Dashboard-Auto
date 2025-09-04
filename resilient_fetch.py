from __future__ import annotations
import re, time, io, requests, pandas as pd
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

def http_get(url: str, timeout: int = 20) -> requests.Response:
    resp = requests.get(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv;q=0.9,*/*;q=0.8",
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

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(x) for x in tup if str(x) != 'nan']).strip() for tup in df.columns.values]
    df.columns = _make_unique(df.columns)
    return df

def _strip_empty(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df

TABLE_MATCH = re.compile(r"(corn|soy|soybean|cash|basis|bid|cbot|fut|delivery|month|price)", re.I)

def read_tables_any(resp_text: str, base_url: str | None = None) -> Dict[str, Any]:
    diagnostics = {
        "page_tables_found": 0,
        "page_table_shapes": [],
        "iframe_urls_tried": [],
        "iframe_tables_found": 0,
        "iframe_table_shapes": [],
        "csv_urls_tried": [],
        "csv_tables_found": 0,
        "csv_table_shapes": [],
        "snippets": [],
    }
    all_tables: List[pd.DataFrame] = []

    # Try HTML at page-level with/without match
    for flavor in ["lxml", "html5lib"]:
        for match_expr in [None, TABLE_MATCH]:
            try:
                tables = pd.read_html(resp_text, flavor=flavor, match=match_expr)
                for t in tables:
                    all_tables.append(t)
                    diagnostics["page_tables_found"] += 1
                    diagnostics["page_table_shapes"].append(getattr(t, "shape", None))
            except Exception:
                pass

    # Element-level tables
    try:
        soup = BeautifulSoup(resp_text, "html.parser")
        for t in soup.find_all("table"):
            try:
                tables = pd.read_html(str(t))
                for df in tables:
                    all_tables.append(df)
                    diagnostics["page_tables_found"] += 1
                    diagnostics["page_table_shapes"].append(getattr(df, "shape", None))
            except Exception:
                pass
    except Exception:
        pass

    # CSV export link (e.g., /markets/cashbid-download.php)
    try:
        soup = BeautifulSoup(resp_text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"cashbid[-_]download\\.php", href, re.I):
                target = urljoin(base_url, href) if base_url else href
                diagnostics["csv_urls_tried"].append(target)
                try:
                    r_csv = http_get(target, timeout=20)
                    # Attempt to parse CSV; sometimes it's HTML disguised, so try both
                    text = r_csv.text
                    parsed = None
                    try:
                        parsed = pd.read_csv(io.StringIO(text))
                    except Exception:
                        try:
                            parsed = pd.read_html(text)[0]
                        except Exception:
                            parsed = None
                    if isinstance(parsed, pd.DataFrame):
                        parsed = _strip_empty(_flatten_columns(parsed))
                        all_tables.append(parsed)
                        diagnostics["csv_tables_found"] += 1
                        diagnostics["csv_table_shapes"].append(getattr(parsed, "shape", None))
                except Exception:
                    pass
    except Exception:
        pass

    # Follow iframes (in case CSV not present and table is in embedded page)
    try:
        soup = BeautifulSoup(resp_text, "html.parser")
        iframes = soup.find_all("iframe")
        for iframe in iframes:
            src = iframe.get("src") or ""
            if not src:
                continue
            if re.search(r"(bid|bids|cash|market|quote|grain|table)", src, re.I):
                target = urljoin(base_url, src) if base_url else src
                diagnostics["iframe_urls_tried"].append(target)
                try:
                    r_if = http_get(target, timeout=20)
                    if r_if.ok:
                        for flavor in ["lxml", "html5lib"]:
                            for match_expr in [None, TABLE_MATCH]:
                                try:
                                    tables = pd.read_html(r_if.text, flavor=flavor, match=match_expr)
                                    for t in tables:
                                        all_tables.append(t)
                                        diagnostics["iframe_tables_found"] += 1
                                        diagnostics["iframe_table_shapes"].append(getattr(t, "shape", None))
                                except Exception:
                                    pass
                except Exception:
                    pass
    except Exception:
        pass

    # Clean and preview
    cleaned = []
    for df in all_tables:
        try:
            df = _flatten_columns(df.copy())
            df = _strip_empty(df)
            cleaned.append(df)
        except Exception:
            pass

    diagnostics["snippets"] = []
    for i, t in enumerate(cleaned[:3]):
        try:
            previews = t.astype(str).head(5).to_dict(orient="list")
            diagnostics["snippets"].append({"index": i, "shape": getattr(t, "shape", None), "head_rows": previews})
        except Exception:
            pass

    return {"tables": cleaned, "diagnostics": diagnostics}

def _long_form(df: pd.DataFrame) -> pd.DataFrame:
    commodity_like_cols = [c for c in df.columns if re.search(r"corn|soy|bean|soybean", str(c), re.I)]
    delivery_col = next((c for c in df.columns if re.search(r"deliv|month|period|delivery", str(c), re.I)), None)
    location_col = next((c for c in df.columns if re.search(r"location|loc|name", str(c), re.I)), None)

    if commodity_like_cols:
        id_vars = []
        if delivery_col: id_vars.append(delivery_col)
        if location_col: id_vars.append(location_col)
        try:
            melted = df.melt(id_vars=id_vars, value_vars=commodity_like_cols, var_name="commodity", value_name="cash")
            return melted
        except Exception:
            pass

    first_col = df.columns[0]
    if re.search(r"comm|product|crop", str(first_col), re.I):
        return df

    sample = df[first_col].astype(str).head(20).str.lower()
    if sample.str.contains(r"corn|soy|bean|soybean", regex=True).any():
        df = df.rename(columns={first_col: "commodity"})
        return df

    return df

def normalize_bid_table_smart(df: pd.DataFrame, location: str) -> pd.DataFrame:
    df = df.copy()
    df = _flatten_columns(df)
    df = _strip_empty(df)
    df = _long_form(df)

    # Map common column names (including Barchart export)
    cmap: Dict[str, str] = {}
    for c in df.columns:
        lc = str(c).lower().strip()
        if re.search(r"comm|product|crop", lc): cmap[c] = "commodity"
        elif re.search(r"^delivery(\\s|$)|deliv|month|period", lc): cmap[c] = "delivery"
        elif re.search(r"delivery\\s*start", lc): cmap[c] = "delivery_start"
        elif re.search(r"delivery\\s*end", lc): cmap[c] = "delivery_end"
        elif lc == "name": cmap[c] = "location"
        elif "basis" in lc: cmap[c] = "basis"
        elif re.search(r"fut|cbot", lc): cmap[c] = "futures"
        elif re.search(r"(\\$\\s*price|^price$|cash|bid)", lc): cmap.setdefault(c, "cash")
        elif re.search(r"location|loc", lc): cmap[c] = "location"
        else: cmap[c] = c
    df = df.rename(columns=cmap)

    # If only start/end are present, create a compact 'delivery'
    if "delivery" not in df.columns and ("delivery_start" in df.columns or "delivery_end" in df.columns):
        start = df.get("delivery_start").astype(str) if "delivery_start" in df.columns else ""
        end = df.get("delivery_end").astype(str) if "delivery_end" in df.columns else ""
        combo = (start.fillna("") + "–" + end.fillna("")).str.strip("– ").replace({"–": "Nearby"})
        df["delivery"] = combo.replace({"": "Nearby"})

    if "delivery" not in df.columns:
        df["delivery"] = "Nearby"

    # Clean numeric columns
    for col in ["cash", "basis", "futures"]:
        if col in df.columns:
            ser = df[col]
            if isinstance(ser, pd.DataFrame): ser = ser.iloc[:, 0]
            ser = ser.astype(str).str.replace(r"[^0-9.\\-+]", "", regex=True).replace({"": None})
            df[col] = pd.to_numeric(ser, errors="coerce")

    # If missing 'cash', choose a reasonable numeric column
    if "cash" not in df.columns:
        numeric_candidates = []
        for c in df.columns:
            try:
                vals = pd.to_numeric(df[c], errors="coerce")
                if vals.notna().sum() >= max(1, len(df)//3):
                    numeric_candidates.append((c, vals.notna().sum()))
            except Exception:
                pass
        if numeric_candidates:
            numeric_candidates.sort(key=lambda x: x[1], reverse=True)
            best = numeric_candidates[0][0]
            df = df.rename(columns={best: "cash"})

    # Filter commodity labels if present
    if "commodity" in df.columns:
        m = df["commodity"].astype(str).str.lower().str.contains("corn|soy|bean|soybean", regex=True, na=False)
        if m.any():
            df = df[m].copy()

    if "location" not in df.columns:
        df["location"] = location

    if "basis" not in df.columns and all(c in df.columns for c in ["cash", "futures"]):
        df["basis"] = df["cash"] - df["futures"]

    if "cash" in df.columns or "basis" in df.columns:
        df = df[(df.get("cash").notna() | df.get("basis").notna())]

    df["delivery"] = df["delivery"].astype(str).str.replace(r"\\s+", " ", regex=True).str.strip()
    df["last_refresh_epoch"] = int(time.time())
    order = [c for c in ["commodity","delivery","cash","basis","futures","location","last_refresh_epoch"] if c in df.columns]
    rest = [c for c in df.columns if c not in order]
    return df[order + rest].reset_index(drop=True)

def fetch_coop_table(url: str, location: str) -> Dict[str, Any]:
    meta = {"url": url, "location": location}
    try:
        resp = http_get(url)
    except Exception as e:
        return {"ok": False, "error": f"http_error: {e}", **meta}
    try:
        html = resp.text
        parsed = read_tables_any(html, base_url=url)
        tables = parsed["tables"]
        diags = parsed["diagnostics"]

        if not tables:
            return {
                "ok": False,
                "error": "no_tables_found",
                **meta,
                "status_code": resp.status_code,
                "content_len": len(html),
                "has_table_tag": "<table" in html.lower(),
                "diags": diags,
            }

        best = None; best_rows = 0; best_shape = None; best_preview = None
        per_table_meta = []
        for idx, t in enumerate(tables):
            rows = 0
            try:
                norm = normalize_bid_table_smart(t, location)
                rows = len(norm)
                if rows > best_rows:
                    best_rows = rows
                    best = norm
                    best_shape = getattr(t, "shape", None)
                    try:
                        best_preview = t.astype(str).head(5).to_dict(orient="list")
                    except Exception:
                        best_preview = None
            except Exception as e:
                per_table_meta.append({"index": idx, "norm_error": str(e), "shape": getattr(t, "shape", None)})
                continue
            per_table_meta.append({"index": idx, "normalized_rows": rows, "shape": getattr(t, "shape", None)})

        if best is None or best.empty:
            return {
                "ok": False,
                "error": "normalized_empty",
                **meta,
                "best_table_shape": best_shape,
                "diags": {**diags, "per_table": per_table_meta, "best_preview": best_preview},
            }

        return {
            "ok": True,
            "data": best,
            **meta,
            "best_table_shape": best_shape,
            "diags": {**diags, "per_table": per_table_meta, "best_preview": best_preview},
        }

    except Exception as e:
        return {"ok": False, "error": f"parse_error: {e}", **meta}
