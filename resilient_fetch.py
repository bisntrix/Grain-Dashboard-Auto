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

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(x) for x in tup if str(x) != 'nan']).strip() for tup in df.columns.values]
    df.columns = _make_unique(df.columns)
    return df

def _strip_empty(df: pd.DataFrame) -> pd.DataFrame:
    # drop rows/cols that are completely empty
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df

def read_tables_any(resp_text: str) -> List[pd.DataFrame]:
    # Try both page-level and element-level parsing
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
    # Clean/flatten/uniquify each
    cleaned = []
    for df in out:
        try:
            df = _flatten_columns(df.copy())
            df = _strip_empty(df)
            cleaned.append(df)
        except Exception:
            pass
    return cleaned

KEYWORDS_COL = re.compile(r"(comm|product|crop|deliv|month|period|basis|fut|cbot|price|cash|bid|location|loc|soy|bean|corn)", re.I)
KEYWORDS_CELL = re.compile(r"(corn|soy|soybean|cedar\s*rapids|adm|cargill|srsp|shell\s*rock|basis|cash|bid|\$)", re.I)

def _first_row_as_header_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    # If column names look generic/unnamed and first row has many keyword hits, promote it
    col_hits = sum(bool(KEYWORDS_COL.search(str(c))) for c in df.columns)
    if col_hits >= max(1, len(df.columns)//3):
        return df
    if df.empty: return df
    first = df.iloc[0].astype(str)
    cell_hits = sum(bool(KEYWORDS_CELL.search(x)) for x in first)
    if cell_hits >= max(1, len(first)//3):
        new_cols = [str(x).strip() for x in first]
        rest = df.iloc[1:].copy()
        rest.columns = _make_unique(new_cols)
        return rest
    return df

def _long_form(df: pd.DataFrame) -> pd.DataFrame:
    cols_lower = [str(c).lower() for c in df.columns]
    commodity_like_cols = [c for c in df.columns if re.search(r"corn|soy|bean|soybean", str(c), re.I)]
    delivery_col = next((c for c in df.columns if re.search(r"deliv|month|period|delivery", str(c), re.I)), None)
    location_col = next((c for c in df.columns if re.search(r"location|loc", str(c), re.I)), None)

    # If commodity columns exist, melt them even if no delivery column
    if commodity_like_cols:
        id_vars = []
        if delivery_col: id_vars.append(delivery_col)
        if location_col: id_vars.append(location_col)
        try:
            melted = df.melt(id_vars=id_vars, value_vars=commodity_like_cols, var_name="commodity", value_name="cash")
            return melted
        except Exception:
            pass  # fall through to other heuristics

    # If first column looks like commodity labels
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
    df = _first_row_as_header_if_needed(df)
    df = _long_form(df)

    # Standardize names
    cmap = {}
    for c in df.columns:
        lc = str(c).lower().strip()
        if re.search(r"comm|product|crop", lc): cmap[c] = "commodity"
        elif re.search(r"deliv|month|period|delivery", lc): cmap[c] = "delivery"
        elif "basis" in lc: cmap[c] = "basis"
        elif re.search(r"fut|cbot", lc): cmap[c] = "futures"
        elif re.search(r"cash|bid|price|\$/?\s*bu", lc): cmap.setdefault(c, "cash")
        elif re.search(r"location|loc", lc): cmap[c] = "location"
        else: cmap[c] = c
    df = df.rename(columns=cmap)

    # If no delivery survived, default to Nearby
    if "delivery" not in df.columns:
        df["delivery"] = "Nearby"

    # Clean numbers (cash/basis/futures if present)
    for col in ["cash", "basis", "futures"]:
        if col in df.columns:
            ser = df[col]
            if isinstance(ser, pd.DataFrame):
                ser = ser.iloc[:, 0]
            ser = ser.astype(str).str.replace(r"[^0-9.\-+]", "", regex=True).replace({"": None})
            df[col] = pd.to_numeric(ser, errors="coerce")

    # If still no 'cash', try to pick best numeric column as cash
    if "cash" not in df.columns:
        numeric_candidates = []
        for c in df.columns:
            try:
                vals = pd.to_numeric(
                    df[c].astype(str).str.replace(r"[^0-9.\-+]", "", regex=True),
                    errors="coerce"
                )
                if vals.notna().sum() >= max(1, len(vals) // 3):
                    numeric_candidates.append((c, vals.notna().sum()))
            except Exception:
                pass
        if numeric_candidates:
            numeric_candidates.sort(key=lambda x: x[1], reverse=True)
            best_col = numeric_candidates[0][0]
            df = df.rename(columns={best_col: "cash"})
            df["cash"] = pd.to_numeric(
                df["cash"].astype(str).str.replace(r"[^0-9.\-+]", "", regex=True),
                errors="coerce"
            )

    if "commodity" in df.columns:
        m = df["commodity"].astype(str).str.lower().str.contains("corn|soy|bean|soybean", regex=True, na=False)
        if m.any():
            df = df[m].copy()

    if "location" not in df.columns:
        df["location"] = location

    if "basis" not in df.columns and all(c in df.columns for c in ["cash", "futures"]):
        df["basis"] = df["cash"] - df["futures"]

    # Keep rows with some price info if possible
    if "cash" in df.columns or "basis" in df.columns:
        df = df[(df.get("cash").notna() | df.get("basis").notna())]

    if "delivery" in df.columns:
        df["delivery"] = df["delivery"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    df["last_refresh_epoch"] = int(time.time())
    order = [c for c in ["commodity", "delivery", "cash", "basis", "futures", "location", "last_refresh_epoch"] if c in df.columns]
    rest = [c for c in df.columns if c not in order]
    return df[order + rest].reset_index(drop=True)

def normalize_bid_table_smart(df: pd.DataFrame, location: str) -> pd.DataFrame:
    df = df.copy()
    df = _flatten_columns(df)
    df = _strip_empty(df)
    df = _first_row_as_header_if_needed(df)
    df = _long_form(df)

    # Standardize names
    cmap = {}
    for c in df.columns:
        lc = str(c).lower().strip()
        if re.search(r"comm|product|crop", lc): cmap[c]="commodity"
        elif re.search(r"deliv|month|period|delivery", lc): cmap[c]="delivery"
        elif "basis" in lc: cmap[c]="basis"
        elif re.search(r"fut|cbot", lc): cmap[c]="futures"
        elif re.search(r"cash|bid|price|\$/?\s*bu", lc): cmap.setdefault(c,"cash")
        elif re.search(r"location|loc", lc): cmap[c]="location"
        else: cmap[c]=c
    df = df.rename(columns=cmap)

    # Clean numbers
    for col in ["cash","basis","futures"]:
        if col in df.columns:
            ser = df[col]
            if isinstance(ser, pd.DataFrame): ser = ser.iloc[:,0]
            ser = ser.astype(str).str.replace(r"[^0-9.\-+]", "", regex=True).replace({"": None})
            df[col] = pd.to_numeric(ser, errors="coerce")

    # Commodity filter only if it keeps some rows
    if "commodity" in df.columns:
        ser = df["commodity"]
        if isinstance(ser, pd.DataFrame): ser = ser.iloc[:,0]
        m = ser.astype(str).str.lower().str.contains("corn|soy|bean|soybean", regex=True, na=False)
        if m.any():
            df = df[m].copy()

    # If we have melted cash from columns, ensure 'cash' is there or pick best numeric
    if "cash" not in df.columns:
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if numeric_cols:
            df = df.rename(columns={numeric_cols[0]:"cash"})

    # Location stamp
    if "location" not in df.columns:
        df["location"] = location

    # Compute basis if possible
    if "basis" not in df.columns and all(c in df.columns for c in ["cash","futures"]):
        df["basis"] = df["cash"] - df["futures"]

    # Keep rows with some price info
    if "cash" in df.columns or "basis" in df.columns:
        df = df[(df.get("cash").notna() | df.get("basis").notna())]

    # Delivery cleanup: keep short labels
    if "delivery" in df.columns:
        df["delivery"] = df["delivery"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    df["last_refresh_epoch"] = int(time.time())
    # Keep key cols first
    order = [c for c in ["commodity","delivery","cash","basis","futures","location","last_refresh_epoch"] if c in df.columns]
    rest = [c for c in df.columns if c not in order]
    df = df[order + rest]
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
        # Try to normalize each table; pick the one that yields most priced rows
        best = None; best_rows = 0; best_shape = None
        for t in tables:
            try:
                norm = normalize_bid_table_smart(t, location)
                rows = len(norm)
                if rows > best_rows:
                    best_rows = rows; best = norm; best_shape = t.shape
            except Exception:
                continue
        if best is None or best.empty:
            return {"ok": False, "error": "normalized_empty", **meta, "table_shape": best_shape or (None,None)}
        return {"ok": True, "data": best, **meta}
    except Exception as e:
        return {"ok": False, "error": f"parse_error: {e}", **meta}
