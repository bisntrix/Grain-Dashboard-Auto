# Simple helper to uniquify duplicate column names like ['cash','cash'] -> ['cash','cash_2']
from __future__ import annotations
import pandas as pd

def patch_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(map(str, df.columns))
    seen = {}
    new_cols = []
    for c in cols:
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
    out = df.copy()
    out.columns = new_cols
    return out
