import pandas as pd

def patch_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure unique column names by appending suffixes to duplicates.
    """
    if df is None or getattr(df, "empty", True):
        return df
    df = df.copy()
    # A simple, dependency-free de-duplication
    seen = {}
    new_cols = []
    for c in map(str, df.columns):
        if c not in seen:
            seen[c] = 1
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
    df.columns = new_cols
    return df
