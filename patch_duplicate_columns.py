import pandas as pd
import streamlit as st

def enforce_unique_columns(df: pd.DataFrame, mode: str = "suffix") -> pd.DataFrame:
    if df is None:
        return df
    df = df.copy()
    if mode == "drop":
        # Drop duplicate columns, keep first
        return df.loc[:, ~df.columns.duplicated()].copy()
    else:
        # Rename duplicates with suffixes: e.g., Bid, Bid_1, Bid_2
        seen = {}
        new_cols = []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)
        df.columns = new_cols
        return df

def display_dataframe_safe(df: pd.DataFrame, *, mode: str = "suffix", **st_kwargs):
    if df is None:
        st.info("No data to display.")
        return
    fixed = enforce_unique_columns(df, mode=mode)
    try:
        orig_cols = pd.Series(df.columns)
        dups = orig_cols[orig_cols.duplicated(keep=False)]
        if not dups.empty:
            st.warning(f"Duplicate column names were auto-fixed: {sorted(set(dups))}", icon="⚠️")
    except Exception:
        pass
    st.dataframe(fixed, **st_kwargs)
