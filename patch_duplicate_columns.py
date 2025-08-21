import pandas as pd
import streamlit as st

def enforce_unique_columns(df: pd.DataFrame, mode: str = "suffix") -> pd.DataFrame:
    if df is None:
        return df
    df = df.copy()
    if mode == "drop":
        df = df.loc[:, ~pd.Index(df.columns).duplicated()].copy()
        return df
    df.columns = pd.io.parsers.ParserBase({'names': df.columns})._maybe_dedup_names(df.columns)
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
