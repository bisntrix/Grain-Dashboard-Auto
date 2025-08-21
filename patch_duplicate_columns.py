import pandas as pd
import streamlit as st

def enforce_unique_columns(df: pd.DataFrame, mode: str = "suffix") -> pd.DataFrame:
    """Return a copy of df with unique column names.
    mode="suffix" -> renames duplicates with .1, .2, ...
    mode="drop"   -> drops duplicate-named columns after the first.
    """
    if df is None:
        return df
    df = df.copy()
    if mode == "drop":
        df = df.loc[:, ~pd.Index(df.columns).duplicated()].copy()
        return df
    # default: suffix
    # Use Pandas' internal de-duper to create unique names (e.g., 'Bid', 'Bid.1', ...)
    df.columns = pd.io.parsers.ParserBase({'names': df.columns})._maybe_dedup_names(df.columns)
    return df

def display_dataframe_safe(df: pd.DataFrame, *, mode: str = "suffix", **st_dataframe_kwargs):
    """Safely display a DataFrame in Streamlit, auto-fixing duplicate columns and
    surfacing debug info instead of crashing.
    Usage:
        display_dataframe_safe(table, use_container_width=True, height=420)
    """
    if df is None:
        st.info("No data to display.")
        return

    # Deduplicate columns
    fixed = enforce_unique_columns(df, mode=mode)

    # If duplicates existed, show a small note
    cols = pd.Series(fixed.columns)
    # We can't detect originals directly after suffixing; compute duplicates on the *original* if possible
    try:
        orig_cols = pd.Series(df.columns)
        dups = orig_cols[orig_cols.duplicated(keep=False)]
        if not dups.empty:
            st.warning(f"Duplicate column names were detected and auto-fixed: {sorted(set(dups))}", icon="⚠️")
    except Exception:
        pass

    # Attempt to render
    try:
        st.dataframe(fixed, **st_dataframe_kwargs)
    except Exception as e:
        with st.expander("Debug: DataFrame/columns info"):
            st.write("Columns:", list(fixed.columns))
            st.write("Shape:", fixed.shape)
        st.error(f"Display error: {e}")
