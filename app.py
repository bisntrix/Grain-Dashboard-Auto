import streamlit as st
import pandas as pd

def display_dataframe_safe(df: pd.DataFrame, *, mode: str = "suffix", **st_kwargs):
    """Show a DataFrame in Streamlit, fixing duplicate column names automatically."""
    if df is None:
        st.info("No data to display.")
        return

    df = df.copy()
    if mode == "drop":
        # drop extra columns if names are duplicated
        df = df.loc[:, ~pd.Index(df.columns).duplicated()].copy()
    else:
        # rename duplicates with suffixes (Bid, Bid.1, Bid.2, …)
        df.columns = pd.io.parsers.ParserBase({'names': df.columns})._maybe_dedup_names(df.columns)

    # warn if duplicates were fixed
    orig_cols = pd.Series(df.columns)
    dups = orig_cols[orig_cols.duplicated(keep=False)]

# ... your existing imports and code above ...

# Assume `table` has already been created at this point

# --- Fix duplicate columns before display ---
if table is not None:
    table = table.copy()
    # Rename duplicates with suffixes instead of crashing
    table.columns = pd.io.parsers.ParserBase({'names': table.columns})._maybe_dedup_names(table.columns)

    try:
        st.dataframe(table, use_container_width=True, height=420)
    except Exception as e:
        with st.expander("Debug: DataFrame columns (duplicates highlighted)"):
            cols = pd.Series(table.columns)
            dups = cols[cols.duplicated(keep=False)]
            st.write("Duplicate columns:", list(dups))
            st.write("All columns:", list(table.columns))
        st.error(f"Display error: {e}")
