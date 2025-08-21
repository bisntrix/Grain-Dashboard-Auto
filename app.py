import streamlit as st
import pandas as pd

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
