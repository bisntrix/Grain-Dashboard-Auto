from __future__ import annotations
import streamlit as st
import pandas as pd

def display_dataframe_safe(df: pd.DataFrame, **kwargs):
    try:
        st.dataframe(df, **kwargs)
    except Exception as e:
        st.error(f"Display error: {e}")
        st.write("Showing head(50) as fallback:")
        st.table(df.head(50))
