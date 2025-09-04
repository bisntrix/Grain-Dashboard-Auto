import streamlit as st
import pandas as pd

def display_dataframe_safe(df: pd.DataFrame, **kwargs):
    try:
        st.dataframe(df, **kwargs)
    except Exception as e:
        st.write("Display error:", e)
        try:
            st.table(df.head())
        except Exception:
            st.write("Could not render preview.")
