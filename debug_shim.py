import streamlit as st
import pandas as pd

def display_dataframe_safe(df: pd.DataFrame, **kwargs):
    try:
        st.dataframe(df, **kwargs)
    except Exception as e:
        st.write("Display error:", e)
        st.write(df.head())
