import streamlit as st
import pandas as pd

def boot_banner(title: str = "Grain Marketing Dashboard — Debug Mode"):
    """Show a guaranteed banner so you know the app actually started."""
    try:
        st.set_page_config(page_title=title, layout="wide")
    except Exception:
        # set_page_config can only be called once; ignore if already set
        pass
    st.markdown(":white_check_mark: **App booted** — if you can see this, Streamlit is running.")

def safe_render_df(df, **st_kwargs):
    """Render a DataFrame only if it exists and is valid; otherwise show an inline diagnostic."""
    try:
        import pandas as pd
        from patch_duplicate_columns import display_dataframe_safe
    except Exception as e:
        st.error("Failed to import helpers for table rendering.")
        st.exception(e)
        return

    if 'df' in locals() and isinstance(df, pd.DataFrame):
        try:
            display_dataframe_safe(df, **st_kwargs)
        except Exception as e:
            st.error("Display failed.")
            st.exception(e)
    else:
        st.warning("No DataFrame available to display (variable was not built).")
