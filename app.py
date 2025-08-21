import streamlit as st
import pandas as pd

from debug_shim import boot_banner, safe_render_df
from patch_duplicate_columns import display_dataframe_safe

# Always show a banner so you know the app started
boot_banner()

# >>> DO NOT touch 'table' up here <<<
# Just build your data and DataFrame later in the file.

if 'table' in globals() and isinstance(table, pd.DataFrame):
    display_dataframe_safe(table, use_container_width=True, height=420)
