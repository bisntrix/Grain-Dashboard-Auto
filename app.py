import streamlit as st
import pandas as pd
from patch_duplicate_columns import display_dataframe_safe

# ... your existing code above that builds the DataFrame 'table' ...

# Example placeholder for building table
# table = build_table_somehow()

# When ready to display the DataFrame:
import pandas as pd  # (safe if already imported)
from patch_duplicate_columns import display_dataframe_safe

if 'table' in globals() and isinstance(table, pd.DataFrame):
    display_dataframe_safe(table, use_container_width=True, height=420)
