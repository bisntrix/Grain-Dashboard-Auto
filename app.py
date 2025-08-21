# ===== TOP HEADER =====
import streamlit as st
import pandas as pd

from debug_shim import boot_banner, safe_render_df
from patch_duplicate_columns import display_dataframe_safe

# Show banner so you know the app started
boot_banner()
# ===== MIDDLE: BUILD YOUR DATAFRAME (no scraper needed) =====
# Futures price (you can change the default)
st.subheader("Futures & Cash Bids")
futures_contract = st.selectbox("Contract", ["Corn (Dec) CZ", "Corn (Mar) CH", "Soybeans (Nov) SX", "Soybeans (Jan) SF"])
default_futures = 4.60 if "Corn" in futures_contract else 11.50
fut_price = st.number_input("Futures price ($/bu)", value=float(default_futures), step=0.01)

# Co-ops you care about
coops = [
    "ADM Cedar Rapids",
    "Cargill Cedar Rapids (Soy)",
    "Shell Rock (Soy)",
    "Port Corn Fairbank",
    "Port Corn Shell Rock",
    "Dunkerton Coop",
    "Heartland Coop (Washburn)",
    "Mid Iowa Coop (La Porte City)",
]

# Simple inputs for each co-op & commodity
st.markdown("### Enter Cash Bids (any you know right now)")
rows = []
for loc in coops:
    with st.expander(loc, expanded=False):
        cash_corn = st.number_input(f"{loc} — Cash **Corn** ($/bu)", value=0.00, step=0.01, key=f"{loc}_corn")
        cash_soy  = st.number_input(f"{loc} — Cash **Soybeans** ($/bu)", value=0.00, step=0.01, key=f"{loc}_soy")
        if cash_corn > 0:
            rows.append({"Location": loc, "Commodity": "Corn", "Futures": fut_price, "Cash": cash_corn, "Basis": round(cash_corn - fut_price, 2)})
        if cash_soy > 0:
            # If you selected a corn futures but input soy cash, we still compute vs the same fut_price for simplicity.
            # (Later we can add separate futures per commodity.)
            rows.append({"Location": loc, "Commodity": "Soybeans", "Futures": fut_price, "Cash": cash_soy, "Basis": round(cash_soy - fut_price, 2)})

# Optional: upload CSV (headers: Location,Commodity,Cash,Futures) to prefill/append
uploaded = st.file_uploader("Optional: Upload CSV with columns Location,Commodity,Cash,Futures", type=["csv"])
if uploaded is not None:
    try:
        csv_df = pd.read_csv(uploaded)
        # Basic sanity
        need_cols = {"Location","Commodity","Cash","Futures"}
        if need_cols.issubset(set(csv_df.columns)):
            csv_df["Basis"] = (csv_df["Cash"] - csv_df["Futures"]).round(2)
            rows += csv_df.to_dict(orient="records")
        else:
            st.warning("CSV missing required columns: Location, Commodity, Cash, Futures")
    except Exception as e:
        st.error("Could not read CSV."); st.exception(e)

# Build the table
if rows:
    bids_table = pd.DataFrame(rows)
    # Nice ordering
    bids_table = bids_table[["Location","Commodity","Futures","Cash","Basis"]]
else:
    # If nothing entered yet, show an example so the app still renders
    bids_table = pd.DataFrame([
        {"Location":"ADM Cedar Rapids","Commodity":"Corn","Futures":fut_price,"Cash":fut_price-0.12,"Basis":-0.12},
        {"Location":"Cargill Cedar Rapids (Soy)","Commodity":"Soybeans","Futures":fut_price,"Cash":fut_price-0.08,"Basis":-0.08},
    ])

# ✅ Ensure final DataFrame is named 'table'
table = bids_table
# ===== END MIDDLE =====
safe_render_df(table, use_container_width=True, height=420)
# ===== END RENDER =====
