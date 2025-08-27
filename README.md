# Grain Marketing Dashboard (Row-Routing Edition)

Clean Streamlit app for Iowa co-op bids with **row routing** to pick out ADM Cedar Rapids (corn),
Cargill Cedar Rapids (soybeans), and SRSP rows from co-op pages like Dunkerton.

## Quick Start
1. Push this folder to a **GitHub repo**.
2. In `app.py`, the Dunkerton URL is already set. Add more co-op URLs in the `COOPS` list as needed.
3. Deploy on Streamlit Cloud (https://share.streamlit.io → New app → main file: `app.py`).
4. (Optional) Add a manual CSV/Google Sheet URL in Cloud **Secrets** as `MANUAL_FEED_URL`.

## Notes
- The app fetches HTML tables using multiple parsers and normalizes columns.
- It then **routes** rows that mention `ADM Cedar Rapids`, `Cargill Cedar Rapids`, or `Shell Rock Soy Processing`.
- Use the sidebar to set **futures overrides** so **basis** can be computed when sources omit it.
- Export to CSV/Excel is included.
