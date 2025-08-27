# Grain Marketing Dashboard (Clean Starter)

This is a clean, ready-to-deploy Streamlit app for auto-fetching Iowa co-op cash bids,
showing basis, and exporting CSV/Excel. It uses resilient scraping and graceful fallbacks.

## Quick Start
1. **Set the co-op URLs** inside `app.py` (search for `COOPS`).
2. (Optional) Add a manual CSV/Google Sheet URL in Streamlit Cloud **Secrets** as `MANUAL_FEED_URL`.
3. Push this folder to a **GitHub repo**.
4. Go to https://share.streamlit.io/ → New app → select your repo/branch → main file: `app.py`.
5. Deploy. Your app link will look like `https://<repo>.streamlit.app`.

## Manual Feed (Optional)
- If co-op sites are JS-rendered or down, use a CSV/Sheet as a fallback.
- In Streamlit Cloud, add a secret called `MANUAL_FEED_URL` (a direct CSV URL).
- Template for secrets is in `.streamlit/secrets.toml.example`.

## Notes
- If some co-op pages list multiple delivery points (ADM CR, Cargill CR), the app can filter rows for those processors via `route_rows_to_processors` in `app.py` (enabled by default).
- Use the sidebar to override current futures to compute basis when missing.
