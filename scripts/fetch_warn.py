"""
Fetch WARN Act data from WARN Firehose API and save as consolidated CSV.
Requires WARN_FIREHOSE_API_KEY environment variable.
"""
import os
import sys
import requests
import pandas as pd

API_KEY = os.environ.get("WARN_FIREHOSE_API_KEY", "")
BASE_URL = "https://warnfirehose.com/api/records"

if not API_KEY:
    print("Warning: WARN_FIREHOSE_API_KEY not set — proceeding without auth (rate limits may apply)")

headers = {}
if API_KEY:
    headers["Authorization"] = f"Bearer {API_KEY}"

COLUMN_MAP = {
    "company_name": "company",
    "employees_affected": "workers",
    "notice_date": "date",
    "num_employees": "workers",
    "layoff_date": "date",
    "received_date": "date",
}

all_records = []
page = 1

print("Fetching WARN records from Firehose...")
while True:
    params = {"page": page, "limit": 1000}
    try:
        resp = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"HTTP error on page {page}: {e}", file=sys.stderr)
        sys.exit(1)

    payload = resp.json()

    # Handle both list response and paginated object response
    if isinstance(payload, list):
        records = payload
    else:
        records = payload.get("data") or payload.get("records") or payload.get("results") or []

    if not records:
        break

    all_records.extend(records)
    print(f"  page {page}: {len(records)} records (total so far: {len(all_records)})")

    # Stop if fewer than limit returned (last page)
    if len(records) < 1000:
        break

    page += 1

if not all_records:
    print("No records fetched — check API key and endpoint.", file=sys.stderr)
    sys.exit(1)

df = pd.DataFrame(all_records)

# Normalize column names to lowercase / underscored
df.columns = [c.lower().replace(" ", "_") for c in df.columns]

# Rename to standard app column names
df = df.rename(columns=COLUMN_MAP)

# Ensure required columns exist
for col in ["state", "company", "workers", "date"]:
    if col not in df.columns:
        df[col] = None

df["date"] = pd.to_datetime(df["date"], errors="coerce")

os.makedirs("data/processed", exist_ok=True)
out_path = "data/processed/consolidated.csv"
df.to_csv(out_path, index=False)
print(f"Saved {len(df)} records to {out_path}")
