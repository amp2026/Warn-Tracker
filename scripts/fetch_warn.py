"""
Fetch WARN Act data from WARN Firehose API and save as consolidated CSV.
Requires WARN_FIREHOSE_API_KEY environment variable.

API docs: https://warnfirehose.com/developers
Auth:     X-API-Key header
Endpoint: GET https://warnfirehose.com/api/records
"""
import os
import sys
import requests
import pandas as pd

API_KEY = os.environ.get("WARN_FIREHOSE_API_KEY", "")
BASE_URL = "https://warnfirehose.com/api/records"

if not API_KEY:
    print("ERROR: WARN_FIREHOSE_API_KEY environment variable not set.", file=sys.stderr)
    sys.exit(1)

headers = {"X-API-Key": API_KEY}

# Map WARN Firehose field names → app column names
COLUMN_MAP = {
    # company
    "company_name":    "company",
    "employer_name":   "company",
    # workers
    "employees_affected": "workers",
    "num_employees":   "workers",
    "affected_workers": "workers",
    # date
    "notice_date":     "date",
    "received_date":   "date",
    "layoff_date":     "date",
    "event_date":      "date",
    # type (layoff vs closure)
    "layoff_type":     "type",
    "event_type":      "type",
    "notice_type":     "type",
    "action_type":     "type",
    "type_of_layoff":  "type",
    # city
    "city_name":       "city",
    "municipality":    "city",
    "facility_city":   "city",
    "location":        "city",
}

all_records = []
page = 1

print(f"Fetching WARN records from {BASE_URL} ...")
while True:
    resp = requests.get(
        BASE_URL,
        headers=headers,
        params={"page": page, "limit": 1000},
        timeout=60,
    )

    if resp.status_code == 401:
        print("Authentication failed — check WARN_FIREHOSE_API_KEY.", file=sys.stderr)
        sys.exit(1)
    resp.raise_for_status()

    payload = resp.json()

    # Support both plain list and paginated envelope responses
    if isinstance(payload, list):
        records = payload
    else:
        records = (
            payload.get("data")
            or payload.get("records")
            or payload.get("results")
            or []
        )

    if not records:
        break

    all_records.extend(records)
    print(f"  page {page}: {len(records)} records (running total: {len(all_records)})")

    if len(records) < 1000:
        break  # last page

    page += 1

if not all_records:
    print("No records returned — the dataset may be empty or the plan has no access.", file=sys.stderr)
    sys.exit(1)

df = pd.DataFrame(all_records)

# Normalize column names to lowercase snake_case
df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]

# Rename to standardized app column names
df = df.rename(columns=COLUMN_MAP)

# Ensure required columns exist (fill with None if missing)
for col in ("state", "company", "workers", "date", "city", "type"):
    if col not in df.columns:
        df[col] = None

df["date"] = pd.to_datetime(df["date"], errors="coerce")

os.makedirs("data/processed", exist_ok=True)
out_path = "data/processed/consolidated.csv"
df.to_csv(out_path, index=False)
print(f"Saved {len(df)} records → {out_path}")
