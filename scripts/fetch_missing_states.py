"""
Fetches WARN Act data for the 10 US states not covered by warn-scraper,
using the WARN Firehose API with per-state requests.

States covered here: AR, MA, MN, MS, NC, NV, NH, ND, WV, WY
Requires: WARN_FIREHOSE_API_KEY environment variable
Output:   data/processed/missing_states.csv  (merged into consolidated.csv by workflow)
"""
import os
import sys
import requests
import pandas as pd

# States that warn-scraper does not support
MISSING_STATES = ["AR", "MA", "MN", "MS", "NC", "NV", "NH", "ND", "WV", "WY"]

API_KEY  = os.environ.get("WARN_FIREHOSE_API_KEY", "")
BASE_URL = "https://warnfirehose.com/api/records"

if not API_KEY:
    print("WARN_FIREHOSE_API_KEY not set — skipping missing-states fetch.", file=sys.stderr)
    sys.exit(0)   # soft exit: warn-scraper data is still usable

headers = {"X-API-Key": API_KEY}

COLUMN_MAP = {
    "company_name":       "company",
    "employer_name":      "company",
    "employees_affected": "workers",
    "num_employees":      "workers",
    "affected_workers":   "workers",
    "notice_date":        "date",
    "received_date":      "date",
    "layoff_date":        "date",
    "event_date":         "date",
    "layoff_type":        "type",
    "event_type":         "type",
    "notice_type":        "type",
    "action_type":        "type",
    "type_of_layoff":     "type",
    "city_name":          "city",
    "municipality":       "city",
    "facility_city":      "city",
    "location":           "city",
}

all_records = []

for state in MISSING_STATES:
    print(f"  Fetching {state}...", flush=True)
    page = 1
    state_count = 0

    while True:
        try:
            resp = requests.get(
                BASE_URL,
                headers=headers,
                params={"state": state, "limit": 1000, "page": page},
                timeout=30,
            )
        except requests.RequestException as e:
            print(f"    Network error for {state}: {e}", file=sys.stderr)
            break

        if resp.status_code == 401:
            print("Authentication failed — check WARN_FIREHOSE_API_KEY.", file=sys.stderr)
            sys.exit(0)

        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code} for {state} — skipping.", file=sys.stderr)
            break

        payload = resp.json()
        records = (
            payload
            if isinstance(payload, list)
            else (payload.get("data") or payload.get("records")
                  or payload.get("results") or [])
        )

        if not records:
            break

        # Tag state in case the API doesn't return it
        for r in records:
            if not r.get("state"):
                r["state"] = state

        all_records.extend(records)
        state_count += len(records)

        if len(records) < 1000:
            break
        page += 1

    print(f"    {state}: {state_count} records")

if not all_records:
    print("No records returned for missing states — exiting cleanly.")
    sys.exit(0)

df = pd.DataFrame(all_records)
df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]
df = df.rename(columns=COLUMN_MAP)

for col in ("state", "company", "workers", "date", "city", "type"):
    if col not in df.columns:
        df[col] = None

df["workers"] = pd.to_numeric(df["workers"], errors="coerce").fillna(0).astype(int)
df["date"]    = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"])

os.makedirs("data/processed", exist_ok=True)
out = "data/processed/missing_states.csv"
df.to_csv(out, index=False)
print(f"Saved {len(df)} records for {len(MISSING_STATES)} missing states → {out}")
