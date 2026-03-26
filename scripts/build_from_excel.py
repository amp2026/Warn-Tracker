"""
build_from_excel.py
-------------------
Reads the two authoritative Excel spreadsheets:
  data/raw/warn_2026.xlsx      – 2026 WARN notices
  data/raw/warn_master.xlsx    – All WARN notices prior to 2026

Normalises them into the schema used by data/processed/consolidated.csv
and writes that file.  This is the single source of truth for the dashboard.
"""

import os
import re
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR   = os.path.join(os.path.dirname(__file__), "..")
RAW_2026   = os.path.join(BASE_DIR, "data", "raw", "warn_2026.xlsx")
RAW_MASTER = os.path.join(BASE_DIR, "data", "raw", "warn_master.xlsx")
OUTPUT     = os.path.join(BASE_DIR, "data", "processed", "consolidated.csv")

# ── State name → 2-letter code ───────────────────────────────────────────────

STATE_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}

# ── Type normalisation ────────────────────────────────────────────────────────

_CLOSURE_PATTERNS = re.compile(
    r"clos|ceas|shut|terminat|plant.?clos|facility.?clos|clos.*oper",
    re.IGNORECASE,
)


def normalise_type(raw_type: str) -> str:
    """Map free-text WARN type to 'Closure' or 'Layoff'."""
    if not isinstance(raw_type, str) or not raw_type.strip():
        return "Layoff"
    t = raw_type.strip()
    if _CLOSURE_PATTERNS.search(t):
        return "Closure"
    return "Layoff"


# ── Load helper ───────────────────────────────────────────────────────────────

def load_excel(path: str, label: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")

    # Drop fully-empty rows / columns
    df = df.dropna(how="all").reset_index(drop=True)
    df = df.loc[:, df.columns.notna()]

    # Standardise column names (strip whitespace, collapse spaces/slashes)
    df.columns = [str(c).strip() for c in df.columns]

    # Rename to internal schema
    renames = {
        "State":              "state",
        "Company":            "company",
        "City":               "city",
        "County":             "county",
        "Number of Workers":  "workers",
        "WARN Received Date": "date",
        "Effective Date":     "effective_date",
        "Closure / Layoff":   "raw_type",   # 2026 file
        "Closure/Layoff":     "raw_type",   # master file
        "Temporary/Permanent":"temp_perm",
        "Union":              "union",
        "Region":             "region",
        "Industry":           "industry",
        "Notes":              "notes",
    }
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})

    # Map full state names → 2-letter codes
    df["state"] = df["state"].map(STATE_MAP).fillna(df.get("state", ""))

    # Normalise type
    df["type"] = df["raw_type"].apply(normalise_type) if "raw_type" in df.columns else "Layoff"

    # Parse dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "effective_date" in df.columns:
        df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce")

    # Numeric workers
    df["workers"] = pd.to_numeric(df.get("workers", 0), errors="coerce").fillna(0).astype(int)

    df["source_quality"] = label

    print(f"  {label}: {len(df)} rows, {df['state'].nunique()} states")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

    print("Loading Excel files …")
    df_2026   = load_excel(RAW_2026,   "excel_2026")
    df_master = load_excel(RAW_MASTER, "excel_master")

    df = pd.concat([df_2026, df_master], ignore_index=True)

    # Drop rows with no date or no company
    df = df.dropna(subset=["date"])
    df = df[df["company"].notna() & (df["company"].str.strip() != "")]

    # Dedup: prefer 2026 file when same record appears in both
    before = len(df)
    df["_rank"] = df["source_quality"].map({"excel_2026": 0, "excel_master": 1}).fillna(9)
    df = df.sort_values("_rank")
    df = df.drop_duplicates(subset=["company", "state", "date", "workers"], keep="first")
    df = df.drop(columns=["_rank"])
    print(f"Deduped: {before} → {len(df)} records")

    # Keep only the columns the dashboard needs (plus useful extras)
    keep = [c for c in (
        "date", "effective_date", "company", "city", "county", "state",
        "workers", "type", "industry", "union", "temp_perm", "notes",
        "raw_type", "source_quality",
    ) if c in df.columns]
    df = df[keep]

    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    df.to_csv(OUTPUT, index=False)
    print(f"Wrote {len(df)} records to {OUTPUT}")

    # Summary
    print("\nBy year:")
    print(df.groupby(df["date"].dt.year)["company"].count().to_string())
    print("\nBy type:")
    print(df["type"].value_counts().to_string())


if __name__ == "__main__":
    main()
