"""
Fetches WARN Act data for the 10 states not covered by warn-scraper
from layoffdata.com (WARN Database).

layoffdata.com aggregates official state WARN notices into standard HTML
tables, updated regularly.  It covers all states that publish public data;
AR and WY have no records (confidential by state law) and are skipped.

States targeted:
  MA  NH  MN  MS  NC  NV  ND  WV   → data confirmed available
  AR  WY                            → no public data; skipped gracefully

This script is a supplement to fetch_missing_states.py (direct state-site
scrapers).  Running both provides redundancy; the merge step deduplicates,
preferring records from direct sources over layoffdata.com aggregations.

Output: data/processed/layoffdata_states.csv
"""
import time
import os
import re
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL  = "https://layoffdata.com"
OUT_PATH  = "data/processed/layoffdata_states.csv"
DELAY_SEC = 1.5      # polite delay between requests
TIMEOUT   = 30
MAX_PAGES = 200      # safety cap per state

UA = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
    "Gecko/20100101 Firefox/120.0"
)

# ── State → URL slug ──────────────────────────────────────────────────────────
# AR and WY have no public WARN data (state law); they're listed here so we
# can detect and skip them cleanly rather than treating a failure as an error.
NO_PUBLIC_DATA = {"AR", "WY"}

STATES = {
    "AR": "arkansas",        # confidential — A.C.A. § 11-10-314
    "MA": "massachusetts",
    "MN": "minnesota",
    "MS": "mississippi",
    "NC": "north-carolina",
    "NV": "nevada",
    "NH": "new-hampshire",
    "ND": "north-dakota",
    "WV": "west-virginia",
    "WY": "wyoming",         # confidential — Wyo. Stat. § 9-2-2607
}

# Phrases that appear on "no data" pages so we can detect them cheaply
_NO_DATA_PHRASES = [
    "not publicly available",
    "not available",
    "data is not available",
    "confidential",
    "no records",
    "no data",
]


def _get(url, **kwargs):
    kwargs.setdefault("timeout", TIMEOUT)
    kwargs.setdefault("headers", {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    resp = requests.get(url, **kwargs)
    resp.raise_for_status()
    return resp


def _is_no_data_page(soup):
    """Return True if the page explicitly says no data is available."""
    text = soup.get_text(" ", strip=True).lower()
    return any(phrase in text for phrase in _NO_DATA_PHRASES)


def _parse_table(soup):
    """
    Return (headers, rows) from the first meaningful <table> on the page.
    Falls back to an empty result if no table is found.
    """
    table = soup.find("table")
    if not table:
        return [], []

    headers = []
    rows = []

    for i, tr in enumerate(table.find_all("tr")):
        ths = tr.find_all("th")
        tds = tr.find_all("td")

        if ths and not tds:
            # Header row
            headers = [th.get_text(strip=True) for th in ths]
        elif tds:
            row = [td.get_text(" ", strip=True) for td in tds]
            # Skip blank or separator rows
            if any(cell for cell in row):
                rows.append(row)

    return headers, rows


def _next_page_url(soup, current_url):
    """
    Look for a "Next" or "›" pagination link and return its absolute URL,
    or None if we're on the last page.
    Handles both:
      • <a href="?page=N">Next</a>   (query-param pagination)
      • <a href="/minnesota/?page=N">Next</a>  (full-path pagination)
    """
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        if text in ("next", "next »", "›", "»", "next page", ">"):
            href = a["href"]
            if href.startswith("http"):
                return href
            if href.startswith("/"):
                return BASE_URL + href
            # relative
            base = re.sub(r"\?.*$", "", current_url)
            return base + ("" if base.endswith("/") else "/") + href

    # No explicit "next" link — try incrementing ?page= ourselves
    # (caller handles detecting when we've gone past the last page)
    return None


def scrape_state(state_code, slug):
    """
    Scrape all pages for one state.  Returns a DataFrame (possibly empty).
    """
    url = f"{BASE_URL}/{slug}/"
    print(f"  {state_code}: {url}", flush=True)

    all_rows = []
    headers  = []
    page_num = 1

    while url and page_num <= MAX_PAGES:
        try:
            resp = _get(url)
        except requests.HTTPError as e:
            print(f"    {state_code} p{page_num}: HTTP {e.response.status_code} — stopping",
                  flush=True)
            break
        except requests.RequestException as e:
            print(f"    {state_code} p{page_num}: request error ({e}) — stopping",
                  flush=True)
            break

        soup = BeautifulSoup(resp.content, "html.parser")

        # First page: check for "no data" notice
        if page_num == 1 and _is_no_data_page(soup):
            print(f"    {state_code}: page says data not publicly available — skipping",
                  flush=True)
            return pd.DataFrame()

        pg_headers, pg_rows = _parse_table(soup)

        if page_num == 1 and pg_headers:
            headers = pg_headers

        if not pg_rows:
            print(f"    {state_code} p{page_num}: no rows — stopping", flush=True)
            break

        all_rows.extend(pg_rows)
        print(f"    {state_code} p{page_num}: {len(pg_rows)} rows "
              f"(running total {len(all_rows)})", flush=True)

        # Pagination
        next_url = _next_page_url(soup, url)
        if next_url and next_url != url:
            url = next_url
            page_num += 1
            time.sleep(DELAY_SEC)
        else:
            break

    if not all_rows:
        return pd.DataFrame()

    # Build DataFrame — use discovered headers or positional fallback
    if headers and len(headers) == len(all_rows[0]):
        df = pd.DataFrame(all_rows, columns=headers)
    else:
        df = pd.DataFrame(all_rows)

    df["state"] = state_code
    return df


# ── Column normalisation ──────────────────────────────────────────────────────

RENAMES = {
    "company_name":        "company",
    "employer":            "company",
    "employer_name":       "company",
    "business":            "company",
    "employees":           "workers",
    "employees_affected":  "workers",
    "num_employees":       "workers",
    "number_affected":     "workers",
    "affected_workers":    "workers",
    "jobs":                "workers",
    "layoffs":             "workers",
    "notice_date":         "date",
    "effective_date":      "date",
    "layoff_date":         "date",
    "warn_date":           "date",
    "event_date":          "date",
    "received_date":       "date",
    "type_of_action":      "type",
    "layoff_type":         "type",
    "event_type":          "type",
    "action_type":         "type",
    "notice_type":         "type",
    "warn_type":           "type",
    "city_name":           "city",
    "location":            "city",
    "municipality":        "city",
    "facility_city":       "city",
    "county":              "city",
}


def normalise(df):
    df.columns = [
        re.sub(r"[\s\-]+", "_", c.strip().lower()) for c in df.columns
    ]
    df = df.rename(columns=RENAMES)
    for col in ("state", "company", "workers", "date", "city", "type",
                "source_quality"):
        if col not in df.columns:
            df[col] = None
    df["source_quality"] = "html_scraped"
    df["workers"] = pd.to_numeric(df["workers"], errors="coerce").fillna(0).astype(int)
    df["date"]    = pd.to_datetime(df["date"], errors="coerce")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    frames = []

    for state, slug in STATES.items():
        if state in NO_PUBLIC_DATA:
            print(f"\n{state}: skipping — no public WARN data by state law", flush=True)
            continue

        print(f"\nFetching {state} from layoffdata.com…", flush=True)
        try:
            df = scrape_state(state, slug)
        except Exception as e:
            print(f"  {state}: unexpected error — {e}", flush=True)
            df = pd.DataFrame()

        if df.empty:
            print(f"  {state}: 0 records", flush=True)
            continue

        df = normalise(df)
        before = len(df)
        df = df.dropna(subset=["date"])
        print(f"  {state}: {len(df)} records with valid dates "
              f"(dropped {before - len(df)} undated)", flush=True)

        if not df.empty:
            frames.append(df)

        # Polite delay between states
        time.sleep(DELAY_SEC)

    if not frames:
        print("\nlayoffdata.com: no records retrieved — exiting cleanly.")
        sys.exit(0)

    result = pd.concat(frames, ignore_index=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    result.to_csv(OUT_PATH, index=False)
    print(f"\nSaved {len(result)} records → {OUT_PATH}")
    print(f"States: {sorted(result['state'].dropna().unique().tolist())}")


if __name__ == "__main__":
    main()
