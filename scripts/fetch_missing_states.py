"""
Fetches WARN Act data for states not covered by warn-scraper,
by scraping each state's official labor department website directly.

Coverage & status:
  MA  - mass.gov  CSV download (confirmed working URL pattern)
  NC  - nccommerce.com WARN reports page (HTML scrape)
  WV  - workforcewv.org listing page (HTML scrape)
  MS  - mdes.ms.gov WARN information page (HTML scrape)
  NV  - detr.nv.gov  PDF (requires pdfplumber; skipped if unavailable)

Not available (no public data source):
  AR  - confidential by state law (A.C.A. § 11-10-314)
  WY  - confidential by state law (Wyo. Stat. § 9-2-2607)
  MN  - individual PDFs per notice only; no bulk download
  NH  - no structured public list published
  ND  - no structured public table found

Output: data/processed/missing_states.csv
"""
import io
import os
import re
import sys
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
TIMEOUT = 30
OUT_PATH = "data/processed/missing_states.csv"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get(url, **kwargs):
    kwargs.setdefault("timeout", TIMEOUT)
    kwargs.setdefault("headers", {"User-Agent": UA})
    resp = requests.get(url, **kwargs)
    resp.raise_for_status()
    return resp


def _clean(text):
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _parse_html_table(soup, table_index=0):
    """Return (headers, rows) from the Nth table on a BeautifulSoup page."""
    tables = soup.find_all("table")
    if not tables or table_index >= len(tables):
        return [], []
    table = tables[table_index]
    rows = []
    headers = []
    for i, tr in enumerate(table.find_all("tr")):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        values = [_clean(c.get_text()) for c in cells]
        if i == 0 or all(c.name == "th" for c in cells):
            headers = values
        else:
            rows.append(values)
    return headers, rows


def _rows_to_df(headers, rows, state_code):
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=headers if headers else None)
    df.columns = [_clean(c).lower().replace(" ", "_").replace("-", "_")
                  for c in df.columns]
    if "state" not in df.columns:
        df["state"] = state_code
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Massachusetts  —  mass.gov fiscal-year CSV downloads
# ──────────────────────────────────────────────────────────────────────────────

def fetch_ma():
    """
    MA publishes WARN data as CSV (or Excel) files by fiscal year.
    FY runs July 1 – June 30.  FY number = the year the FY ends.
      Today March 2026  → FY2026 (current) + FY2025 (previous)
    Direct URL pattern: https://www.mass.gov/doc/warn-report-fiscal-year-{FY}/download
    """
    today = date.today()
    current_fy = today.year if today.month <= 6 else today.year + 1
    fiscal_years = [current_fy, current_fy - 1]

    frames = []
    for fy in fiscal_years:
        url = f"https://www.mass.gov/doc/warn-report-fiscal-year-{fy}/download"
        print(f"  MA FY{fy}: {url}", flush=True)
        try:
            resp = _get(url, allow_redirects=True)
        except requests.RequestException as e:
            print(f"    MA FY{fy}: skipped ({e})", flush=True)
            continue

        ct = resp.headers.get("content-type", "")
        try:
            if "spreadsheetml" in ct or "excel" in ct or url.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(resp.content))
            else:
                # Try CSV; fall back to Excel bytes
                try:
                    df = pd.read_csv(io.StringIO(resp.text))
                except Exception:
                    df = pd.read_excel(io.BytesIO(resp.content))
        except Exception as e:
            print(f"    MA FY{fy}: parse error ({e})", flush=True)
            continue

        if df.empty:
            continue
        df["state"] = "MA"
        frames.append(df)
        print(f"    MA FY{fy}: {len(df)} rows", flush=True)

    if not frames:
        print("  MA: no data retrieved", flush=True)
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ──────────────────────────────────────────────────────────────────────────────
# North Carolina  —  nccommerce.com WARN reports page
# ──────────────────────────────────────────────────────────────────────────────

def fetch_nc():
    url = "https://www.nccommerce.com/workforce/businesses/warn-information/warn-reports"
    print(f"  NC: {url}", flush=True)
    try:
        resp = _get(url)
    except requests.RequestException as e:
        print(f"  NC: skipped ({e})", flush=True)
        return pd.DataFrame()

    soup = BeautifulSoup(resp.content, "html.parser")

    # Look for a downloadable Excel/CSV link first
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"\.(xlsx?|csv)($|\?)", href, re.I):
            full_url = href if href.startswith("http") else "https://www.nccommerce.com" + href
            print(f"  NC: downloading {full_url}", flush=True)
            try:
                file_resp = _get(full_url)
                if ".csv" in full_url.lower():
                    df = pd.read_csv(io.StringIO(file_resp.text))
                else:
                    df = pd.read_excel(io.BytesIO(file_resp.content))
                if not df.empty:
                    df["state"] = "NC"
                    print(f"  NC: {len(df)} rows from download", flush=True)
                    return df
            except Exception as e:
                print(f"  NC: download failed ({e})", flush=True)

    # Fall back: parse largest HTML table
    headers, rows = _parse_html_table(soup)
    if rows:
        df = _rows_to_df(headers, rows, "NC")
        print(f"  NC: {len(df)} rows from HTML table", flush=True)
        return df

    print("  NC: no data found", flush=True)
    return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# West Virginia  —  workforcewv.org WARN listing
# ──────────────────────────────────────────────────────────────────────────────

def fetch_wv():
    url = "https://workforcewv.org/job-seeker/layoffs-downsizing/warn-listing/"
    print(f"  WV: {url}", flush=True)
    try:
        resp = _get(url)
    except requests.RequestException as e:
        print(f"  WV: skipped ({e})", flush=True)
        return pd.DataFrame()

    soup = BeautifulSoup(resp.content, "html.parser")

    # Try HTML table first
    headers, rows = _parse_html_table(soup)
    if rows:
        df = _rows_to_df(headers, rows, "WV")
        print(f"  WV: {len(df)} rows from HTML table", flush=True)
        return df

    # Fall back: look for listed items in divs/lists that contain dates + company names
    records = []
    for item in soup.select("li, .warn-item, .entry-content li"):
        text = _clean(item.get_text())
        if not text or len(text) < 5:
            continue
        records.append({"company": text, "state": "WV", "date": None,
                        "workers": None, "city": None, "type": None})

    if records:
        df = pd.DataFrame(records)
        print(f"  WV: {len(df)} items parsed from list", flush=True)
        return df

    print("  WV: no data found", flush=True)
    return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Mississippi  —  mdes.ms.gov WARN information
# ──────────────────────────────────────────────────────────────────────────────

def fetch_ms():
    url = "https://mdes.ms.gov/information-center/warn-information/"
    print(f"  MS: {url}", flush=True)
    try:
        resp = _get(url)
    except requests.RequestException as e:
        print(f"  MS: skipped ({e})", flush=True)
        return pd.DataFrame()

    soup = BeautifulSoup(resp.content, "html.parser")

    # Try Excel/CSV link
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"\.(xlsx?|csv)($|\?)", href, re.I):
            full_url = href if href.startswith("http") else "https://mdes.ms.gov" + href
            print(f"  MS: downloading {full_url}", flush=True)
            try:
                file_resp = _get(full_url)
                df = pd.read_excel(io.BytesIO(file_resp.content))
                if not df.empty:
                    df["state"] = "MS"
                    print(f"  MS: {len(df)} rows", flush=True)
                    return df
            except Exception as e:
                print(f"  MS: download failed ({e})", flush=True)

    headers, rows = _parse_html_table(soup)
    if rows:
        df = _rows_to_df(headers, rows, "MS")
        print(f"  MS: {len(df)} rows from HTML table", flush=True)
        return df

    print("  MS: no data found", flush=True)
    return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Nevada  —  detr.nv.gov (PDF; requires pdfplumber)
# ──────────────────────────────────────────────────────────────────────────────

def fetch_nv():
    index_url = "https://detr.nv.gov/Page/WARN"
    print(f"  NV: {index_url}", flush=True)
    try:
        resp = _get(index_url)
    except requests.RequestException as e:
        print(f"  NV: skipped ({e})", flush=True)
        return pd.DataFrame()

    soup = BeautifulSoup(resp.content, "html.parser")

    # Find the most recent PDF link
    pdf_links = [
        a["href"] for a in soup.find_all("a", href=True)
        if re.search(r"WARN.*\.pdf", a["href"], re.I)
    ]
    if not pdf_links:
        # Fallback: any PDF on the page
        pdf_links = [a["href"] for a in soup.find_all("a", href=True)
                     if a["href"].lower().endswith(".pdf")]

    if not pdf_links:
        print("  NV: no PDF links found on page", flush=True)
        return pd.DataFrame()

    # Take the most recently named PDF (sort descending by filename)
    pdf_links.sort(reverse=True)
    pdf_url = pdf_links[0]
    if not pdf_url.startswith("http"):
        pdf_url = "https://detr.nv.gov" + pdf_url

    print(f"  NV: downloading {pdf_url}", flush=True)
    try:
        pdf_resp = _get(pdf_url)
    except requests.RequestException as e:
        print(f"  NV: PDF download failed ({e})", flush=True)
        return pd.DataFrame()

    try:
        import pdfplumber
    except ImportError:
        print("  NV: pdfplumber not installed — skipping PDF parse", flush=True)
        return pd.DataFrame()

    try:
        records = []
        with pdfplumber.open(io.BytesIO(pdf_resp.content)) as pdf:
            for page in pdf.pages:
                for tbl in (page.extract_tables() or []):
                    if not tbl:
                        continue
                    for row in tbl:
                        if row and any(cell for cell in row):
                            records.append([_clean(c) for c in row])

        if not records:
            print("  NV: no tables extracted from PDF", flush=True)
            return pd.DataFrame()

        # First row is likely headers
        df = pd.DataFrame(records[1:], columns=records[0])
        df.columns = [_clean(c).lower().replace(" ", "_").replace("-", "_")
                      for c in df.columns]
        df["state"] = "NV"
        print(f"  NV: {len(df)} rows from PDF", flush=True)
        return df
    except Exception as e:
        print(f"  NV: PDF parse error ({e})", flush=True)
        return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# States with no publicly accessible data
# ──────────────────────────────────────────────────────────────────────────────

NO_PUBLIC_DATA = {
    "AR": "confidential by state law (A.C.A. § 11-10-314)",
    "WY": "confidential by state law (Wyo. Stat. § 9-2-2607)",
    "MN": "only individual PDFs per notice — no bulk data source",
    "NH": "no structured public list published by NHES",
    "ND": "no public bulk data table found on jobsnd.com",
}


# ──────────────────────────────────────────────────────────────────────────────
# Column normalisation (shared with main workflow)
# ──────────────────────────────────────────────────────────────────────────────

RENAMES = {
    "company_name":          "company",
    "employer_name":         "company",
    "employer":              "company",
    "business_name":         "company",
    "employees_affected":    "workers",
    "num_employees":         "workers",
    "affected_workers":      "workers",
    "number_of_employees":   "workers",
    "employees":             "workers",
    "jobs":                  "workers",
    "layoffs":               "workers",
    "notice_date":           "date",
    "received_date":         "date",
    "layoff_date":           "date",
    "event_date":            "date",
    "effective_date":        "date",
    "warn_date":             "date",
    "layoff_type":           "type",
    "event_type":            "type",
    "notice_type":           "type",
    "action_type":           "type",
    "warn_type":             "type",
    "type_of_action":        "type",
    "city_name":             "city",
    "municipality":          "city",
    "facility_city":         "city",
    "location":              "city",
    "county":                "city",
}


def normalise(df):
    df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    df = df.rename(columns=RENAMES)
    for col in ("state", "company", "workers", "date", "city", "type"):
        if col not in df.columns:
            df[col] = None
    df["workers"] = pd.to_numeric(df["workers"], errors="coerce").fillna(0).astype(int)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    for state, reason in NO_PUBLIC_DATA.items():
        print(f"  {state}: skipping — {reason}")

    fetchers = [
        ("MA", fetch_ma),
        ("NC", fetch_nc),
        ("WV", fetch_wv),
        ("MS", fetch_ms),
        ("NV", fetch_nv),
    ]

    frames = []
    for state, fn in fetchers:
        print(f"\nFetching {state}…", flush=True)
        try:
            df = fn()
        except Exception as e:
            print(f"  {state}: unexpected error — {e}", flush=True)
            df = pd.DataFrame()

        if df.empty:
            print(f"  {state}: 0 records", flush=True)
            continue

        df = normalise(df)
        df = df.dropna(subset=["date"])
        frames.append(df)
        print(f"  {state}: {len(df)} records after normalisation", flush=True)

    if not frames:
        print("\nNo records from any direct scraper — exiting cleanly.")
        sys.exit(0)

    result = pd.concat(frames, ignore_index=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    result.to_csv(OUT_PATH, index=False)
    print(f"\nSaved {len(result)} total records → {OUT_PATH}")
    covered = sorted(result["state"].dropna().unique().tolist())
    print(f"States with data: {covered}")


if __name__ == "__main__":
    main()
