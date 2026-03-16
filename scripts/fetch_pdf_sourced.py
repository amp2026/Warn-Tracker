"""
Fetches WARN Act data for states whose official publications are PDFs.

All records are tagged with source_quality="pdf_sourced" to signal:
  - Dates may be imprecise (varying formats, OCR artifacts)
  - Worker counts may be missing or mis-parsed
  - Freshness is lower than CSV/API sources (PDFs are published weekly
    to quarterly, not daily)
  - Deduplication is less reliable (no canonical IDs)

States:
  MN  — mn.gov/deed individual-notice PDFs
  ND  — jobsnd.com  (PDF or HTML; best-effort)

Output: data/processed/pdf_sourced_states.csv
"""
import io
import re
import os
import sys
from datetime import date, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    _HAS_PDFPLUMBER = True
except ImportError:
    _HAS_PDFPLUMBER = False
    print("WARNING: pdfplumber not installed — PDF extraction disabled", flush=True)

SOURCE_QUALITY = "pdf_sourced"
OUT_PATH = "data/processed/pdf_sourced_states.csv"
TIMEOUT = 30
UA = "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"

# Only fetch PDFs from the last N days to keep runtime bounded.
# PDF states update infrequently; going back 18 months catches a full cycle.
MAX_AGE_DAYS = 548   # ~18 months


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


def _is_recent(url_or_text, cutoff_year=None):
    """Return True if the URL or text contains a recent year (>= cutoff)."""
    if cutoff_year is None:
        cutoff_year = 2024   # include 2024 and later
    years = re.findall(r"\b(20\d{2})\b", url_or_text)
    return any(int(y) >= cutoff_year for y in years)


# ──────────────────────────────────────────────────────────────────────────────
# PDF parsing helpers
# ──────────────────────────────────────────────────────────────────────────────

def _tables_from_pdf_bytes(pdf_bytes):
    """
    Return a list of (headers, rows) from tables found in a PDF.
    Falls back to empty list if pdfplumber is unavailable or extraction fails.
    """
    if not _HAS_PDFPLUMBER:
        return []
    results = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for tbl in (page.extract_tables() or []):
                    if not tbl or len(tbl) < 2:
                        continue
                    # First row = headers
                    raw_headers = [_clean(c) for c in tbl[0]]
                    rows = [[_clean(c) for c in row] for row in tbl[1:]]
                    results.append((raw_headers, rows))
    except Exception as e:
        print(f"    PDF table extraction error: {e}", flush=True)
    return results


def _text_from_pdf_bytes(pdf_bytes):
    """Return concatenated text from all pages of a PDF."""
    if not _HAS_PDFPLUMBER:
        return ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        print(f"    PDF text extraction error: {e}", flush=True)
        return ""


# Patterns for key-value style PDFs (e.g. individual MN notice PDFs)
# Each WARN notice PDF typically has lines like:
#   Company: Acme Corp          OR   Employer: Acme Corp
#   Effective Date: 03/15/2025
#   Number of Employees: 120
#   Location: Minneapolis, MN
#   Type of Action: Layoff / Closure
_KV_PATTERNS = {
    "company": re.compile(
        r"(?:company|employer|business|firm)\s*[:\-]\s*(.+)", re.I),
    "date": re.compile(
        r"(?:effective\s+date|notice\s+date|layoff\s+date|warn\s+date|date)\s*[:\-]\s*(.+)",
        re.I),
    "workers": re.compile(
        r"(?:number\s+of\s+employees|employees\s+affected|workers|jobs)\s*[:\-]\s*(\d[\d,]*)",
        re.I),
    "city": re.compile(
        r"(?:location|city|facility|address)\s*[:\-]\s*(.+)", re.I),
    "type": re.compile(
        r"(?:type\s+of\s+action|action|layoff\s+type|event\s+type)\s*[:\-]\s*(.+)", re.I),
}


def _kv_extract(text, state_code):
    """
    Extract a single WARN record from key-value formatted PDF text.
    Returns a dict or None if the minimum fields (company + date) are absent.
    """
    record = {"state": state_code, "source_quality": SOURCE_QUALITY}
    for field, pattern in _KV_PATTERNS.items():
        m = pattern.search(text)
        if m:
            record[field] = _clean(m.group(1))

    # Need at minimum a company name to be useful
    if not record.get("company"):
        return None
    return record


def _normalise_cols(df):
    """Lowercase and snake_case column names; rename common aliases."""
    df.columns = [
        re.sub(r"[\s\-]+", "_", c.lower().strip()) for c in df.columns
    ]
    aliases = {
        "company_name": "company", "employer_name": "company",
        "employer": "company", "business_name": "company",
        "employees_affected": "workers", "num_employees": "workers",
        "number_of_employees": "workers", "affected_workers": "workers",
        "jobs": "workers", "layoffs": "workers",
        "notice_date": "date", "effective_date": "date",
        "layoff_date": "date", "event_date": "date", "warn_date": "date",
        "layoff_type": "type", "event_type": "type",
        "notice_type": "type", "action_type": "type", "type_of_action": "type",
        "city_name": "city", "municipality": "city",
        "facility_city": "city", "facility_location": "city",
    }
    df = df.rename(columns=aliases)
    for col in ("state", "company", "workers", "date", "city", "type",
                "source_quality"):
        if col not in df.columns:
            df[col] = None
    if "source_quality" in df.columns:
        df["source_quality"] = df["source_quality"].fillna(SOURCE_QUALITY)
    else:
        df["source_quality"] = SOURCE_QUALITY
    df["workers"] = pd.to_numeric(df["workers"], errors="coerce").fillna(0).astype(int)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Minnesota  —  mn.gov/deed individual notice PDFs
# ──────────────────────────────────────────────────────────────────────────────

_MN_INDEX_URLS = [
    "https://mn.gov/deed/warn/",
    "https://mn.gov/deed/programs-services/dislocated-worker-program/reports/",
]


def fetch_mn():
    """
    MN publishes one PDF per WARN notice on mn.gov/deed.
    We scrape the index page(s) for PDF links, download recent ones,
    and extract records from each PDF.

    Expectations:
      - Each PDF is a structured (not scanned) document
      - pdfplumber can extract text without OCR
      - Field extraction is done via key-value regex patterns
      - Records may be incomplete for older or non-standard notices
    """
    if not _HAS_PDFPLUMBER:
        print("  MN: pdfplumber unavailable — skipping", flush=True)
        return pd.DataFrame()

    # Collect PDF links from all index pages
    pdf_links = []
    for index_url in _MN_INDEX_URLS:
        print(f"  MN index: {index_url}", flush=True)
        try:
            resp = _get(index_url)
        except requests.RequestException as e:
            print(f"    MN: index fetch failed ({e})", flush=True)
            continue

        soup = BeautifulSoup(resp.content, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # MN WARN PDFs: /deed/assets/warn-YYYY-...
            if re.search(r"warn.+\.pdf", href, re.I):
                full = href if href.startswith("http") else f"https://mn.gov{href}"
                if full not in pdf_links:
                    pdf_links.append(full)

    if not pdf_links:
        print("  MN: no PDF links found", flush=True)
        return pd.DataFrame()

    # Filter to recent years only to keep runtime manageable
    cutoff_year = 2024
    recent = [u for u in pdf_links if _is_recent(u, cutoff_year)]
    if not recent:
        # Fall back to all found if year filter removes everything
        recent = pdf_links

    print(f"  MN: {len(recent)} recent PDFs to process "
          f"(of {len(pdf_links)} total)", flush=True)

    records = []
    for url in recent:
        print(f"    MN: {url}", flush=True)
        try:
            pdf_resp = _get(url)
        except requests.RequestException as e:
            print(f"      skipped ({e})", flush=True)
            continue

        pdf_bytes = pdf_resp.content

        # Try table extraction first (more structured)
        tables = _tables_from_pdf_bytes(pdf_bytes)
        parsed_via_table = False
        for headers, rows in tables:
            if not rows:
                continue
            df_tbl = pd.DataFrame(rows, columns=headers or None)
            df_tbl["state"] = "MN"
            df_tbl["source_quality"] = SOURCE_QUALITY
            df_tbl["_pdf_source"] = url
            records.append(df_tbl)
            parsed_via_table = True

        if not parsed_via_table:
            # Fall back to key-value text extraction
            text = _text_from_pdf_bytes(pdf_bytes)
            if text:
                rec = _kv_extract(text, "MN")
                if rec:
                    rec["_pdf_source"] = url
                    records.append(pd.DataFrame([rec]))

    if not records:
        print("  MN: no records extracted from PDFs", flush=True)
        return pd.DataFrame()

    df = pd.concat(records, ignore_index=True)
    df = _normalise_cols(df)
    print(f"  MN: {len(df)} records (pre-date-filter)", flush=True)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# North Dakota  —  jobsnd.com
# ──────────────────────────────────────────────────────────────────────────────

_ND_URLS = [
    "https://www.jobsnd.com/warn-act-notices",
    "https://www.jobsnd.com/unemployment-business-tax/employers-guide/"
    "employer-responsibilities-employee-separations",
]


def fetch_nd():
    """
    North Dakota WARN data is maintained by Job Service ND.
    We try known pages for downloadable files (Excel, CSV, PDF) or an
    HTML table.  If only a PDF is found it is parsed with pdfplumber.

    Expectations:
      - Format not confirmed — this is best-effort
      - Data may be sparse or unavailable on some runs
    """
    for url in _ND_URLS:
        print(f"  ND: trying {url}", flush=True)
        try:
            resp = _get(url)
        except requests.RequestException as e:
            print(f"    ND: skipped ({e})", flush=True)
            continue

        soup = BeautifulSoup(resp.content, "html.parser")

        # 1. Look for Excel / CSV download
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"\.(xlsx?|csv)($|\?)", href, re.I):
                full = href if href.startswith("http") else "https://www.jobsnd.com" + href
                print(f"    ND: downloading {full}", flush=True)
                try:
                    import io as _io
                    file_resp = _get(full)
                    if ".csv" in full.lower():
                        df = pd.read_csv(_io.StringIO(file_resp.text))
                    else:
                        df = pd.read_excel(_io.BytesIO(file_resp.content))
                    if not df.empty:
                        df["state"] = "ND"
                        df["source_quality"] = SOURCE_QUALITY
                        df = _normalise_cols(df)
                        print(f"    ND: {len(df)} rows from {full}", flush=True)
                        return df
                except Exception as e:
                    print(f"    ND: file parse failed ({e})", flush=True)

        # 2. Look for PDF
        if _HAS_PDFPLUMBER:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"warn.+\.pdf|\.pdf.*(warn)", href, re.I):
                    full = href if href.startswith("http") else "https://www.jobsnd.com" + href
                    if not _is_recent(full):
                        continue
                    print(f"    ND: PDF {full}", flush=True)
                    try:
                        pdf_resp = _get(full)
                    except requests.RequestException as e:
                        print(f"      skipped ({e})", flush=True)
                        continue

                    tables = _tables_from_pdf_bytes(pdf_resp.content)
                    rows_found = []
                    for headers, rows in tables:
                        for row in rows:
                            rows_found.append(dict(zip(headers, row)))
                    if rows_found:
                        df = pd.DataFrame(rows_found)
                        df["state"] = "ND"
                        df["source_quality"] = SOURCE_QUALITY
                        df = _normalise_cols(df)
                        print(f"    ND: {len(df)} rows from PDF", flush=True)
                        return df

                    # Try key-value text
                    text = _text_from_pdf_bytes(pdf_resp.content)
                    rec = _kv_extract(text, "ND")
                    if rec:
                        df = pd.DataFrame([rec])
                        df = _normalise_cols(df)
                        return df

        # 3. Fall back to HTML table
        tables = soup.find_all("table")
        for table in tables:
            rows = []
            headers = []
            for i, tr in enumerate(table.find_all("tr")):
                cells = tr.find_all(["th", "td"])
                vals = [_clean(c.get_text()) for c in cells]
                if not vals:
                    continue
                if i == 0 or all(c.name == "th" for c in cells):
                    headers = vals
                else:
                    rows.append(vals)
            if rows:
                df = pd.DataFrame(rows, columns=headers or None)
                df["state"] = "ND"
                df["source_quality"] = SOURCE_QUALITY
                df = _normalise_cols(df)
                print(f"    ND: {len(df)} rows from HTML table at {url}",
                      flush=True)
                return df

    print("  ND: no data found on any known URL", flush=True)
    return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    fetchers = [
        ("MN", fetch_mn),
        ("ND", fetch_nd),
    ]

    frames = []
    for state, fn in fetchers:
        print(f"\nFetching {state} (pdf_sourced)…", flush=True)
        try:
            df = fn()
        except Exception as e:
            print(f"  {state}: unexpected error — {e}", flush=True)
            df = pd.DataFrame()

        if df.empty:
            print(f"  {state}: 0 usable records", flush=True)
            continue

        # Drop rows with no parseable date
        before = len(df)
        df = df.dropna(subset=["date"])
        print(f"  {state}: {len(df)} records with valid dates "
              f"(dropped {before - len(df)} undated)", flush=True)

        if df.empty:
            continue

        frames.append(df)

    if not frames:
        print("\npdf_sourced: no records from MN or ND — exiting cleanly.")
        sys.exit(0)

    result = pd.concat(frames, ignore_index=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    result.to_csv(OUT_PATH, index=False)
    print(f"\nSaved {len(result)} pdf_sourced records → {OUT_PATH}")
    print(f"States: {sorted(result['state'].dropna().unique().tolist())}")
    if "source_quality" in result.columns:
        print(f"source_quality values: {result['source_quality'].unique().tolist()}")


if __name__ == "__main__":
    main()
