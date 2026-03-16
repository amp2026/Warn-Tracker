"""
Fetch Washington State WARN Act data from fortress.wa.gov.

Replaces warn-scraper's wa.py which has a while-True pagination loop that
never exits: when you POST for a page beyond the last one, ASP.NET returns
the last page again with no error, so the loop spins forever.

Fix: after each POST we check the page-selector in the ASP.NET GridView.
The current page is rendered as a plain <span> (not a link); if the page
number we see as "current" is less than the page we just requested, we've
overshot the end and stop.

Output: data/raw/wa.csv  (same path warn-scraper would use)
"""
import csv
import os
import re
import sys

import requests
from bs4 import BeautifulSoup, Tag

URL = "https://fortress.wa.gov/esd/file/warn/Public/SearchWARN.aspx"
UA  = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:68.0) Gecko/20100101 Firefox/68.0"
MAX_PAGES = 500   # hard safety cap


def _clean(text):
    if text is None:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _parse_rows(table):
    """Extract data rows from the GridView table (skip header/footer rows)."""
    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        rows.append([_clean(c.get_text()) for c in cells])
    # warn-scraper slices [2:-2] to remove header/pager rows
    return rows[2:len(rows) - 2] if len(rows) > 4 else rows


def _current_page(soup):
    """
    Return the page number that ASP.NET thinks is the current page.
    In GridView pagination, the active page is a <span> (not an <a>),
    sitting inside the pager row's <td>.
    Returns None if no pager is found (single-page result).
    """
    for span in soup.find_all("span"):
        text = span.get_text(strip=True)
        if text.isdigit() and span.parent and span.parent.name == "td":
            return int(text)
    return None


def _viewstate(soup):
    vs  = soup.find("input", attrs={"name": "__VIEWSTATE"})
    ev  = soup.find("input", attrs={"name": "__EVENTVALIDATION"})
    if isinstance(vs, Tag) and isinstance(ev, Tag):
        return vs["value"], ev["value"]
    return None, None


def scrape(out_path="data/raw/wa.csv"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = UA

    print("WA: fetching page 1 …", flush=True)
    resp = session.get(URL, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, "html5lib")
    tables = soup.find_all("table")
    if not tables:
        print("WA: no table found on first page — aborting", file=sys.stderr)
        sys.exit(1)

    grid = tables[0]

    # Extract column headers from the <th> row
    header_row = grid.find("tr")
    if header_row:
        headers = [_clean(th.get_text()) for th in header_row.find_all("th")]
    else:
        headers = []

    all_rows = _parse_rows(grid)
    print(f"  page 1: {len(all_rows)} rows", flush=True)

    for page in range(2, MAX_PAGES + 1):
        vs, ev = _viewstate(soup)
        if vs is None:
            print(f"  page {page}: no VIEWSTATE — stopping (single-page result)")
            break

        formdata = {
            "__EVENTTARGET":   "ucPSW$gvMain",
            "__EVENTARGUMENT": f"Page${page}",
            "__VIEWSTATE":     vs,
            "__EVENTVALIDATION": ev,
        }
        try:
            resp = session.post(URL, data=formdata, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  page {page}: request error ({e}) — stopping", flush=True)
            break

        soup = BeautifulSoup(resp.content, "html5lib")

        # Detect last page: ASP.NET returns the last page again when we overshoot.
        # The "current page" span will show a number < page if we've overshot.
        active = _current_page(soup)
        if active is not None and active < page:
            print(f"  page {page}: server returned page {active} — reached end", flush=True)
            break

        tables = soup.find_all("table")
        if not tables:
            print(f"  page {page}: no table — stopping", flush=True)
            break

        rows = _parse_rows(tables[0])
        if not rows:
            print(f"  page {page}: empty table — stopping", flush=True)
            break

        all_rows.extend(rows)
        print(f"  page {page}: {len(rows)} rows (total {len(all_rows)})", flush=True)

    print(f"WA: writing {len(all_rows)} rows to {out_path}", flush=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if headers:
            writer.writerow(headers)
        writer.writerows(all_rows)

    return out_path


if __name__ == "__main__":
    scrape()
