#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RBI Press Release Scraper — with deterministic pdf_filename (title + date)
- Detects DATE HEADER rows (e.g., "Dec 12, 2025").
- Applies that date to all following item rows until next header.
- Extracts top 10 items only.
- Extracts page link (title link) AND pdf_link (if present in row).
- Generates pdf_filename deterministically from title + date (slugified).
- Stable ID (sha256(title|link|date)).
- Stores new entries into data/rbi_master.csv.
- Writes only new rows to data/rbi_new_entries.json.
"""

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

SOURCE_PAGE = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"
MASTER_CSV = "data/rbi_master.csv"
NEW_JSON = "data/rbi_new_entries.json"
MAX_ENTRIES = 10

CSV_HEADER = [
    "id", "date", "title", "link", "pdf_link",
    "pdf_filename", "pdf_downloaded",
    "source_page", "created_at"
]


def log(msg: str):
    print(msg, flush=True)


# ---------------------- DATE & REGEX ----------------------
_MONTH_REGEX = re.compile(
    r'(?:\d{1,2}\s+)?(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
    r'[.,]?\s*\d{4}',
    re.I
)
_YEAR_RANGE_REGEX = re.compile(r'(\d{4})\s*[-–]\s*(\d{2,4})')
_DATE_NUMERIC = re.compile(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}')


def looks_like_date(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    if _MONTH_REGEX.search(t):
        return True
    if _DATE_NUMERIC.search(t):
        return True
    if re.search(r'\b\d{4}\b', t) and len(t) <= 12:
        return True
    return False


def parse_date_to_mmddyyyy(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    t = text.strip()

    # 1) Month-name date
    m = _MONTH_REGEX.search(t)
    if m:
        try:
            dt = dateparser.parse(m.group(0))
            return dt.strftime("%m-%d-%Y")
        except:
            pass

    # 2) Numeric date dd/mm/yyyy or dd-mm-yyyy
    m2 = _DATE_NUMERIC.search(t)
    if m2:
        try:
            dt = dateparser.parse(m2.group(0), fuzzy=True)
            return dt.strftime("%m-%d-%Y")
        except:
            pass

    # 3) Year range 2024-25 -> choose first year as 01-01-year
    yr = _YEAR_RANGE_REGEX.search(t)
    if yr:
        try:
            year = int(yr.group(1))
            dt = datetime(year, 1, 1)
            return dt.strftime("%m-%d-%Y")
        except:
            pass

    # 4) Fallback fuzzy parse
    try:
        dt = dateparser.parse(t, fuzzy=True)
        return dt.strftime("%m-%d-%Y")
    except:
        return None


# ----------------- PDF LINK HELPER -----------------------
def find_pdf_link_in_row(tr) -> Optional[str]:
    # 1) anchors with .pdf in href
    for a in tr.find_all("a", href=True):
        href = a["href"].strip()
        if ".pdf" in href.lower():
            return requests.compat.urljoin(SOURCE_PAGE, href)

    # 2) anchors with img that might be PDF icon
    for a in tr.find_all("a", href=True):
        img = a.find("img")
        if img is not None:
            alt = img.get("alt", "") or ""
            src = img.get("src", "") or ""
            if "pdf" in alt.lower() or "pdf" in src.lower():
                return requests.compat.urljoin(SOURCE_PAGE, a["href"].strip())

    # 3) anchors with text like 'PDF' or 'kb'
    for a in tr.find_all("a", href=True):
        text = a.get_text(" ", strip=True) or ""
        if re.search(r'\bpdf\b', text, re.I) or re.search(r'\bkb\b', text, re.I):
            href = a["href"].strip()
            if href:
                return requests.compat.urljoin(SOURCE_PAGE, href)

    return None


# ----------------- HTML PARSING --------------------------
def fetch_page(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; rbi-scraper/1.0)"}, timeout=30)
    r.raise_for_status()
    return r.text


def find_first_populated_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    for tbl in tables:
        if tbl.find("a"):
            return tbl
    return None


def extract_rows_from_table(tbl, limit=10):
    results: List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = []
    current_date: Optional[str] = None

    for tr in tbl.find_all("tr"):
        if tr.find("th"):
            continue

        tds = tr.find_all("td")
        tr_text = tr.get_text(" ", strip=True)
        a_title = tr.find("a")  # first anchor (usually title)
        # detect date-header (no anchor and looks like date)
        if not a_title:
            if looks_like_date(tr_text) and len(tr_text) <= 40:
                current_date = tr_text
                continue
            found_hdr = False
            for td in tds:
                td_text = td.get_text(" ", strip=True)
                if looks_like_date(td_text) and len(td_text) <= 40:
                    current_date = td_text
                    found_hdr = True
                    break
            if found_hdr:
                continue
            continue

        # item row
        title = a_title.get_text(" ", strip=True) or None
        title_href = a_title.get("href", "").strip()
        link = requests.compat.urljoin(SOURCE_PAGE, title_href) if title_href else None

        # determine pdf link in this row
        pdf_link = find_pdf_link_in_row(tr)

        # date preference: header -> row cells -> title substring
        date_text = current_date
        if not date_text:
            for td in tds:
                td_text = td.get_text(" ", strip=True)
                if looks_like_date(td_text):
                    date_text = td_text
                    break
            if not date_text and title:
                if _MONTH_REGEX.search(title):
                    date_text = title

        results.append((date_text, title, link, pdf_link))
        if len(results) >= limit:
            break

    return results


# ------------------ FILENAME (title+date) ---------------------
def slugify_for_filename(s: str, maxlen: int = 120) -> str:
    # lower, replace non-alnum with underscores, collapse underscores, trim
    if not s:
        return ""
    s = s.lower()
    # replace non-alphanumeric with underscore
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'__+', '_', s)
    s = s.strip('_')
    if len(s) > maxlen:
        s = s[:maxlen]
    return s


def make_pdf_filename_from_title_and_date(title: Optional[str], date_mmdd: Optional[str]) -> str:
    # Use safe defaults
    base_title = title or "rbi_press_release"
    title_slug = slugify_for_filename(base_title, maxlen=100)
    date_part = date_mmdd if date_mmdd else "no-date"
    filename = f"{title_slug}_{date_part}.pdf"
    # ensure filename length reasonable
    if len(filename) > 160:
        filename = filename[:160]
        if not filename.lower().endswith(".pdf"):
            filename = filename.rstrip("._-") + ".pdf"
    return filename


# ------------------ MASTER CSV HANDLING ---------------------
def load_master_csv():
    if not os.path.exists(MASTER_CSV):
        return pd.DataFrame(columns=CSV_HEADER)
    try:
        df = pd.read_csv(MASTER_CSV, dtype=str)
    except Exception as e:
        log(f"WARNING: Could not read {MASTER_CSV}: {e}. Creating fresh dataframe.")
        df = pd.DataFrame(columns=CSV_HEADER)
    for col in CSV_HEADER:
        if col not in df.columns:
            df[col] = ""
    return df[CSV_HEADER]


def entry_exists(df, title, link):
    if title is None:
        title = ""
    if link is None:
        link = ""

    def norm(s):
        if pd.isna(s):
            return ""
        return " ".join(str(s).lower().split())

    norm_title = " ".join(title.lower().split())
    link = (link or "").strip()

    matched = df.apply(lambda row:
                       norm(row.get("title", "")) == norm_title
                       and str(row.get("link", "")).strip() == link,
                       axis=1)
    return matched.any()


def generate_id(title, link, date_):
    title_norm = " ".join((title or "").lower().split())
    parts = [title_norm, link or "", date_ or ""]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def now_ist_iso():
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(IST).isoformat()


# ------------------------ MAIN ------------------------------
def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)
    verbose = args.verbose

    log("Starting RBI scraper...")

    # Fetch
    try:
        html = fetch_page(SOURCE_PAGE)
    except Exception as e:
        log(f"ERROR: Failed to fetch page: {e}")
        with open(NEW_JSON, "w") as f:
            json.dump([], f)
        sys.exit(2)

    soup = BeautifulSoup(html, "html.parser")
    table = find_first_populated_table(soup)
    if not table:
        log("ERROR: No suitable table found on the page.")
        with open(NEW_JSON, "w") as f:
            json.dump([], f)
        sys.exit(3)

    raw_rows = extract_rows_from_table(table, limit=MAX_ENTRIES)
    if verbose:
        log(f"Extracted {len(raw_rows)} rows (raw).")

    parsed_items = []
    for idx, (date_text, title, link, pdf_link) in enumerate(raw_rows):
        parsed_date = parse_date_to_mmddyyyy(date_text)
        if parsed_date is None and date_text:
            log(f"WARNING: Could not parse date '{date_text}' for row {idx+1}; setting date=None")

        # Build deterministic pdf_filename from title + date if pdf_link exists
        pdf_filename = ""
        if pdf_link:
            pdf_filename = make_pdf_filename_from_title_and_date(title, parsed_date)

        if verbose:
            log(f"[{idx+1}] title={repr(title)} link={repr(link)} pdf_link={repr(pdf_link)} pdf_filename={repr(pdf_filename)} date_raw={repr(date_text)} -> parsed={parsed_date}")

        parsed_items.append({
            "date": parsed_date,
            "title": title,
            "link": link,
            "pdf_link": pdf_link,
            "pdf_filename": pdf_filename
        })

    master_df = load_master_csv()
    new_rows = []
    new_json_items = []

    for it in parsed_items:
        t = it["title"]
        l = it["link"]
        d = it["date"]
        pdf_l = it["pdf_link"]
        pdf_fn = it["pdf_filename"]

        if entry_exists(master_df, t, l):
            if verbose:
                log(f"SKIP duplicate: {t}")
            continue

        sid = generate_id(t, l, d)
        created_at = now_ist_iso()

        row = {
            "id": sid,
            "date": d,
            "title": t,
            "link": l,
            "pdf_link": pdf_l,
            "pdf_filename": pdf_fn,
            "pdf_downloaded": False,
            "source_page": SOURCE_PAGE,
            "created_at": created_at
        }
        new_rows.append(row)

        json_item = {
            "id": sid,
            "title": t,
            "link": l,
            "pdf_link": pdf_l,
            "date": d,
            "pdf_filename": pdf_fn,
            "source_page": SOURCE_PAGE
        }
        new_json_items.append(json_item)

    # Save outputs
    if new_rows:
        master_df = pd.concat([master_df, pd.DataFrame(new_rows, columns=CSV_HEADER)], ignore_index=True)
        master_df.to_csv(MASTER_CSV, index=False)

        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(new_json_items, f, ensure_ascii=False, indent=2)

        log(f"Added {len(new_rows)} new rows and wrote {NEW_JSON}.")
        sys.exit(0)
    else:
        with open(NEW_JSON, "w", encoding="utf-8") as f:
            json.dump([], f)
        log("No new entries found; wrote empty new_entries JSON.")
        sys.exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
