#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scrapes https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx
- extracts up to top 10 entries from the FIRST table on the page (or first table that looks populated)
- normalizes date to MM-DD-YYYY (or None)
- generates stable SHA256 id using normalized title|link|date
- appends only NEW entries (exact match on title+link) to data/rbi_master.csv
- writes data/rbi_new_entries.json containing only the newly discovered entries
- verbose logging
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

SOURCE_PAGE = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"
MASTER_CSV = "data/rbi_master.csv"
NEW_JSON = "data/rbi_new_entries.json"
MAX_ENTRIES = 10
CSV_HEADER = ["id", "date", "title", "link", "pdf_filename", "pdf_downloaded", "source_page", "created_at"]


def log(msg: str):
    print(msg, flush=True)


def fetch_page(url: str) -> str:
    headers = {
        "User-Agent": "rbi-scraper/1.0 (+https://github.com/)"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def find_first_populated_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    tables = soup.find_all("table")
    for tbl in tables:
        rows = tbl.find_all("tr")
        # consider populated if >0 data rows (skip pure layout tables)
        if len(rows) >= 1:
            # further check if there is at least one <a> within rows (likely links to items)
            if tbl.find("a"):
                return tbl
            # or if tr contains at least 1 td with text
            for tr in rows:
                if tr.find("td") and tr.get_text(strip=True):
                    return tbl
    return None


def extract_rows_from_table(tbl: BeautifulSoup, limit: int = 10) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    Return list of (date, title, link) tuples. Try to be robust:
    - For each row, look for <a> (title+link)
    - Attempt to find a date in any <td> (first cell preferred)
    """
    results = []
    trs = tbl.find_all("tr")
    for tr in trs:
        # skip header-like rows that contain <th>
        if tr.find("th"):
            continue
        tds = tr.find_all("td")
        if not tds:
            continue

        # Extract title+link: find first <a>
        a = tr.find("a")
        title = None
        link = None
        if a:
            title = a.get_text(" ", strip=True)
            href = a.get("href", "").strip()
            if href:
                # convert relative to absolute if needed
                link = requests.compat.urljoin(SOURCE_PAGE, href)

        # Extract date: try first td, but also scan all tds for a probable date pattern
        date_text = None
        # prefer first td if seems date-like
        if len(tds) >= 1:
            cand = tds[0].get_text(" ", strip=True)
            if looks_like_date(cand):
                date_text = cand

        if not date_text:
            for td in tds:
                cand = td.get_text(" ", strip=True)
                if cand and looks_like_date(cand):
                    date_text = cand
                    break

        # normalize whitespace for title
        if title:
            title = " ".join(title.split())

        # Append only if we have some meaningful content (title or link or date)
        if title or link or date_text:
            results.append((date_text, title, link))
        if len(results) >= limit:
            break

    return results


_DATE_RE = re.compile(r'\b\d{1,2}\s+[A-Za-z]{3,}\s+\d{4}\b')  # e.g., 8 Dec 2025


def looks_like_date(text: str) -> bool:
    if not text:
        return False
    text = text.strip()
    # quick heuristics: contains year-like or month names
    if re.search(r'\b\d{4}\b', text):
        return True
    if re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b', text, re.I):
        return True
    # mm/dd/yyyy or dd-mm-yyyy etc
    if re.search(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}', text):
        return True
    return False


def parse_date_to_mmddyyyy(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    try:
        dt = dateparser.parse(text, dayfirst=False, fuzzy=True)
        if not dt:
            return None
        return dt.strftime("%m-%d-%Y")
    except Exception:
        # try alternative parse
        try:
            dt = dateparser.parse(text, dayfirst=True, fuzzy=True)
            if dt:
                return dt.strftime("%m-%d-%Y")
        except Exception:
            return None
    return None


def normalize_title(t: Optional[str]) -> str:
    if not t:
        return ""
    t2 = " ".join(t.strip().split())
    return t2.lower()


def generate_id(title: Optional[str], link: Optional[str], date_mmdd: Optional[str]) -> str:
    parts = [
        normalize_title(title),
        (link or ""),
        (date_mmdd or "")
    ]
    joined = "|".join(parts)
    h = hashlib.sha256(joined.encode("utf-8")).hexdigest()
    return h


def load_master_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        df = pd.DataFrame(columns=CSV_HEADER)
        return df
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as e:
        log(f"WARNING: Failed to read master CSV '{path}': {e}. Creating a fresh one.")
        df = pd.DataFrame(columns=CSV_HEADER)
    # ensure all expected columns exist
    for c in CSV_HEADER:
        if c not in df.columns:
            df[c] = ""
    return df[CSV_HEADER]


def entry_exists(df: pd.DataFrame, title: Optional[str], link: Optional[str]) -> bool:
    # exact match on both title and link (both must match)
    # treat None/NaN as empty string for comparison
    if title is None:
        title = ""
    if link is None:
        link = ""
    # normalize title in master to lower+collapse spaces for fair compare
    def norm(s):
        if pd.isna(s):
            return ""
        return " ".join(str(s).split()).lower()
    norm_title = normalize_title(title)
    # find rows where both normalized title and link match
    matches = df.apply(
        lambda row: (norm(row.get("title", "")) == norm_title) and (str(row.get("link", "")).strip() == (link or "").strip()),
        axis=1
    )
    return matches.any()


def now_ist_iso() -> str:
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).isoformat()


def write_master_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)


def write_new_json(items: List[dict], path: str):
    # ensure nulls are actual nulls in JSON
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(items, fh, ensure_ascii=False, indent=2)


def main(argv):
    parser = argparse.ArgumentParser(description="Scrape RBI press releases (top 10)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    verbose = args.verbose

    log("Starting RBI scraper")
    if verbose:
        log(f"Fetching page: {SOURCE_PAGE}")

    try:
        html = fetch_page(SOURCE_PAGE)
    except Exception as e:
        log(f"ERROR: Failed to fetch page: {e}")
        sys.exit(2)

    soup = BeautifulSoup(html, "html.parser")
    tbl = find_first_populated_table(soup)
    if not tbl:
        log("ERROR: Could not find a populated table on the page.")
        # create/overwrite new_entries.json with empty array for visibility
        write_new_json([], NEW_JSON)
        sys.exit(3)

    rows = extract_rows_from_table(tbl, limit=MAX_ENTRIES)
    if verbose:
        log(f"Found {len(rows)} candidate rows (limited to {MAX_ENTRIES}).")

    parsed_items = []
    for idx, (date_text, title, link) in enumerate(rows):
        parsed_date = parse_date_to_mmddyyyy(date_text) if date_text else None
        if parsed_date is None and date_text:
            log(f"WARNING: Could not parse date '{date_text}' for row {idx+1}; setting date=None")

        item = {
            "date": parsed_date,            # MM-DD-YYYY or None
            "title": title or None,
            "link": link or None,
        }
        parsed_items.append(item)
        if verbose:
            log(f"Row {idx+1}: title={repr(title)}, link={repr(link)}, date_raw={repr(date_text)}, date_parsed={parsed_date}")

    # load master CSV
    master_df = load_master_csv(MASTER_CSV)
    new_rows = []
    new_json_items = []

    for it in parsed_items:
        title = it["title"]
        link = it["link"]
        date_mm = it["date"]

        if entry_exists(master_df, title, link):
            if verbose:
                log(f"SKIP (duplicate): title={repr(title)} link={repr(link)}")
            continue

        # create id
        sid = generate_id(title, link, date_mm)
        created_at = now_ist_iso()

        # prepare master CSV row (strings)
        row = {
            "id": sid,
            "date": date_mm if date_mm is not None else None,
            "title": title if title is not None else None,
            "link": link if link is not None else None,
            "pdf_filename": "",            # we are not downloading
            "pdf_downloaded": False,
            "source_page": SOURCE_PAGE,
            "created_at": created_at
        }
        new_rows.append(row)

        # prepare minimal JSON item (as requested)
        json_item = {
            "id": sid,
            "title": title if title is not None else None,
            "link": link if link is not None else None,
            "date": date_mm if date_mm is not None else None,
            "pdf_filename": "",
            "source_page": SOURCE_PAGE
        }
        new_json_items.append(json_item)

        if verbose:
            log(f"NEW: id={sid} title={repr(title)} date={date_mm} link={repr(link)}")

    # Append to master_df if new_rows exist
    if new_rows:
        # Convert existing DF to str-safe columns
        # Append new rows preserving column order
        append_df = pd.DataFrame(new_rows, columns=CSV_HEADER)
        # Ensure boolean displayed as False/True not NaN
        append_df['pdf_downloaded'] = append_df['pdf_downloaded'].astype(bool)
        master_df = pd.concat([master_df, append_df], ignore_index=True, sort=False)
        write_master_csv(master_df, MASTER_CSV)
        write_new_json(new_json_items, NEW_JSON)
        log(f"Added {len(new_rows)} new rows to {MASTER_CSV} and wrote {NEW_JSON}.")
        # exit 0 so workflow can decide to commit
        sys.exit(0)
    else:
        # No new entries: write empty JSON (or keep previous?) - per plan, overwrite with [] for visibility
        write_new_json([], NEW_JSON)
        log("No new entries found. Wrote empty new_entries JSON and will not commit master CSV.")
        # we still leave JSON written so Action artifacts / checks can read it
        sys.exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
