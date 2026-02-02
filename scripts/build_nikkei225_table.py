#!/usr/bin/env python3
import csv
import datetime as dt
import html.parser
import os
import re
import sqlite3
import urllib.request

# === Configuration (edit here; no CLI args) ===
DB_PATH = "/Users/yuma/Output/Trade/edinet.db"
OUTPUT_CSV = "/Users/yuma/Projects/Trade/data/nikkei225.csv"
WRITE_CSV = True
NIKKEI225_SOURCE_URL = "https://indexes.nikkei.co.jp/en/nkave/index/component"
TABLE_NAME = "Teble_nikkei225"
SOURCE_MODE = "web"  # "web" or "csv"
INPUT_CSV = "/Users/yuma/Projects/Trade/data/nikkei225_source.csv"
DEBUG_HTML_PATH = "/Users/yuma/Output/Trade/nikkei225_source.html"


class TextExtractor(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        if data and data.strip():
            self.parts.append(data.strip())

    def text(self) -> str:
        return "\n".join(self.parts)


def fetch_html(url: str) -> str:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_nikkei225(html: str) -> tuple[list[tuple[str, str]], str | None]:
    parser = TextExtractor()
    parser.feed(html)
    text = parser.text()
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    update_match = None
    for line in lines:
        if "Update" in line:
            update_match = line
            break
    as_of = None
    if update_match:
        m = re.search(r"Update[:：]\s*([A-Za-z]{3}/\d{2}/\d{4})", update_match)
        if m:
            as_of = m.group(1)

    pattern = re.compile(r"^(\d{4})\s+(.+)$")
    rows = []
    for line in lines:
        m = pattern.match(line)
        if not m:
            continue
        code, name = m.group(1), m.group(2)
        if name.lower().startswith("code"):
            continue
        rows.append((code, name))

    # Fallback: try HTML table patterns
    if not rows:
        table_patterns = [
            re.compile(r"(\d{4})\s*</td>\s*<td[^>]*>\s*([^<]+)", re.IGNORECASE),
            re.compile(r"\"code\"\\s*:\\s*\"(\\d{4})\"\\s*,\\s*\"name\"\\s*:\\s*\"([^\"]+)\"", re.IGNORECASE),
        ]
        for pat in table_patterns:
            for match in pat.finditer(html):
                rows.append((match.group(1), match.group(2)))

    # Deduplicate while preserving order
    seen = set()
    unique_rows = []
    for code, name in rows:
        if code in seen:
            continue
        seen.add(code)
        unique_rows.append((code, name))

    return unique_rows, as_of


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            sec_code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            as_of TEXT,
            source_url TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        )
        """
    )


def save_to_db(conn: sqlite3.Connection, rows: list[tuple[str, str]], as_of: str | None) -> int:
    fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()
    payload = [
        (code, name, as_of, NIKKEI225_SOURCE_URL, fetched_at) for code, name in rows
    ]
    conn.executemany(
        f"""
        INSERT INTO {TABLE_NAME} (sec_code, company_name, as_of, source_url, fetched_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(sec_code) DO UPDATE SET
            company_name=excluded.company_name,
            as_of=excluded.as_of,
            source_url=excluded.source_url,
            fetched_at=excluded.fetched_at
        """,
        payload,
    )
    conn.commit()
    return len(rows)


def save_to_csv(rows: list[tuple[str, str]], as_of: str | None) -> None:
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sec_code", "company_name", "as_of", "source_url"])
        for code, name in rows:
            writer.writerow([code, name, as_of, NIKKEI225_SOURCE_URL])


def load_from_csv(path: str) -> list[tuple[str, str]]:
    if not os.path.exists(path):
        return []
    rows: list[tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if not row:
                continue
            code = row[0].strip()
            name = row[1].strip() if len(row) > 1 else ""
            if code.isdigit() and name:
                rows.append((code.zfill(4), name))
    return rows


def main() -> int:
    html = None
    if SOURCE_MODE == "csv":
        rows = load_from_csv(INPUT_CSV)
        as_of = None
    else:
        html = fetch_html(NIKKEI225_SOURCE_URL)
        rows, as_of = parse_nikkei225(html)
    if not rows:
        if SOURCE_MODE == "web":
            os.makedirs(os.path.dirname(DEBUG_HTML_PATH), exist_ok=True)
            with open(DEBUG_HTML_PATH, "w", encoding="utf-8") as f:
                f.write(html or "")
            print(f"[INFO] Saved source HTML to: {DEBUG_HTML_PATH}")
            print("[INFO] You can switch SOURCE_MODE to 'csv' and set INPUT_CSV.")
        else:
            print("[ERROR] Failed to load Nikkei 225 constituents from CSV.")
        print("[ERROR] Failed to parse Nikkei 225 constituents.")
        return 1

    if WRITE_CSV:
        save_to_csv(rows, as_of)

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_table(conn)
        saved = save_to_db(conn, rows, as_of)
    finally:
        conn.close()

    print(f"[INFO] Saved {saved} Nikkei 225 constituents.")
    if as_of:
        print(f"[INFO] As of: {as_of}")
    if WRITE_CSV:
        print(f"[INFO] CSV: {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
