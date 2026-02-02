#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request

API_BASE_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
DEFAULT_KEYWORDS = ["有価証券報告書", "四半期報告書", "半期報告書"]


def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value


def jst_today_str() -> str:
    now_utc = dt.datetime.now(dt.timezone.utc)
    jst = now_utc.astimezone(dt.timezone(dt.timedelta(hours=9)))
    return jst.date().isoformat()


def fetch_documents(api_key: str, date_str: str, type_value: int = 2) -> dict:
    params = {
        "date": date_str,
        "type": type_value,
        "Subscription-Key": api_key,
    }
    url = f"{API_BASE_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def extract_documents(payload: dict) -> list:
    for key in ("results", "documents", "result", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def looks_like_financial(doc: dict, keywords: list[str]) -> bool:
    desc = str(doc.get("docDescription") or doc.get("docdescription") or "")
    return any(k in desc for k in keywords)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinet_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_date TEXT NOT NULL,
            doc_id TEXT,
            edinet_code TEXT,
            filer_name TEXT,
            doc_description TEXT,
            submit_datetime TEXT,
            raw_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_edinet_documents_doc_id ON edinet_documents (doc_id)"
    )


def save_documents(conn: sqlite3.Connection, date_str: str, documents: list[dict]) -> int:
    fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()
    inserted = 0
    for doc in documents:
        doc_id = doc.get("docID") or doc.get("docId")
        row = (
            date_str,
            doc_id,
            doc.get("edinetCode"),
            doc.get("filerName"),
            doc.get("docDescription"),
            doc.get("submitDateTime"),
            json.dumps(doc, ensure_ascii=False),
            fetched_at,
        )
        conn.execute(
            """
            INSERT INTO edinet_documents (
                fetched_date, doc_id, edinet_code, filer_name, doc_description, submit_datetime, raw_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(doc_id) DO UPDATE SET
                fetched_date=excluded.fetched_date,
                edinet_code=excluded.edinet_code,
                filer_name=excluded.filer_name,
                doc_description=excluded.doc_description,
                submit_datetime=excluded.submit_datetime,
                raw_json=excluded.raw_json,
                fetched_at=excluded.fetched_at
            """
        )
        inserted += 1
    conn.commit()
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch EDINET financial filings and store them into a SQLite DB.")
    parser.add_argument("--date", default=jst_today_str(), help="Target date (YYYY-MM-DD). Default: today in JST.")
    parser.add_argument("--db", default="edinet.db", help="SQLite DB file path.")
    parser.add_argument("--include-all", action="store_true", help="Store all documents without filtering.")
    parser.add_argument(
        "--keywords",
        default=",".join(DEFAULT_KEYWORDS),
        help="Comma-separated keywords for filtering docDescription.",
    )
    args = parser.parse_args()

    load_dotenv(".env")
    api_key = os.environ.get("EDINET_API_KEY")
    if not api_key:
        print("EDINET_API_KEY is not set. Please set it in .env or environment variables.", file=sys.stderr)
        return 1

    try:
        payload = fetch_documents(api_key, args.date, type_value=2)
    except Exception as e:
        print(f"Failed to fetch documents: {e}", file=sys.stderr)
        return 1

    documents = extract_documents(payload)
    if not args.include_all:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
        documents = [doc for doc in documents if looks_like_financial(doc, keywords)]

    conn = sqlite3.connect(args.db)
    try:
        ensure_schema(conn)
        inserted = save_documents(conn, args.date, documents)
    finally:
        conn.close()

    print(f"Saved {inserted} documents to {args.db} (date={args.date}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
