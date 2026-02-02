#!/usr/bin/env python3
import datetime as dt
import io
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

API_BASE_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
API_DOC_URL = "https://api.edinet-fsa.go.jp/api/v2/documents/{}"
DEFAULT_KEYWORDS = ["有価証券報告書", "四半期報告書", "半期報告書"]

# === Configuration (edit here; no CLI args) ===
START_DATE = "2025-01-01"
END_DATE = "2025-06-30"
DB_PATH = "/Users/yuma/Output/Trade/edinet.db"
INCLUDE_ALL_DOCS = False  # True: no filtering by keywords
INCLUDE_ALL_METRICS = False  # True: store all numeric facts in JSON
MAX_WORKERS = 6
MARKET_FILTER = "nikkei225"  # None / "prime" / "growth" / "standard" / "nikkei225"
MARKET_MAPPING_CSV = "/Users/yuma/Projects/Trade/data/tse_listed.csv"
NIKKEI225_CSV = "/Users/yuma/Projects/Trade/data/nikkei225.csv"
NIKKEI225_TABLE = "Teble_nikkei225"
EDINET_CODE_CSV = "/Users/yuma/Projects/Trade/data/edinet_code.csv"
COMMON_METRICS = {
    "sales_amount": [
        "netsales",
        "netsalessummaryofbusinessresults",
        "revenue",
        "revenuesummaryofbusinessresults",
        "operatingrevenue",
        "operatingrevenuesummaryofbusinessresults",
    ],
    "operating_income": [
        "operatingincome",
        "operatingincomesummaryofbusinessresults",
    ],
    "ordinary_income": [
        "ordinaryincome",
        "ordinaryincomesummaryofbusinessresults",
    ],
    "net_income": [
        "profitloss",
        "profitlosssummaryofbusinessresults",
        "profitlossattributabletoownersofparent",
        "profitlossattributabletoownersofparentsummaryofbusinessresults",
    ],
    "total_assets": ["assets"],
    "total_liabilities": ["liabilities"],
    "total_equity": ["equity", "netassets"],
    "cash_and_equivalents": ["cashandcashequivalents"],
    "operating_cf": ["netcashprovidedbyusedinoperatingactivities"],
    "investing_cf": [
        "netcashprovidedbyusedininvestingactivities",
        "netcashprovidedbyusedininvestmentactivities",
    ],
    "financing_cf": ["netcashprovidedbyusedinfinancingactivities"],
    "eps": [
        "earningspershare",
        "basicearningspershare",
        "earningspersharesummaryofbusinessresults",
    ],
    "bps": ["bookvaluepershare", "netassetspershare"],
    "roe": [
        "returnonequity",
        "returnonequitysummaryofbusinessresults",
        "rateofreturnonequity",
    ],
    "roa": [
        "returnonassets",
        "returnonassetssummaryofbusinessresults",
        "rateofreturnonassets",
    ],
    "employee_count": ["numberofemployees", "numberofemployeesaverage"],
}
EXTRA_COLUMNS = [
    ("period_end", "TEXT"),
    ("operating_income", "REAL"),
    ("ordinary_income", "REAL"),
    ("net_income", "REAL"),
    ("total_assets", "REAL"),
    ("total_liabilities", "REAL"),
    ("total_equity", "REAL"),
    ("cash_and_equivalents", "REAL"),
    ("operating_cf", "REAL"),
    ("investing_cf", "REAL"),
    ("financing_cf", "REAL"),
    ("eps", "REAL"),
    ("bps", "REAL"),
    ("roe", "REAL"),
    ("roa", "REAL"),
    ("gross_margin", "REAL"),
    ("operating_margin", "REAL"),
    ("net_margin", "REAL"),
    ("equity_ratio", "REAL"),
    ("cash_ratio", "REAL"),
    ("all_numeric_facts_json", "TEXT"),
]


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


def daterange(start_date: dt.date, end_date: dt.date):
    current = start_date
    while current <= end_date:
        yield current
        current += dt.timedelta(days=1)


def fetch_documents(api_key: str, date_str: str, type_value: int = 2) -> dict:
    params = {
        "date": date_str,
        "type": type_value,
        "Subscription-Key": api_key,
    }
    url = f"{API_BASE_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    print(f"[INFO] Fetching documents list for {date_str}")
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def fetch_document_zip(api_key: str, doc_id: str, type_value: int = 1) -> bytes:
    params = {"type": type_value, "Subscription-Key": api_key}
    url = f"{API_DOC_URL.format(doc_id)}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    print(f"[INFO] Downloading XBRL ZIP for doc_id={doc_id}")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def extract_documents(payload: dict) -> list:
    for key in ("results", "documents", "result", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def looks_like_financial(doc: dict, keywords: list[str]) -> bool:
    desc = str(doc.get("docDescription") or doc.get("docdescription") or "")
    return any(k in desc for k in keywords)


def parse_number(value: str) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    text = text.replace(",", "")
    try:
        num = float(text)
    except ValueError:
        return None
    if negative:
        num = -num
    return num


def load_market_mapping(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        return {}
    mapping: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        header = None
        for line in f:
            line = line.strip()
            if not line:
                continue
            cols = [c.strip() for c in line.split(",")]
            if header is None:
                header = cols
                continue
            row = dict(zip(header, cols))
            code = row.get("sec_code") or row.get("code") or row.get("SecurityCode")
            market = row.get("market") or row.get("Market") or row.get("market_segment")
            if not code or not market:
                continue
            mapping[code.zfill(4)] = market.strip().lower()
    return mapping


def load_code_list(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    codes: set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            code = line.split(",")[0].strip()
            if code.isdigit():
                codes.add(code.zfill(4))
    return codes


def load_nikkei225_from_db(path: str, table_name: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    try:
        conn = sqlite3.connect(path)
        try:
            rows = conn.execute(f"SELECT sec_code FROM {table_name}").fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return set()
    return {str(row[0]).zfill(4) for row in rows}


def load_edinet_code_map(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        return {}
    mapping: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        header = None
        for line in f:
            line = line.strip()
            if not line:
                continue
            cols = [c.strip() for c in line.split(",")]
            if header is None:
                header = cols
                continue
            row = dict(zip(header, cols))
            edinet_code = (
                row.get("edinet_code")
                or row.get("EdinetCode")
                or row.get("edinetCode")
            )
            sec_code = (
                row.get("securities_code")
                or row.get("SecuritiesCode")
                or row.get("securitiesCode")
                or row.get("sec_code")
            )
            if not edinet_code or not sec_code:
                continue
            mapping[edinet_code.strip()] = sec_code.strip().zfill(4)
    return mapping


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def parse_contexts(root: ET.Element) -> dict[str, dict]:
    contexts: dict[str, dict] = {}
    for ctx in root.iter():
        if local_name(ctx.tag) != "context":
            continue
        ctx_id = ctx.attrib.get("id")
        if not ctx_id:
            continue
        period_end = None
        consolidated = False
        for child in ctx.iter():
            tag = local_name(child.tag)
            if tag == "instant":
                period_end = child.text
            elif tag == "endDate":
                period_end = child.text
            elif tag == "explicitMember":
                member = child.text or ""
                if "Consolidated" in member:
                    consolidated = True
        contexts[ctx_id] = {
            "period_end": period_end,
            "consolidated": consolidated,
        }
    return contexts


def extract_xbrl_bytes(zip_bytes: bytes) -> bytes | None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        candidates = [
            name
            for name in zf.namelist()
            if name.lower().endswith((".xbrl", ".xml", ".xhtml", ".html"))
        ]
        candidates.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        for name in candidates:
            try:
                content = zf.read(name)
                root = ET.fromstring(content)
            except Exception:
                continue
            if local_name(root.tag) in {"xbrl", "html"}:
                return content
    return None


def collect_numeric_facts(root: ET.Element) -> list[dict]:
    contexts = parse_contexts(root)
    facts: list[dict] = []
    for elem in root.iter():
        tag = local_name(elem.tag)
        context_ref = elem.attrib.get("contextRef")
        unit_ref = elem.attrib.get("unitRef")
        decimals = elem.attrib.get("decimals")
        scale = elem.attrib.get("scale")
        if tag == "nonFraction" and "name" in elem.attrib:
            name = elem.attrib.get("name", "").split(":")[-1]
            context_ref = elem.attrib.get("contextRef")
            unit_ref = elem.attrib.get("unitRef")
            decimals = elem.attrib.get("decimals")
            scale = elem.attrib.get("scale")
            value = parse_number(elem.text)
        elif context_ref:
            name = tag
            value = parse_number(elem.text)
        else:
            continue
        if value is None:
            continue
        if scale:
            try:
                value *= 10 ** int(scale)
            except ValueError:
                pass
        context_info = contexts.get(context_ref or "", {})
        facts.append(
            {
                "name": name,
                "value": value,
                "contextRef": context_ref,
                "unitRef": unit_ref,
                "decimals": decimals,
                "period_end": context_info.get("period_end"),
                "consolidated": context_info.get("consolidated", False),
            }
        )
    return facts


def select_metric_value(facts: list[dict], candidates: list[str]) -> dict | None:
    if not facts:
        return None
    candidates_set = {c.lower() for c in candidates}
    matched = [f for f in facts if f["name"].lower() in candidates_set]
    if not matched:
        return None

    def sort_key(fact: dict) -> tuple:
        period_end = fact.get("period_end") or ""
        consolidated = 1 if fact.get("consolidated") else 0
        return (period_end, consolidated)

    return max(matched, key=sort_key)


def extract_metrics_from_xbrl(xbrl_bytes: bytes, include_all: bool) -> tuple[dict, str | None]:
    root = ET.fromstring(xbrl_bytes)
    facts = collect_numeric_facts(root)
    metrics: dict[str, float | None] = {}
    period_ends = []
    for key, candidates in COMMON_METRICS.items():
        selected = select_metric_value(facts, candidates)
        if selected:
            value = selected["value"]
            if key == "employee_count":
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    value = None
            metrics[key] = value
            if selected.get("period_end"):
                period_ends.append(selected["period_end"])
        else:
            metrics[key] = None
    metrics["period_end"] = max(period_ends) if period_ends else None
    all_facts_json = None
    if include_all:
        all_facts_json = json.dumps(facts, ensure_ascii=False)
    return metrics, all_facts_json


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
            period_end TEXT,
            sales_amount REAL,
            employee_count INTEGER,
            operating_income REAL,
            ordinary_income REAL,
            net_income REAL,
            total_assets REAL,
            total_liabilities REAL,
            total_equity REAL,
            cash_and_equivalents REAL,
            operating_cf REAL,
            investing_cf REAL,
            financing_cf REAL,
            eps REAL,
            bps REAL,
            roe REAL,
            roa REAL,
            gross_margin REAL,
            operating_margin REAL,
            net_margin REAL,
            equity_ratio REAL,
            cash_ratio REAL,
            all_numeric_facts_json TEXT,
            raw_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_edinet_documents_doc_id ON edinet_documents (doc_id)"
    )
    existing = {row[1] for row in conn.execute("PRAGMA table_info(edinet_documents)")}
    for column, col_type in EXTRA_COLUMNS:
        if column not in existing:
            conn.execute(f"ALTER TABLE edinet_documents ADD COLUMN {column} {col_type}")


def save_documents(conn: sqlite3.Connection, date_str: str, documents: list[dict]) -> int:
    fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()
    columns = [
        "fetched_date",
        "doc_id",
        "edinet_code",
        "filer_name",
        "doc_description",
        "submit_datetime",
        "period_end",
        "sales_amount",
        "employee_count",
        "operating_income",
        "ordinary_income",
        "net_income",
        "total_assets",
        "total_liabilities",
        "total_equity",
        "cash_and_equivalents",
        "operating_cf",
        "investing_cf",
        "financing_cf",
        "eps",
        "bps",
        "roe",
        "roa",
        "gross_margin",
        "operating_margin",
        "net_margin",
        "equity_ratio",
        "cash_ratio",
        "all_numeric_facts_json",
        "raw_json",
        "fetched_at",
    ]
    placeholders = ", ".join(["?"] * len(columns))
    update_assignments = ", ".join(
        [f"{col}=excluded.{col}" for col in columns if col != "doc_id"]
    )
    inserted = 0
    rows = []
    for doc in documents:
        doc_id = doc.get("docID") or doc.get("docId")
        row = (
            date_str,
            doc_id,
            doc.get("edinetCode"),
            doc.get("filerName"),
            doc.get("docDescription"),
            doc.get("submitDateTime"),
            doc.get("period_end"),
            doc.get("sales_amount"),
            doc.get("employee_count"),
            doc.get("operating_income"),
            doc.get("ordinary_income"),
            doc.get("net_income"),
            doc.get("total_assets"),
            doc.get("total_liabilities"),
            doc.get("total_equity"),
            doc.get("cash_and_equivalents"),
            doc.get("operating_cf"),
            doc.get("investing_cf"),
            doc.get("financing_cf"),
            doc.get("eps"),
            doc.get("bps"),
            doc.get("roe"),
            doc.get("roa"),
            doc.get("gross_margin"),
            doc.get("operating_margin"),
            doc.get("net_margin"),
            doc.get("equity_ratio"),
            doc.get("cash_ratio"),
            doc.get("all_numeric_facts_json"),
            json.dumps(doc, ensure_ascii=False),
            fetched_at,
        )
        rows.append(row)
    if rows:
        conn.executemany(
            f"""
            INSERT INTO edinet_documents ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(doc_id) DO UPDATE SET
                {update_assignments}
            """,
            rows,
        )
        inserted = len(rows)
    conn.commit()
    return inserted


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def compute_derived(metrics: dict) -> dict:
    sales = metrics.get("sales_amount")
    operating_income = metrics.get("operating_income")
    net_income = metrics.get("net_income")
    total_assets = metrics.get("total_assets")
    total_equity = metrics.get("total_equity")
    cash = metrics.get("cash_and_equivalents")

    return {
        "gross_margin": None,  # Not available without gross profit
        "operating_margin": safe_div(operating_income, sales),
        "net_margin": safe_div(net_income, sales),
        "equity_ratio": safe_div(total_equity, total_assets),
        "cash_ratio": safe_div(cash, total_assets),
    }


def parse_date_config(value: str | None, label: str) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        print(f"Invalid {label} format. Use YYYY-MM-DD.", file=sys.stderr)
        return None


def filter_by_market(documents: list[dict]) -> list[dict]:
    if MARKET_FILTER is None:
        return documents
    filter_value = MARKET_FILTER.lower()
    edinet_map = load_edinet_code_map(EDINET_CODE_CSV)
    if filter_value == "nikkei225":
        nikkei_codes = load_nikkei225_from_db(DB_PATH, NIKKEI225_TABLE) or load_code_list(
            NIKKEI225_CSV
        )
        if not nikkei_codes:
            print("[WARN] Nikkei225 list not found in DB or CSV.", file=sys.stderr)
            return []

        def sec_from_doc(doc: dict) -> str:
            sec = str(doc.get("secCode") or doc.get("sec_code") or "").zfill(4)
            if sec and sec != "0000":
                return sec
            edinet_code = str(doc.get("edinetCode") or doc.get("edinet_code") or "")
            return edinet_map.get(edinet_code, "")

        return [
            doc
            for doc in documents
            if sec_from_doc(doc) in nikkei_codes
        ]

    market_map = load_market_mapping(MARKET_MAPPING_CSV)
    if not market_map:
        print(f"[WARN] Market mapping CSV not found: {MARKET_MAPPING_CSV}", file=sys.stderr)
        return []

    def match_market(doc: dict) -> bool:
        code = str(doc.get("secCode") or doc.get("sec_code") or "").zfill(4)
        if not code or code == "0000":
            edinet_code = str(doc.get("edinetCode") or doc.get("edinet_code") or "")
            code = edinet_map.get(edinet_code, "")
        market = market_map.get(code, "")
        return market == filter_value

    return [doc for doc in documents if match_market(doc)]


def enrich_document(api_key: str, doc: dict, include_all_metrics: bool) -> dict:
    doc_id = doc.get("docID") or doc.get("docId")
    xbrl_flag = str(doc.get("xbrlFlag") or doc.get("XBRLFlag") or "")
    metrics = {k: None for k in COMMON_METRICS}
    all_facts_json = None
    if doc_id and xbrl_flag == "1":
        zip_bytes = fetch_document_zip(api_key, doc_id, type_value=1)
        xbrl_bytes = extract_xbrl_bytes(zip_bytes)
        if xbrl_bytes:
            metrics, all_facts_json = extract_metrics_from_xbrl(
                xbrl_bytes, include_all=include_all_metrics
            )
    doc.update(metrics)
    doc["all_numeric_facts_json"] = all_facts_json
    doc.update(compute_derived(metrics))
    return doc


def main() -> int:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, "..", ".env"))
    api_key = os.environ.get("EDINET_API_KEY")
    if not api_key:
        print("EDINET_API_KEY is not set. Please set it in .env or environment variables.", file=sys.stderr)
        return 1

    start_date = parse_date_config(START_DATE, "START_DATE")
    end_date = parse_date_config(END_DATE, "END_DATE")
    if start_date is None:
        start_date = dt.date.fromisoformat(jst_today_str())
    if end_date is None:
        end_date = dt.date.fromisoformat(jst_today_str())

    if start_date > end_date:
        print("start-date must be <= end-date.", file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    all_documents: list[dict] = []
    for current_date in daterange(start_date, end_date):
        date_str = current_date.isoformat()
        try:
            payload = fetch_documents(api_key, date_str, type_value=2)
        except Exception as e:
            print(f"Failed to fetch documents for {date_str}: {e}", file=sys.stderr)
            return 1

        documents = extract_documents(payload)
        if not INCLUDE_ALL_DOCS:
            keywords = [k.strip() for k in DEFAULT_KEYWORDS if k.strip()]
            documents = [doc for doc in documents if looks_like_financial(doc, keywords)]
        if MARKET_FILTER is not None:
            documents = filter_by_market(documents)
        print(
            f"[INFO] {date_str}: {len(documents)} documents "
            f"(filtered={not INCLUDE_ALL_DOCS}, market={MARKET_FILTER})"
        )
        for doc in documents:
            doc["_fetched_date"] = date_str
        all_documents.extend(documents)

    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_schema(conn)
        inserted = 0
        enriched_by_date: dict[str, list[dict]] = {}
        total_docs = len(all_documents)
        print(f"[INFO] Total documents to process: {total_docs}")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(enrich_document, api_key, doc, INCLUDE_ALL_METRICS): doc
                for doc in all_documents
            }
            completed = 0
            for future in as_completed(futures):
                doc = futures[future]
                try:
                    enriched = future.result()
                except Exception as e:
                    doc_id = doc.get("docID") or doc.get("docId")
                    print(f"Failed to parse XBRL for {doc_id}: {e}", file=sys.stderr)
                    enriched = doc
                enriched_by_date.setdefault(enriched["_fetched_date"], []).append(enriched)
                completed += 1
                if completed % 10 == 0 or completed == total_docs:
                    print(f"[INFO] XBRL processed: {completed}/{total_docs}")

        for date_str, docs in enriched_by_date.items():
            count = save_documents(conn, date_str, docs)
            inserted += count
            print(f"[INFO] Saved {count} docs for {date_str}")
    finally:
        conn.close()

    print(f"Saved {inserted} documents to {DB_PATH} (date={START_DATE}..{END_DATE}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
