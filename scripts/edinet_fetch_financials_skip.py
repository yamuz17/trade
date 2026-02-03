#!/usr/bin/env python3
import datetime as dt
import csv
import io
import json
import os
import re
import sqlite3
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

API_BASE_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
API_DOC_URL = "https://api.edinet-fsa.go.jp/api/v2/documents/{}"
#DEFAULT_KEYWORDS = ["有価証券報告書", "四半期報告書", "半期報告書"]
DEFAULT_KEYWORDS = ["有価証券報告書"]

# === Configuration (edit here; no CLI args) ===
START_DATE = "2025-01-01"
END_DATE = "2025-01-31"
DB_PATH = "/Users/yuma/Output/Trade/edinet.db"
INCLUDE_ALL_DOCS = False  # True: no filtering by keywords
INCLUDE_ALL_METRICS = False  # True: store all numeric facts in JSON
MAX_WORKERS = 6
EDINET_CODE_CSV = "/Users/yuma/Projects/Trade/data/edinet_code.csv"
EDINET_CODE_URL = "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip"
NIKKEI225_SOURCE_URL = "https://indexes.nikkei.co.jp/en/nkave/index/component"
MASTER_COMPANY_TABLE = "master_company"
JPX_LISTING_PAGE_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
JPX_LISTING_XLSX_PATH = "/Users/yuma/Projects/Trade/data/jpx_listed.xlsx"

_NIKKEI225_CACHE: set[str] | None = None
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


def normalize_sec_code(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    # EDINET secCode can be 5 digits with trailing 0; normalize to 4-digit code.
    if text.isdigit():
        if len(text) == 5 and text.endswith("0"):
            text = text[:-1]
        return text.zfill(4)
    return text


def read_csv_rows(path: str) -> tuple[list[str], list[list[str]]]:
    encodings = ["utf-8-sig", "cp932", "utf-8"]
    last_error = None
    for encoding in encodings:
        try:
            with open(path, "r", encoding=encoding, newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return [], []
                rows = [row for row in reader]
            return header, rows
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"Failed to read CSV: {last_error}")


def clean_header(name: str) -> str:
    return name.strip().lstrip("\ufeff")


def detect_edinet_code_column(headers: list[str]) -> str | None:
    candidates = {
        "edinet_code",
        "EdinetCode",
        "edinetCode",
        "ＥＤＩＮＥＴコード",
        "EDINETコード",
    }
    for name in headers:
        cleaned = clean_header(name)
        if cleaned in candidates:
            return name
    return None


def detect_security_code_column(headers: list[str]) -> str | None:
    candidates = {
        "securities_code",
        "SecuritiesCode",
        "securitiesCode",
        "sec_code",
        "証券コード",
    }
    for name in headers:
        cleaned = clean_header(name)
        if cleaned in candidates:
            return name
    return None


def detect_company_name_column(headers: list[str]) -> str | None:
    candidates = {
        "提出者名",
        "会社名",
        "会社名称",
        "FilerName",
        "filer_name",
        "name",
    }
    for name in headers:
        cleaned = clean_header(name)
        if cleaned in candidates or "提出者名" in cleaned:
            return name
    return None


def detect_market_column(headers: list[str]) -> str | None:
    candidates = {
        "上場市場",
        "市場区分",
        "市場",
        "Market",
        "market",
    }
    for name in headers:
        cleaned = clean_header(name)
        if cleaned in candidates or "市場" in cleaned:
            return name
    return None


def detect_sector_column(headers: list[str]) -> str | None:
    candidates = {
        "業種",
        "提出者業種",
        "Industry",
        "sector",
        "セクター",
        "33業種区分",
    }
    for name in headers:
        cleaned = clean_header(name)
        if cleaned in candidates or "業種" in cleaned or "セクター" in cleaned:
            return name
    return None


def load_edinet_csv_mapping(
    path: str, target_codes: set[str]
) -> dict[str, dict[str, str | None]]:
    if not os.path.exists(path):
        path = download_edinet_code_csv(path)
        if not path or not os.path.exists(path):
            return {}
    try:
        headers, rows = read_csv_rows(path)
    except Exception as e:
        print(f"[WARN] Failed to read EDINET code CSV: {e}", file=sys.stderr)
        return {}
    if not headers:
        return {}
    edinet_col = detect_edinet_code_column(headers)
    sec_col = detect_security_code_column(headers)
    name_col = detect_company_name_column(headers)
    market_col = detect_market_column(headers)
    sector_col = detect_sector_column(headers)
    if not edinet_col:
        print("[WARN] EDINET code column not found in CSV headers.", file=sys.stderr)
        return {}
    edinet_idx = headers.index(edinet_col)
    sec_idx = headers.index(sec_col) if sec_col else None
    name_idx = headers.index(name_col) if name_col else None
    market_idx = headers.index(market_col) if market_col else None
    sector_idx = headers.index(sector_col) if sector_col else None

    mapping: dict[str, dict[str, str | None]] = {}
    for row in rows:
        if len(row) <= edinet_idx:
            continue
        edinet_code = row[edinet_idx].strip()
        if not edinet_code or edinet_code not in target_codes:
            continue
        securities_code = row[sec_idx].strip() if sec_idx is not None and len(row) > sec_idx else ""
        company_name = row[name_idx].strip() if name_idx is not None and len(row) > name_idx else ""
        market = row[market_idx].strip() if market_idx is not None and len(row) > market_idx else ""
        sector = row[sector_idx].strip() if sector_idx is not None and len(row) > sector_idx else ""
        mapping[edinet_code] = {
            "securities_code": securities_code.zfill(4) if securities_code.isdigit() else securities_code,
            "company_name": company_name or None,
            "market": market or None,
            "sector": sector or None,
        }
    return mapping


def ensure_master_company_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MASTER_COMPANY_TABLE} (
            edinet_code TEXT PRIMARY KEY,
            securities_code TEXT,
            company_name TEXT,
            market TEXT,
            sector TEXT,
            group_name TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_master_company_securities ON {MASTER_COMPANY_TABLE} (securities_code)"
    )


def fetch_nikkei225_codes() -> set[str]:
    global _NIKKEI225_CACHE
    if _NIKKEI225_CACHE is not None:
        return _NIKKEI225_CACHE
    try:
        req = urllib.request.Request(
            NIKKEI225_SOURCE_URL,
            method="GET",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[WARN] Failed to fetch Nikkei225 list: {e}", file=sys.stderr)
        _NIKKEI225_CACHE = set()
        return _NIKKEI225_CACHE

    codes = set()
    for match in re.finditer(r"<td[^>]*>\\s*(\\d{4})\\s*</td>", html, re.IGNORECASE):
        codes.add(match.group(1))
    if not codes:
        text = re.sub(r"<[^>]+>", "\\n", html)
        for line in text.splitlines():
            m = re.match(r"^(\\d{4})\\s+.+$", line.strip())
            if m:
                codes.add(m.group(1))
    if not codes:
        print("[WARN] Failed to parse Nikkei225 list.", file=sys.stderr)
    _NIKKEI225_CACHE = {code.zfill(4) for code in codes}
    return _NIKKEI225_CACHE


def fetch_jpx_listing_excel_url() -> str | None:
    try:
        req = urllib.request.Request(
            JPX_LISTING_PAGE_URL,
            method="GET",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[WARN] Failed to fetch JPX listing page: {e}", file=sys.stderr)
        return None

    match = re.search(r'href="([^"]+\\.(?:xlsx|xls))"', html, re.IGNORECASE)
    if not match:
        return None
    href = match.group(1)
    if href.startswith("http"):
        return href
    return urllib.parse.urljoin(JPX_LISTING_PAGE_URL, href)


def download_jpx_listing_file(url: str, path: str) -> str | None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        req = urllib.request.Request(
            url,
            method="GET",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
        with open(path, "wb") as f:
            f.write(data)
        print(f"[INFO] Downloaded JPX listing file: {path}")
        return path
    except Exception as e:
        print(f"[WARN] Failed to download JPX listing file: {e}", file=sys.stderr)
        return None


def column_index_from_ref(cell_ref: str) -> int:
    letters = re.sub(r"[^A-Z]", "", cell_ref.upper())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def parse_xlsx_rows(xlsx_bytes: bytes) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as zf:
        shared_strings = []
        if "xl/sharedStrings.xml" in zf.namelist():
            shared = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in shared.iter():
                if local_name(si.tag) == "t":
                    shared_strings.append(si.text or "")

        sheet_name = None
        for name in zf.namelist():
            if name.startswith("xl/worksheets/sheet"):
                sheet_name = name
                break
        if not sheet_name:
            return []

        root = ET.fromstring(zf.read(sheet_name))
        rows = []
        for row in root.iter():
            if local_name(row.tag) != "row":
                continue
            cells = {}
            for cell in row:
                if local_name(cell.tag) != "c":
                    continue
                ref = cell.attrib.get("r")
                if not ref:
                    continue
                value = ""
                cell_type = cell.attrib.get("t")
                v = None
                for child in cell:
                    if local_name(child.tag) == "v":
                        v = child.text
                        break
                if v is None:
                    value = ""
                elif cell_type == "s":
                    try:
                        value = shared_strings[int(v)]
                    except Exception:
                        value = ""
                else:
                    value = v
                cells[column_index_from_ref(ref)] = value
            if cells:
                max_idx = max(cells.keys())
                row_values = [cells.get(i, "") for i in range(max_idx + 1)]
                rows.append(row_values)
        return rows


def load_jpx_listing_mapping() -> dict[str, dict[str, str]]:
    url = fetch_jpx_listing_excel_url()
    if not url:
        print("[WARN] JPX listing file URL not found.", file=sys.stderr)
        return {}
    path = download_jpx_listing_file(url, JPX_LISTING_XLSX_PATH)
    if not path or not os.path.exists(path):
        return {}
    if path.lower().endswith(".xls"):
        print("[WARN] JPX listing file is .xls; parsing requires pandas/openpyxl.", file=sys.stderr)
        return {}
    try:
        with open(path, "rb") as f:
            rows = parse_xlsx_rows(f.read())
    except Exception as e:
        print(f"[WARN] Failed to parse JPX listing xlsx: {e}", file=sys.stderr)
        return {}
    if not rows:
        return {}
    headers = [clean_header(h) for h in rows[0]]
    data_rows = rows[1:]
    code_candidates = {"コード", "銘柄コード", "証券コード", "Code"}
    market_candidates = {"市場区分", "市場・商品区分", "市場", "Market"}
    sector_candidates = {"33業種区分", "業種", "セクター"}

    def find_col(candidates: set[str]) -> int | None:
        for idx, name in enumerate(headers):
            if name in candidates:
                return idx
        return None

    code_idx = find_col(code_candidates)
    market_idx = find_col(market_candidates)
    sector_idx = find_col(sector_candidates)
    if code_idx is None:
        return {}

    mapping: dict[str, dict[str, str]] = {}
    for row in data_rows:
        if len(row) <= code_idx:
            continue
        code = row[code_idx].strip()
        if not code.isdigit():
            continue
        market = row[market_idx].strip() if market_idx is not None and len(row) > market_idx else ""
        sector = row[sector_idx].strip() if sector_idx is not None and len(row) > sector_idx else ""
        mapping[code.zfill(4)] = {"market": market, "sector": sector}
    return mapping


def ensure_master_company_entries(db_path: str, csv_path: str, edinet_codes: set[str]) -> None:
    codes = {c for c in edinet_codes if c}
    if not codes:
        return
    conn = sqlite3.connect(db_path)
    try:
        ensure_master_company_schema(conn)
        existing = set()
        codes_list = list(codes)
        chunk = 900
        for i in range(0, len(codes_list), chunk):
            subset = codes_list[i : i + chunk]
            placeholders = ", ".join(["?"] * len(subset))
            rows = conn.execute(
                f"SELECT edinet_code FROM {MASTER_COMPANY_TABLE} WHERE edinet_code IN ({placeholders})",
                subset,
            ).fetchall()
            existing.update(row[0] for row in rows)
        missing = codes - existing

        if missing:
            mapping = load_edinet_csv_mapping(csv_path, missing)
            now = dt.datetime.now(dt.timezone.utc).isoformat()
            payload = []
            for code in missing:
                info = mapping.get(code, {})
                payload.append(
                    (
                        code,
                        info.get("securities_code"),
                        info.get("company_name"),
                        info.get("market"),
                        info.get("sector"),
                        None,
                        now,
                    )
                )
            conn.executemany(
                f"""
                INSERT INTO {MASTER_COMPANY_TABLE}
                    (edinet_code, securities_code, company_name, market, sector, group_name, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(edinet_code) DO UPDATE SET
                    securities_code=COALESCE(excluded.securities_code, {MASTER_COMPANY_TABLE}.securities_code),
                    company_name=COALESCE(excluded.company_name, {MASTER_COMPANY_TABLE}.company_name),
                    market=COALESCE(excluded.market, {MASTER_COMPANY_TABLE}.market),
                    sector=COALESCE(excluded.sector, {MASTER_COMPANY_TABLE}.sector),
                    updated_at=excluded.updated_at
                """,
                payload,
            )
            conn.commit()
            print(f"[INFO] master_company updated: {len(payload)} rows added.")

        need_jpx = conn.execute(
            f"""
            SELECT 1 FROM {MASTER_COMPANY_TABLE}
            WHERE (market IS NULL OR market = '')
               OR (sector IS NULL OR sector = '')
            LIMIT 1
            """
        ).fetchone()
        if need_jpx:
            jpx_map = load_jpx_listing_mapping()
            if jpx_map:
                now = dt.datetime.now(dt.timezone.utc).isoformat()
                for sec_code, info in jpx_map.items():
                    conn.execute(
                        f"""
                        UPDATE {MASTER_COMPANY_TABLE}
                        SET market=COALESCE(?, market),
                            sector=COALESCE(?, sector),
                            updated_at=?
                        WHERE securities_code=?
                        """,
                        (info.get("market") or None, info.get("sector") or None, now, sec_code),
                    )
                conn.commit()

        need_nikkei = conn.execute(
            f"""
            SELECT 1 FROM {MASTER_COMPANY_TABLE}
            WHERE (group_name IS NULL OR group_name = '')
            LIMIT 1
            """
        ).fetchone()
        if need_nikkei:
            nikkei_codes = fetch_nikkei225_codes()
            if nikkei_codes:
                codes_list = list(nikkei_codes)
                chunk = 900
                now = dt.datetime.now(dt.timezone.utc).isoformat()
                for i in range(0, len(codes_list), chunk):
                    subset = codes_list[i : i + chunk]
                    placeholders = ", ".join(["?"] * len(subset))
                    conn.execute(
                        f"""
                        UPDATE {MASTER_COMPANY_TABLE}
                        SET group_name='Nikkei225', updated_at=?
                        WHERE securities_code IN ({placeholders})
                          AND (group_name IS NULL OR group_name='')
                        """,
                        [now, *subset],
                    )
                conn.commit()
    finally:
        conn.close()


def download_edinet_code_csv(path: str) -> str | None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        req = urllib.request.Request(
            EDINET_CODE_URL,
            method="GET",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
        if data.startswith(b"PK"):
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_names = [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if not csv_names:
                    raise RuntimeError("EDINET code zip does not contain CSV.")
                data = zf.read(csv_names[0])
        with open(path, "wb") as f:
            f.write(data)
        print(f"[INFO] Downloaded EDINET code list: {path}")
        return path
    except Exception as e:
        print(f"[WARN] Failed to download EDINET code list: {e}", file=sys.stderr)
        return None


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
            sec_code TEXT,
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
    if "sec_code" not in existing:
        conn.execute("ALTER TABLE edinet_documents ADD COLUMN sec_code TEXT")
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
        "sec_code",
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
            normalize_sec_code(doc.get("secCode") or doc.get("sec_code")),
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
            ON CONFLICT(doc_id) DO NOTHING
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
        print(
            f"[INFO] {date_str}: {len(documents)} documents "
            f"(filtered={not INCLUDE_ALL_DOCS})"
        )
        for doc in documents:
            doc["_fetched_date"] = date_str
        all_documents.extend(documents)

    edinet_codes = {
        str(doc.get("edinetCode") or doc.get("edinet_code") or "").strip()
        for doc in all_documents
    }
    ensure_master_company_entries(DB_PATH, EDINET_CODE_CSV, edinet_codes)
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
