# Trade

EDINETの提出書類一覧APIとXBRLを使って、企業の財務指標をSQLiteに保存するスクリプト。
PDFなどのファイルは保存せず、数値情報のみ取得します。

## 使い方

1) `.env` にEDINETのAPIキーを設定

```
EDINET_API_KEY=your_key_here
```

2) `scripts/edinet_fetch_financials.py` の設定ブロックを編集

```python
# === Configuration (edit here; no CLI args) ===
START_DATE = "2026-01-01"
END_DATE = "2026-01-31"
DB_PATH = "/Users/yuma/Output/Trade/edinet.db"
INCLUDE_ALL_DOCS = False
INCLUDE_ALL_METRICS = False
MAX_WORKERS = 6
MARKET_FILTER = None  # None / "prime" / "growth" / "standard" / "nikkei225"
MARKET_MAPPING_CSV = "/Users/yuma/Projects/Trade/data/tse_listed.csv"
NIKKEI225_CSV = "/Users/yuma/Projects/Trade/data/nikkei225.csv"
NIKKEI225_TABLE = "Teble_nikkei225"
EDINET_CODE_CSV = "/Users/yuma/Projects/Trade/data/edinet_code.csv"
```

3) 実行

```
/Users/yuma/.pyenv/versions/3.12.0/bin/python /Users/yuma/Projects/Trade/scripts/edinet_fetch_financials.py
```

## 日経平均採用銘柄テーブル

日経平均採用銘柄（225銘柄）のテーブルを作成するスクリプトを追加しています。
実行すると `Teble_nikkei225` テーブルが作成され、DBとCSVに保存されます。

```
/Users/yuma/.pyenv/versions/3.12.0/bin/python /Users/yuma/Projects/Trade/scripts/build_nikkei225_table.py
```

設定は `scripts/build_nikkei225_table.py` の先頭で変更できます。
`SOURCE_MODE="web"` で取得できない場合は `SOURCE_MODE="csv"` にして
`INPUT_CSV` にCSVを指定してください（先頭列=4桁コード、2列目=会社名）。

## 仕様

- 取得期間は `START_DATE`〜`END_DATE`（JST基準）
- EDINET提出書類一覧API（type=2）から対象書類を取得
- `INCLUDE_ALL_DOCS=False` の場合は以下のキーワードで絞り込み
  - 有価証券報告書 / 四半期報告書 / 半期報告書
- `MARKET_FILTER` を指定すると市場/指数で絞り込み
  - `"prime"` / `"growth"` / `"standard"` は `MARKET_MAPPING_CSV` を参照
  - `"nikkei225"` は `NIKKEI225_TABLE` テーブル優先、無ければ `NIKKEI225_CSV` を参照
  - `secCode` が空の場合は `EDINET_CODE_CSV` の `EdinetCode` → `SecuritiesCode` で補完
- 対象書類がXBRL対応（`xbrlFlag=1`）の場合のみXBRL ZIPを取得
- XBRLから数値項目を抽出し、SQLiteに保存
- 進捗は標準出力に表示

## 保存先

- SQLite DB: `/Users/yuma/Output/Trade/edinet.db`

## DBスキーマ（主なカラム）

`edinet_documents` テーブル

- `doc_id`, `edinet_code`, `filer_name`, `doc_description`, `submit_datetime`
- `period_end`
- 主要指標
  - `sales_amount`, `operating_income`, `ordinary_income`, `net_income`
  - `total_assets`, `total_liabilities`, `total_equity`, `cash_and_equivalents`
  - `operating_cf`, `investing_cf`, `financing_cf`
  - `eps`, `bps`, `roe`, `roa`, `employee_count`
- 派生指標
  - `operating_margin`（営業利益率）
  - `net_margin`（純利益率）
  - `equity_ratio`（自己資本比率）
  - `cash_ratio`（現金比率）
- 追加情報
  - `all_numeric_facts_json`（`INCLUDE_ALL_METRICS=True` の場合のみ保存）
  - `raw_json`（提出書類一覧APIの生データ）

## 派生計算

- `operating_margin = operating_income / sales_amount`
- `net_margin = net_income / sales_amount`
- `equity_ratio = total_equity / total_assets`
- `cash_ratio = cash_and_equivalents / total_assets`

## 注意点

- XBRLのタグは一般的な名称でマッチングしています。会社や報告書により取得できない場合があります。
- `INCLUDE_ALL_METRICS=True` にするとXBRLの数値ファクトをJSON保存します（DBサイズが増えます）。
- `.env` とDBは `.gitignore` で除外済み。
- 市場/指数の絞り込みには、銘柄コードの対応表ファイルが必要です。
  - `MARKET_MAPPING_CSV` の形式: `sec_code,market`（例: `7203,prime`）
  - `NIKKEI225_CSV` の形式: 先頭列に4桁コード（例: `9984`）
  - `Teble_nikkei225` を作るには `scripts/build_nikkei225_table.py` を実行してください。
