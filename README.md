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
EDINET_CODE_CSV = "/Users/yuma/Projects/Trade/data/edinet_code.csv"
EDINET_CODE_URL = "https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip"
NIKKEI225_SOURCE_URL = "https://indexes.nikkei.co.jp/en/nkave/index/component"
MASTER_COMPANY_TABLE = "master_company"
```

3) 実行

```
/Users/yuma/.pyenv/versions/3.12.0/bin/python /Users/yuma/Projects/Trade/scripts/edinet_fetch_financials.py
```

## 日経平均採用銘柄の扱い

`master_company.group_name` に `Nikkei225` を付与するため、日経平均採用銘柄の一覧を参照します。
取得に失敗する場合は未設定のままになります。

## 仕様

- 取得期間は `START_DATE`〜`END_DATE`（JST基準）
- EDINET提出書類一覧API（type=2）から対象書類を取得
- `INCLUDE_ALL_DOCS=False` の場合は以下のキーワードで絞り込み
  - 有価証券報告書 / 四半期報告書 / 半期報告書
- `master_company` テーブルを自動更新
  - EDINETコード（企業コード）を主キーとして登録
  - 株価コードが空の場合は `EDINET_CODE_CSV`（無ければ `EDINET_CODE_URL` から自動取得）で補完
  - 日経平均採用銘柄は `group_name` に `Nikkei225` を登録
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
- `master_company` は `edinet_code` / `securities_code` / `company_name` / `market` / `group_name` / `updated_at` を保持します。


##　未対応20260203
- masterCompanyに証券コードが登録されてない
- HistoryRunテーブルを作成する。
- SecCodeがNullの情報は正直不要。どうするか

