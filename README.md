# End-to-End ETL Pipeline — E-Commerce Orders

Turning messy e-commerce transaction data into analytics-ready tables — built with **Python**, **Dagster**, and **DuckDB**.

**Author:** Alifia Chika Intan (Nevie) · Telkom University
📧 alifiaintan144@gmail.com · [LinkedIn](https://linkedin.com/in/alifia-chika-intan-880b94202) · [GitHub](https://github.com/Euijoo097)

---

## 1. Overview

This pipeline processes daily transaction data from two raw CSV files (`raw_orders` and `raw_products`) that contain data quality issues common in real-world e-commerce systems:

| Issue | Description |
|---|---|
| **Duplicate orders** | Some `order_id`s appear more than once due to re-exports / double submissions, inflating revenue figures |
| **Inconsistent date formats** | Four different formats in a single column: `DD/MM/YYYY`, `YYYY-MM-DD`, timestamp, and `"Mon DD, YYYY"` |
| **Inconsistent text casing** | `SURABAYA` / `surabaya` / `Surabaya` are treated as different entities in `GROUP BY` if not standardized |
| **Missing / negative values** | `total_harga` is empty or negative in some rows — needs to be recalculated from `quantity × harga_satuan` |

## 2. Architecture

Three Dagster asset groups — **raw → cleaned → marts** — with automatic lineage tracking. Each node represents one table/DataFrame that can be independently rematerialized.

```
raw_orders ─┐
            ├─► cleaned_orders ─┬─► sales_by_category_city
raw_products┘                   └─► sales_by_channel_status
```

## 3. Pipeline Stages

### Extract
- **Source:** `raw_orders.csv` (~130 rows, 11 columns) & `raw_products.csv` (~10 rows, 4 columns)
- **Format:** CSV, read directly with `pandas.read_csv()`
- **Volume:** small enough to load fully into memory without chunking

### Transform — 4 cleaning steps
1. **Remove duplicates** — `drop_duplicates()` on `order_id`, preventing inflated revenue/quantity from re-exports
2. **Normalize dates** — sequential parsing across 4 known formats, falling back to pandas inference
3. **Normalize text** — cities title-cased, channel/status/category lowercased for consistent `GROUP BY`
4. **Fix `total_harga`** — missing/negative values recalculated from `quantity × harga_satuan` via a join to `cleaned_products`
   *(Note: negative values may represent refunds rather than errors — flagged for confirmation with the data owner.)*

### Load — DuckDB as OLAP store
Clean data is loaded into a local DuckDB file (`sales.duckdb`) — a lightweight OLAP store for fast analytical queries without a separate database server.

| Table | Description |
|---|---|
| `cleaned_orders` | Deduplicated orders with normalized dates/text and corrected `total_harga` |
| `sales_by_category_city` | Revenue & volume by product category × city, `completed` orders only |
| `sales_by_channel_status` | Order count & revenue by channel × status, for funnel monitoring |

## 4. Orchestration

- **Tool:** Dagster (not Airflow) — asset-based model fits a DataFrame/table-centric pipeline, with automatic lineage graphs and native DuckDB support
- **Language:** Python (pandas) for transforms; SQL via DuckDB connection for mart-layer aggregation
- **Schedule:** currently manual via "Materialize all"; production plan is a daily `@schedule` (e.g. 02:00) or a sensor on new files

### Airflow → Dagster concept migration

This pipeline was originally designed as an Airflow DAG, then converted to a Dagster job:

| Airflow | Dagster |
|---|---|
| task (`PythonOperator`) | op |
| DAG | job (ops + dependency graph) |
| `schedule="0 6 * * *"` | `@schedule` with the same cron |
| `retries` in `default_args` | `RetryPolicy` per-op |
| `email_on_failure` | `run_status_sensor` / hook (not yet implemented) |
| `EmptyOperator` start/end | not needed — order is implied by data dependencies |

- **Schedule:** cron `0 6 * * *` (06:00 WIB) · timezone `Asia/Jakarta` · status: stopped (start manually from the UI)
- **Retry policy:** `max_retries=3`, 5-minute delay per op — equivalent to Airflow's `default_args`
- **Not yet implemented:** an `email_on_failure` equivalent — needs a `run_status_sensor` or hook to send Slack/email on job failure

## 5. Reliability

**What happens when the pipeline fails?**

- **Date fails to parse** → logged as `WARNING`, value set to `NaT` — does not crash the run; `NaT` rows are reviewed manually before time-based analysis
- **`product_id` not found in catalog** → price lookup returns `NaN` (a soft failure, not a hard error); a Dagster Asset Check is recommended so this doesn't silently reach the marts layer
- **DuckDB resource fails (file locked/corrupt)** → tables use `CREATE OR REPLACE TABLE`, so automatic retries are safe (idempotent) with no risk of duplicate data

**Monitoring:** the Dagster UI shows Success/Failed status per asset and job, with execution duration and event logs. Data quality is currently checked via info-level logs (duplicates removed, `total_harga` corrections, failed date parses). Planned improvements: formal Dagster Asset Checks (e.g. "no negative `total_harga`", "`order_count` in marts must match row count in `cleaned_orders`") and a Dagster sensor for automatic Slack/email notifications on failed runs.

## 6. Results

| Metric | Value |
|---|---|
| Orders processed | 130 → 110 (20 rejected: 10 duplicates + 10 problematic values) |
| Total revenue (completed orders) | Rp 562,530,000 |
| Pipeline execution time (Dagster UI) | 0.37s |
| Pipeline execution time (CLI/notebook) | 0.23s |

**Summary by category** (`summary_report.csv`):

| Category | Orders | Total Revenue | Avg Revenue |
|---|---|---|---|
| Elektronik | 81 | Rp 435,180,000 | Rp 5,372,593 |
| Furniture | 29 | Rp 127,350,000 | Rp 4,391,379 |

The same pipeline can also be triggered directly via `orchestrator.py` from a Jupyter Notebook or terminal, independent of the Dagster UI — useful for automated scheduling (cron / CI/CD). Each stage (extract, transform, validate, load, notify) retries up to 3 times, and results (110 orders, Rp 562,530,000 revenue, 4 validation checks passed) match the Dagster UI run exactly, slightly faster due to no UI overhead.

## 7. Tech Stack

| Tool | Role |
|---|---|
| **Python** | Transformation & scripting |
| **Pandas** | Data cleaning & manipulation |
| **Dagster** | Asset-based orchestration |
| **DuckDB** | Analytical OLAP store |

## 8. Repository Structure

```
sales-pipeline-dagster/
├── data/
│   ├── raw/                  # raw_orders.csv, raw_products.csv
│   └── processed/            # orders_clean.csv, summary_report.csv
├── src/
│   ├── etl_pipeline.py       # core Extract–Transform–Load logic
│   ├── orchestrator.py       # CLI/notebook entrypoint with retry + notify
│   ├── assets/               # Dagster asset definitions (raw/cleaned/marts)
│   ├── resources/            # DuckDB resource config
│   ├── checks/               # Dagster Asset Checks
│   └── utils/                # logging & notification helpers
├── dagster_project/          # Definitions, schedules, sensors
├── notebooks/                # Jupyter trigger notebook
├── docs/                     # etl_design.md, migration notes
├── tests/
└── README.md
```

## 9. Deliverables

- ✅ `etl_pipeline.py` — full Extract–Load script
- ✅ `etl_design.md` — pipeline design document
- ✅ `orders_clean.csv` & `summary_report.csv` — output data
- ✅ Screenshots of successful runs (Dagster UI + CLI/notebook) — see [Section 10](#10-screenshots)

## 10. Screenshots

### 1. Assets Grouped by Layer (Dagster UI)
![Dagster assets list](docs/screenshots/dagster_assets_list.png)

### 2. Global Asset Lineage Graph (Dagster UI)
![Dagster global asset lineage](docs/screenshots/dagster_asset_lineage.png)

### 3. Successful Run Detail (Dagster UI)
![Dagster run success detail](docs/screenshots/dagster_run_success.png)

### 4. Runs List (Dagster UI)
![Dagster runs list](docs/screenshots/dagster_runs_list.png)

### 5. Code Location Overview (Dagster UI)
![Dagster code location overview](docs/screenshots/dagster_code_location.png)

### 6. Full Pipeline Log (`orchestrator.py`)
![Orchestrator full run output](docs/screenshots/orchestrator_full_run.png)

### 7. Pipeline Start / Stage-by-Stage Log (Notebook)
![Notebook pipeline start log](docs/screenshots/notebook_pipeline_start.png)

### 8. Final Notification Summary (CLI)
![CLI notification summary](docs/screenshots/cli_notification_summary.png)

### 9. Spyder Run (`etl_pipeline_spyder.py`)
![Spyder full log](docs/screenshots/spyder_full_log.png)

## Getting Started

```bash
# clone the repo
git clone https://github.com/Euijoo097/sales-pipeline-dagster.git
cd sales-pipeline-dagster

# install dependencies
pip install -r requirements.txt

# run via Dagster UI
dagster dev

# or run directly via CLI / notebook
python src/orchestrator.py
```

---

*This is a portfolio project for data engineering practice. Feel free to reach out for discussion or feedback.*
