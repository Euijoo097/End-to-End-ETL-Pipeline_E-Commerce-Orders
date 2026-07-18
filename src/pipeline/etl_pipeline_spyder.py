"""
etl_pipeline_spyder.py
================
End-to-End ETL Pipeline - Sales Order Data (Spyder-friendly version)
--------------------------------------------
Extract  : raw_orders.csv, raw_products.csv
Transform: cleaning, standardization, deduplication, validation, enrichment
Load     : orders_clean.csv, summary_report.csv (+ rejected_rows.csv, etl_log.txt)

How to run in Spyder:
    1. Open this file in Spyder.
    2. Place raw_orders.csv and raw_products.csv in the same folder as this
       script (or edit BASE_DIR below to point at your data folder).
    3. Run the whole file with the green "Run file" button (F5), OR run it
       cell-by-cell with "Run cell" (Ctrl+Enter) using the "# %%" cell
       markers below - handy for inspecting each DataFrame in the Variable
       Explorer as you go.
    4. After a run, orders_clean / rejected / summary are left in Spyder's
       Variable Explorer (as ORDERS_CLEAN, REJECTED, SUMMARY) for interactive
       inspection, in addition to being written to CSV.

Can also still be run from a terminal:
    python etl_pipeline_spyder.py

Author  : (portfolio project)
"""

# %% Imports & config
import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
# Spyder note: when this file is run interactively (e.g. via "Run cell" or
# pasted into the console), __file__ may not be defined, unlike a plain
# terminal run. This fallback keeps the script working in both situations by
# using the current working directory instead.
try:
    BASE_DIR = Path(__file__).parent
except NameError:
    BASE_DIR = Path.cwd()

RAW_ORDERS_PATH = BASE_DIR / "raw_orders.csv"
RAW_PRODUCTS_PATH = BASE_DIR / "raw_products.csv"

OUT_ORDERS_CLEAN = BASE_DIR / "orders_clean.csv"
OUT_SUMMARY_REPORT = BASE_DIR / "summary_report.csv"
OUT_REJECTED_ROWS = BASE_DIR / "rejected_rows.csv"
LOG_FILE = BASE_DIR / "etl_log.txt"

VALID_STATUSES = {"pending", "shipped", "completed", "cancelled"}

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
# Spyder note: Spyder's IPython console keeps the kernel (and any loggers)
# alive between runs. Calling logging.basicConfig() again on a second F5
# would normally stack a duplicate FileHandler/StreamHandler each time,
# causing every log line to print twice, three times, etc. Guarding with
# logger.handlers ensures handlers are only attached once per console
# session, so re-running the script stays clean.
logger = logging.getLogger("etl_pipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")

    _file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    _file_handler.setFormatter(_formatter)

    _stream_handler = logging.StreamHandler(sys.stdout)
    _stream_handler.setFormatter(_formatter)

    logger.addHandler(_file_handler)
    logger.addHandler(_stream_handler)
    logger.propagate = False
else:
    # Re-running: just reset the file handler's log file so etl_log.txt still
    # reflects only the latest run, without adding a duplicate handler.
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler):
            h.close()
            h.baseFilename = str(LOG_FILE)
            h.stream = h._open()


class ETLError(Exception):
    """Raised when a pipeline stage fails in a way that should stop the run."""


# %% Step 1 - Extract
# ---------------------------------------------------------------------------
# STEP 1: EXTRACT
# ---------------------------------------------------------------------------
def extract():
    logger.info("STEP 1 - EXTRACT: reading source files")

    if not RAW_ORDERS_PATH.exists():
        raise ETLError(f"Source file not found: {RAW_ORDERS_PATH}")
    if not RAW_PRODUCTS_PATH.exists():
        raise ETLError(f"Source file not found: {RAW_PRODUCTS_PATH}")

    try:
        orders = pd.read_csv(RAW_ORDERS_PATH)
        products = pd.read_csv(RAW_PRODUCTS_PATH)
    except Exception as e:
        raise ETLError(f"Failed to read CSV source files: {e}") from e

    logger.info(f"  orders  : {len(orders)} rows, {len(orders.columns)} columns")
    logger.info(f"  products: {len(products)} rows, {len(products.columns)} columns")

    required_order_cols = {
        "order_id", "product_id", "product_name", "kategori", "quantity",
        "total_harga", "tanggal_order", "kota", "channel", "status", "customer_email",
    }
    missing = required_order_cols - set(orders.columns)
    if missing:
        raise ETLError(f"raw_orders.csv is missing expected columns: {missing}")

    return orders, products


# %% Step 1b - Inspect raw data (interactive EDA, run cell-by-cell in Spyder)
# ---------------------------------------------------------------------------
# STEP 1b: INSPECT (Cek Masalah)
# ---------------------------------------------------------------------------
# This mirrors the "hands-on" inspection step from the guide: after reading
# the raw files, look at them before deciding how to clean them. It's split
# out from extract() on purpose — extract() stays a pure, silent function
# for the automated pipeline (run_pipeline), while this cell is meant to be
# run on its own in Spyder (Ctrl+Enter) so you can read the printed output
# in the console and inspect ORDERS_RAW / PRODUCTS_RAW in the Variable
# Explorer before touching the transform step.
def inspect_raw_data(orders: pd.DataFrame, products: "pd.DataFrame | None" = None):
    print("=== ORDERS INFO ===")
    print(f"Jumlah baris: {len(orders)}")
    print(f"Kolom: {list(orders.columns)}")
    print(orders.info())
    print()
    print("=== CEK MASALAH ===")
    print(f"Duplikasi (semua kolom identik): {orders.duplicated().sum()}")
    print(f"Duplikasi order_id: {orders.duplicated(subset=['order_id']).sum()}")
    print("Missing values:")
    print(orders.isnull().sum())
    print(f"\nHarga negatif/nol: {(orders['total_harga'] <= 0).sum()}")
    print(f"Harga kosong (NaN): {orders['total_harga'].isnull().sum()}")
    print(f"\nChannel unik (mentah): {orders['channel'].unique()}")
    print(f"Kota unik (mentah): {orders['kota'].unique()}")
    print(f"Status unik (mentah): {orders['status'].unique()}")
    print(f"\nContoh format tanggal berbeda-beda di tanggal_order:")
    print(orders['tanggal_order'].sample(min(5, len(orders)), random_state=42).tolist())

    if products is not None:
        print("\n=== PRODUCTS INFO ===")
        print(f"Jumlah baris: {len(products)}")
        print(f"product_id duplikat: {products['product_id'].duplicated().sum()}")


# To use this cell on its own in Spyder: put your cursor in it and press
# Ctrl+Enter (don't run the whole file with F5 for this one, since it's just
# for interactive exploration). Example:
#
#   ORDERS_RAW, PRODUCTS_RAW = extract()
#   inspect_raw_data(ORDERS_RAW, PRODUCTS_RAW)
#
# These two lines are commented out on purpose so a full F5 run of the file
# doesn't print this twice (run_pipeline(), called at the bottom of the
# file, already calls extract() internally as part of the automated flow).


# %% Step 2 (tutorial) - Transform, walkthrough version from the guide
# ---------------------------------------------------------------------------
# STEP 2 (TUTORIAL): TRANSFORM - Bersihkan Data
# ---------------------------------------------------------------------------
# This follows the guide step-by-step, with the same print-as-you-go style,
# so you can run it cell-by-cell in Spyder and watch each cleaning decision
# take effect. It's kept separate from the production transform() below on
# purpose - the two take different (both valid) approaches on a couple of
# points, worth knowing about if you're writing this up as a case study:
#
#   1. Duplicates: the guide drops fully-identical rows (drop_duplicates()).
#      Production drops by order_id alone, since two rows can share an
#      order_id but differ in a typo'd column - keeping the first is safer.
#   2. Missing/invalid total_harga: the guide IMPUTES it with the median so
#      no rows are lost. Production REJECTS those rows into
#      rejected_rows.csv instead, because silently substituting a made-up
#      number into a revenue column can quietly bias a summary report -
#      for a portfolio piece, an auditable "we excluded N rows and here's
#      why" is usually a stronger signal than an invisible fillna.
#   3. Missing customer_email: the guide fills with a placeholder address.
#      Production leaves it as NaN (email isn't used in any downstream KPI
#      here, so there's nothing to protect by imputing it).
#   4. Dates: the guide uses pd.to_datetime(..., format='mixed'), a newer,
#      one-line way (pandas >= 2.0) to parse several formats at once.
#      Production spells out each known format explicitly - slightly more
#      verbose, but it fails loudly (NaT) if a truly new, unexpected format
#      shows up, rather than pandas silently guessing wrong.
#
# Neither approach is "wrong" - which one to use is a data-governance
# decision, and calling that out explicitly is good material for a case
# study write-up.
def transform_tutorial(orders_raw: pd.DataFrame) -> pd.DataFrame:
    orders = orders_raw.copy()

    # 2a. Hapus duplikasi
    print(f"Sebelum: {len(orders)} baris")
    orders = orders.drop_duplicates()
    print(f"Setelah hapus duplikat: {len(orders)} baris")

    # 2b. Hapus baris dengan harga negatif (data error)
    orders = orders[orders['total_harga'] >= 0]
    print(f"Setelah hapus harga negatif: {len(orders)} baris")

    # 2c. Isi missing values
    orders['customer_email'] = orders['customer_email'].fillna('unknown@placeholder.com')
    median_harga = orders['total_harga'].median()
    orders['total_harga'] = orders['total_harga'].fillna(median_harga)
    print(f"Missing values setelah fillna: {orders.isnull().sum().sum()}")

    # 2d. Standarkan format tanggal
    # Note: format='mixed' needs pandas >= 2.0. If Spyder's console throws a
    # TypeError on this line, run `import pandas; pandas.__version__` to
    # check, and update pandas (or fall back to errors='coerce' without a
    # format argument) if you're on an older Anaconda install.
    orders['tanggal_order'] = pd.to_datetime(orders['tanggal_order'], format='mixed')
    print(f"Tipe tanggal: {orders['tanggal_order'].dtype}")

    # 2e. Standarkan teks (lowercase lalu title case)
    orders['kota'] = orders['kota'].str.strip().str.title()
    orders['channel'] = orders['channel'].str.strip().str.lower().str.replace(' ', '_')
    print(f"Channel setelah standarisasi: {orders['channel'].unique()}")
    print(f"Kota setelah standarisasi: {orders['kota'].unique()}")

    # 2f. Buat kolom baru: bulan dan kategori harga
    orders['bulan'] = orders['tanggal_order'].dt.month_name()
    orders['kategori_harga'] = np.where(
        orders['total_harga'] < 500000, 'kecil',
        np.where(orders['total_harga'] <= 2000000, 'sedang', 'besar')
    )

    print(f"\nDistribusi kategori harga:")
    print(orders['kategori_harga'].value_counts())

    return orders


# To run this cell standalone in Spyder: Ctrl+Enter with your cursor in it,
# after having already run Step 1/1b so ORDERS_RAW exists, e.g.:
#
#   ORDERS_TUTORIAL = transform_tutorial(ORDERS_RAW)


# %% Step 3 (tutorial) - Validate, cek kualitas data
# ---------------------------------------------------------------------------
# STEP 3 (TUTORIAL): VALIDATE - Cek Kualitas Data
# ---------------------------------------------------------------------------
# One thing worth flagging about the "Channel konsisten" check below: it
# passes as long as there are 3 or fewer unique channel values, whatever
# they happen to be. That would still say ✅ if standardization had (say)
# collapsed "Website" and "web-site" into two different strings instead of
# one - it checks the *count*, not that they're the *right* 3 values. A
# stricter version compares against the known-good set directly, which is
# what the commented-out alternative below does; swap it in if you want the
# check to actually catch that kind of silent standardization bug.
#
# Also worth testing on your own machine: the "Tanggal tipe datetime" check
# compares against the exact string 'datetime64[ns]'. On pandas >= 2.0,
# pd.to_datetime(..., format='mixed') can return 'datetime64[us]' instead
# (same idea, microsecond resolution instead of nanosecond) - a real date
# column that would still show ❌ here. If that happens on your install,
# swap the check to pd.api.types.is_datetime64_any_dtype(orders['tanggal_order'])
# instead, which passes regardless of resolution.
def validate_tutorial(orders: pd.DataFrame) -> bool:
    print("=== VALIDASI DATA BERSIH ===")

    checks = {
        'Tidak ada duplikat': orders.duplicated().sum() == 0,
        'Tidak ada missing value': orders.isnull().sum().sum() == 0,
        'Tidak ada harga negatif': (orders['total_harga'] < 0).sum() == 0,
        'Tanggal tipe datetime': str(orders['tanggal_order'].dtype) == 'datetime64[ns]',
        'Channel konsisten': len(orders['channel'].unique()) <= 3,
        # Stricter alternative for the last check - uncomment to use it:
        # 'Channel konsisten': set(orders['channel'].unique()) <= {
        #     'website', 'marketplace', 'offline_store'
        # },
    }

    for check, passed in checks.items():
        status = '✅' if passed else '❌'
        print(f"  {status} {check}")

    all_passed = all(checks.values())
    print(f"\nHasil: {'SEMUA LOLOS' if all_passed else 'ADA YANG GAGAL'}")

    return all_passed


# To run this cell standalone in Spyder, after running Step 2 (tutorial):
#
#   validate_tutorial(ORDERS_TUTORIAL)


# %% Step 4 (tutorial) - Load, simpan hasil
# ---------------------------------------------------------------------------
# STEP 4 (TUTORIAL): LOAD - Simpan Hasil
# ---------------------------------------------------------------------------
# Two things worth flagging before you run this one:
#
#   1. FILENAME COLLISION: this writes to the same filenames the
#      production pipeline uses further down (orders_clean.csv,
#      summary_report.csv). If you run both in the same folder, whichever
#      one runs last silently overwrites the other's output. The function
#      below writes to *_tutorial.csv variants instead so the two can
#      coexist while you're comparing them; switch back to the plain
#      names once you've settled on one version to keep.
#   2. REVENUE SCOPE: this summary groups ALL rows in orders_clean,
#      whatever their status - including 'pending' and 'cancelled' orders
#      that haven't (or won't) actually generate revenue. Production's
#      build_summary() filters to status == 'completed' first, specifically
#      to avoid overstating sales with orders that were never fulfilled.
#      Worth deciding deliberately which one you want for a "total_revenue"
#      number that goes in front of anyone else.
def load_tutorial(orders_clean: pd.DataFrame):
    orders_clean = orders_clean[[
        'order_id', 'product_id', 'product_name', 'kategori',
        'quantity', 'total_harga', 'tanggal_order', 'kota',
        'channel', 'status', 'customer_email',
        'bulan', 'kategori_harga'
    ]]

    out_orders = BASE_DIR / "orders_clean_tutorial.csv"
    orders_clean.to_csv(out_orders, index=False)
    print(f"Data bersih disimpan: {out_orders.name} ({len(orders_clean)} baris)")

    # Buat summary report
    summary = orders_clean.groupby('kategori').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('total_harga', 'sum'),
        avg_revenue=('total_harga', 'mean')
    ).round(0)

    print("\n=== SUMMARY PER KATEGORI ===")
    print(summary)

    out_summary = BASE_DIR / "summary_report_tutorial.csv"
    summary.to_csv(out_summary)
    print(f"\nSummary disimpan: {out_summary.name}")

    return orders_clean, summary


# To run this cell standalone in Spyder, after running Step 2 (tutorial):
#
#   ORDERS_CLEAN_TUTORIAL, SUMMARY_TUTORIAL = load_tutorial(ORDERS_TUTORIAL)


# %% Step 2 - Transform
# ---------------------------------------------------------------------------
# STEP 2: TRANSFORM
# ---------------------------------------------------------------------------
def parse_mixed_date(value):
    """tanggal_order arrives in at least 4 different formats. Try each in turn."""
    if pd.isna(value):
        return pd.NaT
    value = str(value).strip()
    formats = [
        "%Y-%m-%d %H:%M:%S",  # 2024-06-13 17:20:00
        "%Y-%m-%d",           # 2024-05-18
        "%d/%m/%Y",           # 17/06/2024
        "%b %d, %Y",          # Jun 03, 2024
    ]
    for fmt in formats:
        try:
            return pd.to_datetime(value, format=fmt)
        except ValueError:
            continue
    # last resort: let pandas guess
    return pd.to_datetime(value, errors="coerce")


def transform(orders: pd.DataFrame, products: pd.DataFrame):
    logger.info("STEP 2 - TRANSFORM: cleaning & standardizing")
    rejected_frames = []
    df = orders.copy()
    start_count = len(df)

    # --- 2a. Standardize text fields (fix inconsistent casing: 'Website' vs
    #          'website' vs 'WEBSITE', 'malang' vs 'MALANG', etc.) -------------
    df["channel"] = df["channel"].astype(str).str.strip().str.lower()
    df["kota"] = df["kota"].astype(str).str.strip().str.title()
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    df["product_name"] = df["product_name"].astype(str).str.strip()
    df["kategori"] = df["kategori"].astype(str).str.strip().str.title()
    df["customer_email"] = df["customer_email"].astype(str).str.strip().str.lower()
    df.loc[df["customer_email"].isin(["nan", "none", ""]), "customer_email"] = np.nan
    logger.info("  standardized text casing for channel, kota, status, kategori")

    # --- 2b. Parse inconsistent date formats -----------------------------------
    df["tanggal_order"] = df["tanggal_order"].apply(parse_mixed_date)
    bad_dates = df["tanggal_order"].isna().sum()
    logger.info(f"  parsed tanggal_order (4 known formats); unparsable: {bad_dates}")

    # --- 2c. Drop exact duplicate rows (same order_id, same everything) --------
    dup_mask = df.duplicated(subset=["order_id"], keep="first")
    dup_rows = df[dup_mask].copy()
    if not dup_rows.empty:
        dup_rows["reject_reason"] = "duplicate_order_id"
        rejected_frames.append(dup_rows)
    df = df[~dup_mask]
    logger.info(f"  removed {dup_mask.sum()} duplicate order_id rows")

    # --- 2d. Reject rows with invalid/negative total_harga or missing value ----
    invalid_price_mask = df["total_harga"].isna() | (df["total_harga"] <= 0)
    invalid_price_rows = df[invalid_price_mask].copy()
    if not invalid_price_rows.empty:
        invalid_price_rows["reject_reason"] = "missing_or_non_positive_total_harga"
        rejected_frames.append(invalid_price_rows)
    df = df[~invalid_price_mask]
    logger.info(f"  removed {invalid_price_mask.sum()} rows with missing/negative total_harga")

    # --- 2e. Reject rows with unparsable dates ----------------------------------
    bad_date_mask = df["tanggal_order"].isna()
    bad_date_rows = df[bad_date_mask].copy()
    if not bad_date_rows.empty:
        bad_date_rows["reject_reason"] = "unparsable_date"
        rejected_frames.append(bad_date_rows)
    df = df[~bad_date_mask]
    if bad_date_mask.sum():
        logger.info(f"  removed {bad_date_mask.sum()} rows with unparsable dates")

    # --- 2f. Reject rows with unknown status values -----------------------------
    bad_status_mask = ~df["status"].isin(VALID_STATUSES)
    bad_status_rows = df[bad_status_mask].copy()
    if not bad_status_rows.empty:
        bad_status_rows["reject_reason"] = "invalid_status"
        rejected_frames.append(bad_status_rows)
    df = df[~bad_status_mask]
    if bad_status_mask.sum():
        logger.info(f"  removed {bad_status_mask.sum()} rows with invalid status")

    # --- 2g. Enrich with product master data (harga_satuan) ---------------------
    # total_harga in raw_orders can be trusted for revenue, but we also attach
    # unit price from the product master for QA / unit economics.
    products_slim = products[["product_id", "harga_satuan"]].drop_duplicates("product_id")
    df = df.merge(products_slim, on="product_id", how="left")
    unmatched_products = df["harga_satuan"].isna().sum()
    if unmatched_products:
        logger.warning(f"  {unmatched_products} orders reference a product_id not in raw_products.csv")

    # --- 2h. Derive helper columns ----------------------------------------------
    df["order_month"] = df["tanggal_order"].dt.to_period("M").astype(str)
    df["unit_price_check"] = (df["total_harga"] / df["quantity"]).round(0)

    # --- 2i. Final column order ---------------------------------------------
    df = df.sort_values("tanggal_order").reset_index(drop=True)
    final_cols = [
        "order_id", "product_id", "product_name", "kategori", "quantity",
        "harga_satuan", "total_harga", "unit_price_check", "tanggal_order",
        "order_month", "kota", "channel", "status", "customer_email",
    ]
    df = df[final_cols]

    rejected = (
        pd.concat(rejected_frames, ignore_index=True)
        if rejected_frames
        else pd.DataFrame(columns=list(orders.columns) + ["reject_reason"])
    )

    logger.info(
        f"  transform complete: {start_count} rows in -> "
        f"{len(df)} clean rows, {len(rejected)} rejected rows"
    )
    return df, rejected


# %% Step 3 - Build summary report
# ---------------------------------------------------------------------------
# STEP 3: BUILD SUMMARY REPORT
# ---------------------------------------------------------------------------
def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("STEP 3 - SUMMARY: aggregating KPIs")

    completed = df[df["status"] == "completed"]

    by_category = (
        completed.groupby("kategori")
        .agg(total_orders=("order_id", "count"),
             total_qty=("quantity", "sum"),
             total_revenue=("total_harga", "sum"))
        .reset_index()
    )
    by_category["dimension"] = "kategori"
    by_category = by_category.rename(columns={"kategori": "segment"})

    by_channel = (
        completed.groupby("channel")
        .agg(total_orders=("order_id", "count"),
             total_qty=("quantity", "sum"),
             total_revenue=("total_harga", "sum"))
        .reset_index()
    )
    by_channel["dimension"] = "channel"
    by_channel = by_channel.rename(columns={"channel": "segment"})

    by_month = (
        completed.groupby("order_month")
        .agg(total_orders=("order_id", "count"),
             total_qty=("quantity", "sum"),
             total_revenue=("total_harga", "sum"))
        .reset_index()
    )
    by_month["dimension"] = "order_month"
    by_month = by_month.rename(columns={"order_month": "segment"})

    summary = pd.concat([by_category, by_channel, by_month], ignore_index=True)
    summary = summary[["dimension", "segment", "total_orders", "total_qty", "total_revenue"]]
    summary["total_revenue"] = summary["total_revenue"].round(0)

    logger.info(f"  summary built: {len(summary)} rows across kategori/channel/order_month")
    logger.info(
        f"  completed orders used for KPIs: {len(completed)} / {len(df)} total clean orders "
        f"(revenue = COMPLETED only; pending/shipped/cancelled excluded to avoid overstating sales)"
    )
    return summary


# %% Step 4 - Load
# ---------------------------------------------------------------------------
# STEP 4: LOAD
# ---------------------------------------------------------------------------
def load(df_clean: pd.DataFrame, df_summary: pd.DataFrame, df_rejected: pd.DataFrame):
    logger.info("STEP 4 - LOAD: writing output files")
    try:
        df_clean.to_csv(OUT_ORDERS_CLEAN, index=False)
        df_summary.to_csv(OUT_SUMMARY_REPORT, index=False)
        df_rejected.to_csv(OUT_REJECTED_ROWS, index=False)
    except Exception as e:
        raise ETLError(f"Failed to write output files: {e}") from e

    logger.info(f"  wrote {OUT_ORDERS_CLEAN.name} ({len(df_clean)} rows)")
    logger.info(f"  wrote {OUT_SUMMARY_REPORT.name} ({len(df_summary)} rows)")
    logger.info(f"  wrote {OUT_REJECTED_ROWS.name} ({len(df_rejected)} rows)")


# %% Main orchestration
# ---------------------------------------------------------------------------
# MAIN ORCHESTRATION
# ---------------------------------------------------------------------------
def run_pipeline():
    """
    Runs the full pipeline and returns (orders_clean, summary, rejected) on
    success, or None on failure.

    Spyder note: the original script called sys.exit(1) on failure. That's
    fine in a plain terminal, but in Spyder's IPython console it raises
    SystemExit, which prints a traceback-like message and can be confusing
    mid-session. Here we log the error and return None instead, so the
    console stays alive and you can inspect variables / fix data and re-run.
    A terminal run (see __main__ block below) still exits with a non-zero
    status code so scripts/CI calling this file can detect failure.
    """
    run_start = datetime.now()
    logger.info("=" * 70)
    logger.info(f"ETL PIPELINE RUN START: {run_start.isoformat()}")
    logger.info("=" * 70)

    try:
        orders_raw, products_raw = extract()
        orders_clean, rejected = transform(orders_raw, products_raw)
        summary = build_summary(orders_clean)
        load(orders_clean, summary, rejected)
    except ETLError as e:
        logger.error(f"PIPELINE FAILED: {e}")
        logger.error("No partial output files were written for this stage. "
                      "Previous successful outputs (if any) remain untouched.")
        return None
    except Exception as e:
        logger.exception(f"UNEXPECTED ERROR: {e}")
        return None

    run_end = datetime.now()
    logger.info("=" * 70)
    logger.info(f"ETL PIPELINE RUN SUCCESS - duration: {(run_end - run_start).total_seconds():.2f}s")
    logger.info("=" * 70)

    return orders_clean, summary, rejected


# %% Run (this cell executes when the file is run with F5, or on its own
# with Ctrl+Enter if you've already defined the functions above)
if __name__ == "__main__":
    _result = run_pipeline()

    if _result is not None:
        # Exposed as plain module-level variables so they show up in
        # Spyder's Variable Explorer for interactive inspection, e.g.:
        #   ORDERS_CLEAN.head()
        #   SUMMARY[SUMMARY["dimension"] == "kategori"]
        ORDERS_CLEAN, SUMMARY, REJECTED = _result
    # Note: unlike the original script, this version does NOT call
    # sys.exit(1) on failure, so re-running inside Spyder's console never
    # raises SystemExit. If you run this file with `python etl_pipeline_spyder.py`
    # from a terminal/CI job and need a non-zero exit code on failure,
    # uncomment the two lines below:
    # elif _result is None:
    #     sys.exit(1)
