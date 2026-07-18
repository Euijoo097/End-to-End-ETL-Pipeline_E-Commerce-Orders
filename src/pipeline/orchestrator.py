"""
orchestrator.py
================
Mini ETL Orchestrator (Spyder-friendly version)
------------------------------------------------
Simulasi cara kerja Apache Airflow - bisa langsung dijalankan, tanpa install
Airflow. Mendemonstrasikan konsep inti: task dependency, sequential
execution, retry (dengan exponential backoff), logging, dan validation gate.

DAG (urutan task):
    extract >> transform >> validate >> load >> report >> notify

Kalau task 'validate' gagal, pipeline BERHENTI - data kotor tidak pernah
sampai ke 'load'. Ini yang disebut validation gate.

How to run in Spyder:
    1. Simpan file ini satu folder dengan raw_orders.csv.
    2. F5 untuk jalankan semuanya sekaligus, ATAU Ctrl+Enter per cell untuk
       melihat tiap task jalan satu per satu di Variable Explorer.
    3. Setelah selesai, cek pipeline_log.txt untuk timeline lengkap semua
       attempt/retry, dan DF_RESULT / SUMMARY_RESULT di Variable Explorer.

Can also be run from a terminal:
    python orchestrator.py

Author  : (portfolio project)
"""

# %% Imports & config
import pandas as pd
import numpy as np
import time
import os
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
# Spyder note: same as etl_pipeline_spyder.py - __file__ isn't always
# defined when a script is run interactively/cell-by-cell, so fall back to
# the current working directory in that case. Using BASE_DIR (rather than
# bare filenames like the original 'raw_orders.csv') also means this still
# works correctly if Spyder's working directory isn't the script's folder.
try:
    BASE_DIR = Path(__file__).parent
except NameError:
    BASE_DIR = Path.cwd()

INPUT_FILE = BASE_DIR / 'raw_orders.csv'
OUTPUT_FILE = BASE_DIR / 'orders_clean.csv'
REPORT_FILE = BASE_DIR / 'summary_report.csv'
LOG_FILE = BASE_DIR / 'pipeline_log.txt'
MAX_RETRIES = 3


# %% Logger
# ---------------------------------------------------------------------------
# LOGGER
# ---------------------------------------------------------------------------
def log(task_name, status, message=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] [{task_name}] [{status}] {message}"
    print(entry)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(entry + '\n')


# %% Task runner
# ---------------------------------------------------------------------------
# TASK RUNNER (simulates Airflow's task execution)
# ---------------------------------------------------------------------------
def run_task(task_name, task_func, retries=MAX_RETRIES):
    """
    Jalankan sebuah task dengan retry logic.
    Sama seperti Airflow: jika gagal, coba ulang sampai max retries.
    """
    for attempt in range(1, retries + 1):
        try:
            log(task_name, "RUNNING", f"Attempt {attempt}/{retries}")
            start = time.time()
            result = task_func()
            duration = round(time.time() - start, 2)
            log(task_name, "SUCCESS", f"Selesai dalam {duration}s")
            return result
        except Exception as e:
            log(task_name, "FAILED", f"Error: {e}")
            if attempt < retries:
                wait = attempt * 2  # exponential backoff
                log(task_name, "RETRY", f"Menunggu {wait}s sebelum retry...")
                time.sleep(wait)
            else:
                log(task_name, "FATAL", f"Gagal setelah {retries} percobaan!")
                raise


# %% Task definitions
# ============================================
# TASK DEFINITIONS (setiap fungsi = 1 task)
# ============================================
def task_extract():
    """Task 1: Baca data mentah dari sumber"""
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"{INPUT_FILE} tidak ditemukan!")
    df = pd.read_csv(INPUT_FILE)
    log("extract", "INFO", f"Loaded {len(df)} baris dari {INPUT_FILE.name}")
    return df


def task_transform(df):
    """Task 2: Bersihkan dan transformasi data"""
    before = len(df)
    # Hapus duplikat
    df = df.drop_duplicates()
    log("transform", "INFO", f"Hapus {before - len(df)} duplikat")
    # Hapus harga negatif
    neg_count = (df['total_harga'] < 0).sum()
    df = df[df['total_harga'] >= 0]
    log("transform", "INFO", f"Hapus {neg_count} harga negatif")
    # Isi missing values
    df['customer_email'] = df['customer_email'].fillna('unknown@placeholder.com')
    df['total_harga'] = df['total_harga'].fillna(df['total_harga'].median())
    # Standarkan format
    df['tanggal_order'] = pd.to_datetime(df['tanggal_order'], format='mixed')
    df['kota'] = df['kota'].str.strip().str.title()
    df['channel'] = df['channel'].str.strip().str.lower().str.replace(' ', '_')
    # Kolom baru
    df['bulan'] = df['tanggal_order'].dt.month_name()
    df['kategori_harga'] = np.where(
        df['total_harga'] < 500000, 'kecil',
        np.where(df['total_harga'] <= 2000000, 'sedang', 'besar')
    )
    log("transform", "INFO", f"Output: {len(df)} baris bersih")
    return df


def task_validate(df):
    """Task 3: Validasi kualitas data - GATE sebelum load"""
    checks = {
        'zero_duplicates': df.duplicated().sum() == 0,
        'zero_nulls': df.isnull().sum().sum() == 0,
        'zero_negative_price': (df['total_harga'] < 0).sum() == 0,
        # startswith('datetime') passes for datetime64[ns] AND
        # datetime64[us] - the mixed-format parser can return either
        # depending on your pandas version, and both are equally valid.
        'datetime_type': str(df['tanggal_order'].dtype).startswith('datetime'),
    }
    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ValueError(f"VALIDASI GAGAL: {failed}")
    log("validate", "INFO", f"Semua {len(checks)} check PASSED ✅")
    return df


def task_load(df):
    """Task 4: Simpan ke 'warehouse' (simulasi: CSV file)"""
    df.to_csv(OUTPUT_FILE, index=False)
    log("load", "INFO", f"Data disimpan ke {OUTPUT_FILE.name} ({len(df)} baris)")
    return df


def task_report(df):
    """
    Task 5: Generate summary report

    Note: this groups ALL rows regardless of status, so total_revenue
    includes 'pending' and 'cancelled' orders that may never actually be
    fulfilled. If you want a revenue figure that only counts money that's
    actually landed, filter to df[df['status'] == 'completed'] before the
    groupby - worth a deliberate choice if this number goes in a report.
    """
    summary = df.groupby('kategori').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('total_harga', 'sum'),
        avg_revenue=('total_harga', 'mean')
    ).round(0)
    summary.to_csv(REPORT_FILE)
    log("report", "INFO", f"Report disimpan ke {REPORT_FILE.name}")
    return summary


def task_notify(summary):
    """Task 6: Kirim notifikasi (simulasi: print)"""
    total_orders = summary['total_orders'].sum()
    total_revenue = summary['total_revenue'].sum()
    message = (
        f"\n{'='*50}\n"
        f"📧 PIPELINE NOTIFICATION\n"
        f"{'='*50}\n"
        f"Status: ✅ SUCCESS\n"
        f"Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Total orders diproses: {int(total_orders)}\n"
        f"Total revenue: Rp {int(total_revenue):,}\n"
        f"Output: {OUTPUT_FILE.name}, {REPORT_FILE.name}\n"
        f"{'='*50}"
    )
    print(message)
    log("notify", "INFO", "Notifikasi terkirim")


# %% DAG definition / run_pipeline
# ============================================
# DAG DEFINITION - urutan eksekusi task
# ============================================
def run_pipeline():
    """
    DAG: etl_ecommerce_daily
    Schedule: manual (di Airflow: '0 6 * * *' = tiap hari jam 6)

    Task Flow:
      extract >> transform >> validate >> load >> report >> notify

    Jika validate gagal, pipeline BERHENTI - data kotor tidak di-load.

    Spyder note: unlike a raised, uncaught exception, a failure here is
    caught, logged, and printed - it does NOT raise SystemExit or otherwise
    kill your console, so you can inspect variables and re-run right away
    after fixing whatever caused the failure.

    Returns the final (df, summary) tuple on success, or None on failure -
    same pattern as run_pipeline() in etl_pipeline_spyder.py.
    """
    print("\n" + "="*50)
    print("🚀 STARTING ETL PIPELINE")
    print("="*50 + "\n")

    # Clear log
    open(LOG_FILE, 'w', encoding='utf-8').close()

    start_time = time.time()

    try:
        # Task 1: Extract
        df = run_task("extract", task_extract)

        # Task 2: Transform (depends on extract)
        df = run_task("transform", lambda: task_transform(df))

        # Task 3: Validate (depends on transform)
        # INI ADALAH GATE - jika gagal, load TIDAK dijalankan
        df = run_task("validate", lambda: task_validate(df))

        # Task 4: Load (depends on validate)
        df = run_task("load", lambda: task_load(df))

        # Task 5: Report (depends on load)
        summary = run_task("report", lambda: task_report(df))

        # Task 6: Notify (depends on report)
        run_task("notify", lambda: task_notify(summary))

        total_time = round(time.time() - start_time, 2)
        log("pipeline", "COMPLETED", f"Total waktu: {total_time}s")

        return df, summary

    except Exception as e:
        log("pipeline", "FAILED", f"Pipeline gagal: {e}")
        print(f"\n❌ PIPELINE GAGAL: {e}")
        print(f"Lihat {LOG_FILE.name} untuk detail.")
        return None


# %% Run (F5 runs this cell; results land in the Variable Explorer as
# DF_RESULT / SUMMARY_RESULT)
if __name__ == '__main__':
    _result = run_pipeline()
    if _result is not None:
        DF_RESULT, SUMMARY_RESULT = _result
