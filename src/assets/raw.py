import os
import pandas as pd
from dagster import asset

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


@asset(group_name="raw", compute_kind="pandas")
def raw_orders() -> pd.DataFrame:
    """Raw orders data, loaded as-is from CSV (dirty: dupes, mixed date formats, missing/negative prices)."""
    return pd.read_csv(os.path.join(DATA_DIR, "raw_orders.csv"))


@asset(group_name="raw", compute_kind="pandas")
def raw_products() -> pd.DataFrame:
    """Raw product catalog with unit prices."""
    return pd.read_csv(os.path.join(DATA_DIR, "raw_products.csv"))


raw_assets = [raw_orders, raw_products]
