import pandas as pd
from dagster import asset, AssetExecutionContext


@asset(group_name="cleaned", compute_kind="pandas")
def cleaned_products(raw_products: pd.DataFrame) -> pd.DataFrame:
    """Product catalog with normalized text fields."""
    df = raw_products.copy()
    df["product_name"] = df["product_name"].str.strip()
    df["kategori"] = df["kategori"].str.strip().str.title()
    return df


@asset(group_name="cleaned", compute_kind="pandas")
def cleaned_orders(
    context: AssetExecutionContext,
    raw_orders: pd.DataFrame,
    cleaned_products: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cleaned orders:
    - drop exact duplicate order_ids (keep first)
    - parse tanggal_order from mixed formats (DD/MM/YYYY, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, "Mon DD, YYYY")
    - normalize kota (title case) and channel (lowercase, snake_case)
    - recompute total_harga from quantity * harga_satuan when missing or negative
    """
    df = raw_orders.copy()

    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="first")
    context.log.info(f"Dropped {before - len(df)} duplicate order_id rows")

    # Parse mixed date formats: try a few explicit formats, fall back to pandas inference
    def parse_date(value):
        if pd.isna(value):
            return pd.NaT
        value = str(value).strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%b %d, %Y"):
            try:
                return pd.to_datetime(value, format=fmt)
            except ValueError:
                continue
        return pd.to_datetime(value, errors="coerce")

    df["tanggal_order"] = df["tanggal_order"].apply(parse_date)
    unparsed = df["tanggal_order"].isna().sum()
    if unparsed:
        context.log.warning(f"{unparsed} rows had unparseable tanggal_order")

    df["kota"] = df["kota"].str.strip().str.title()
    df["channel"] = df["channel"].str.strip().str.lower()

    # Backfill / fix total_harga using product unit price * quantity
    price_lookup = cleaned_products.set_index("product_id")["harga_satuan"]
    expected_total = df["product_id"].map(price_lookup) * df["quantity"]

    needs_fix = df["total_harga"].isna() | (df["total_harga"] < 0)
    fixed_count = int(needs_fix.sum())
    df.loc[needs_fix, "total_harga"] = expected_total[needs_fix]
    context.log.info(f"Recomputed total_harga for {fixed_count} rows (missing or negative)")

    df["status"] = df["status"].str.strip().str.lower()
    df["customer_email"] = df["customer_email"].str.strip().str.lower()

    return df.reset_index(drop=True)


cleaned_assets = [cleaned_products, cleaned_orders]
