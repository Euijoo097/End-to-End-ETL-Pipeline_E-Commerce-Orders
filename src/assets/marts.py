import pandas as pd
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource


@asset(group_name="marts", compute_kind="duckdb")
def orders_in_duckdb(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    cleaned_orders: pd.DataFrame,
) -> None:
    """Persist cleaned orders into DuckDB for downstream SQL analysis."""
    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE cleaned_orders AS SELECT * FROM cleaned_orders")
    context.log.info(f"Wrote {len(cleaned_orders)} rows to cleaned_orders table")


@asset(group_name="marts", compute_kind="duckdb", deps=[orders_in_duckdb])
def sales_by_category_city(context: AssetExecutionContext, duckdb: DuckDBResource) -> pd.DataFrame:
    """Completed-order revenue and volume, broken out by product category and city."""
    query = """
        SELECT
            kategori,
            kota,
            COUNT(*) AS order_count,
            SUM(quantity) AS total_quantity,
            SUM(total_harga) AS total_revenue
        FROM cleaned_orders
        WHERE status = 'completed'
        GROUP BY kategori, kota
        ORDER BY total_revenue DESC
    """
    with duckdb.get_connection() as conn:
        df = conn.execute(query).fetch_df()
        conn.execute("CREATE OR REPLACE TABLE sales_by_category_city AS SELECT * FROM df")
    context.log.info(f"Computed {len(df)} category/city rows")
    return df


@asset(group_name="marts", compute_kind="duckdb", deps=[orders_in_duckdb])
def sales_by_channel_status(context: AssetExecutionContext, duckdb: DuckDBResource) -> pd.DataFrame:
    """Order counts and revenue broken out by sales channel and order status."""
    query = """
        SELECT
            channel,
            status,
            COUNT(*) AS order_count,
            SUM(total_harga) AS total_revenue
        FROM cleaned_orders
        GROUP BY channel, status
        ORDER BY channel, status
    """
    with duckdb.get_connection() as conn:
        df = conn.execute(query).fetch_df()
        conn.execute("CREATE OR REPLACE TABLE sales_by_channel_status AS SELECT * FROM df")
    context.log.info(f"Computed {len(df)} channel/status rows")
    return df


mart_assets = [orders_in_duckdb, sales_by_category_city, sales_by_channel_status]
