"""Data ingestion — lazy scanning, schema enforcement, and canonical enrichment.

Every downstream module reads through the ``load_transactions`` / ``load_enriched``
helpers rather than touching parquet paths directly. That lets us swap in a
different backend (CSV, S3, Postgres → Arrow) without rippling changes through
the codebase. Everything returns ``pl.LazyFrame`` — the query only materializes
when the caller calls ``.collect()``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from retailvelocity.config import (
    CUSTOMERS_PARQUET,
    PRODUCTS_PARQUET,
    TRANSACTIONS_PARQUET,
)

TRANSACTIONS_SCHEMA: dict[str, pl.DataType] = {
    "invoice_id": pl.Utf8,
    "customer_id": pl.Int64,
    "product_id": pl.Int64,
    "purchase_ts": pl.Datetime("ns"),
    "quantity": pl.Int32,
    "unit_price": pl.Float32,
    "discount_pct": pl.Float32,
    "net_revenue": pl.Float32,
    "profit": pl.Float32,
}


def _resolve(path: Path | str | None, default: Path) -> Path:
    return Path(path) if path is not None else default


def load_transactions(path: Path | str | None = None) -> pl.LazyFrame:
    """Lazy scan of the transactions parquet with typed schema and derived calendar fields."""
    p = _resolve(path, TRANSACTIONS_PARQUET)
    lf = pl.scan_parquet(p)
    return lf.with_columns(
        pl.col("purchase_ts").dt.date().alias("purchase_date"),
        pl.col("purchase_ts").dt.year().alias("year"),
        pl.col("purchase_ts").dt.month().alias("month"),
        pl.col("purchase_ts").dt.weekday().alias("weekday"),
        pl.col("purchase_ts").dt.hour().alias("hour"),
        pl.col("purchase_ts").dt.truncate("1mo").dt.date().alias("month_start"),
    )


def load_customers(path: Path | str | None = None) -> pl.LazyFrame:
    p = _resolve(path, CUSTOMERS_PARQUET)
    return pl.scan_parquet(p)


def load_products(path: Path | str | None = None) -> pl.LazyFrame:
    p = _resolve(path, PRODUCTS_PARQUET)
    return pl.scan_parquet(p)


def load_enriched(
    transactions_path: Path | str | None = None,
    customers_path: Path | str | None = None,
    products_path: Path | str | None = None,
) -> pl.LazyFrame:
    """Join transactions with customer + product metadata — the canonical analytic frame."""
    tx = load_transactions(transactions_path)
    customers = load_customers(customers_path).select(["customer_id", "signup_date", "country"])
    products = load_products(products_path).select(
        ["product_id", "sku", "category", "margin_pct", "stock_on_hand"]
    )
    return tx.join(customers, on="customer_id", how="left").join(
        products, on="product_id", how="left"
    )


def dataset_summary(lf: pl.LazyFrame) -> dict[str, int | float]:
    """Cheap KPI summary — runs one collect() so callers don't pay twice."""
    df = lf.select(
        [
            pl.len().alias("rows"),
            pl.col("customer_id").n_unique().alias("customers"),
            pl.col("product_id").n_unique().alias("products"),
            pl.col("invoice_id").n_unique().alias("orders"),
            pl.col("net_revenue").sum().alias("gross_revenue"),
            pl.col("profit").sum().alias("gross_profit"),
            pl.col("purchase_date").min().alias("first_day"),
            pl.col("purchase_date").max().alias("last_day"),
        ]
    ).collect()
    return df.to_dicts()[0]
