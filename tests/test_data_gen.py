"""Synthetic data generator — shape, referential integrity, schema."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def test_three_files_written(small_dataset: dict[str, Path]) -> None:
    for key in ("customers", "products", "transactions"):
        assert small_dataset[key].exists(), f"{key} parquet missing"


def test_transaction_columns(small_dataset: dict[str, Path]) -> None:
    tx = pl.read_parquet(small_dataset["transactions"])
    expected = {
        "invoice_id",
        "customer_id",
        "product_id",
        "purchase_ts",
        "quantity",
        "unit_price",
        "discount_pct",
        "net_revenue",
        "profit",
    }
    assert expected.issubset(set(tx.columns))


def test_referential_integrity(small_dataset: dict[str, Path]) -> None:
    tx = pl.read_parquet(small_dataset["transactions"])
    customers = pl.read_parquet(small_dataset["customers"])
    products = pl.read_parquet(small_dataset["products"])

    orphan_customers = tx.join(customers, on="customer_id", how="anti").height
    orphan_products = tx.join(products, on="product_id", how="anti").height
    assert orphan_customers == 0
    assert orphan_products == 0


def test_row_count_near_target(small_dataset: dict[str, Path]) -> None:
    tx = pl.read_parquet(small_dataset["transactions"])
    # Poisson jitter + min-1 per customer; accept ±25% of target.
    assert 3000 <= tx.height <= 6000


def test_no_purchases_before_signup(small_dataset: dict[str, Path]) -> None:
    tx = pl.read_parquet(small_dataset["transactions"])
    customers = pl.read_parquet(small_dataset["customers"])
    joined = tx.join(customers, on="customer_id").with_columns(
        pl.col("purchase_ts").dt.date().alias("pdate")
    )
    assert joined.filter(pl.col("pdate") < pl.col("signup_date")).height == 0


def test_positive_revenue(small_dataset: dict[str, Path]) -> None:
    tx = pl.read_parquet(small_dataset["transactions"])
    assert tx["net_revenue"].min() > 0
    assert tx["quantity"].min() >= 1
