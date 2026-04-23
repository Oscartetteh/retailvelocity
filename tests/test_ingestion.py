"""Ingestion — lazy scan, joins, summary."""

from __future__ import annotations

import polars as pl

from retailvelocity.ingestion import dataset_summary, load_enriched, load_transactions


def test_load_transactions_is_lazy(small_dataset):
    lf = load_transactions(small_dataset["transactions"])
    assert isinstance(lf, pl.LazyFrame)


def test_enriched_has_joined_columns(small_dataset):
    lf = load_enriched(
        transactions_path=small_dataset["transactions"],
        customers_path=small_dataset["customers"],
        products_path=small_dataset["products"],
    )
    df = lf.head(5).collect()
    assert "country" in df.columns
    assert "category" in df.columns
    assert "margin_pct" in df.columns


def test_calendar_fields_derived(small_dataset):
    lf = load_transactions(small_dataset["transactions"])
    df = lf.head(1).collect()
    for col in ("purchase_date", "year", "month", "weekday", "hour", "month_start"):
        assert col in df.columns


def test_summary_shape(enriched):
    summary = dataset_summary(enriched)
    for key in (
        "rows",
        "customers",
        "products",
        "orders",
        "gross_revenue",
        "gross_profit",
        "first_day",
        "last_day",
    ):
        assert key in summary
    assert summary["rows"] > 0
    assert summary["gross_revenue"] > 0
