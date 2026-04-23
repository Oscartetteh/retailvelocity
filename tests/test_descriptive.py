"""Descriptive aggregations."""

from __future__ import annotations

import polars as pl

from retailvelocity.descriptive import (
    revenue_by_category,
    revenue_by_country,
    revenue_over_time,
    rolling_revenue,
    top_products,
    weekday_hour_heatmap,
)


def test_revenue_over_time_daily(enriched):
    df = revenue_over_time(enriched, grain="day").collect()
    assert df.height > 0
    assert "revenue" in df.columns
    assert df["bucket"].is_sorted()


def test_rolling_adds_ma_column(enriched):
    df = rolling_revenue(enriched, window_days=7, grain="day").collect()
    assert "revenue_ma_7" in df.columns
    assert df["revenue_ma_7"].null_count() == 0


def test_category_revenue_sums_to_total(enriched):
    by_cat = revenue_by_category(enriched).collect()
    total = by_cat["revenue"].sum()
    raw = enriched.select(pl.col("net_revenue").sum()).collect().item()
    assert abs(float(total) - float(raw)) / float(raw) < 0.01


def test_country_and_heatmap(enriched):
    c = revenue_by_country(enriched).collect()
    assert c.height > 0
    h = weekday_hour_heatmap(enriched).collect()
    assert h["weekday"].n_unique() <= 7
    assert h["hour"].n_unique() <= 24


def test_top_products_bounded(enriched):
    df = top_products(enriched, n=10).collect()
    assert df.height <= 10
    assert df["revenue"].is_sorted(descending=True)
