"""Descriptive analytics — time-series aggregations and breakdowns.

All functions accept and return Polars ``LazyFrame``s so they can be composed
before a single ``.collect()`` at the end of a pipeline.
"""

from __future__ import annotations

from typing import Literal

import polars as pl

TimeGrain = Literal["day", "week", "month"]

_TRUNCATE_MAP: dict[TimeGrain, str] = {
    "day": "1d",
    "week": "1w",
    "month": "1mo",
}


def revenue_over_time(lf: pl.LazyFrame, grain: TimeGrain = "day") -> pl.LazyFrame:
    """Aggregate revenue, profit, orders, and customers by time bucket."""
    interval = _TRUNCATE_MAP[grain]
    return (
        lf.with_columns(pl.col("purchase_ts").dt.truncate(interval).dt.date().alias("bucket"))
        .group_by("bucket")
        .agg(
            pl.col("net_revenue").sum().alias("revenue"),
            pl.col("profit").sum().alias("profit"),
            pl.col("invoice_id").n_unique().alias("orders"),
            pl.col("customer_id").n_unique().alias("customers"),
            pl.col("quantity").sum().alias("units"),
        )
        .sort("bucket")
    )


def rolling_revenue(
    lf: pl.LazyFrame, window_days: int = 7, grain: TimeGrain = "day"
) -> pl.LazyFrame:
    """Rolling mean of daily revenue — surfaces the underlying trend under weekly noise."""
    daily = revenue_over_time(lf, grain=grain).sort("bucket")
    return daily.with_columns(
        pl.col("revenue")
        .rolling_mean(window_size=window_days, min_samples=1)
        .alias(f"revenue_ma_{window_days}")
    )


def revenue_by_category(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Revenue, profit, and share by product category."""
    return (
        lf.group_by("category")
        .agg(
            pl.col("net_revenue").sum().alias("revenue"),
            pl.col("profit").sum().alias("profit"),
            pl.col("quantity").sum().alias("units"),
            pl.col("invoice_id").n_unique().alias("orders"),
        )
        .sort("revenue", descending=True)
        .with_columns(
            (pl.col("revenue") / pl.col("revenue").sum()).alias("revenue_share"),
            (pl.col("profit") / pl.col("revenue")).alias("profit_margin"),
        )
    )


def revenue_by_country(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.group_by("country")
        .agg(
            pl.col("net_revenue").sum().alias("revenue"),
            pl.col("profit").sum().alias("profit"),
            pl.col("customer_id").n_unique().alias("customers"),
        )
        .sort("revenue", descending=True)
    )


def weekday_hour_heatmap(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Sales intensity by weekday x hour - useful for email/notification timing."""
    return (
        lf.group_by(["weekday", "hour"])
        .agg(
            pl.col("net_revenue").sum().alias("revenue"),
            pl.len().alias("transactions"),
        )
        .sort(["weekday", "hour"])
    )


def top_products(lf: pl.LazyFrame, n: int = 20) -> pl.LazyFrame:
    return (
        lf.group_by(["product_id", "sku", "category"])
        .agg(
            pl.col("net_revenue").sum().alias("revenue"),
            pl.col("profit").sum().alias("profit"),
            pl.col("quantity").sum().alias("units"),
        )
        .sort("revenue", descending=True)
        .head(n)
    )
