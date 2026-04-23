"""Prescriptive analytics — turning signals into a to-do list.

Three outputs:
- ``reorder_report``: per-SKU expected demand over a lead-time window with a
  safety-stock buffer, plus a Red/Yellow/Green traffic light.
- ``dead_stock``: products with inventory sitting idle (zero sales in the
  lookback window).
- ``at_risk_revenue``: dollars parked in the win-back list from ``rfm.py``.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from retailvelocity.forecasting import ForecastResult


def reorder_report(
    forecasts: list[ForecastResult],
    products: pl.DataFrame,
    lead_time_days: int = 14,
    service_level_z: float = 1.65,
) -> pl.DataFrame:
    """Compute expected demand over the lead time + safety stock, flag stock adequacy.

    Safety stock = z * sigma * sqrt(lead_time).
    Traffic light:
      red    — stock_on_hand < expected_demand
      yellow — stock_on_hand < expected + safety_stock
      green  — otherwise
    """
    rows = []
    for r in forecasts:
        window = r.forecast.head(lead_time_days)
        mean_demand = float(window["yhat"].sum())
        sigma = float((window["yhat_upper"] - window["yhat"]).mean()) / 1.96
        safety_stock = service_level_z * sigma * (lead_time_days**0.5)
        rows.append(
            {
                "product_id": r.product_id,
                "sku": r.sku,
                "expected_demand": mean_demand,
                "safety_stock": safety_stock,
                "reorder_point": mean_demand + safety_stock,
                "mape_pct": r.mape,
            }
        )
    reorder = pl.DataFrame(rows)
    if reorder.is_empty():
        return reorder

    joined = reorder.join(
        products.select(["product_id", "stock_on_hand", "category", "unit_price"]),
        on="product_id",
        how="left",
    )

    return joined.with_columns(
        pl.when(pl.col("stock_on_hand") < pl.col("expected_demand"))
        .then(pl.lit("red"))
        .when(pl.col("stock_on_hand") < pl.col("reorder_point"))
        .then(pl.lit("yellow"))
        .otherwise(pl.lit("green"))
        .alias("status"),
        (pl.col("expected_demand") - pl.col("stock_on_hand")).alias("shortfall_units"),
    ).sort(["status", "shortfall_units"], descending=[False, True])


def dead_stock(
    lf: pl.LazyFrame,
    products: pl.DataFrame,
    lookback_days: int = 180,
    reference_date: date | None = None,
    min_stock_value: float = 100.0,
) -> pl.DataFrame:
    """Products with inventory on hand but no sales in the last N days."""
    ref = reference_date or lf.select(pl.col("purchase_date").max()).collect().item()
    cutoff = ref - timedelta(days=lookback_days)

    recent = (
        lf.filter(pl.col("purchase_date") >= cutoff)
        .group_by("product_id")
        .agg(pl.col("quantity").sum().alias("recent_units"))
        .collect()
    )

    result = (
        products.join(recent, on="product_id", how="left")
        .with_columns(pl.col("recent_units").fill_null(0))
        .filter(pl.col("recent_units") == 0)
        .with_columns((pl.col("stock_on_hand") * pl.col("unit_price")).alias("parked_value"))
        .filter(pl.col("parked_value") >= min_stock_value)
        .sort("parked_value", descending=True)
    )
    return result


def at_risk_revenue(at_risk_lf: pl.LazyFrame) -> dict[str, float]:
    """Total $ sitting on the at-risk customer list."""
    df = at_risk_lf.select(
        pl.len().alias("customers"),
        pl.col("monetary").sum().alias("dollars_at_risk"),
        pl.col("monetary").mean().alias("avg_customer_value"),
    ).collect()
    return df.to_dicts()[0]
