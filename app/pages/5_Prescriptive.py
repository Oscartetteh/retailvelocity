"""Prescriptive dashboard — reorder, dead stock, at-risk revenue."""

from __future__ import annotations

import polars as pl
import streamlit as st

from retailvelocity.forecasting import forecast_many, top_skus_by_revenue
from retailvelocity.ingestion import load_enriched, load_products
from retailvelocity.prescriptive import at_risk_revenue, dead_stock, reorder_report
from retailvelocity.rfm import at_risk_customers, compute_rfm

st.set_page_config(page_title="Prescriptive · RetailVelocity", page_icon="🎯", layout="wide")


@st.cache_data(show_spinner="Fitting forecasts and computing reorder report...")
def _reorder(n_skus: int, lead_time: int) -> pl.DataFrame:
    lf = load_enriched()
    products = load_products().collect()
    ids = top_skus_by_revenue(lf, n=n_skus)
    forecasts = forecast_many(lf, ids, horizon_days=lead_time)
    return reorder_report(forecasts, products, lead_time_days=lead_time)


@st.cache_data(show_spinner="Computing dead stock...")
def _dead_stock(lookback_days: int) -> pl.DataFrame:
    lf = load_enriched()
    products = load_products().collect()
    return dead_stock(lf, products, lookback_days=lookback_days)


@st.cache_data(show_spinner="Computing at-risk revenue...")
def _at_risk_dollars() -> dict[str, float]:
    rfm = compute_rfm(load_enriched())
    at_risk = at_risk_customers(rfm)
    return at_risk_revenue(at_risk)


def _status_color(value: str) -> str:
    return {"red": "🔴", "yellow": "🟡", "green": "🟢"}.get(value, "⚪")


def main() -> None:
    st.title("Prescriptive — Actionable Intelligence")

    c1, c2 = st.columns(2)
    n_skus = c1.slider("Top-N SKUs to forecast", 10, 100, 25)
    lead_time = c2.slider("Lead time (days)", 3, 60, 14)

    st.subheader("Reorder status")
    report = _reorder(n_skus, lead_time)
    if report.is_empty():
        st.warning("No forecasts produced — try generating more data or raising the SKU count.")
    else:
        totals = report.group_by("status").agg(pl.len().alias("count")).to_dicts()
        status_counts = {row["status"]: row["count"] for row in totals}
        rc, yc, gc = st.columns(3)
        rc.metric("🔴 Red — stock out risk", status_counts.get("red", 0))
        yc.metric("🟡 Yellow — below reorder point", status_counts.get("yellow", 0))
        gc.metric("🟢 Green — healthy", status_counts.get("green", 0))

        display = report.with_columns(
            pl.col("status").map_elements(_status_color, return_dtype=pl.Utf8).alias("flag")
        ).select(
            [
                "flag",
                "sku",
                "category",
                "expected_demand",
                "safety_stock",
                "reorder_point",
                "stock_on_hand",
                "shortfall_units",
                "mape_pct",
            ]
        )
        st.dataframe(display, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("At-risk revenue (win-back list)")
    ar = _at_risk_dollars()
    c1, c2, c3 = st.columns(3)
    c1.metric("At-risk customers", f"{int(ar['customers']):,}")
    c2.metric("Dollars at risk", f"${ar['dollars_at_risk']:,.0f}")
    c3.metric("Avg customer value", f"${ar['avg_customer_value']:,.0f}")

    st.divider()
    st.subheader("Dead stock — inventory not selling")
    lookback = st.slider("Lookback window (days)", 30, 365, 180)
    dead = _dead_stock(lookback)
    if dead.is_empty():
        st.success("No dead stock detected in the lookback window.")
    else:
        total_parked = float(dead["parked_value"].sum())
        st.metric("Total $ parked", f"${total_parked:,.0f}")
        st.dataframe(
            dead.select(["sku", "category", "stock_on_hand", "unit_price", "parked_value"]).head(
                200
            ),
            use_container_width=True,
            hide_index=True,
        )


main()
