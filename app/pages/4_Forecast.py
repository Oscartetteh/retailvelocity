"""Per-SKU demand forecast explorer."""

from __future__ import annotations

import altair as alt
import polars as pl
import streamlit as st

from retailvelocity.forecasting import forecast_product, top_skus_by_revenue
from retailvelocity.ingestion import load_enriched

st.set_page_config(page_title="Forecast · RetailVelocity", page_icon="🔮", layout="wide")


@st.cache_data(show_spinner="Ranking top SKUs...")
def _top_skus(n: int = 25) -> list[tuple[int, str]]:
    lf = load_enriched()
    top = top_skus_by_revenue(lf, n=n)
    skus = (
        lf.filter(pl.col("product_id").is_in(top)).select(["product_id", "sku"]).unique().collect()
    )
    return list(zip(skus["product_id"].to_list(), skus["sku"].to_list(), strict=False))


@st.cache_data(show_spinner="Fitting forecast...")
def _forecast(product_id: int, horizon: int, test_days: int):
    return forecast_product(
        load_enriched(), product_id=product_id, horizon_days=horizon, test_days=test_days
    )


def main() -> None:
    st.title("SKU Demand Forecast")

    skus = _top_skus(n=25)
    if not skus:
        st.warning("No SKUs available — generate data first.")
        return

    labels = {pid: f"{sku} (id={pid})" for pid, sku in skus}
    c1, c2, c3 = st.columns(3)
    pid = c1.selectbox("SKU", options=list(labels.keys()), format_func=lambda p: labels[p])
    horizon = c2.slider("Horizon (days)", 7, 90, 30)
    test_days = c3.slider("Backtest window (days)", 7, 60, 30)

    result = _forecast(pid, horizon, test_days)
    if result is None:
        st.error("Insufficient history for this SKU — pick a higher-volume one.")
        return

    c1, c2 = st.columns(2)
    c1.metric("MAPE (backtest)", f"{result.mape:.1f}%")
    c2.metric("RMSE (backtest)", f"{result.rmse:.2f}")

    hist = result.history.rename({"ds": "date", "y": "units"}).with_columns(
        pl.lit("history").alias("series")
    )
    fc = result.forecast.rename({"ds": "date", "yhat": "units"}).with_columns(
        pl.lit("forecast").alias("series")
    )

    base_layer = alt.Chart(hist).mark_line(color="#3498db").encode(x="date:T", y="units:Q")
    forecast_layer = (
        alt.Chart(fc).mark_line(color="#e74c3c", strokeDash=[4, 4]).encode(x="date:T", y="units:Q")
    )
    ci_layer = (
        alt.Chart(result.forecast.rename({"ds": "date"}))
        .mark_area(opacity=0.25, color="#e74c3c")
        .encode(x="date:T", y="yhat_lower:Q", y2="yhat_upper:Q")
    )
    st.altair_chart(base_layer + ci_layer + forecast_layer, use_container_width=True)

    with st.expander("Forecast table"):
        st.dataframe(result.forecast, use_container_width=True, hide_index=True)


main()
