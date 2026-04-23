"""Sales trends — time-series revenue, profit, and weekday × hour heatmap."""

from __future__ import annotations

import altair as alt
import polars as pl
import streamlit as st

from retailvelocity.descriptive import (
    revenue_by_category,
    revenue_by_country,
    rolling_revenue,
    weekday_hour_heatmap,
)
from retailvelocity.ingestion import load_enriched

st.set_page_config(page_title="Trends · RetailVelocity", page_icon="📈", layout="wide")


@st.cache_data(show_spinner="Computing trend aggregates...")
def _rolling(grain: str, window: int) -> pl.DataFrame:
    lf = load_enriched()
    return rolling_revenue(lf, window_days=window, grain=grain).collect()  # type: ignore[arg-type]


@st.cache_data(show_spinner="Computing category split...")
def _by_category() -> pl.DataFrame:
    return revenue_by_category(load_enriched()).collect()


@st.cache_data(show_spinner="Computing country split...")
def _by_country() -> pl.DataFrame:
    return revenue_by_country(load_enriched()).collect()


@st.cache_data(show_spinner="Computing weekday × hour heatmap...")
def _heatmap() -> pl.DataFrame:
    return weekday_hour_heatmap(load_enriched()).collect()


def main() -> None:
    st.title("Sales Trends")

    colg, colw = st.columns(2)
    grain = colg.selectbox("Grain", ["day", "week", "month"], index=0)
    window = colw.slider("Rolling window (bars)", 3, 30, 7)

    series = _rolling(grain, window).rename({"bucket": "date"})
    ma_col = f"revenue_ma_{window}"

    base = alt.Chart(series.to_pandas() if False else series).encode(x="date:T")
    rev = (
        alt.Chart(series)
        .mark_line(opacity=0.3, color="#3498db")
        .encode(x="date:T", y=alt.Y("revenue:Q", title="Revenue"))
    )
    ma = (
        alt.Chart(series)
        .mark_line(color="#e74c3c", strokeWidth=2)
        .encode(x="date:T", y=alt.Y(f"{ma_col}:Q", title=f"{window}-bar MA"))
    )
    st.altair_chart(rev + ma, use_container_width=True)

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Revenue by Category")
        cat = _by_category()
        chart = (
            alt.Chart(cat)
            .mark_bar()
            .encode(
                x=alt.X("revenue:Q", title="Revenue"),
                y=alt.Y("category:N", sort="-x"),
                color=alt.Color("profit_margin:Q", scale=alt.Scale(scheme="viridis")),
                tooltip=["category", "revenue", "profit_margin"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    with c2:
        st.subheader("Revenue by Country")
        country = _by_country()
        chart = (
            alt.Chart(country)
            .mark_bar()
            .encode(
                x=alt.X("revenue:Q"),
                y=alt.Y("country:N", sort="-x"),
                tooltip=["country", "revenue", "customers"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    st.divider()
    st.subheader("When do customers shop? (weekday × hour)")
    heat = _heatmap()
    chart = (
        alt.Chart(heat)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("weekday:O", title="Weekday (1=Mon)"),
            color=alt.Color("revenue:Q", scale=alt.Scale(scheme="inferno")),
            tooltip=["weekday", "hour", "revenue", "transactions"],
        )
        .properties(height=260)
    )
    st.altair_chart(chart, use_container_width=True)


main()
