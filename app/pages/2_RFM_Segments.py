"""Customer RFM segmentation page."""

from __future__ import annotations

import altair as alt
import polars as pl
import streamlit as st

from retailvelocity.ingestion import load_enriched
from retailvelocity.rfm import at_risk_customers, compute_rfm, tier_summary

st.set_page_config(page_title="RFM Segments · RetailVelocity", page_icon="💎", layout="wide")


@st.cache_data(show_spinner="Computing RFM scores...")
def _rfm() -> pl.DataFrame:
    return compute_rfm(load_enriched()).collect()


@st.cache_data
def _tier_summary() -> pl.DataFrame:
    return tier_summary(_rfm().lazy()).collect()


@st.cache_data
def _at_risk(threshold_days: int, percentile: float) -> pl.DataFrame:
    return at_risk_customers(
        _rfm().lazy(),
        recency_threshold_days=threshold_days,
        min_monetary_percentile=percentile,
    ).collect()


def main() -> None:
    st.title("Customer Segmentation · RFM")

    tiers = _tier_summary()
    c1, c2, c3, c4 = st.columns(4)
    for col, t in zip((c1, c2, c3, c4), ["Platinum", "Gold", "Silver", "Bronze"], strict=False):
        row = tiers.filter(pl.col("tier") == t)
        if row.is_empty():
            col.metric(t, "–")
            continue
        r = row.row(0, named=True)
        col.metric(
            t,
            f"{r['customers']:,} customers",
            delta=f"{r['revenue_share'] * 100:.1f}% of revenue",
        )

    st.divider()

    c1, c2 = st.columns([3, 2])
    with c1:
        st.subheader("Tier composition")
        chart = (
            alt.Chart(tiers)
            .mark_arc(innerRadius=60)
            .encode(
                theta="revenue:Q",
                color=alt.Color("tier:N", legend=alt.Legend(title="Tier")),
                tooltip=["tier", "customers", "revenue", "revenue_share"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    with c2:
        st.subheader("Summary table")
        st.dataframe(tiers, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("RFM distribution — Frequency vs Monetary")
    rfm = _rfm()
    sample = rfm.sample(n=min(5000, rfm.height), seed=42)
    scatter = (
        alt.Chart(sample)
        .mark_circle(opacity=0.4)
        .encode(
            x=alt.X("frequency:Q", scale=alt.Scale(type="log")),
            y=alt.Y("monetary:Q", scale=alt.Scale(type="log")),
            color="tier:N",
            tooltip=["customer_id", "frequency", "monetary", "recency_days", "tier"],
        )
        .properties(height=420)
    )
    st.altair_chart(scatter, use_container_width=True)

    st.divider()
    st.subheader("At-risk customers (high value, gone quiet)")
    ct1, ct2 = st.columns(2)
    days = ct1.slider("Inactivity threshold (days)", 30, 365, 90, step=15)
    pct = ct2.slider("Minimum monetary percentile", 0.5, 0.95, 0.75, step=0.05)
    at_risk = _at_risk(days, pct)
    st.metric("Win-back candidates", f"{at_risk.height:,}")
    if not at_risk.is_empty():
        st.metric("Total $ at risk", f"${at_risk['monetary'].sum():,.0f}")
        st.dataframe(
            at_risk.select(["customer_id", "recency_days", "frequency", "monetary", "tier"]).head(
                200
            ),
            use_container_width=True,
            hide_index=True,
        )


main()
