"""Cohort retention heatmap."""

from __future__ import annotations

import altair as alt
import polars as pl
import streamlit as st

from retailvelocity.cohort import monthly_cohort_matrix
from retailvelocity.ingestion import load_enriched

st.set_page_config(page_title="Cohort Retention · RetailVelocity", page_icon="📉", layout="wide")


@st.cache_data(show_spinner="Computing cohort matrix...")
def _matrix() -> pl.DataFrame:
    return monthly_cohort_matrix(load_enriched()).collect()


def main() -> None:
    st.title("Acquisition Cohort Retention")
    st.caption("% of each cohort's customers still active each subsequent month.")

    max_periods = st.slider("Periods to display", 3, 24, 12)
    mat = _matrix().filter(pl.col("months_since") <= max_periods)

    heatmap = (
        alt.Chart(mat)
        .mark_rect()
        .encode(
            x=alt.X("months_since:O", title="Months since acquisition"),
            y=alt.Y("cohort_month:T", title="Cohort"),
            color=alt.Color(
                "retention:Q",
                scale=alt.Scale(scheme="blues", domain=[0, 1]),
                legend=alt.Legend(format=".0%"),
            ),
            tooltip=[
                "cohort_month:T",
                "months_since:Q",
                alt.Tooltip("retention:Q", format=".1%"),
                "active_customers:Q",
                "cohort_size:Q",
            ],
        )
        .properties(height=500)
    )
    st.altair_chart(heatmap, use_container_width=True)

    st.divider()
    st.subheader("Average retention curve")
    avg = (
        mat.group_by("months_since")
        .agg(pl.col("retention").mean().alias("avg_retention"))
        .sort("months_since")
    )
    curve = (
        alt.Chart(avg)
        .mark_line(point=True, color="#3498db")
        .encode(
            x=alt.X("months_since:O"),
            y=alt.Y("avg_retention:Q", axis=alt.Axis(format=".0%")),
            tooltip=["months_since", alt.Tooltip("avg_retention:Q", format=".2%")],
        )
        .properties(height=300)
    )
    st.altair_chart(curve, use_container_width=True)


main()
