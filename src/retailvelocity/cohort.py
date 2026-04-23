"""Monthly acquisition cohort analysis.

Each customer is bucketed by the month of their first purchase. For every
subsequent month we measure how many of that cohort transacted. The output is
a long-form frame ready for a heatmap: (cohort_month, months_since, retention).
"""

from __future__ import annotations

import polars as pl


def _first_purchase_month(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.group_by("customer_id").agg(pl.col("month_start").min().alias("cohort_month"))


def monthly_cohort_matrix(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Long-form cohort retention table.

    Columns: ``cohort_month``, ``period_month``, ``months_since``, ``active_customers``,
    ``cohort_size``, ``retention``.
    """
    first = _first_purchase_month(lf)

    activity = (
        lf.join(first, on="customer_id")
        .group_by(["cohort_month", "month_start"])
        .agg(pl.col("customer_id").n_unique().alias("active_customers"))
        .rename({"month_start": "period_month"})
        .with_columns(
            (
                (pl.col("period_month").dt.year() - pl.col("cohort_month").dt.year()) * 12
                + (pl.col("period_month").dt.month() - pl.col("cohort_month").dt.month())
            ).alias("months_since")
        )
    )

    cohort_sizes = first.group_by("cohort_month").agg(pl.len().alias("cohort_size"))

    return (
        activity.join(cohort_sizes, on="cohort_month")
        .with_columns((pl.col("active_customers") / pl.col("cohort_size")).alias("retention"))
        .sort(["cohort_month", "months_since"])
    )


def cohort_heatmap_pivot(matrix_lf: pl.LazyFrame, max_periods: int = 12) -> pl.DataFrame:
    """Pivoted (rows=cohort, cols=months_since) retention matrix for a heatmap."""
    df = matrix_lf.filter(pl.col("months_since") <= max_periods).collect()
    return df.pivot(
        values="retention",
        index="cohort_month",
        on="months_since",
        aggregate_function="first",
    ).sort("cohort_month")
