"""Cohort retention."""

from __future__ import annotations

import polars as pl

from retailvelocity.cohort import cohort_heatmap_pivot, monthly_cohort_matrix


def test_cohort_matrix_columns(enriched):
    df = monthly_cohort_matrix(enriched).collect()
    for col in ("cohort_month", "period_month", "months_since", "retention", "cohort_size"):
        assert col in df.columns


def test_month_zero_retention_is_one(enriched):
    df = monthly_cohort_matrix(enriched).collect()
    first_period = df.filter(pl.col("months_since") == 0)
    assert (first_period["retention"] == 1.0).all()


def test_pivot_shape(enriched):
    matrix = monthly_cohort_matrix(enriched)
    pivoted = cohort_heatmap_pivot(matrix, max_periods=6)
    assert "cohort_month" in pivoted.columns
    # At least one period-offset column present.
    assert pivoted.width >= 2
