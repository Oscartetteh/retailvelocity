"""RFM (Recency / Frequency / Monetary) customer segmentation.

Produces per-customer scores and a named tier (Platinum / Gold / Silver / Bronze)
derived from quartile ranks across the three dimensions. The heavy lifting is
done inside Polars expressions — no Python loops, no materialisation until the
caller collects.
"""

from __future__ import annotations

from datetime import date

import polars as pl


def _reference_date(lf: pl.LazyFrame) -> date:
    """Latest purchase date in the frame + 1 day — serves as "today" for recency."""
    max_date = lf.select(pl.col("purchase_date").max()).collect().item()
    return max_date  # type: ignore[return-value]


def compute_rfm(lf: pl.LazyFrame, reference_date: date | None = None) -> pl.LazyFrame:
    """Compute RFM values, scores (1-4), a composite RFM score, and tier label."""
    ref = reference_date or _reference_date(lf)
    ref_expr = pl.lit(ref)

    base = lf.group_by("customer_id").agg(
        (ref_expr - pl.col("purchase_date").max()).dt.total_days().alias("recency_days"),
        pl.col("invoice_id").n_unique().alias("frequency"),
        pl.col("net_revenue").sum().alias("monetary"),
        pl.col("purchase_date").min().alias("first_purchase"),
        pl.col("purchase_date").max().alias("last_purchase"),
    )

    # qcut labels are Categorical; cast Utf8 → Int32 to get the numeric score.
    # For recency, lower days = better, so we reverse (5 - score).
    def _qscore(col: str) -> pl.Expr:
        return pl.col(col).qcut(4, labels=["1", "2", "3", "4"]).cast(pl.Utf8).cast(pl.Int32)

    scored = base.with_columns(
        (5 - _qscore("recency_days")).alias("r_score"),
        _qscore("frequency").alias("f_score"),
        _qscore("monetary").alias("m_score"),
    )

    scored = scored.with_columns(
        (pl.col("r_score") + pl.col("f_score") + pl.col("m_score")).alias("rfm_score"),
    ).with_columns(
        pl.when(pl.col("rfm_score") >= 10)
        .then(pl.lit("Platinum"))
        .when(pl.col("rfm_score") >= 8)
        .then(pl.lit("Gold"))
        .when(pl.col("rfm_score") >= 6)
        .then(pl.lit("Silver"))
        .otherwise(pl.lit("Bronze"))
        .alias("tier"),
    )

    return scored


def tier_summary(rfm_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Revenue, customer count, and average spend per tier."""
    return (
        rfm_lf.group_by("tier")
        .agg(
            pl.len().alias("customers"),
            pl.col("monetary").sum().alias("revenue"),
            pl.col("monetary").mean().alias("avg_monetary"),
            pl.col("frequency").mean().alias("avg_frequency"),
            pl.col("recency_days").mean().alias("avg_recency_days"),
        )
        .sort("revenue", descending=True)
        .with_columns(
            (pl.col("revenue") / pl.col("revenue").sum()).alias("revenue_share"),
            (pl.col("customers") / pl.col("customers").sum()).alias("customer_share"),
        )
    )


def at_risk_customers(
    rfm_lf: pl.LazyFrame,
    recency_threshold_days: int = 90,
    min_monetary_percentile: float = 0.75,
) -> pl.LazyFrame:
    """High-value customers who have gone quiet — the marketing win-back list."""
    threshold = rfm_lf.select(pl.col("monetary").quantile(min_monetary_percentile)).collect().item()
    return rfm_lf.filter(
        (pl.col("recency_days") >= recency_threshold_days) & (pl.col("monetary") >= threshold)
    ).sort("monetary", descending=True)
