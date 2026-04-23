"""RFM scoring + tier assignment."""

from __future__ import annotations

import polars as pl

from retailvelocity.rfm import at_risk_customers, compute_rfm, tier_summary


def test_rfm_produces_scores_and_tier(enriched):
    df = compute_rfm(enriched).collect()
    for col in ("r_score", "f_score", "m_score", "rfm_score", "tier"):
        assert col in df.columns
    assert set(df["tier"].unique().to_list()).issubset({"Platinum", "Gold", "Silver", "Bronze"})
    assert df["r_score"].min() >= 1
    assert df["r_score"].max() <= 4


def test_platinum_has_highest_avg_monetary(enriched):
    rfm = compute_rfm(enriched).collect()
    avg = rfm.group_by("tier").agg(pl.col("monetary").mean().alias("avg_m"))
    lookup = {row["tier"]: row["avg_m"] for row in avg.iter_rows(named=True)}
    # Platinum must dominate Bronze — canonical validity check.
    assert lookup["Platinum"] > lookup["Bronze"]


def test_tier_summary_sums_to_one(enriched):
    summary = tier_summary(compute_rfm(enriched)).collect()
    assert abs(float(summary["customer_share"].sum()) - 1.0) < 1e-5
    assert abs(float(summary["revenue_share"].sum()) - 1.0) < 1e-5


def test_at_risk_filter(enriched):
    rfm = compute_rfm(enriched)
    ar = at_risk_customers(rfm, recency_threshold_days=60, min_monetary_percentile=0.5).collect()
    if not ar.is_empty():
        assert ar["recency_days"].min() >= 60
