"""Prescriptive — reorder, dead stock, at-risk revenue."""

from __future__ import annotations

from retailvelocity.forecasting import forecast_many, top_skus_by_revenue
from retailvelocity.ingestion import load_products
from retailvelocity.prescriptive import at_risk_revenue, dead_stock, reorder_report
from retailvelocity.rfm import at_risk_customers, compute_rfm


def test_reorder_report_produces_status(enriched, small_dataset):
    ids = top_skus_by_revenue(enriched, n=5)
    forecasts = forecast_many(enriched, ids, horizon_days=14)
    products = load_products(small_dataset["products"]).collect()
    report = reorder_report(forecasts, products, lead_time_days=14)
    if report.is_empty():
        return
    assert set(report["status"].unique().to_list()).issubset({"red", "yellow", "green"})
    assert "reorder_point" in report.columns


def test_dead_stock_schema(enriched, small_dataset):
    products = load_products(small_dataset["products"]).collect()
    df = dead_stock(enriched, products, lookback_days=30, min_stock_value=0.0)
    # Should be a DataFrame (maybe empty) with expected columns.
    assert "parked_value" in df.columns


def test_at_risk_revenue_returns_dict(enriched):
    rfm = compute_rfm(enriched)
    ar = at_risk_customers(rfm)
    result = at_risk_revenue(ar)
    for key in ("customers", "dollars_at_risk", "avg_customer_value"):
        assert key in result
