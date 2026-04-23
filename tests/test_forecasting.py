"""Forecasting — smoke test on highest-volume SKU."""

from __future__ import annotations

from retailvelocity.forecasting import forecast_product, top_skus_by_revenue


def test_top_skus_returns_sorted_list(enriched):
    ids = top_skus_by_revenue(enriched, n=5)
    assert len(ids) <= 5
    assert all(isinstance(i, int) for i in ids)


def test_forecast_result_shape(enriched):
    ids = top_skus_by_revenue(enriched, n=3)
    result = None
    for pid in ids:
        result = forecast_product(enriched, pid, horizon_days=14, test_days=14)
        if result is not None:
            break
    assert result is not None, "no SKU had enough history — raise test dataset size"
    assert result.forecast.height == 14
    for col in ("ds", "yhat", "yhat_lower", "yhat_upper"):
        assert col in result.forecast.columns
    # CI bands must bracket the point forecast.
    assert (result.forecast["yhat_lower"] <= result.forecast["yhat"]).all()
    assert (result.forecast["yhat"] <= result.forecast["yhat_upper"]).all()
    assert result.mape >= 0
    assert result.rmse >= 0
