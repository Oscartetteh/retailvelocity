"""SKU-level demand forecasting using statsmodels Exponential Smoothing.

Prophet is intentionally avoided — it drags a heavy cmdstan/pystan dependency
chain that hurts reproducibility. Holt-Winters (ETS) is close enough for the
retail use case, ships with statsmodels, and is fast enough to fit hundreds of
SKUs sequentially.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl
from statsmodels.tsa.holtwinters import ExponentialSmoothing


@dataclass(frozen=True)
class ForecastResult:
    """Per-SKU forecast output."""

    product_id: int
    sku: str
    history: pl.DataFrame  # ds, y (daily units)
    forecast: pl.DataFrame  # ds, yhat, yhat_lower, yhat_upper
    mape: float
    rmse: float


def _daily_units(lf: pl.LazyFrame, product_id: int) -> pl.DataFrame:
    """Per-product daily unit series with missing days filled as zero."""
    df = (
        lf.filter(pl.col("product_id") == product_id)
        .group_by("purchase_date")
        .agg(pl.col("quantity").sum().alias("units"))
        .sort("purchase_date")
        .collect()
    )
    if df.height == 0:
        return df
    full_range = pl.DataFrame(
        {
            "purchase_date": pl.date_range(
                df["purchase_date"].min(),
                df["purchase_date"].max(),
                interval="1d",
                eager=True,
            )
        }
    )
    return full_range.join(df, on="purchase_date", how="left").with_columns(
        pl.col("units").fill_null(0)
    )


def _fit_ets(train: np.ndarray, seasonal_periods: int = 7):
    """Fit Holt-Winters with additive seasonality; fall back to trend-only on short series."""
    if len(train) >= 2 * seasonal_periods:
        model = ExponentialSmoothing(
            train,
            trend="add",
            seasonal="add",
            seasonal_periods=seasonal_periods,
            initialization_method="estimated",
        )
    else:
        model = ExponentialSmoothing(
            train, trend="add", seasonal=None, initialization_method="estimated"
        )
    return model.fit(optimized=True)


def _mape(actual: np.ndarray, predicted: np.ndarray) -> float:
    denom = np.where(np.abs(actual) < 1e-9, 1.0, np.abs(actual))
    return float(np.mean(np.abs((actual - predicted) / denom)) * 100.0)


def _rmse(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.sqrt(np.mean((actual - predicted) ** 2)))


def forecast_product(
    lf: pl.LazyFrame,
    product_id: int,
    horizon_days: int = 30,
    test_days: int = 30,
    ci: float = 0.95,
) -> ForecastResult | None:
    """Fit, backtest, and forecast a single SKU. Returns ``None`` if too little history."""
    history = _daily_units(lf, product_id)
    if history.height < 60:
        return None

    sku = lf.filter(pl.col("product_id") == product_id).select("sku").unique().collect().item()

    y = history["units"].to_numpy().astype(float)
    if len(y) <= test_days + 30:
        test_days = max(7, len(y) // 5)

    train, test = y[:-test_days], y[-test_days:]
    fit = _fit_ets(train)
    preds = fit.forecast(test_days)
    mape = _mape(test, preds)
    rmse = _rmse(test, preds)

    # Refit on the full series for the forward forecast.
    full_fit = _fit_ets(y)
    yhat = full_fit.forecast(horizon_days)
    residuals = y - full_fit.fittedvalues
    sigma = float(np.nanstd(residuals))
    z = 1.96 if ci >= 0.95 else 1.28
    lower = yhat - z * sigma
    upper = yhat + z * sigma

    last_date = history["purchase_date"].max()
    future_dates = pl.date_range(
        start=last_date + np.timedelta64(1, "D").astype("timedelta64[ms]").astype(object),
        end=last_date + np.timedelta64(horizon_days, "D").astype("timedelta64[ms]").astype(object),
        interval="1d",
        eager=True,
    )

    forecast_df = pl.DataFrame(
        {
            "ds": future_dates,
            "yhat": np.maximum(yhat, 0),
            "yhat_lower": np.maximum(lower, 0),
            "yhat_upper": np.maximum(upper, 0),
        }
    )

    return ForecastResult(
        product_id=product_id,
        sku=sku,
        history=history.rename({"purchase_date": "ds", "units": "y"}),
        forecast=forecast_df,
        mape=mape,
        rmse=rmse,
    )


def top_skus_by_revenue(lf: pl.LazyFrame, n: int = 50) -> list[int]:
    """Pick the product_ids we'd actually bother forecasting."""
    df = (
        lf.group_by("product_id")
        .agg(pl.col("net_revenue").sum().alias("rev"))
        .sort("rev", descending=True)
        .head(n)
        .collect()
    )
    return df["product_id"].to_list()


def forecast_many(
    lf: pl.LazyFrame, product_ids: list[int], horizon_days: int = 30
) -> list[ForecastResult]:
    """Sequentially forecast a list of SKUs — skips ones with insufficient history."""
    results: list[ForecastResult] = []
    for pid in product_ids:
        r = forecast_product(lf, pid, horizon_days=horizon_days)
        if r is not None:
            results.append(r)
    return results
