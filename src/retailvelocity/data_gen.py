"""Synthetic e-commerce transaction data generator.

Produces three parquet files — customers, products, transactions — with realistic
structure: weekly/yearly seasonality, cohort acquisition, SKU popularity power law,
and a long-tail of churned customers. Scales to millions of rows using Polars
vectorized expressions rather than Python loops.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

from retailvelocity.config import (
    CUSTOMERS_PARQUET,
    DEFAULT_SEED,
    PRODUCTS_PARQUET,
    RAW_DIR,
    TRANSACTIONS_PARQUET,
)

CATEGORIES = [
    "Electronics",
    "Home & Kitchen",
    "Apparel",
    "Beauty",
    "Sports & Outdoors",
    "Books",
    "Toys & Games",
    "Grocery",
]

COUNTRIES = [
    "United States",
    "United Kingdom",
    "Germany",
    "France",
    "Canada",
    "Australia",
    "Netherlands",
    "Spain",
]

COUNTRY_WEIGHTS = np.array([0.45, 0.15, 0.10, 0.07, 0.08, 0.05, 0.05, 0.05])


@dataclass(frozen=True)
class GenConfig:
    """Parameters for synthetic data generation."""

    n_customers: int = 50_000
    n_products: int = 2_000
    start_date: date = date(2022, 1, 1)
    end_date: date = date(2024, 12, 31)
    target_rows: int = 1_000_000
    seed: int = DEFAULT_SEED


def _make_products(cfg: GenConfig, rng: np.random.Generator) -> pl.DataFrame:
    n = cfg.n_products
    product_id = np.arange(1, n + 1, dtype=np.int64)
    category = rng.choice(CATEGORIES, size=n)
    # Unit prices follow a log-normal — lots of cheap items, a few expensive.
    unit_price = np.round(rng.lognormal(mean=2.7, sigma=0.9, size=n), 2)
    unit_price = np.clip(unit_price, 1.5, 999.0)
    # Margin varies by category (Electronics low, Beauty high).
    cat_margin = {
        "Electronics": 0.12,
        "Home & Kitchen": 0.28,
        "Apparel": 0.42,
        "Beauty": 0.55,
        "Sports & Outdoors": 0.30,
        "Books": 0.20,
        "Toys & Games": 0.35,
        "Grocery": 0.18,
    }
    margin = np.array([cat_margin[c] for c in category]) + rng.normal(0, 0.04, n)
    margin = np.clip(margin, 0.05, 0.75)
    # Inventory on hand — roughly proportional to popularity (we'll use product_id rank later).
    stock = rng.integers(5, 2000, size=n)

    return pl.DataFrame(
        {
            "product_id": product_id,
            "sku": [f"SKU-{i:06d}" for i in product_id],
            "category": category,
            "unit_price": unit_price,
            "margin_pct": margin.round(3),
            "stock_on_hand": stock,
        }
    )


def _make_customers(cfg: GenConfig, rng: np.random.Generator) -> pl.DataFrame:
    n = cfg.n_customers
    customer_id = np.arange(1, n + 1, dtype=np.int64)

    total_days = (cfg.end_date - cfg.start_date).days
    # Customers acquired with a mild growth trend (later = more likely).
    weights = np.linspace(0.6, 1.4, total_days + 1)
    weights /= weights.sum()
    signup_offset = rng.choice(total_days + 1, size=n, p=weights)
    signup_date = np.array(
        [cfg.start_date + timedelta(days=int(d)) for d in signup_offset],
        dtype="datetime64[D]",
    )

    country = rng.choice(COUNTRIES, size=n, p=COUNTRY_WEIGHTS)

    # Latent per-customer spend propensity (drives frequency).
    propensity = rng.gamma(shape=1.3, scale=1.0, size=n).astype(np.float32)

    return pl.DataFrame(
        {
            "customer_id": customer_id,
            "signup_date": signup_date,
            "country": country,
            "propensity": propensity,
        }
    )


def _assign_transaction_counts(
    customers: pl.DataFrame, target_rows: int, rng: np.random.Generator
) -> np.ndarray:
    """Distribute the target row count across customers using propensity weights."""
    prop = customers["propensity"].to_numpy()
    weights = prop / prop.sum()
    # Poisson draw around each customer's expected share.
    expected = weights * target_rows
    counts = rng.poisson(lam=expected).astype(np.int64)
    # Force every customer to have at least one txn for a clean cohort table.
    counts = np.maximum(counts, 1)
    return counts


def _seasonal_day_weights(start: date, end: date) -> tuple[np.ndarray, np.ndarray]:
    """Return (days array, probability weights) reflecting seasonality + weekday effect."""
    n_days = (end - start).days + 1
    days = np.array([start + timedelta(days=i) for i in range(n_days)])
    day_of_year = np.array([d.timetuple().tm_yday for d in days])
    weekday = np.array([d.weekday() for d in days])

    # Annual cycle — peak late-November (Black Friday / holiday).
    annual = 1.0 + 0.35 * np.cos((day_of_year - 330) * 2 * np.pi / 365.0)
    # Mild weekly seasonality — higher weekend traffic.
    weekly = np.where(weekday >= 5, 1.25, 1.0)
    # Linear growth trend.
    trend = np.linspace(0.85, 1.25, n_days)

    weights = annual * weekly * trend
    weights /= weights.sum()
    return day_of_year, weights


def _build_transactions(
    customers: pl.DataFrame,
    products: pl.DataFrame,
    cfg: GenConfig,
    rng: np.random.Generator,
) -> pl.DataFrame:
    counts = _assign_transaction_counts(customers, cfg.target_rows, rng)
    total_rows = int(counts.sum())

    customer_ids = np.repeat(customers["customer_id"].to_numpy(), counts)
    signup_dates = np.repeat(customers["signup_date"].to_numpy(), counts)

    # Product popularity — Zipf-like distribution.
    n_products = products.height
    pop_rank = rng.zipf(a=1.4, size=total_rows) % n_products
    product_ids = products["product_id"].to_numpy()[pop_rank]
    unit_prices = products["unit_price"].to_numpy()[pop_rank]
    margins = products["margin_pct"].to_numpy()[pop_rank]

    # Purchase day — constrained to be >= signup and within the window.
    n_days = (cfg.end_date - cfg.start_date).days + 1
    _, day_weights = _seasonal_day_weights(cfg.start_date, cfg.end_date)
    day_offsets = rng.choice(n_days, size=total_rows, p=day_weights)
    purchase_dates = np.array(cfg.start_date, dtype="datetime64[D]") + day_offsets.astype(
        "timedelta64[D]"
    )
    # Clip purchases before signup to the signup day.
    purchase_dates = np.maximum(purchase_dates, signup_dates)

    # Purchase hour — bimodal (lunch + evening).
    hour_pool = np.concatenate(
        [
            rng.normal(13, 1.5, size=total_rows // 2),
            rng.normal(20, 2.0, size=total_rows - total_rows // 2),
        ]
    )
    rng.shuffle(hour_pool)
    hours = np.clip(hour_pool, 0, 23).astype(np.int8)
    minutes = rng.integers(0, 60, size=total_rows, dtype=np.int8)

    # Quantity — right-skewed, mostly 1-3, occasionally larger.
    quantity = rng.poisson(lam=1.2, size=total_rows).astype(np.int32) + 1

    # Occasional promotional discount (20% of txns get 5-30% off).
    has_discount = rng.random(total_rows) < 0.2
    discount_pct = np.where(has_discount, rng.uniform(0.05, 0.30, total_rows), 0.0)

    gross = unit_prices * quantity
    net_revenue = gross * (1.0 - discount_pct)
    profit = net_revenue * margins

    # Invoice IDs — one per (customer, day), so group hash then dense-rank inside Polars.
    invoice_seed = customer_ids.astype(np.int64) * 100_000 + day_offsets.astype(np.int64)

    ts_ns = (
        purchase_dates.astype("datetime64[ns]").astype(np.int64)
        + hours.astype(np.int64) * 3_600_000_000_000
        + minutes.astype(np.int64) * 60_000_000_000
    )

    df = pl.DataFrame(
        {
            "customer_id": customer_ids,
            "product_id": product_ids,
            "invoice_seed": invoice_seed,
            "purchase_ts": pl.from_numpy(ts_ns, schema=["t"]).to_series().cast(pl.Datetime("ns")),
            "quantity": quantity,
            "unit_price": unit_prices.astype(np.float32),
            "discount_pct": discount_pct.astype(np.float32).round(3),
            "net_revenue": net_revenue.astype(np.float32).round(2),
            "profit": profit.astype(np.float32).round(2),
        }
    )

    # Dense-rank the invoice seed into a clean INV-XXXXXXX id.
    df = (
        df.with_columns(
            pl.col("invoice_seed").rank("dense").cast(pl.Int64).alias("_inv_rank"),
        )
        .with_columns(
            ("INV-" + pl.col("_inv_rank").cast(pl.Utf8).str.zfill(8)).alias("invoice_id"),
        )
        .drop(["invoice_seed", "_inv_rank"])
    )

    return df.select(
        [
            "invoice_id",
            "customer_id",
            "product_id",
            "purchase_ts",
            "quantity",
            "unit_price",
            "discount_pct",
            "net_revenue",
            "profit",
        ]
    ).sort("purchase_ts")


def generate(cfg: GenConfig | None = None, out_dir: Path | None = None) -> dict[str, Path]:
    """Generate and persist customers, products, and transactions as parquet files.

    Returns a mapping of table-name to output path.
    """
    cfg = cfg or GenConfig()
    out_dir = out_dir or RAW_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(cfg.seed)
    products = _make_products(cfg, rng)
    customers = _make_customers(cfg, rng)
    transactions = _build_transactions(customers, products, cfg, rng)

    paths = {
        "products": out_dir / PRODUCTS_PARQUET.name,
        "customers": out_dir / CUSTOMERS_PARQUET.name,
        "transactions": out_dir / TRANSACTIONS_PARQUET.name,
    }
    products.write_parquet(paths["products"], compression="zstd")
    customers.write_parquet(paths["customers"], compression="zstd")
    transactions.write_parquet(paths["transactions"], compression="zstd")
    return paths


def cli() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data.")
    parser.add_argument("--customers", type=int, default=50_000)
    parser.add_argument("--products", type=int, default=2_000)
    parser.add_argument("--rows", type=int, default=1_000_000, help="Target transaction rows")
    parser.add_argument("--start", type=str, default="2022-01-01")
    parser.add_argument("--end", type=str, default="2024-12-31")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--out", type=Path, default=RAW_DIR)
    args = parser.parse_args()

    cfg = GenConfig(
        n_customers=args.customers,
        n_products=args.products,
        start_date=datetime.strptime(args.start, "%Y-%m-%d").date(),
        end_date=datetime.strptime(args.end, "%Y-%m-%d").date(),
        target_rows=args.rows,
        seed=args.seed,
    )
    paths = generate(cfg, out_dir=args.out)
    for name, path in paths.items():
        print(f"{name:>13}: {path}")


if __name__ == "__main__":
    cli()
