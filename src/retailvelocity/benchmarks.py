"""Performance benchmarks — Polars eager vs lazy on the full pipeline.

This is the "show me the numbers" module. The narrative for recruiters:
- Lazy execution lets Polars plan the entire query (predicate pushdown,
  projection pushdown, CSE), often running 2-5x faster than eager.
- Arrow-backed columnar data + a multi-threaded engine chews through
  millions of rows in seconds on a laptop.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from retailvelocity.config import (
    CUSTOMERS_PARQUET,
    PRODUCTS_PARQUET,
    TRANSACTIONS_PARQUET,
)
from retailvelocity.rfm import compute_rfm


@dataclass(frozen=True)
class BenchResult:
    name: str
    mode: str
    rows: int
    seconds: float

    @property
    def rows_per_sec(self) -> float:
        return self.rows / self.seconds if self.seconds > 0 else float("inf")


def _time(fn: Callable[[], pl.DataFrame]) -> tuple[pl.DataFrame, float]:
    t0 = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t0


def bench_groupby(
    transactions_path: Path = TRANSACTIONS_PARQUET,
    customers_path: Path = CUSTOMERS_PARQUET,
    products_path: Path = PRODUCTS_PARQUET,
) -> list[BenchResult]:
    """Category-level aggregation with joins — eager vs lazy."""
    eager_tx = pl.read_parquet(transactions_path)
    eager_p = pl.read_parquet(products_path)
    rows = eager_tx.height

    def eager() -> pl.DataFrame:
        return (
            eager_tx.join(eager_p, on="product_id")
            .group_by("category")
            .agg(
                pl.col("net_revenue").sum().alias("rev"),
                pl.col("profit").sum().alias("profit"),
                pl.col("invoice_id").n_unique().alias("orders"),
            )
            .sort("rev", descending=True)
        )

    def lazy() -> pl.DataFrame:
        return (
            pl.scan_parquet(transactions_path)
            .join(pl.scan_parquet(products_path), on="product_id")
            .group_by("category")
            .agg(
                pl.col("net_revenue").sum().alias("rev"),
                pl.col("profit").sum().alias("profit"),
                pl.col("invoice_id").n_unique().alias("orders"),
            )
            .sort("rev", descending=True)
            .collect()
        )

    _, eager_s = _time(eager)
    _, lazy_s = _time(lazy)
    return [
        BenchResult("groupby_category", "eager", rows, eager_s),
        BenchResult("groupby_category", "lazy", rows, lazy_s),
    ]


def bench_rfm(
    transactions_path: Path = TRANSACTIONS_PARQUET,
    customers_path: Path = CUSTOMERS_PARQUET,
    products_path: Path = PRODUCTS_PARQUET,
) -> list[BenchResult]:
    """End-to-end RFM scoring."""
    eager_tx = pl.read_parquet(transactions_path).with_columns(
        pl.col("purchase_ts").dt.date().alias("purchase_date"),
        pl.col("purchase_ts").dt.truncate("1mo").dt.date().alias("month_start"),
    )
    rows = eager_tx.height

    def eager() -> pl.DataFrame:
        return compute_rfm(eager_tx.lazy()).collect()

    def lazy() -> pl.DataFrame:
        from retailvelocity.ingestion import load_transactions

        return compute_rfm(load_transactions(transactions_path)).collect()

    _, eager_s = _time(eager)
    _, lazy_s = _time(lazy)
    return [
        BenchResult("rfm_full", "eager", rows, eager_s),
        BenchResult("rfm_full", "lazy", rows, lazy_s),
    ]


def run_all() -> pl.DataFrame:
    """Execute every benchmark and return a tidy result frame."""
    results: list[BenchResult] = []
    results.extend(bench_groupby())
    results.extend(bench_rfm())
    return pl.DataFrame(
        [
            {
                "benchmark": r.name,
                "mode": r.mode,
                "rows": r.rows,
                "seconds": round(r.seconds, 3),
                "rows_per_sec": int(r.rows_per_sec),
            }
            for r in results
        ]
    )


if __name__ == "__main__":
    print(run_all())
