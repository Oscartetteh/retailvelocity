"""Central configuration constants."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

TRANSACTIONS_PARQUET = RAW_DIR / "transactions.parquet"
PRODUCTS_PARQUET = RAW_DIR / "products.parquet"
CUSTOMERS_PARQUET = RAW_DIR / "customers.parquet"

DEFAULT_SEED = 42

RFM_TIERS = ("Platinum", "Gold", "Silver", "Bronze")
