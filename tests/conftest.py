"""Shared pytest fixtures.

We generate a small synthetic dataset once per session and expose it to every
test — keeps tests fast (~1s per suite) while still exercising the real pipeline.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from retailvelocity.data_gen import GenConfig, generate
from retailvelocity.ingestion import load_enriched


@pytest.fixture(scope="session")
def small_dataset(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    out = tmp_path_factory.mktemp("rv_data")
    cfg = GenConfig(
        n_customers=300,
        n_products=80,
        target_rows=4000,
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        seed=7,
    )
    return generate(cfg, out_dir=out)


@pytest.fixture
def enriched(small_dataset: dict[str, Path]):
    return load_enriched(
        transactions_path=small_dataset["transactions"],
        customers_path=small_dataset["customers"],
        products_path=small_dataset["products"],
    )
