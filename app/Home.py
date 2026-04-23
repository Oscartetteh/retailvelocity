"""RetailVelocity — Streamlit home page.

Run: ``uv run streamlit run app/Home.py``
"""

from __future__ import annotations

import polars as pl
import streamlit as st

from retailvelocity.config import TRANSACTIONS_PARQUET
from retailvelocity.ingestion import dataset_summary, load_enriched

st.set_page_config(
    page_title="RetailVelocity",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(show_spinner="Loading summary...")
def _summary() -> dict[str, int | float]:
    return dataset_summary(load_enriched())


def _format_dollars(x: float) -> str:
    if x >= 1_000_000:
        return f"${x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x / 1_000:.1f}K"
    return f"${x:.0f}"


def main() -> None:
    st.title("RetailVelocity")
    st.caption("High-performance e-commerce analytics · Polars + Streamlit")

    if not TRANSACTIONS_PARQUET.exists():
        st.warning(
            "No transaction data found. Run `uv run retailvelocity generate` "
            "(or `uv run rv-generate`) to create the synthetic dataset."
        )
        return

    s = _summary()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Transactions", f"{s['rows']:,}")
    col2.metric("Customers", f"{s['customers']:,}")
    col3.metric("Gross Revenue", _format_dollars(float(s["gross_revenue"])))
    col4.metric(
        "Gross Profit",
        _format_dollars(float(s["gross_profit"])),
        delta=f"{float(s['gross_profit']) / float(s['gross_revenue']) * 100:.1f}% margin",
    )

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Orders", f"{s['orders']:,}")
    col6.metric("Products", f"{s['products']:,}")
    col7.metric("First day", str(s["first_day"]))
    col8.metric("Last day", str(s["last_day"]))

    st.divider()
    st.markdown(
        """
### Navigation

- **Trends** — revenue, profit, orders over time with rolling smoothing
- **RFM Segments** — customer tiers + win-back list
- **Cohort Retention** — acquisition-month cohort heatmap
- **Forecast** — per-SKU demand forecast with confidence bands
- **Prescriptive** — reorder report, dead-stock, at-risk dollars

Use the sidebar to switch pages.
        """
    )


if __name__ == "__main__":
    main()
