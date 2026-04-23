"""Top-level CLI — thin dispatcher over data_gen / benchmarks / summary."""

from __future__ import annotations

import argparse
import sys

from retailvelocity import __version__


def _cmd_generate(args: argparse.Namespace) -> int:
    from datetime import datetime

    from retailvelocity.data_gen import GenConfig, generate

    cfg = GenConfig(
        n_customers=args.customers,
        n_products=args.products,
        target_rows=args.rows,
        start_date=datetime.strptime(args.start, "%Y-%m-%d").date(),
        end_date=datetime.strptime(args.end, "%Y-%m-%d").date(),
        seed=args.seed,
    )
    paths = generate(cfg)
    for name, path in paths.items():
        print(f"{name:>13}: {path}")
    return 0


def _cmd_summary(_: argparse.Namespace) -> int:
    from retailvelocity.ingestion import dataset_summary, load_enriched

    summary = dataset_summary(load_enriched())
    width = max(len(k) for k in summary)
    for k, v in summary.items():
        print(f"{k.rjust(width)} : {v}")
    return 0


def _cmd_benchmark(_: argparse.Namespace) -> int:
    from retailvelocity.benchmarks import run_all

    print(run_all())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="retailvelocity", description="RetailVelocity CLI")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="Generate synthetic data")
    g.add_argument("--customers", type=int, default=50_000)
    g.add_argument("--products", type=int, default=2_000)
    g.add_argument("--rows", type=int, default=1_000_000)
    g.add_argument("--start", default="2022-01-01")
    g.add_argument("--end", default="2024-12-31")
    g.add_argument("--seed", type=int, default=42)
    g.set_defaults(func=_cmd_generate)

    s = sub.add_parser("summary", help="Print dataset summary")
    s.set_defaults(func=_cmd_summary)

    b = sub.add_parser("benchmark", help="Run performance benchmarks")
    b.set_defaults(func=_cmd_benchmark)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
