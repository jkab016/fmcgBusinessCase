"""Utilities for argparse and logging."""
from __future__ import annotations
import argparse, logging, os

def setup_logging(verbosity: int = 1) -> None:
    """Configure logging."""
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

def parse_args() -> argparse.Namespace:
    """Parse CLI args and ensure output directories exist."""
    p = argparse.ArgumentParser(prog="fmcgBusinessCase", description="KPIs pipeline")
    p.add_argument("--input_path", default="C:/Users/jkab0/OneDrive/Documents/GitHub/fmcgBusinessCase/data/Test_Data.xlsx")
    p.add_argument("--output_dir", default="C:/Users/jkab0/OneDrive/Documents/GitHub/fmcgBusinessCase/output")
    p.add_argument("--reports_dir", default="C:/Users/jkab0/OneDrive/Documents/GitHub/fmcgBusinessCase/reports")
    p.add_argument("--viz_dir", default="C:/Users/jkab0/OneDrive/Documents/GitHub/fmcgBusinessCase/viz")
    p.add_argument("--promo_discount_threshold", type=float, default=0.10)
    p.add_argument("--promo_min_days", type=int, default=2)
    p.add_argument("--extreme_price_factor", type=float, default=10.0)
    p.add_argument("--save_parquet", action="store_true")
    p.add_argument("-v","--verbose", action="count", default=0)
    sub = p.add_subparsers(dest="command", required=True)
    for cmd in ["data-quality","promos","pricing","profile","run-all"]:
        sub.add_parser(cmd)
    args = p.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.reports_dir, exist_ok=True)
    os.makedirs(args.viz_dir, exist_ok=True)
    return args
