"""
CLI entrypoint orchestrating the pipeline with enhanced logging and timing.

Subcommands:
- data-quality  : compute data health scores by store/supplier
- promos        : detect promotions and compute uplift/coverage/price deltas
- pricing       : compute Bidco vs peers price index (store + roll-up)
- profile       : generate ydata-profiling HTML report
- run-all       : run all stages in sequence
"""
from __future__ import annotations
import time
import logging
from typing import Any

from modules.utils import parse_args, setup_logging
from modules.logging_utils import get_logger, timeit
from modules.io_ops import load_any, write_table
from modules.data_quality import score_health
from modules.promotions import detect_promotions
from modules.pricing_index import compute_price_index
from modules.reporting import generate_profile_html
setup_logging()
logger = get_logger(__name__)


@timeit(logger, "cmd_data_quality")
def cmd_data_quality(df, args) -> None:
    """
    Run data health scoring and write outputs.
    """
    logger.info(
        "Starting data-quality stage (extreme_price_factor=%s)",
        args.extreme_price_factor,
    )
    dq_store, dq_supplier = score_health(
        df, extreme_price_factor=args.extreme_price_factor
    )
    logger.info(
        "Data-quality summaries generated: stores=%d, suppliers=%d",
        len(dq_store),
        len(dq_supplier),
    )
    write_table(dq_store, args.output_dir,
                "data_quality_store", args.save_parquet)
    write_table(
        dq_supplier, args.output_dir, "data_quality_supplier", args.save_parquet
    )
    logger.info("Data-quality outputs written to %s", args.output_dir)


@timeit(logger, "cmd_promos")
def cmd_promos(df, args) -> None:
    """
    Run promotion detection and KPI computation and write outputs.
    """
    logger.info(
        "Starting promotions stage (discount_threshold=%.2f, min_days=%d)",
        args.promo_discount_threshold,
        args.promo_min_days,
    )
    promo_summary = detect_promotions(
        df, args.promo_discount_threshold, args.promo_min_days
    )
    logger.info("Promo summary shape: rows=%d, cols=%d", *promo_summary.shape)
    write_table(promo_summary, args.output_dir,
                "promo_summary", args.save_parquet)
    logger.info("Promotions output written to %s", args.output_dir)


@timeit(logger, "cmd_pricing")
def cmd_pricing(df, args) -> None:
    """
    Run Bidco vs peers pricing index and write outputs.
    """
    logger.info("Starting pricing stage")
    price_idx, rollup = compute_price_index(df)
    logger.info(
        "Computed pricing index (grain rows=%d), roll-up available=%s",
        len(price_idx),
        not rollup.empty,
    )
    write_table(price_idx, args.output_dir, "price_index", args.save_parquet)
    write_table(rollup, args.output_dir,
                "price_index_rollup", args.save_parquet)
    logger.info("Pricing outputs written to %s", args.output_dir)


@timeit(logger, "cmd_profile")
def cmd_profile(df, args) -> None:
    """
    Generate a ydata-profiling HTML report.
    """
    logger.info("Starting profiling stage (reports_dir=%s)", args.reports_dir)
    path = generate_profile_html(df, args.reports_dir)
    logger.info("Profile report generated at %s", path)


def main() -> None:
    """
    Execute the requested subcommand with configured parameters and logging.
    """
    args = parse_args()
    setup_logging(args.verbose)

    logger.info("=== Duck × Bidco Case Pipeline Started ===")
    try:
        logger.debug("Arguments: %s", vars(args))
    except Exception:
        # In rare cases, argparse namespace may not be serializable; ignore.
        pass

    logger.info("Loading data from %s", args.input_path)
    df = load_any(args.input_path)
    logger.info("Loaded %d rows", len(df))

    if args.command == "data-quality":
        cmd_data_quality(df, args)
    elif args.command == "promos":
        cmd_promos(df, args)
    elif args.command == "pricing":
        cmd_pricing(df, args)
    elif args.command == "profile":
        cmd_profile(df, args)
    elif args.command == "run-all":
        cmd_data_quality(df, args)
        cmd_promos(df, args)
        cmd_pricing(df, args)
        logger.info("Pipeline completed (run-all).")


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logger.info("Pipeline execution finished successfully.")
    logger.info("Total execution time: %.2f seconds", end_time - start_time)
