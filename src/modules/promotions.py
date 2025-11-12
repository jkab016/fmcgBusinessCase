"""
Promotion detection and KPI computations: uplift, coverage, and price deltas.
"""
from __future__ import annotations
import logging
import numpy as np
import pandas as pd
from .logging_utils import get_logger, timeit

logger = get_logger(__name__)


@timeit(logger, "detect_promotions")
def detect_promotions(df: pd.DataFrame, discount_threshold: float = 0.10, promo_min_days: int = 2) -> pd.DataFrame:
    """
    Tag per-row promo flags and aggregate to SKU/store-level promo days, uplift and coverage.
    Adds baseline vs promo average realised unit prices.
    """
    work = df.copy()
    logger.info("Starting promo detection on %d rows", len(work))
    work["discount_depth"] = (
        work["RRP"] - work["realised_unit_price"]) / work["RRP"]
    work["is_promo_day"] = (work["realised_unit_price"] <= (
        1 - discount_threshold) * work["RRP"]) & work["RRP"].notna() & work["realised_unit_price"].notna()

    promo_days = (
        work.dropna(subset=["Date_Of_Sale"])
        .groupby(["Store_Name", "Item_Code", "Date_Of_Sale"])["is_promo_day"]
        .max()
        .groupby(level=[0, 1])
        .sum()
        .reset_index(name="promo_days")
    )
    work = work.merge(promo_days, on=["Store_Name", "Item_Code"], how="left")
    work["on_promo"] = work["promo_days"].fillna(0) >= promo_min_days
    logger.info("SKUs on_promo across stores: %d", int(
        work.groupby(["Store_Name", "Item_Code"])["on_promo"].max().sum()))

    daily = work.groupby(["Store_Name", "Item_Code", "Date_Of_Sale"]).agg(
        units=("Quantity", "sum"),
        price=("realised_unit_price", "mean"),
        rrp=("RRP", "mean"),
        promo=("is_promo_day", "max"),
    ).reset_index()

    base = daily.loc[~daily["promo"].fillna(False)].groupby(["Store_Name", "Item_Code"])[
        "units"].mean().reset_index().rename(columns={"units": "baseline_units"})
    prom = daily.loc[daily["promo"].fillna(False)].groupby(["Store_Name", "Item_Code"])[
        "units"].mean().reset_index().rename(columns={"units": "promo_units"})
    uplift = base.merge(prom, on=["Store_Name", "Item_Code"], how="outer").fillna(
        {"baseline_units": 0.0, "promo_units": 0.0})
    # uplift["promo_uplift_pct"] = np.where(uplift["baseline_units"]>0, (uplift["promo_units"]-uplift["baseline_units"])/uplift["baseline_units"], np.nan)
    uplift["promo_days_count"] = (
        daily[daily["promo"].fillna(False)]
        .groupby(["Store_Name", "Item_Code"])["Date_Of_Sale"]
        .nunique()
        .reset_index(drop=True)
    )

    uplift["baseline_days_count"] = (
        daily[~daily["promo"].fillna(False)]
        .groupby(["Store_Name", "Item_Code"])["Date_Of_Sale"]
        .nunique()
        .reset_index(drop=True)
    )

    # only compute if both sides have enough data
    mask = (uplift["baseline_days_count"] >= 2) & (
        uplift["promo_days_count"] >= 2)
    uplift.loc[~mask, "promo_uplift_pct"] = np.nan

    # avoid -1 artefact from literal 0 promo units
    uplift.loc[uplift["promo_units"] == 0, "promo_uplift_pct"] = np.nan

    base_p = daily.loc[~daily["promo"].fillna(False)].groupby(["Item_Code"])[
        "price"].mean().reset_index().rename(columns={"price": "baseline_avg_price"})
    promo_p = daily.loc[daily["promo"].fillna(False)].groupby(["Item_Code"])[
        "price"].mean().reset_index().rename(columns={"price": "promo_avg_price"})

    sku_store_promo = work.groupby(["Item_Code", "Store_Name"])[
        "on_promo"].max().reset_index()
    sku_coverage = sku_store_promo.groupby("Item_Code")["on_promo"].mean(
    ).reset_index().rename(columns={"on_promo": "promo_coverage_sku"})

    price_stats = work.groupby(["Item_Code", "on_promo"]).agg(
        avg_price=("realised_unit_price", "mean"),
        avg_rrp=("RRP", "mean"),
        avg_discount_depth=("discount_depth", "mean"),
        units=("Quantity", "sum"),
    ).reset_index()

    summary = (
        uplift
        .merge(work[["Item_Code", "Description", "Supplier", "Sub_Department", "Section"]].drop_duplicates(), on="Item_Code", how="left")
        .merge(sku_coverage, on="Item_Code", how="left")
        .merge(price_stats.groupby("Item_Code").agg(
            avg_price_all=("avg_price", "mean"),
            avg_rrp_all=("avg_rrp", "mean"),
            avg_discount_depth_all=("avg_discount_depth", "mean"),
            units_all=("units", "sum"),
        ).reset_index(), on="Item_Code", how="left")
        .merge(base_p, on="Item_Code", how="left")
        .merge(promo_p, on="Item_Code", how="left")
    )
    logger.info("Promotion summary rows: %d", len(summary))
    return summary
