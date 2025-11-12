"""Data health scoring per store/supplier with logging."""
from __future__ import annotations
import pandas as pd, numpy as np
from .logging_utils import get_logger, timeit

logger = get_logger(__name__)

@timeit(logger, "score_health")
def score_health(df: pd.DataFrame, extreme_price_factor: float = 10.0):
    work = df.copy()
    logger.info("Scoring data health on %d rows", len(work))
    work["missing_any"] = work.isna().any(axis=1)
    work["neg_qty"] = work["Quantity"] < 0
    work["bad_rrp"] = (work["RRP"] <= 0) | (work["RRP"].isna())
    work["extreme_price"] = (work["realised_unit_price"] > extreme_price_factor * work["RRP"]) | (work["realised_unit_price"] < (1.0/extreme_price_factor) * work["RRP"])

    key_cols = ["Store_Name","Date_Of_Sale","Item_Code"]
    if work[key_cols].isna().any().any():
        key_cols = ["Store_Name","Date_Of_Sale","Item_Barcode","Description"]
    work["dup_key"] = work.duplicated(subset=key_cols, keep=False)

    def summarize(group_cols):
        g = work.groupby(group_cols)
        res = pd.DataFrame({
            "rows": g.size(),
            "missing_rate": g["missing_any"].mean(),
            "dup_rate": g["dup_key"].mean(),
            "neg_qty_rate": g["neg_qty"].mean(),
            "bad_rrp_rate": g["bad_rrp"].mean(),
            "extreme_price_rate": g["extreme_price"].mean(),
        }).reset_index()
        validity_penalty = res[["neg_qty_rate","bad_rrp_rate","extreme_price_rate"]].max(axis=1)
        res["score_completeness"] = 1 - res["missing_rate"]
        res["score_uniqueness"] = 1 - res["dup_rate"]
        res["score_validity"] = 1 - validity_penalty
        rrps = work.groupby(group_cols + ["Item_Code"])["RRP"].agg(["mean","std"]).reset_index()
        stab = rrps.groupby(group_cols)["std"].mean().reset_index().rename(columns={"std":"avg_rrp_std"})
        res = res.merge(stab, on=group_cols, how="left")
        s = res["avg_rrp_std"]
        res["score_consistency"] = 1 - (s - s.min()) / (s.max() - s.min()) if s.notna().sum()>1 else 1.0
        w = {"score_completeness":0.30, "score_uniqueness":0.30, "score_validity":0.25, "score_consistency":0.15}
        res["data_health_score"] = (res["score_completeness"]*w["score_completeness"] + res["score_uniqueness"]*w["score_uniqueness"] + res["score_validity"]*w["score_validity"] + res["score_consistency"]*w["score_consistency"]) * 100
        num_cols = ["missing_rate","dup_rate","neg_qty_rate","bad_rrp_rate","extreme_price_rate","data_health_score"]
        res[num_cols] = res[num_cols].round(4)
        return res

    store, supplier = summarize(["Store_Name"]), summarize(["Supplier"])
    logger.info("Health summaries — stores: %d, suppliers: %d", len(store), len(supplier))
    return store, supplier
