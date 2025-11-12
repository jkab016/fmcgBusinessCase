"""Price index for Bidco vs peers by store × sub-dept × section with logging."""
from __future__ import annotations
import numpy as np, pandas as pd
from .logging_utils import get_logger, timeit

logger = get_logger(__name__)

@timeit(logger, "compute_price_index")
def compute_price_index(df: pd.DataFrame):
    work = df.copy()
    logger.info("Computing price index on %d rows", len(work))
    grp = work.groupby(["Store_Name","Sub_Department","Section","Supplier"]).agg(
        avg_price=("realised_unit_price","mean"),
        units=("Quantity","sum"),
    ).reset_index()

    bidco = grp[grp["Supplier"].str.contains("bidco", case=False, na=False)].copy()
    peers = grp[~grp["Supplier"].str.contains("bidco", case=False, na=False)].copy()

    peer_agg = peers.groupby(["Store_Name","Sub_Department","Section"]).apply(
        lambda g: pd.Series({
            "peer_avg_price": np.average(g["avg_price"], weights=g["units"]) if g["units"].sum()>0 else np.nan,
            "peer_units": g["units"].sum()
        })
    ).reset_index()

    bidco_agg = bidco.groupby(["Store_Name","Sub_Department","Section"]).apply(
        lambda g: pd.Series({
            "bidco_avg_price": np.average(g["avg_price"], weights=g["units"]) if g["units"].sum()>0 else np.nan,
            "bidco_units": g["units"].sum()
        })
    ).reset_index()

    idx = bidco_agg.merge(peer_agg, on=["Store_Name","Sub_Department","Section"], how="outer")
    idx["price_index"] = idx["bidco_avg_price"] / idx["peer_avg_price"]
    logger.info("Computed price index rows: %d", len(idx))

    def wavg(series, weights):
        mask = series.notna() & weights.notna()
        return np.average(series[mask], weights=weights[mask]) if mask.any() else np.nan

    rollup = pd.DataFrame({
        "bidco_avg_price_rollup": [wavg(idx["bidco_avg_price"], idx["bidco_units"])],
        "peer_avg_price_rollup": [wavg(idx["peer_avg_price"], idx["peer_units"])],
    })
    rollup["price_index_rollup"] = rollup["bidco_avg_price_rollup"] / rollup["peer_avg_price_rollup"]
    logger.info("Roll-up price index: %s", rollup["price_index_rollup"].iloc[0])
    return idx, rollup
