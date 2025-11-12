"""Streamlit dashboard for Duck × Bidco KPIs."""
import os, pandas as pd, streamlit as st

def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

def main():
    st.set_page_config(page_title="Duck × Bidco — Retail KPIs", layout="wide")
    st.title("Duck × Bidco — Retail KPIs")

    out_dir = st.sidebar.text_input("Output directory", value="output")
    dq_store = load_csv(os.path.join(out_dir, "data_quality_store.csv"))
    dq_supp  = load_csv(os.path.join(out_dir, "data_quality_supplier.csv"))
    promo    = load_csv(os.path.join(out_dir, "promo_summary.csv"))
    pindex   = load_csv(os.path.join(out_dir, "price_index.csv"))
    rollup   = load_csv(os.path.join(out_dir, "price_index_rollup.csv"))

    tab1, tab2, tab3, tab4 = st.tabs(["Data Health", "Promotions", "Pricing Index", "Insights"])

    with tab1:
        st.subheader("Data Health — by Store")
        if not dq_store.empty:
            thresh = st.slider("Unreliable score threshold", 0, 100, 70, 1)
            dq = dq_store.copy()
            max_issue = dq[["missing_rate","dup_rate","neg_qty_rate","bad_rrp_rate","extreme_price_rate"]].max(axis=1)
            dq["unreliable"] = (dq["data_health_score"] < thresh) | (max_issue > 0.10)
            st.dataframe(dq.sort_values("data_health_score"))
            st.metric("Median score", f"{dq['data_health_score'].median():.1f}")
            st.metric("Unreliable stores", int(dq['unreliable'].sum()))
        st.subheader("Data Health — by Supplier")
        if not dq_supp.empty:
            st.dataframe(dq_supp.sort_values("data_health_score"))

    with tab2:
        st.subheader("Promotions & Performance")
        if not promo.empty:
            cols = st.multiselect("Columns to view", ["Item_Code","Description","Supplier","Sub_Department","Section",
                                                      "baseline_units","promo_units","promo_uplift_pct","promo_coverage_sku",
                                                      "avg_discount_depth_all","avg_price_all","avg_rrp_all","units_all"],
                                  default=["Item_Code","Description","Supplier","promo_uplift_pct","promo_coverage_sku","avg_discount_depth_all"])
            view = promo[cols].copy()
            st.dataframe(view.sort_values(["promo_uplift_pct","promo_coverage_sku"], ascending=[False, False]).head(50))
            if "avg_discount_depth_all" in promo.columns and "promo_uplift_pct" in promo.columns:
                sc = promo[["avg_discount_depth_all","promo_uplift_pct"]].dropna().rename(
                    columns={"avg_discount_depth_all":"avg_discount_depth", "promo_uplift_pct":"uplift_pct"})
                if not sc.empty:
                    st.scatter_chart(sc)

    with tab3:
        st.subheader("Pricing Index")
        if not pindex.empty:
            stores = ["(All)"] + sorted([s for s in pindex["Store_Name"].dropna().unique().tolist()])
            store = st.selectbox("Store", stores)
            dfv = pindex.copy()
            if store != "(All)":
                dfv = dfv[dfv["Store_Name"] == store]
            st.dataframe(dfv.sort_values("price_index"))
            if not rollup.empty and "price_index_rollup" in rollup.columns:
                st.metric("Roll-up Price Index", f"{rollup['price_index_rollup'].iloc[0]:.3f}")

    with tab4:
        st.subheader("Decision-ready insights")
        if not promo.empty:
            hi_roi = promo[(promo["promo_uplift_pct"] >= 0.4) & (promo["avg_discount_depth_all"] <= 0.15)]
            over_disc = promo[(promo["avg_discount_depth_all"] >= 0.25) & (promo["promo_uplift_pct"] <= 0.10)]
            st.markdown("**High-ROI promos to repeat** (uplift ≥ 40%, discount ≤ 15%):")
            st.dataframe(hi_roi[["Item_Code","Description","Supplier","promo_uplift_pct","avg_discount_depth_all"]].sort_values("promo_uplift_pct", ascending=False).head(20))
            st.markdown("**Over-discounted SKUs** (discount ≥ 25% with low uplift ≤ 10%):")
            st.dataframe(over_disc[["Item_Code","Description","Supplier","promo_uplift_pct","avg_discount_depth_all"]].sort_values("avg_discount_depth_all", ascending=False).head(20))

if __name__ == "__main__":
    main()
