"""I/O helpers for loading data and writing outputs."""
from __future__ import annotations
import os
import pandas as pd
import numpy as np

ALIASES = {
    "Store_Name": ["Store", "StoreName", "Store_Name"],
    "Item_Code": ["Item_Code", "ItemCode", "SKU", "Sku_Code"],
    "Item_Barcode": ["Item_Barcode", "Barcode", "ItemBarcode"],
    "Description": ["Description", "Item_Description", "ItemDesc"],
    "Category": ["Category"],
    "Department": ["Department"],
    "Sub_Department": ["Sub_Department", "SubDepartment", "Sub_Dept"],
    "Section": ["Section", "Segment"],
    "Quantity": ["Quantity", "Qty", "Units"],
    "Total_Sales": ["Total_Sales", "Sales_Value", "Sales"],
    "RRP": ["RRP", "Price_RRP"],
    "Supplier": ["Supplier", "Vendor", "Manufacturer"],
    "Date_Of_Sale": ["Date_Of_Sale", "Sale_Date", "Transaction_Date", "Date"],
}


def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map aliases to a standard schema."""
    df.columns = [c.strip().replace(" ", "_").replace("-", "_")
                  for c in df.columns]
    out = {}
    for std, cands in ALIASES.items():
        chosen = next((c for c in cands if c in df.columns), None)
        out[std] = df[chosen] if chosen else np.nan
    return pd.DataFrame(out)


def load_any(path: str) -> pd.DataFrame:
    """Load Excel/CSV/Parquet and standardize columns and dtypes."""
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        raw = pd.read_excel(path)
    elif ext in [".csv", ".txt"]:
        raw = pd.read_csv(path)
    elif ext in [".parquet", ".pq"]:
        raw = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    df = _map_columns(raw)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Total_Sales"] = pd.to_numeric(df["Total_Sales"], errors="coerce")
    df["RRP"] = pd.to_numeric(df["RRP"], errors="coerce")
    df["Date_Of_Sale"] = pd.to_datetime(
        df["Date_Of_Sale"], errors="coerce").dt.date
    df["realised_unit_price"] = np.where(
        df["Quantity"] > 0, df["Total_Sales"]/df["Quantity"], np.nan)
    return df


def write_table(df: pd.DataFrame, out_dir: str, name: str, save_parquet: bool = False) -> str:
    """Write CSV (and optional Parquet)."""
    csv_path = os.path.join(out_dir, f"{name}.csv")
    df.to_csv(csv_path, index=False)
    if save_parquet:
        df.to_parquet(os.path.join(out_dir, f"{name}.parquet"), index=False)
    return csv_path
