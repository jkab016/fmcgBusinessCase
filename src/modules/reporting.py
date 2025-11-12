"""ydata-profiling integration."""
from __future__ import annotations
import os, pandas as pd

def generate_profile_html(df: pd.DataFrame, reports_dir: str, filename: str = "data_profile.html") -> str:
    """Generate profiling HTML using ydata-profiling and return its path."""
    from ydata_profiling import ProfileReport
    profile = ProfileReport(df, title="Bidco POS Data Profile", explorative=True, minimal=True)
    out_path = os.path.join(reports_dir, filename)
    profile.to_file(out_path)
    return out_path
