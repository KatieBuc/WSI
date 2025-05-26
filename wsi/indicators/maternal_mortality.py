# wsi/indicators/maternal_mortality.py

import pandas as pd
from wsi.utils import raw_data_path, ignore_get_iso

CONFIG = {"maternal_mortality": {"file": "maternal_mortality.xlsx", "sheet": "Goal3"}}


def load_raw(name: str) -> pd.DataFrame:
    """Load the specified sheet from the Excel file."""
    cfg = CONFIG[name]
    path = raw_data_path("indicators", cfg["file"])
    return pd.read_excel(path, sheet_name=cfg.get("sheet"))


def process_maternal_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Map GeoAreaName to ISO, select and rename columns, and optionally filter by iso_codes.
    """
    # Map to ISO, drop aggregates
    df["ISO_code"] = df["GeoAreaName"].apply(ignore_get_iso)
    df = df.dropna(subset=["ISO_code"])

    # Rename and cast
    df = df.rename(columns={"TimePeriod": "Year", "Value": "Maternal Mortality"})
    df["Year"] = df["Year"].astype(int)
    df["Maternal Mortality"] = pd.to_numeric(df["Maternal Mortality"], errors="coerce")

    # Optional ISO filter
    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    return df[["ISO_code", "Year", "Maternal Mortality"]].reset_index(drop=True)


def build_maternal_mortality_df(
    iso_codes: list[str] | None = None,
) -> pd.DataFrame:
    """
    Build maternal mortality DataFrame.
    """
    # Load
    raw = load_raw("maternal_mortality")

    # Process
    maternal_mortality_df = process_maternal_raw(raw, iso_codes)

    return maternal_mortality_df
