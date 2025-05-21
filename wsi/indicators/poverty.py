# wsi/indicators/poverty.py

import pandas as pd
from wsi.utils import raw_data_path
from wsi.map_country_iso import get_iso

CONFIG = {
    "poverty": {
        "file": "Goal1.xlsx",
    }
}


def load_raw(name: str) -> pd.DataFrame:
    """Helper to load the raw Excel by its CONFIG key"""
    file_name = CONFIG[name]["file"]
    full_path = raw_data_path(file_name)
    return pd.read_excel(full_path)


def process_poverty_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Filter to indicator 1.1.1, location ALLAREA, both sexes and all ages,
    map to ISO codes and optionally filter by iso_codes.
    """
    # Filter relevant rows
    df = df.loc[
        (df["Indicator"] == "1.1.1")
        & (df["Location"] == "ALLAREA")
        & (df["Sex"] == "BOTHSEX")
        & (df["Age"] == "ALLAGE")
    ].copy()

    # ignore continents, world bank puts regions into this column too
    def ignore_get_iso(name: str):
        try:
            return get_iso(name)
        except KeyError:
            return pd.NA

    df["ISO_code"] = df["GeoAreaName"].apply(ignore_get_iso)
    df = df.dropna(subset=["ISO_code"])

    # Optional ISO filter
    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    return df.reset_index(drop=True)


def transform_poverty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename and cast columns, select final fields.
    """
    df = df.rename(columns={"TimePeriod": "Year", "Value": "Poverty"})
    df["Year"] = df["Year"].astype(int)
    return df[["ISO_code", "Year", "Poverty"]].reset_index(drop=True)


def build_poverty_df(
    iso_codes: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ """
    # Load raw
    raw = load_raw("poverty")

    # Process
    poverty_raw = process_poverty_raw(raw, iso_codes)

    # Transform
    poverty_df = transform_poverty(poverty_raw)

    return poverty_df
