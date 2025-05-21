# wsi/indicators/child_marriage.py

import pandas as pd
from wsi.utils import raw_data_path, clean_year_columns

CONFIG = {
    "child_marriage": {"file": "WorldBank_WorldDevelopmentIndicators_ChildMarriage.csv"}
}


def load_raw(name: str) -> pd.DataFrame:
    """Load the raw CSV by its CONFIG key."""
    cfg = CONFIG[name]
    path = raw_data_path(cfg["file"])
    return pd.read_csv(path)


def process_child_marriage_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Clean child marriage data.
    """
    # Drop trailing info rows
    df = df.iloc[:532].copy()
    clean_year_columns(df)
    df = df[df["Series Code"] == "SP.M18.2024.FE.ZS"]

    # Melt year columns
    years = [col for col in df.columns if col.isdigit()]
    df = df.melt(
        id_vars=["Country Code"],
        value_vars=years,
        var_name="Year",
        value_name="Child Marriage",
    )

    # Drop missing and cast types
    df = df.dropna(subset=["Child Marriage"]).reset_index(drop=True)
    df["Year"] = df["Year"].astype(int)
    df = df.rename(columns={"Country Code": "ISO_code"})

    # Filter by ISO codes if provided
    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    return df[["ISO_code", "Year", "Child Marriage"]]


def build_child_marriage_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Build a dataframe of child marriage rates by ISO and Year.
    """
    raw = load_raw("child_marriage")
    cm_df = process_child_marriage_raw(raw, iso_codes)
    return cm_df
