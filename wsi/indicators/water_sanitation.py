# wsi/indicators/water_sanitation.py

import pandas as pd
from wsi.utils import raw_data_path, clean_year_columns

# which series codes to include and their weights
CODE_NAME = {
    "SH.STA.BASS.ZS": "basic sanitation services",
    "SH.H2O.BASW.ZS": "basic drinking water",
}
WEIGHTS = {"SH.STA.BASS.ZS": 0.67, "SH.H2O.BASW.ZS": 0.33}

CONFIG = {
    "water_sanitation": {
        "file": "WorldBank_WorldDevelopmentIndicators_WaterSanitation.csv"
    }
}


def load_raw(name: str) -> pd.DataFrame:
    """Load the raw CSV by its CONFIG key, trimming trailing NA rows."""
    cfg = CONFIG[name]
    path = raw_data_path(cfg["file"])
    return pd.read_csv(path)


def process_water_sanitation_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Process water & sanitation data.
    """
    df = df.iloc[:3192, :]  # delete trailing na rows
    clean_year_columns(df)
    df = df[df["Series Code"].isin(CODE_NAME)]

    years = [col for col in df.columns if col.isdigit()]
    df_long = df.melt(
        id_vars=["Country Code", "Series Code"],
        value_vars=years,
        var_name="Year",
        value_name="Value",
    )
    df_long["Year"] = df_long["Year"].astype(int)

    # pivot to have series codes as columns
    pivot = df_long.pivot_table(
        index=["Country Code", "Year"], columns="Series Code", values="Value"
    ).reset_index()
    pivot.columns.name = None

    # fill missing within each country only when one series present
    def fill_group(g):
        if g[["SH.STA.BASS.ZS", "SH.H2O.BASW.ZS"]].notna().any(axis=1).all():
            return g.ffill().bfill()
        return g

    pivot = pivot.groupby("Country Code").apply(fill_group).reset_index(drop=True)

    # compute weighted average
    pivot["Access Water Sanitation"] = (
        pivot["SH.STA.BASS.ZS"] * WEIGHTS["SH.STA.BASS.ZS"]
        + pivot["SH.H2O.BASW.ZS"] * WEIGHTS["SH.H2O.BASW.ZS"]
    ) / sum(WEIGHTS.values())

    # filter iso
    pivot = pivot.rename(columns={"Country Code": "ISO_code"})
    if iso_codes is not None:
        pivot = pivot[pivot["ISO_code"].isin(iso_codes)]

    return pivot[["ISO_code", "Year", "Access Water Sanitation"]]


def build_water_sanitation_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Build Access Water Sanitation indicator by ISO and Year.
    """
    raw = load_raw("water_sanitation")
    ws_df = process_water_sanitation_raw(raw, iso_codes)
    return ws_df
