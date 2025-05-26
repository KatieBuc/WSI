# wsi/indicators/electricity.py

import pandas as pd
from wsi.utils import raw_data_path, clean_year_columns

CONFIG = {
    "access_electricity": {
        "file": "WorldBank_WorldDevelopmentIndicators_AccessElectricity.csv",
        "series_code": "EG.ELC.ACCS.ZS",
        "indicator_name": "Access Electricity",
        "max_rows": 266,
    }
}


def load_raw(name: str) -> pd.DataFrame:
    """Load the raw CSV by its CONFIG key."""
    cfg = CONFIG[name]
    path = raw_data_path("indicators", cfg["file"])
    return pd.read_csv(path)


def process_access_electricity_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Process access to electricity data.
    """
    cfg = CONFIG["access_electricity"]
    df = df.iloc[: cfg["max_rows"], :]  # drop trailing NA rows
    clean_year_columns(df)

    df = df[df["Series Code"] == cfg["series_code"]]
    years = [col for col in df.columns if col.isdigit()]

    df_long = df.melt(
        id_vars=["Country Code"],
        value_vars=years,
        var_name="Year",
        value_name=cfg["indicator_name"],
    )

    df_long = df_long.dropna(subset=[cfg["indicator_name"]]).reset_index(drop=True)
    df_long["Year"] = df_long["Year"].astype(int)
    df_long = df_long.rename(columns={"Country Code": "ISO_code"})

    if iso_codes is not None:
        df_long = df_long[df_long["ISO_code"].isin(iso_codes)]

    return df_long[["ISO_code", "Year", cfg["indicator_name"]]]


def build_access_electricity_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Build Access Electricity indicator by ISO and Year.
    """
    raw = load_raw("access_electricity")
    ae_df = process_access_electricity_raw(raw, iso_codes)
    return ae_df
