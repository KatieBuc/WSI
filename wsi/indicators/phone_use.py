# wsi/indicators/cell_phone_use.py

import pandas as pd
from wsi.utils import raw_data_path

CONFIG = {
    "cell_phone_use": {
        "file": "API_IT.CEL.SETS.P2_DS2_en_csv_v2_10572.csv",
        "series_code": "IT.CEL.SETS.P2",  # included for consistency, though unused
        "indicator_name": "Cell Phone Use",
    }
}


def load_raw(name: str) -> pd.DataFrame:
    """Load raw CSV by CONFIG key, skipping metadata header rows."""
    cfg = CONFIG[name]
    path = raw_data_path("indicators", cfg["file"])
    return pd.read_csv(path, skiprows=4)


def process_cell_phone_use_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Process cell phone use per 100 people data.
    """
    cfg = CONFIG["cell_phone_use"]
    years = [col for col in df.columns if col.isdigit()]

    df_long = df.melt(
        id_vars=["Country Code"],
        value_vars=years,
        var_name="Year",
        value_name=cfg["indicator_name"],
    )
    df_long["Year"] = df_long["Year"].astype(int)

    df_long = df_long.rename(columns={"Country Code": "ISO_code"})
    if iso_codes is not None:
        df_long = df_long[df_long["ISO_code"].isin(iso_codes)]

    return df_long[["ISO_code", "Year", cfg["indicator_name"]]]


def build_cell_phone_use_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Build Cell Phone Use indicator by ISO and Year.
    """
    raw = load_raw("cell_phone_use")
    df = process_cell_phone_use_raw(raw, iso_codes)
    return df
