# wsi/indicators/son_bias.py

import pandas as pd
from wsi.utils import raw_data_path

CONFIG = {
    # Historical estimates
    "son_bias": {
        "file": "WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx",
        "sheet": "Estimates",
        "skiprows": 16,
    },
    # Medium variant projections
    "son_bias_medium": {
        "file": "WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx",
        "sheet": "Medium variant",
        "skiprows": 16,
    },
}


def load_raw(name: str) -> pd.DataFrame:
    """Load the specified sheet from the Excel file, applying skiprows."""
    cfg = CONFIG[name]
    path = raw_data_path(cfg["file"])
    return pd.read_excel(path, sheet_name=cfg["sheet"], skiprows=cfg.get("skiprows", 0))


def process_son_bias_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Select & rename columns, drop missing, cast year, filter by iso_codes.
    """
    cols = [
        "ISO3 Alpha-code",
        "Year",
        "Sex Ratio at Birth (males per 100 female births)",
    ]
    df = df[cols].dropna(subset=cols)

    df = df.rename(
        columns={
            "ISO3 Alpha-code": "ISO_code",
            "Sex Ratio at Birth (males per 100 female births)": "Son Bias",
        }
    )
    df["Year"] = df["Year"].astype(int)
    df["Son Bias"] = pd.to_numeric(df["Son Bias"], errors="coerce")

    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    return df.reset_index(drop=True)


def build_son_bias_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Build son-bias dataframe including historical estimates and
    medium-variant projections for 2024 & 2025.
    """
    # Historical data
    raw_hist = load_raw("son_bias")
    hist_son_bias = process_son_bias_raw(raw_hist, iso_codes)

    # Projection data (years 2024 & 2025)
    raw_proj = load_raw("son_bias_medium")
    proj_son_bias = process_son_bias_raw(raw_proj, iso_codes).query(
        "Year in [2024, 2025]"
    )

    # Combine
    son_bias_df = pd.concat(
        [
            hist_son_bias[["ISO_code", "Year", "Son Bias"]],
            proj_son_bias[["ISO_code", "Year", "Son Bias"]],
        ],
        ignore_index=True,
    )

    return son_bias_df
