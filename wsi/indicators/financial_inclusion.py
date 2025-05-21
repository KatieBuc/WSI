# wsi/indicators/financial_inclusion.py

import pandas as pd
from wsi.utils import raw_data_path

CONFIG = {
    "financial_inclusion": {
        "file": "DatabankWide.xlsx",
        "column_map": {
            "Country code": "ISO_code",
            "Financial institution account, female (% age 15+)": "Financial Inclusion",
        },
        "indicator_name": "Financial Inclusion",
    }
}


def load_raw(name: str) -> pd.DataFrame:
    """Load the raw Excel file from the config."""
    cfg = CONFIG[name]
    path = raw_data_path(cfg["file"])
    return pd.read_excel(path)


def process_financial_inclusion_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Process financial inclusion data.
    """
    cfg = CONFIG["financial_inclusion"]
    df = df.rename(columns=cfg["column_map"])
    df["Year"] = df["Year"].astype(int)

    df = df[["ISO_code", "Year", cfg["indicator_name"]]]
    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    return df


def build_financial_inclusion_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Build Financial Inclusion indicator by ISO and Year.
    """
    raw = load_raw("financial_inclusion")
    df = process_financial_inclusion_raw(raw, iso_codes)
    return df
