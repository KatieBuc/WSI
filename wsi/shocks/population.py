import pandas as pd
from wsi.utils import raw_data_path

def load_population_raw(file: str, sheet: str) -> pd.DataFrame:
    path = raw_data_path("shocks", file)
    return pd.read_excel(path, sheet_name=sheet, skiprows=16)


def process_population_data(df: pd.DataFrame, iso_codes: list[str] | None = None) -> pd.DataFrame:
    df = df[["ISO3 Alpha-code", "Year", "Total Population, as of 1 January (thousands)"]].dropna()
    df = df.rename(columns={
        "ISO3 Alpha-code": "ISO_code",
        "Total Population, as of 1 January (thousands)": "Population",
    })
    df["Year"] = df["Year"].astype(int)
    df["Population"] = pd.to_numeric(df["Population"], errors="coerce")

    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    return df.reset_index(drop=True)


def build_population_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    file = "WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx"
    df_hist = process_population_data(load_population_raw(file, "Estimates"), iso_codes)
    df_proj = process_population_data(load_population_raw(file, "Medium variant"), iso_codes)
    df_proj = df_proj[df_proj["Year"].isin([2024, 2025])]

    combined = pd.concat([df_hist, df_proj], ignore_index=True)
    combined['Population'] *= 1000
    combined.dropna(inplace=True)
    return combined
