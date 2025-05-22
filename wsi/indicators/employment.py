# wsi/indicators/employment.py

import pandas as pd
from wsi.utils import raw_data_path
from wsi.mapping.country_iso import get_iso


CONFIG = {
    "population": {
        "file": "Population_estimates_projections.csv",
    },
    "employment": {
        "file": "EMP_DWAP_SEX_AGE_RT_A-filtered-2025-05-21.csv",
    },
}


def load_raw(name: str) -> pd.DataFrame:
    """Helper to load the raw CSV by its CONFIG key"""
    file_name = CONFIG[name]["file"]
    full_path = raw_data_path(file_name)  # e.g. .../WSI/data/raw/NATMON_....csv
    return pd.read_csv(full_path)


def process_population_data(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    # Apply ISO filter
    if iso_codes is not None:
        df = df[df["Country Code"].isin(iso_codes)]

    # Simplify year columns
    df.columns = [col.split()[0] if "YR" in col else col for col in df.columns]
    year_columns = df.columns[4:]

    # Melt to long format
    df = pd.melt(
        df,
        id_vars=["Country Name", "Country Code", "Series Name", "Series Code"],
        value_vars=year_columns,
        var_name="Year",
        value_name="Population",
    ).rename(columns={"Country Code": "ISO_code"})
    df["Population"] = pd.to_numeric(df["Population"], errors="coerce")
    df["Year"] = df["Year"].astype(int)

    # Map and filter age groups
    age_group_mapping = {
        "SP.POP.2529.FE": "25-34",
        "SP.POP.3034.FE": "25-34",
        "SP.POP.3539.FE": "35-44",
        "SP.POP.4044.FE": "35-44",
        "SP.POP.4549.FE": "45-54",
        "SP.POP.5054.FE": "45-54",
        "SP.POP.5559.FE": "55-64",
        "SP.POP.6064.FE": "55-64",
    }
    df["Age Group"] = df["Series Code"].map(age_group_mapping)
    df = df[df["Age Group"].notna()]

    # Aggregate
    return df.groupby(["ISO_code", "Year", "Age Group"], as_index=False)[
        "Population"
    ].sum()


def process_employment_data(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    df["ISO_code"] = df["ref_area.label"].apply(lambda name: get_iso(name))

    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    df = df.rename(columns={"time": "Year", "obs_value": "Employment%"})
    df["Age Group"] = df["classif1.label"].str.extract(r"(\d{2}-\d{2})")
    df["Year"] = df["Year"].astype(int)
    return df[["ISO_code", "Year", "Age Group", "Employment%"]]


def calculate_employment_metric(
    em_df: pd.DataFrame, pop_df: pd.DataFrame
) -> pd.DataFrame:
    merged = em_df.merge(pop_df, on=["ISO_code", "Year", "Age Group"], how="left")
    merged["Employment"] = merged["Population"] * merged["Employment%"]
    merged = merged.dropna(subset=["Employment"])
    agg = merged.groupby(["ISO_code", "Year"], as_index=False).agg(
        {"Population": "sum", "Employment": "sum"}
    )
    agg["Employment"] = agg["Employment"] / agg["Population"]
    return agg[["ISO_code", "Year", "Employment"]].reset_index(drop=True)


def build_employment_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    # Load raw
    pop_raw = load_raw("population")
    emp_raw = load_raw("employment")

    # Process
    pop_df = process_population_data(pop_raw, iso_codes)
    emp_df = process_employment_data(emp_raw, iso_codes)

    # Calculate metrics
    filtered = calculate_employment_metric(emp_df, pop_df)

    return filtered
