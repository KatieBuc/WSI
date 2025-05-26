# -*- coding: utf-8 -*-
"""compute education ratio"""

import pandas as pd
from wsi.utils import raw_data_path

CONFIG = {
    "female": {
        "file": "NATMON_DS_30092024194536316.csv",
        "indicator": "Mean years of schooling (ISCED 1 or higher), population 25+ years, female",
    },
    "male": {
        "file": "NATMON_DS_27102024204132612.csv",
        "indicator": "Mean years of schooling (ISCED 1 or higher), population 25+ years, male",
    },
}


def load_raw(name: str) -> pd.DataFrame:
    """Helper to load the raw CSV by its CONFIG key ('female' or 'male')."""
    file_name = CONFIG[name]["file"]
    full_path = raw_data_path(
        "indicators", file_name
    )  # e.g. .../WSI/data/raw/NATMON_....csv
    return pd.read_csv(full_path)


def process_data(
    df: pd.DataFrame,
    indicator_col: str,
    value_col_name: str,
    indicator_name: str,
    iso_codes: list[str] | None = None,
) -> pd.DataFrame:
    """
    Filter rows by `indicator_name` in column `indicator_col`, rename
    the value column to `value_col_name`, and (optionally) restrict to iso_codes.
    """
    df = df[df[indicator_col] == indicator_name]
    if iso_codes is not None:
        df = df[df["LOCATION"].isin(iso_codes)]
    return df[["Time", "Value", "LOCATION"]].rename(
        columns={"Time": "Year", "Value": value_col_name, "LOCATION": "ISO_code"}
    )


def calculate_ratio(
    df_num: pd.DataFrame,
    df_den: pd.DataFrame,
    num_col: str,
    den_col: str,
    ratio_col: str,
) -> pd.DataFrame:
    """
    Merge numerator and denominator dataframes and compute ratio_col = num_col/den_col.
    """
    merged = pd.merge(df_num, df_den, on=["Year", "ISO_code"], how="inner")
    merged[ratio_col] = merged[num_col] / merged[den_col]
    return merged


def build_education_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Returns a DataFrame with columns ['Year','ISO_code','Education']
    filtered to iso_codes if provided.
    """
    # load raw
    df_f = load_raw("female")
    df_m = load_raw("male")
    # process
    female_df = process_data(
        df_f, "Indicator", "Female", CONFIG["female"]["indicator"], iso_codes
    )
    male_df = process_data(
        df_m, "Indicator", "Male", CONFIG["male"]["indicator"], iso_codes
    )
    # ratio
    edu = calculate_ratio(female_df, male_df, "Female", "Male", "Education")
    return edu[["Year", "ISO_code", "Education"]]
