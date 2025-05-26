# wsi/indicators/attitudes_violence.py

import pandas as pd
from wsi.utils import raw_data_path

CONFIG = {
    "attitudes_violence": {
        "file": "WVS_Time_Series_1981-2022_csv_v5_0.csv",
    }
}


def load_raw(name: str) -> pd.DataFrame:
    """Load the raw WVS CSV by its CONFIG key."""
    cfg = CONFIG[name]
    path = raw_data_path("indicators", cfg["file"])
    return pd.read_csv(path)


def process_attitudes_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Process raw WVS data: rename, filter, compute Year, and % agree on F199.

    World Values Survey

    F199 Justifiable: For a man to beat his wife (% agree)
        1  – Never justifiable
        ...
        10 – Always justifiable

    X001: Sex
        1 – Male
        2 – Female
    """
    df = df.rename(columns={"COUNTRY_ALPHA": "ISO_code"})
    df = df[["S020", "S002VS", "ISO_code", "F199"]]

    # Optional ISO filter
    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    # Create the 'year' column based on the minimum year for each survey wave
    # When surveys span multiple years, include all answers in one year on aggregation
    wave_year = df.groupby(["ISO_code", "S002VS"])["S020"].min().reset_index()
    wave_map = wave_year.set_index(["ISO_code", "S002VS"])["S020"].to_dict()

    # Assign Year based on wave_map
    df["Year"] = df.apply(
        lambda row: wave_map.get((row["ISO_code"], row["S002VS"])), axis=1
    )

    # Compute agreement flag
    df["F199agree"] = (df["F199"] > 5).astype(int)

    return df


def build_attitudes_violence_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Build 'Attitudes Towards Violence' indicator by ISO and Year.
    """
    # Load raw data
    raw = load_raw("attitudes_violence")

    # Process raw
    df = process_attitudes_raw(raw, iso_codes)

    # Aggregate percentage agreement
    result = (
        df.groupby(["ISO_code", "Year"])
        .apply(
            lambda x: pd.Series(
                {
                    "Attitudes Towards Violence": (
                        (x["F199agree"].sum() / x["F199"].ge(0).sum() * 100)
                        if x["F199"].ge(0).sum() > 0
                        else pd.NA
                    )
                }
            ),
            include_groups=False,
        )
        .dropna()
        .reset_index()
    )

    attitudes_violence_df = result[["ISO_code", "Year", "Attitudes Towards Violence"]]

    return attitudes_violence_df
