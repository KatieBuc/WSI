# wsi/indicators/parliamentary.py

import re
import pandas as pd

from wsi.utils import raw_data_path
from wsi.map_country_iso import get_iso

def build_parliamentary_representation(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Load, clean, and compute % women in parliament (upper + lower) by ISO and Year.
    """
    # Read all sheets from the raw Excel file
    file_path = raw_data_path("women_parliments.xlsx")
    all_sheets = pd.read_excel(file_path, sheet_name=None)

    # Standard column names for every sheet
    columns = [
        "Rank", "Country", 
        "Lower Elections", "Lower Seats", "Lower Women", "Lower pWomen",
        "Upper Elections", "Upper Seats", "Upper Women", "Upper pWomen"
    ]

    # Process each sheet: drop header row, tag year, unify
    frames = []
    for year_label, df in all_sheets.items():
        df.columns = columns
        df = df.iloc[1:].copy()           # drop the duplicated header row
        df["Year"] = int(year_label)      # safe to cast, sheet names are years
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)

    # Coerce missing markers to zero
    df = df.replace({ "---": 0, "-": 0, "?": 0 }).astype({
        "Lower Seats": float, "Lower Women": float,
        "Upper Seats": float, "Upper Women": float
    })

    # Compute combined % women in parliament
    def pct_women(row):
        total_seats = row["Lower Seats"] + row["Upper Seats"]
        total_women = row["Lower Women"] + row["Upper Women"]
        return (100.0 * total_women / total_seats) if total_seats > 0 else 0.0

    df["Parliamentary Representation"] = df.apply(pct_women, axis=1)

    df["Country"] = (
        df["Country"]
        .str.replace(r"\*", "", regex=True)
        .str.replace(r"[^\x00-\x7F]+", "", regex=True)
        .str.replace(r"\(\d+\)", "", regex=True)
        .str.replace(r"\d+$", "", regex=True)
        .str.strip()
    )

    df["ISO_code"] = df["Country"].apply(lambda name: get_iso(name))

    # Filter down to requested ISO codes (if provided)
    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    df = (
        df
        .dropna(subset=["Parliamentary Representation", "ISO_code"])
        .loc[:, ["ISO_code", "Year", "Parliamentary Representation"]]
        .reset_index(drop=True)
    )

    return df
