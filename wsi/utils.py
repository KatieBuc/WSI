# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

from wsi.mapping.country_iso import get_iso


def project_root() -> Path:
    # climb out of /wsi to the repo root
    return Path(__file__).resolve().parent.parent


def raw_data_path(*segments) -> Path:
    return project_root() / "data" / "raw" / "indicators" / Path(*segments)


def imgs_path(*segments) -> Path:
    return project_root() / "imgs" / Path(*segments)


def processed_data_path(*segments) -> Path:
    return project_root() / "data" / "processed" / Path(*segments)


def ignore_get_iso(name: str):
    """ignore continents, world bank puts regions into this column too"""
    try:
        return get_iso(name)
    except KeyError:
        return pd.NA


def clean_year_columns(df: pd.DataFrame) -> None:
    """Strip year info (e.g., 'YR2020') from column names."""
    df.columns = [col.split()[0] if "YR" in col else col for col in df.columns]
