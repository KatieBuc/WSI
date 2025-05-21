# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

from wsi.map_country_iso import get_iso


def project_root() -> Path:
    # climb out of /wsi to the repo root
    return Path(__file__).resolve().parent.parent


def raw_data_path(*segments) -> Path:
    return project_root() / "data" / "raw" / Path(*segments)


# ignore continents, world bank puts regions into this column too
def ignore_get_iso(name: str):
    try:
        return get_iso(name)
    except KeyError:
        return pd.NA
