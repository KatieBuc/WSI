# -*- coding: utf-8 -*-
from pathlib import Path


def project_root() -> Path:
    # climb out of src/wsi to the repo root
    return Path(__file__).resolve().parent.parent


def raw_data_path(*segments) -> Path:
    return project_root() / "data" / "raw" / Path(*segments)
