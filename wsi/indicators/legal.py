# wsi/indicators/legal_protection.py

import pandas as pd
from wsi.utils import raw_data_path

CONFIG = {
    "legal_protection": {
        "file": "WBL2024-1-0-Historical-Panel-Data.xlsx",
        "sheet": "WBL Panel 2024",
    }
}


def load_raw(name: str) -> pd.DataFrame:
    """Load the specified sheet from the Excel file."""
    cfg = CONFIG[name]
    path = raw_data_path(cfg["file"])
    return pd.read_excel(path, sheet_name=cfg["sheet"])


def process_legal_raw(
    df: pd.DataFrame, iso_codes: list[str] | None = None
) -> pd.DataFrame:
    """
    Clean columns, map ISO codes, and filter to provided iso_codes.
    """
    # Standardise and cast column names
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={"ISO Code": "ISO_code", "Report Year": "Year"})
    df["Year"] = df["Year"].astype(int)

    # Optional filter on iso_codes
    if iso_codes is not None:
        df = df[df["ISO_code"].isin(iso_codes)]

    return df.reset_index(drop=True)


def transform_legal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert question responses to binary, compute section averages and overall index.
    """
    # Define question groups
    selected_questions = {
        "MOBILITY": [
            "Can a woman choose where to live in the same way as a man?",
            "Can a woman travel outside her home in the same way as a man?",
            "Can a woman travel outside the country in the same way as a man?",
        ],
        "WORKPLACE": [
            "Can a woman get a job in the same way as a man?",
            "Does the law prohibit discrimination in employment based on gender?",
        ],
        "PAY": [
            "Does the law mandate equal remuneration for work of equal value?",
            "Can a woman work at night in the same way as a man?",
            "Can a woman work in a job deemed dangerous in the same way as a man?",
            "Can a woman work in an industrial job in the same way as a man?",
        ],
        "MARRIAGE": [
            "Is the law free of legal provisions that require a married woman to obey her husband?",
            "Can a woman be head of household in the same way as a man?",
            "Can a woman obtain a judgment of divorce in the same way as a man?",
            "Does a woman have the same rights to remarry as a man?",
        ],
        "PARENTHOOD": [
            "Is paid leave of at least 14 weeks available to mothers?",
            "Does the government administer 100 percent of maternity leave benefits?",
            "Is there paid leave available to fathers?",
            "Is dismissal of pregnant workers prohibited?",
        ],
        "ENTREPRENEURSHIP": [
            "Does the law prohibit discrimination in access to credit based on gender?"
        ],
        "ASSETS": [
            "Do women and men have equal ownership rights to immovable property?",
            "Do sons and daughters have equal rights to inherit assets from their parents?",
            "Do male and female surviving spouses have equal rights to inherit assets?",
            "Does the law provide for the valuation of nonmonetary contributions?",
        ],
        "PENSION": [
            "Is the age at which women and men can retire with full pension benefits the same?",
            "Is the age at which women and men can retire with partial pension benefits the same?",
            "Is the mandatory retirement age for women and men the same?",
            "Are periods of absence due to childcare accounted for in pension benefits?",
        ],
    }

    # Convert Yes/No to 1/0
    for section, questions in selected_questions.items():
        df[questions] = df[questions].apply(
            lambda col: col.map(lambda x: 1 if str(x).strip() == "Yes" else 0)
        )

    # Compute section averages
    for section, questions in selected_questions.items():
        df[f"{section}_Average"] = df[questions].mean(axis=1)

    # Overall Legal Protection Index
    section_averages = [f"{sec}_Average" for sec in selected_questions]
    df["Legal Protection Index"] = df[section_averages].mean(axis=1)

    return df[["ISO_code", "Year", "Legal Protection Index"]].reset_index(drop=True)


def build_legal_df(iso_codes: list[str] | None = None) -> pd.DataFrame:
    """
    Produce the legal protection index df.
    """
    # Load
    raw = load_raw("legal_protection")

    # Process raw
    legal_raw = process_legal_raw(raw, iso_codes)

    # Transform
    legal_df = transform_legal(legal_raw)

    return legal_df
