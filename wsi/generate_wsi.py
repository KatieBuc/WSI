import pandas as pd
from scipy.stats import gmean

from wsi.indicators.education import build_education_df
from wsi.indicators.employment import build_employment_df
from wsi.indicators.parliament import build_parliamentary_df
from wsi.indicators.poverty import build_poverty_df
from wsi.indicators.legal import build_legal_df
from wsi.indicators.son_bias import build_son_bias_df
from wsi.indicators.maternal_mortality import build_maternal_mortality_df
from wsi.indicators.attitude_violence import build_attitudes_violence_df
from wsi.indicators.child_marriage import build_child_marriage_df
from wsi.indicators.water_sanitation import build_water_sanitation_df
from wsi.indicators.electricity import build_access_electricity_df
from wsi.indicators.financial_inclusion import build_financial_inclusion_df
from wsi.indicators.phone_use import build_cell_phone_use_df

from wsi.config import INDICATORS, EXCLUDE_ISO
from wsi.mapping.iso_name import ALL_ISO_NAME
from wsi.mapping.iso_income import CODE_INCOME
from wsi.mapping.iso_region import CODE_SUBREGION
from wsi.utils import processed_data_path


def normalize_column(column: pd.Series) -> pd.Series:
    return (
        (column - column.min()) / (column.max() - column.min())
        if column.max() != column.min()
        else column
    )


def apply_indicator_scoring(df: pd.DataFrame) -> pd.DataFrame:
    dimension_groups = {}
    for indicator, cfg in INDICATORS.items():
        dimension = cfg["dimension"]
        dimension_groups.setdefault(dimension, []).append(indicator)

    for indicator, cfg in INDICATORS.items():
        col_score = f"{indicator} (score)"
        df[col_score] = normalize_column(df[indicator])
        if cfg["invert"]:
            df[col_score] = 1 - df[col_score]

    for dimension, indicators in dimension_groups.items():
        score_cols = [f"{indicator} (score)" for indicator in indicators]
        df[dimension] = df[score_cols].mean(axis=1)

    score_columns = ["Equity", "Protection", "Resilience"]
    df["WSI (Baseline)"] = df[score_columns].apply(
        lambda row: gmean(row.dropna()) if not row.isnull().all() else pd.NA, axis=1
    )

    return df


def main():
    """
    For each indicator:
    1. Create a full DataFrame of all ISO codes and years.
    2. Merge the indicator data into this full structure and sort by ISO and Year.
    3. Fill missing values per country using:
       - Linear interpolation
       - Forward and backward fill
       - Track all filled values with a corresponding source column.
    4. Compute region and income group averages from the filled data.
    5. For countries with entirely missing data for an indicator:
       - Fill using the appropriate average (region or income), based on config.
       - Mark these with the appropriate source tag (e.g., 'AVG_REG' or 'AVG_INC').

    Finally, normalise and combine indicators to get dimension scores.
    Combine dimensions to obtain baseline WSI.
    """
    pd.set_option("future.no_silent_downcasting", True)
    years = list(range(1995, 2025))
    indicator_columns = [k for k in INDICATORS]

    indicator_builders = {
        "Education": build_education_df,
        "Employment": build_employment_df,
        "Parliamentary Representation": build_parliamentary_df,
        "Poverty": build_poverty_df,
        "Legal Protection Index": build_legal_df,
        "Son Bias": build_son_bias_df,
        "Maternal Mortality": build_maternal_mortality_df,
        "Attitudes Towards Violence": build_attitudes_violence_df,
        "Child Marriage": build_child_marriage_df,
        "Access Water Sanitation": build_water_sanitation_df,
        "Access Electricity": build_access_electricity_df,
        "Financial Inclusion": build_financial_inclusion_df,
        "Cell Phone Use": build_cell_phone_use_df,
    }

    df_raw = pd.DataFrame(
        [(iso, year) for iso in ALL_ISO_NAME for year in years],
        columns=["ISO_code", "Year"],
    )

    for ind in indicator_columns:
        indicator_df = indicator_builders[ind]()
        df_raw = df_raw.merge(indicator_df, on=["ISO_code", "Year"], how="left")

    df_raw.to_csv(processed_data_path("raw_baseline_indicators.csv"), index=False)

    missing_summary = []
    for iso, group in df_raw.groupby("ISO_code"):
        missing_indicators = [
            col for col in indicator_columns if group[col].isna().all()
        ]
        if missing_indicators:
            missing_summary.append(
                {
                    "ISO_code": iso,
                    "Country": ALL_ISO_NAME.get(iso, "Unknown"),
                    "Missing_Count": len(missing_indicators),
                    "Indicators": ", ".join(missing_indicators),
                }
            )

    summary = pd.DataFrame(missing_summary)
    summary = summary.sort_values(by="Missing_Count", ascending=False).reset_index(
        drop=True
    )
    summary.to_csv(processed_data_path("missing_indicators_summary.csv"), index=False)

    df = df_raw[~df_raw["ISO_code"].isin(EXCLUDE_ISO)].copy()

    # TODO: think about how to handle excluded
    # df_excluded = df_raw[df_raw["ISO_code"].isin(EXCLUDE_ISO)].copy()

    for ind in indicator_columns:
        df[f"{ind} (source)"] = ""
        df.loc[df[ind].notna(), f"{ind} (source)"] = "ORI"

    def fill_missing(group):
        for ind in indicator_columns:
            group[ind] = pd.to_numeric(group[ind], errors="coerce")
            before = group[ind].isnull()
            group[ind] = group[ind].interpolate(limit_direction="both")
            after_interp = before & group[ind].notnull()
            group.loc[after_interp, f"{ind} (source)"] = "TSI"
            group[ind] = group[ind].ffill().bfill()
            after_fill = before & group[ind].notnull()
            group.loc[after_fill, f"{ind} (source)"] = "TSI"
        return group

    df = df.groupby("ISO_code").apply(fill_missing,include_groups=True).reset_index(drop=True) #TODO: fix DeprecationWarning
    df["Region"] = df["ISO_code"].map(CODE_SUBREGION)
    df["Income"] = df["ISO_code"].map(CODE_INCOME)

    region_avgs = df.groupby(["Region", "Year"])[indicator_columns].mean()
    income_avgs = df.groupby(["Income", "Year"])[indicator_columns].mean()

    region_avgs.to_csv(processed_data_path("region_avgs.csv"), index=True)
    income_avgs.to_csv(processed_data_path("income_avgs.csv"), index=True)

    for ind in indicator_columns:
        missing = df.groupby("ISO_code")[ind].apply(lambda x: x.isna().all())
        fill_strategy = INDICATORS[ind]["fill"]
        for iso in missing[missing].index:
            if fill_strategy == "income_avg":
                group = CODE_INCOME.get(iso)
                if group and (group, years[0]) in income_avgs.index:
                    fill_vals = income_avgs.loc[group]
                    df.loc[df["ISO_code"] == iso, ind] = fill_vals[ind].values
                    df.loc[df["ISO_code"] == iso, f"{ind} (source)"] = "AVG_INC"
            elif fill_strategy == "region_avg":
                group = CODE_SUBREGION.get(iso)
                if group and (group, years[0]) in region_avgs.index:
                    fill_vals = region_avgs.loc[group]
                    df.loc[df["ISO_code"] == iso, ind] = fill_vals[ind].values
                    df.loc[df["ISO_code"] == iso, f"{ind} (source)"] = "AVG_REG"

    # data missingness overwrite (Attitudes Towards Violence -> Timor-Leste)
    regions_to_update = ['Melanesia', 'Micronesia', 'Polynesia']
    region_rows = df[df['Region'].isin(regions_to_update)]
    tls_data = df[df['ISO_code'] == 'TLS']
    for region in region_rows['ISO_code'].values:
        df.loc[df['ISO_code'] == region, 'Attitudes Towards Violence'] = tls_data['Attitudes Towards Violence'].values[0]
    df.loc[df['Region'].isin(regions_to_update), 'Attitudes Towards Violence (source)'] = 'AVG_TLS'

    # combine to index
    df_scored = apply_indicator_scoring(df)
    df_scored["Economy"] = df_scored["ISO_code"].map(ALL_ISO_NAME)
    df_scored.to_csv(processed_data_path("womens_safety_index_baseline.csv"), index=False)


if __name__ == "__main__":
    main()
