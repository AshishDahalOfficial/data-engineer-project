"""Transform step: clean the raw CSV and derive analysis-ready columns.

Raw columns: country, year, pop, continent, lifeExp, gdpPercap
Output adds: gdp_total, life_expectancy_band, and snake_case column names.
"""

import logging

import pandas as pd

from pipeline.config import PROCESSED_CSV_PATH, RAW_CSV_PATH

logger = logging.getLogger(__name__)

RENAME_MAP = {
    "country": "country",
    "year": "year",
    "pop": "population",
    "continent": "continent",
    "lifeExp": "life_expectancy",
    "gdpPercap": "gdp_per_capita",
}


def _life_expectancy_band(life_expectancy: float) -> str:
    if life_expectancy < 50:
        return "low"
    if life_expectancy < 70:
        return "medium"
    return "high"


def transform() -> str:
    logger.info("Reading raw data from %s", RAW_CSV_PATH)
    df = pd.read_csv(RAW_CSV_PATH)

    before = len(df)
    df = df.dropna()
    df = df.drop_duplicates(subset=["country", "year"])
    logger.info("Dropped %d rows (nulls/duplicates)", before - len(df))

    df = df.rename(columns=RENAME_MAP)

    df["population"] = df["population"].astype(int)
    df["year"] = df["year"].astype(int)
    df["gdp_total"] = df["population"] * df["gdp_per_capita"]
    df["life_expectancy_band"] = df["life_expectancy"].apply(_life_expectancy_band)

    df = df.sort_values(["country", "year"]).reset_index(drop=True)

    df.to_csv(PROCESSED_CSV_PATH, index=False)
    logger.info("Saved %d processed rows to %s", len(df), PROCESSED_CSV_PATH)
    return str(PROCESSED_CSV_PATH)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform()
