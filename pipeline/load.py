"""Load step: write the processed data into a normalized SQLite schema.

Two tables:
  countries(id, name, continent)              -- one row per country
  yearly_stats(id, country_id, year, ...)      -- one row per country-year

The load is idempotent: tables are rebuilt from the processed CSV on every
run, so re-running the pipeline never produces duplicate rows.
"""

import logging
import sqlite3

import pandas as pd

from pipeline.config import DB_PATH, PROCESSED_CSV_PATH

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    continent TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS yearly_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_id INTEGER NOT NULL REFERENCES countries(id),
    year INTEGER NOT NULL,
    population INTEGER NOT NULL,
    life_expectancy REAL NOT NULL,
    gdp_per_capita REAL NOT NULL,
    gdp_total REAL NOT NULL,
    life_expectancy_band TEXT NOT NULL,
    UNIQUE(country_id, year)
);
"""


def load() -> str:
    logger.info("Reading processed data from %s", PROCESSED_CSV_PATH)
    df = pd.read_csv(PROCESSED_CSV_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.executescript("DROP TABLE IF EXISTS yearly_stats; DROP TABLE IF EXISTS countries;")
        cursor.executescript(SCHEMA)

        countries = df[["country", "continent"]].drop_duplicates()
        country_ids = {}
        for name, continent in countries.itertuples(index=False):
            cursor.execute(
                "INSERT INTO countries (name, continent) VALUES (?, ?)",
                (name, continent),
            )
            country_ids[name] = cursor.lastrowid

        rows = [
            (
                country_ids[r.country],
                int(r.year),
                int(r.population),
                float(r.life_expectancy),
                float(r.gdp_per_capita),
                float(r.gdp_total),
                r.life_expectancy_band,
            )
            for r in df.itertuples(index=False)
        ]
        cursor.executemany(
            """INSERT INTO yearly_stats
               (country_id, year, population, life_expectancy, gdp_per_capita, gdp_total, life_expectancy_band)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
        logger.info("Loaded %d countries and %d yearly_stats rows into %s",
                     len(country_ids), len(rows), DB_PATH)
    finally:
        conn.close()

    return str(DB_PATH)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load()
