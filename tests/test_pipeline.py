"""Sanity checks for each pipeline stage. Run with: pytest"""

import sqlite3

import pandas as pd
import pytest

from pipeline.config import DB_PATH, PROCESSED_CSV_PATH, RAW_CSV_PATH
from pipeline.extract import extract
from pipeline.load import load
from pipeline.transform import transform


@pytest.fixture(scope="module", autouse=True)
def run_pipeline_once():
    extract()
    transform()
    load()


def test_raw_csv_exists_and_has_rows():
    assert RAW_CSV_PATH.exists()
    df = pd.read_csv(RAW_CSV_PATH)
    assert len(df) > 0
    assert {"country", "year", "pop", "continent", "lifeExp", "gdpPercap"}.issubset(df.columns)


def test_processed_csv_has_expected_columns():
    assert PROCESSED_CSV_PATH.exists()
    df = pd.read_csv(PROCESSED_CSV_PATH)
    expected = {
        "country", "year", "population", "continent", "life_expectancy",
        "gdp_per_capita", "gdp_total", "life_expectancy_band",
    }
    assert expected.issubset(df.columns)
    assert df["population"].isna().sum() == 0
    assert df.duplicated(subset=["country", "year"]).sum() == 0


def test_database_has_countries_and_stats():
    assert DB_PATH.exists()
    conn = sqlite3.connect(DB_PATH)
    try:
        countries_count = conn.execute("SELECT COUNT(*) FROM countries").fetchone()[0]
        stats_count = conn.execute("SELECT COUNT(*) FROM yearly_stats").fetchone()[0]
        assert countries_count > 0
        assert stats_count > 0

        orphaned = conn.execute("""
            SELECT COUNT(*) FROM yearly_stats ys
            LEFT JOIN countries c ON ys.country_id = c.id
            WHERE c.id IS NULL
        """).fetchone()[0]
        assert orphaned == 0
    finally:
        conn.close()


def test_load_is_idempotent():
    load()
    load()
    conn = sqlite3.connect(DB_PATH)
    try:
        dupes = conn.execute("""
            SELECT country_id, year, COUNT(*) c
            FROM yearly_stats GROUP BY country_id, year HAVING c > 1
        """).fetchall()
        assert dupes == []
    finally:
        conn.close()
