# Gapminder ETL Pipeline

A beginner-friendly data engineering project: a scheduled ETL pipeline that
extracts world development data (population, life expectancy, GDP by
country/year), cleans and enriches it, and loads it into a queryable SQLite
database — orchestrated with Prefect.

This was built as a first hands-on data engineering project, meant to
demonstrate the core skills the role is about: moving data reliably between
systems, transforming it correctly, and doing it on a repeatable schedule
with visibility into failures.

## What this project demonstrates

- **Extract**: pulling data from an external source (HTTP) and persisting
  the raw copy before touching it
- **Transform**: cleaning (nulls, duplicates), type coercion, and deriving
  new columns from raw data using pandas
- **Load**: writing into a normalized relational schema (two tables with a
  foreign key) in a way that's idempotent — safe to re-run without creating
  duplicates
- **Orchestration**: chaining the three steps into a DAG with automatic
  retries and structured logging, runnable on a cron schedule
- **Testing**: automated checks that catch broken data before it reaches
  the database (missing columns, duplicate rows, orphaned foreign keys)

## Architecture

```
                 ┌───────────┐      ┌─────────────┐      ┌────────────┐
  Public CSV --> │  extract  │ -->  │  transform  │ -->  │    load    │ --> SQLite DB
  (Gapminder)     └───────────┘      └─────────────┘      └────────────┘
                       │                    │                    │
                  data/raw/*.csv    data/processed/*.csv    db/gapminder.db

  All three steps are wired together and orchestrated by Prefect (pipeline/flow.py)
```

**Data source**: [Gapminder dataset](https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv)
(mirrored by Plotly) — country-level population, life expectancy, and GDP
per capita, every 5 years from 1952–2007. Chosen because it's free, needs
no API key, and is a widely-used, clean reference dataset.

**Database schema** (`db/gapminder.db`):

```sql
countries(id, name, continent)
yearly_stats(id, country_id -> countries.id, year, population,
             life_expectancy, gdp_per_capita, gdp_total, life_expectancy_band)
```

`gdp_total` (population × GDP per capita) and `life_expectancy_band`
(low / medium / high) are computed during the transform step — they don't
exist in the raw source.

## Technology used

| Tool | Why |
|---|---|
| **Python 3.12** | Language for the whole pipeline |
| **pandas** | Cleaning, type coercion, and deriving new columns |
| **requests** | Downloading the raw CSV over HTTP |
| **SQLite** (via `sqlite3`, stdlib) | Zero-setup relational database for the final data |
| **Prefect** | Orchestrates extract → transform → load as a DAG, with retries, logging, and cron scheduling — a lighter, Windows-native alternative to Apache Airflow |
| **pytest** | Automated tests that validate data quality after each run |

### Why Prefect instead of Airflow

Apache Airflow is the more commonly cited orchestrator in job postings, but
it doesn't run natively on Windows — it requires Docker or WSL2. Prefect
gives the same core concepts (tasks, flows/DAGs, retries, scheduling,
logging, a run history UI) as a plain `pip install`, so you can learn
orchestration without also debugging a Docker/WSL setup. The pipeline logic
itself (`extract.py`/`transform.py`/`load.py`) doesn't depend on Prefect at
all — swapping in Airflow later would only mean rewriting `flow.py`.

## Project structure

```
data-engineer-project/
├── pipeline/
│   ├── config.py      # paths and settings, single source of truth
│   ├── extract.py      # downloads raw CSV -> data/raw/
│   ├── transform.py     # cleans + enriches -> data/processed/
│   ├── load.py          # writes to SQLite -> db/
│   └── flow.py           # Prefect DAG wiring the three steps together
├── tests/
│   └── test_pipeline.py   # data-quality checks (pytest)
├── data/
│   ├── raw/                # untouched downloaded data (gitignored)
│   └── processed/           # cleaned data (gitignored)
├── db/
│   └── gapminder.db          # final SQLite database (gitignored)
├── requirements.txt
└── README.md
```

## What was done to set this up

1. Checked the machine and found Python, Docker, and WSL were not
   installed — Prefect was chosen over Airflow specifically so the project
   wouldn't require Docker/WSL to get running.
2. Installed Python 3.12 via `winget`.
3. Scaffolded the folder structure above.
4. Verified the data source (Gapminder CSV) was reachable and picked it as
   a clean, well-known beginner dataset.
5. Wrote `extract.py`, `transform.py`, `load.py` as independent, testable
   functions — each one only knows about its own step.
6. Wrote `flow.py` to wire them into a Prefect flow with retries on the
   network-dependent extract step.
7. Wrote `tests/test_pipeline.py` covering schema shape, null/duplicate
   checks, foreign-key integrity, and idempotency (running the pipeline
   twice doesn't duplicate rows).
8. Created a virtual environment, installed dependencies from
   `requirements.txt`.
9. Ran the pipeline end-to-end: it downloaded the dataset, produced
   `data/processed/gapminder_processed.csv`, and loaded
   **142 countries** and **1,704 yearly records** into
   `db/gapminder.db`.
10. Ran the test suite — all 4 tests pass.

## How to run it yourself

```powershell
cd $HOME\data-engineer-project

# one-time setup
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# run the pipeline once
.\.venv\Scripts\python.exe -m pipeline.flow

# or run it continuously on a daily 6am schedule
.\.venv\Scripts\python.exe -m pipeline.flow --serve

# run the tests
.\.venv\Scripts\python.exe -m pytest tests\ -v
```

## Querying the result

```powershell
.\.venv\Scripts\python.exe -c "import sqlite3; c = sqlite3.connect('db/gapminder.db'); print(c.execute('SELECT name, continent FROM countries LIMIT 5').fetchall())"
```

Example query — life expectancy trend for a country:

```sql
SELECT c.name, ys.year, ys.life_expectancy, ys.life_expectancy_band
FROM yearly_stats ys
JOIN countries c ON c.id = ys.country_id
WHERE c.name = 'Nepal'
ORDER BY ys.year;
```

Or open `db/gapminder.db` in any SQLite viewer (e.g. [DB Browser for
SQLite](https://sqlitebrowser.org/), free, no install-heavy dependency)
to explore visually.

## Possible next steps (ideas for extending this)

- Swap the data source for something that updates over time (a live API)
  so the "extract" step has real freshness to demonstrate
- Add a `dbt` layer on top of SQLite for SQL-based transformations
- Move from SQLite to PostgreSQL (via Docker) to practice a more
  production-like database
- Add a simple dashboard (e.g. Streamlit) reading from `gapminder.db`
- Push this project to GitHub and add a CI workflow (GitHub Actions) that
  runs `pytest` on every push
