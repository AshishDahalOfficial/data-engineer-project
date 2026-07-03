"""Central configuration for the pipeline. Kept in one place so paths and
settings aren't scattered/duplicated across extract/transform/load."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DB_DIR = PROJECT_ROOT / "db"
LOG_DIR = PROJECT_ROOT / "logs"

DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
DB_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_URL = "https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv"
RAW_CSV_PATH = DATA_RAW_DIR / "gapminder_raw.csv"
PROCESSED_CSV_PATH = DATA_PROCESSED_DIR / "gapminder_processed.csv"
DB_PATH = DB_DIR / "gapminder.db"
