"""Extract step: download the raw dataset from its public source and save
it to data/raw/ untouched. Keeping raw data immutable lets us re-run
transform/load without re-fetching, and gives us an audit trail."""

import logging

import requests

from pipeline.config import RAW_CSV_PATH, SOURCE_URL

logger = logging.getLogger(__name__)


def extract() -> str:
    logger.info("Downloading dataset from %s", SOURCE_URL)
    response = requests.get(SOURCE_URL, timeout=30)
    response.raise_for_status()

    RAW_CSV_PATH.write_bytes(response.content)
    logger.info("Saved raw data to %s (%d bytes)", RAW_CSV_PATH, len(response.content))
    return str(RAW_CSV_PATH)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract()
