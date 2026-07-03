"""Prefect orchestration: wires extract -> transform -> load into one DAG.

Run once:
    python -m pipeline.flow

Run on a schedule (keeps running, triggers itself daily at 06:00):
    python -m pipeline.flow --serve
"""

import argparse

from prefect import flow, task
from prefect.logging import get_run_logger

from pipeline.extract import extract as extract_impl
from pipeline.load import load as load_impl
from pipeline.transform import transform as transform_impl


@task(retries=2, retry_delay_seconds=10, log_prints=True)
def extract_task() -> str:
    logger = get_run_logger()
    path = extract_impl()
    logger.info("Extract complete: %s", path)
    return path


@task(log_prints=True)
def transform_task(raw_path: str) -> str:
    logger = get_run_logger()
    path = transform_impl()
    logger.info("Transform complete: %s", path)
    return path


@task(log_prints=True)
def load_task(processed_path: str) -> str:
    logger = get_run_logger()
    path = load_impl()
    logger.info("Load complete: %s", path)
    return path


@flow(name="gapminder-etl")
def gapminder_etl_flow():
    raw_path = extract_task()
    processed_path = transform_task(raw_path)
    db_path = load_task(processed_path)
    return db_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true",
                         help="Run continuously on a daily cron schedule instead of once")
    args = parser.parse_args()

    if args.serve:
        gapminder_etl_flow.serve(name="gapminder-daily", cron="0 6 * * *")
    else:
        gapminder_etl_flow()
