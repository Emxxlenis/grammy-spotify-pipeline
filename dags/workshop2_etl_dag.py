"""
workshop2_etl_dag.py — ETL Pipeline: Spotify + Grammy Awards

Task flow:
    extract_spotify_csv  ──────────────────────────────────┐
                                                            ├─→ clean_datasets
    load_grammys_source_db → extract_grammys_db ───────────┘
                                                                     │
                                                            transform_and_merge
                                                                     │
                                              ┌──────────────────────┼─────────────────────┐
                                              ▼                      ▼                     ▼
                                    load_csv_output       load_data_warehouse    store_google_drive

Sources:
    - Spotify  : /opt/airflow/data/spotify_dataset.csv
    - Grammys  : grammys_db (PostgreSQL) — loaded from CSV in the first task

Destinations:
    - Data Warehouse  : data_warehouse (PostgreSQL) — star schema
    - Google Drive    : merged CSV (requires OAuth2 credentials configured)
    - Local           : /opt/airflow/data/output/merged_dataset_final.csv
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from extract import extract_spotify_csv, load_grammys_csv_to_source_db, extract_grammys_from_db
from clean import clean_spotify, clean_grammys
from transform import merge_spotify_grammys
from load import export_dataframe_to_csv, load_star_schema, upload_to_google_drive

logger = logging.getLogger(__name__)

BASE_DATA_DIR  = Path("/opt/airflow/data")
STAGING_DIR    = BASE_DATA_DIR / "staging"
OUTPUT_DIR     = BASE_DATA_DIR / "output"
SCHEMA_SQL     = Path("/opt/airflow/scripts/create_schema.sql")
SPOTIFY_CSV    = BASE_DATA_DIR / "spotify_dataset.csv"
GRAMMYS_CSV    = BASE_DATA_DIR / "the_grammy_awards.csv"


def _conn_uri(conn_id: str) -> str:
    return BaseHook.get_connection(conn_id).get_uri()


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def task_extract_spotify() -> str:
    df = extract_spotify_csv(SPOTIFY_CSV)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / "spotify_raw.csv"
    df.to_csv(out, index=False)
    logger.info("Spotify extracted: %s rows → %s", len(df), out)
    return str(out)


def task_load_grammys_source() -> int:
    rows = load_grammys_csv_to_source_db(GRAMMYS_CSV, _conn_uri("grammys_db"))
    logger.info("Grammys loaded into grammys_db: %s rows", rows)
    return rows


def task_extract_grammys() -> str:
    df = extract_grammys_from_db(_conn_uri("grammys_db"))
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / "grammys_raw.csv"
    df.to_csv(out, index=False)
    logger.info("Grammys extracted from DB: %s rows → %s", len(df), out)
    return str(out)


def task_clean() -> dict:
    spotify_clean  = clean_spotify(pd.read_csv(STAGING_DIR / "spotify_raw.csv"))
    grammys_clean  = clean_grammys(pd.read_csv(STAGING_DIR / "grammys_raw.csv"))
    spotify_clean.to_csv(STAGING_DIR / "spotify_clean.csv",  index=False)
    grammys_clean.to_csv(STAGING_DIR / "grammys_clean.csv",  index=False)
    stats = {"spotify_rows": int(len(spotify_clean)), "grammys_rows": int(len(grammys_clean))}
    logger.info("Clean complete: %s", stats)
    return stats


def task_transform_merge() -> str:
    merged = merge_spotify_grammys(
        pd.read_csv(STAGING_DIR / "spotify_clean.csv"),
        pd.read_csv(STAGING_DIR / "grammys_clean.csv"),
    )
    out = STAGING_DIR / "merged_dataset.csv"
    merged.to_csv(out, index=False)
    logger.info("Merge complete: %s rows → %s", len(merged), out)
    return str(out)


def task_load_csv() -> str:
    merged = pd.read_csv(STAGING_DIR / "merged_dataset.csv")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return export_dataframe_to_csv(merged, OUTPUT_DIR / "merged_dataset_final.csv")


def task_load_dw() -> dict:
    return load_star_schema(
        _conn_uri("data_warehouse"),
        pd.read_csv(STAGING_DIR / "merged_dataset.csv"),
        SCHEMA_SQL,
    )


def task_store_google_drive() -> None:
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
    folder_id        = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
    csv_path         = OUTPUT_DIR / "merged_dataset_final.csv"

    if not credentials_path or not folder_id:
        logger.warning(
            "GOOGLE_CREDENTIALS_PATH or GOOGLE_DRIVE_FOLDER_ID not set. "
            "Skipping Google Drive upload. CSV available at: %s", csv_path,
        )
        return

    upload_to_google_drive(csv_path, folder_id, credentials_path)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="workshop2_etl_pipeline",
    description="ETL Workshop 2 — Spotify + Grammy Awards → Data Warehouse",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["workshop2", "etl", "spotify", "grammys"],
) as dag:

    extract_spotify  = PythonOperator(task_id="extract_spotify_csv",    python_callable=task_extract_spotify)
    load_grammys_src = PythonOperator(task_id="load_grammys_source_db", python_callable=task_load_grammys_source)
    extract_grammys  = PythonOperator(task_id="extract_grammys_db",     python_callable=task_extract_grammys)
    clean_task       = PythonOperator(task_id="clean_datasets",         python_callable=task_clean)
    transform_merge  = PythonOperator(task_id="transform_and_merge",    python_callable=task_transform_merge)
    load_csv         = PythonOperator(task_id="load_csv_output",        python_callable=task_load_csv)
    load_dw          = PythonOperator(task_id="load_data_warehouse",    python_callable=task_load_dw)
    store_gdrive     = PythonOperator(task_id="store_google_drive",     python_callable=task_store_google_drive)

    extract_spotify  >> clean_task
    load_grammys_src >> extract_grammys >> clean_task
    clean_task       >> transform_merge >> [load_csv, load_dw]
    load_csv         >> store_gdrive
