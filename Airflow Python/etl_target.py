# etl_target.py

import pandas as pd
import os
from utils.merger import merge_by_pk
from utils.file import df_to_csv
from utils.logger import write_log
from utils.postgres import exec_postgres_procedure
from datetime import datetime
import json


def load_config():
    with open("data target/config/table_config.json", "r") as f:
        return json.load(f)


def process_table(name, cfg):
    print(f"Processing: {name}")

    df_stage = pd.read_csv(cfg["source"])
    start = datetime.now()

    # Parse date columns if exists
    if "date_columns" in cfg:
        for col, fmt in cfg["date_columns"].items():
            df_stage[col] = pd.to_datetime(df_stage[col], format=fmt, errors="coerce")

    # Write stage file
    df_to_csv(df_stage, cfg["stage"])

    # Load target if exists
    df_target = pd.read_csv(cfg["target"]) if os.path.exists(cfg["target"]) else None

    # Perform merge
    df_merged = merge_by_pk(df_stage, df_target, cfg["pk"])

    # Write merged output
    df_to_csv(df_merged, cfg["target"])

    write_log(
        etl_name=name,
        process="target_merge",
        start_time=start,
        end_time=datetime.now(),
        source=cfg["source"],
        destination=cfg["target"],
        source_count=len(df_stage),
        destination_count=len(df_merged),
        status="SUCCESS"
    )


def target_csv_to_csv():
    config = load_config()
    for name, cfg in config.items():
        process_table(name, cfg)


def target_csv_to_postgres():
    pg_schema = "target"
    start = datetime.now()

    procedures = [
        "merge_transaksi_bus()",
        "merge_transaksi_halte()"
    ]

    # Call procedures (correct usage)
    exec_postgres_procedure(
        process_name="target_csv_to_postgres",
        procedures=procedures,
        schema=pg_schema
    )

    write_log(
        etl_name="target_csv_to_postgres",
        process="target→postgres",
        start_time=start,
        end_time=datetime.now(),
        source="procedures",
        destination=f"postgres.{pg_schema}",
        source_count=None,
        destination_count=None,
        status="SUCCESS"
    )
