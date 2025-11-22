# etl_raw.py

import os
from tracemalloc import start
import pandas as pd
from utils.file import csv_to_df, df_to_csv
from utils.postgres import to_postgres
from utils.logger import write_log
from datetime import datetime

SOURCE_PATH = "data source"
RAW_PATH = "data target/raw"

def raw_csv_processing():
    """
    Copies raw CSV files into raw folder.
    """
    files = [
        "dummy_realisasi_bus.csv",
        "dummy_routes.csv",
        "dummy_shelter_corridor.csv"
    ]

    for file in files:
        start_path = os.path.join(SOURCE_PATH, file)
        dest_path = os.path.join(RAW_PATH, file)

        df = csv_to_df(start_path)
        df_to_csv(df, dest_path)

    start = datetime.now()
    write_log(
        etl_name="raw_csv_processing",
        process="RAW CSV → RAW CSV",
        start_time=start,
        end_time=datetime.now(),
        source=SOURCE_PATH,
        destination=RAW_PATH,
        source_count=len(df),
        destination_count=len(df),
        status="SUCCESS"
    )


def raw_csv_to_postgres():
    """
    Loads raw CSV tables into PostgreSQL (raw schema).
    """
    pg_server = "localhost"
    pg_database = "transjakarta"
    pg_user = "postgres"
    pg_schema = "raw"

    engine = to_postgres(pg_user, pg_server, pg_database)

    files = [
        "dummy_transaksi_bus.csv",
        "dummy_transaksi_halte.csv"
    ]

    for file in files:
        file_path = os.path.join(SOURCE_PATH, file)
        df = csv_to_df(file_path)

        table_name = file.replace("dummy_", "").replace(".csv", "")
        df.to_sql(table_name, engine, if_exists="replace", index=False, schema=pg_schema)

        start = datetime.now()
        write_log(
            etl_name="raw_csv_to_postgres",
            process="raw→postgres",
            start_time=start,
            end_time=datetime.now(),
            source=file_path,
            destination=f"postgres.raw.{table_name}",
            source_count=len(df),
            destination_count=len(df),
            status="SUCCESS"
        )
