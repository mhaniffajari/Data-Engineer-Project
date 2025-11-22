# etl_stage.py

import pandas as pd
from utils.file import df_to_csv
from utils.postgres import exec_postgres_procedure
from utils.logger import write_log
import os
from datetime import datetime


def stage_csv_to_csv():
    """
    Transforms raw CSV → stage CSV.
    """
    file_map = {
        "data target/raw/dummy_realisasi_bus.csv": "data target/stage/realisasi_bus.csv",
        "data target/raw/dummy_routes.csv": "data target/stage/routes.csv",
        "data target/raw/dummy_shelter_corridor.csv": "data target/stage/shelter_corridor.csv"
    }

    for src, dst in file_map.items():
        start = datetime.now()

        df = pd.read_csv(src)

        # Special: realisasi_bus date column
        if "realisasi_bus" in src:
            df["tanggal_realisasi"] = pd.to_datetime(
                df["tanggal_realisasi"],
                format="%d/%m/%Y",
                errors="coerce"
            )

        df_to_csv(df, dst)

        # Logging
        write_log(
            etl_name="stage_csv_to_csv",
            process="RAW → STAGE CSV",
            start_time=start,
            end_time=datetime.now(),
            source=src,
            destination=dst,
            source_count=len(df),
            destination_count=len(df),
            status="SUCCESS"
        )


def stage_csv_to_postgres():
    """
    Executes stage stored procedures on PostgreSQL.
    """
    procedures = [
        "insert_to_transaksi_bus()",
        "insert_to_transaksi_halte()"
    ]

    for proc in procedures:
        start = datetime.now()

        # Execute procedure safely
        exec_postgres_procedure(
            process_name="STAGE → POSTGRES",
            procedures=[proc],
            schema="stage"
        )

        # Logging
        write_log(
            etl_name=proc,
            process="STAGE → POSTGRES",
            start_time=start,
            end_time=datetime.now(),
            source="stage csv",
            destination="postgres.stage",
            status="SUCCESS"
        )
