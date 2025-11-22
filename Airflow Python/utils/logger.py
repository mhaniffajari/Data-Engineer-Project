import csv
import os
from datetime import datetime

LOG_FILE = "logs/etl_log.csv"


def init_log():
    """Ensure log folder and header exist."""
    os.makedirs("logs", exist_ok=True)

    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "etl_name", "process", "start_time", "end_time",
                "source", "destination",
                "source_count", "destination_count",
                "status", "error_message"
            ])


def write_log(
    etl_name,
    process,
    start_time,
    end_time,
    source=None,
    destination=None,
    status="SUCCESS",
    error_message=None,
    source_count=None,
    destination_count=None
):
    """Append log entry into CSV log file."""

    init_log()  # ensure folder + header

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            etl_name,
            process,
            start_time,
            end_time,
            source,
            destination,
            source_count,
            destination_count,
            status,
            error_message
        ])
