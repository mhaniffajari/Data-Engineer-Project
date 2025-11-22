# main.py
import sys
from datetime import datetime

# === Import ETL modules ===
from utils.logger import init_log
from etl_raw import raw_csv_processing, raw_csv_to_postgres
from etl_stage import stage_csv_to_csv, stage_csv_to_postgres
from etl_target import target_csv_to_csv, target_csv_to_postgres


def run_step(step_name, step_func):
    """Wrapper to run each ETL step with console logging & error message."""
    print(f"\n==============================")
    print(f"▶ START: {step_name}")
    print(f"==============================")

    start = datetime.now()

    try:
        step_func()
        print(f"✔ SUCCESS: {step_name}  (Duration: {datetime.now() - start})")

    except Exception as e:
        print(f"❌ ERROR in {step_name}: {str(e)}")
        # Still continue next steps (ETL shouldn't stop completely)
        return


def main():
    print("===========================================")
    print("        TRANSJAKARTA ETL PIPELINE          ")
    print("===========================================\n")

    # Initialize log file
    init_log()

    # === RAW LAYER ===
    run_step("RAW CSV → RAW CSV (Copy Raw Files)", raw_csv_processing)
    run_step("RAW CSV → POSTGRES (Load Raw Tables)", raw_csv_to_postgres)

    # === STAGE LAYER ===
    run_step("RAW CSV → STAGE CSV (Transform)", stage_csv_to_csv)
    run_step("STAGE → POSTGRES (Execute Stage Procedures)", stage_csv_to_postgres)

    # === TARGET LAYER ===
    run_step("STAGE CSV → TARGET CSV (Merge)", target_csv_to_csv)
    run_step("TARGET → POSTGRES (Execute Merge Procedures)", target_csv_to_postgres)

    print("\n===========================================")
    print("         ETL PIPELINE COMPLETED            ")
    print("===========================================\n")


if __name__ == "__main__":
    main()
