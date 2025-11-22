from sqlalchemy import create_engine, text
from utils.logger import write_log
from datetime import datetime  # FIXED
import psycopg2


def to_postgres(
    user="postgres",
    password="postgres",
    server="localhost",
    database="transjakarta",
    port=5432
):
    url = f"postgresql+psycopg2://{user}:{password}@{server}:{port}/{database}"
    return create_engine(url)


def exec_postgres_procedure(process_name, procedures, schema,
                            user="postgres", password="postgres",
                            server="localhost", database="transjakarta"):
    """
    Execute list of stored procedures inside a schema.
    Logs each procedure execution status.
    """

    engine = to_postgres(user, password, server, database)

    with engine.connect() as conn:
        conn.execute(text(f"SET search_path TO {schema}"))
        conn.commit()  # IMPORTANT

        for proc in procedures:
            start = datetime.now()

            try:
                conn.execute(text(f"CALL {proc}()"))  # safer to add ()
                conn.commit()  # IMPORTANT

                write_log(
                    etl_name=proc,
                    process=process_name,
                    start_time=start,
                    end_time=datetime.now(),
                    source_count=None,
                    destination_count=None,
                    status="SUCCESS"
                )

            except Exception as e:
                conn.rollback()

                write_log(
                    etl_name=proc,
                    process=process_name,
                    start_time=start,
                    end_time=datetime.now(),
                    source_count=0,
                    destination_count=0,
                    status="FAILED",
                    error_message=str(e)
                )
