from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from my_module import authenticate_gspread, get_worksheet_data, truncate_table, dataframe_to_postgresql

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

url = 'https://docs.google.com/spreadsheets/d/1zinHUl9YA3i4He3p0POumZ6Fez1oGqzI4ET1CaE_NZY'
index = 0
engine = 'postgresql+psycopg2://postgres:hanif123@34.28.18.184/postgres'
table_name = 'spreadsheet_data'

gc = authenticate_gspread("/home/airflow/gcs/data/key.json", ['https://spreadsheets.google.com/feeds'])

def get_worksheet_data_and_return_dataframe(gc, url, index):
    df = get_worksheet_data(gc, url, index)
    return df

def pass_dataframe_to_context(ti, **kwargs):
    df = ti.xcom_pull(task_ids='spreadsheet_to_df')
    return df

with DAG(
    dag_id="spreadsheet_to_postgresql",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    spreadsheet_to_df = PythonOperator(
        task_id="spreadsheet_to_df",
        python_callable=get_worksheet_data_and_return_dataframe,
        op_kwargs={
            "gc": gc,
            "url": url,
            "index": index,
        },
        provide_context=True,
    )

    truncate_postgresql = PythonOperator(
        task_id="truncate_postgresql",
        python_callable=truncate_table,
        op_kwargs={
            "engine": engine,
            "table_name": table_name,
        },
        provide_context=True,
    )

    df_to_postgresql = PythonOperator(
        task_id="dataframe_to_postgresql",
        python_callable=dataframe_to_postgresql,
        op_kwargs={
            "engine": engine,
            "table_name": table_name,
        },
        provide_context=True,
    )

    pass_dataframe = PythonOperator(
        task_id="pass_dataframe",
        python_callable=pass_dataframe_to_context,
        provide_context=True,
    )

    spreadsheet_to_df >> truncate_postgresql >> pass_dataframe >> df_to_postgresql
