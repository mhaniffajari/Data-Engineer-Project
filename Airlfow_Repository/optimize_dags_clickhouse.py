from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from my_module import authenticate_gspread, sql_to_dataframe, dataframe_to_postgresql,dataframe_to_clickhouse

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}
engine = 'mysqllocalhost'
table_name = 'table_name'
account_setting = 'localhost'
your_columns = ('column1','column2')


def pass_dataframe_to_context(ti, **kwargs):
    df = ti.xcom_pull(task_ids='mysql_to_df')
    return df


with DAG(
    dag_id="mysql_to_clickhouse",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    mysql_to_df = PythonOperator(
        task_id="mysql_to_df",
        python_callable=sql_to_dataframe,
        op_kwargs={
            "engine": engine,
            "table_name": table_name,
        },
        provide_context=True,
    )

    df_to_clickhouse = PythonOperator(
        task_id="df_to_clickhouse",
        python_callable=dataframe_to_clickhouse,
        op_kwargs={
            "account_setting": engine,
            "table_name": table_name,
            "your_columns": your_columns,
        },
        provide_context=True,
    )
    pass_dataframe = PythonOperator(
    task_id="pass_dataframe",
    python_callable=pass_dataframe_to_context,
    provide_context=True,
    )

    mysql_to_df >> pass_dataframe >> df_to_clickhouse
