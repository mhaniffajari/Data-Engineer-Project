import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import types, create_engine
from sqlalchemy.engine.url import URL
import psycopg2
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from sqlalchemy import types, create_engine
from sqlalchemy.engine.url import URL
import psycopg2
from sqlalchemy.orm import sessionmaker

path_to_local_home = "/home/airflow/gcs/data/key.json"
scope = ['https://spreadsheets.google.com/feeds']
worksheet_name = 'Purchase_Dataset'
credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_local_home, scope)
spreadsheet_key = '1zinHUl9YA3i4He3p0POumZ6Fez1oGqzI4ET1CaE_NZY'
gc = gspread.authorize(credentials)
book = gc.open_by_key(spreadsheet_key)
url = 'https://docs.google.com/spreadsheets/d/1zinHUl9YA3i4He3p0POumZ6Fez1oGqzI4ET1CaE_NZY'
book = gc.open_by_url(url)
index = 0
worksheet = book.get_worksheet(index)
table = worksheet.get_all_values()
layanan_pengaduan = pd.DataFrame()
layanan_pengaduan = layanan_pengaduan.append(table)
layanan_pengaduan.columns = layanan_pengaduan.iloc[0]
layanan_pengaduan = layanan_pengaduan.reindex(layanan_pengaduan.index.drop(0))
engine = create_engine('postgresql+psycopg2://postgres:hanif123@34.28.18.184/postgres')


def spreadsheet_to_dataframe(index,url):
    book = gc.open_by_url(url)
    worksheet = book.get_worksheet(index)
    table = worksheet.get_all_values()
    layanan_pengaduan = pd.DataFrame()
    layanan_pengaduan = layanan_pengaduan.append(table)
    layanan_pengaduan.columns = layanan_pengaduan.iloc[0]
    layanan_pengaduan = layanan_pengaduan.reindex(layanan_pengaduan.index.drop(0))

def truncate_table(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute('''TRUNCATE TABLE spreadsheet''')
    session.commit()
    session.close()


def dataframe_to_postgresql(engine,dataframe):
    conn = engine.connect()
    dataframe.to_sql('spreadsheet', engine,if_exists='replace', index=False)



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

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
        python_callable=spreadsheet_to_dataframe,
        op_kwargs={
            "url": url,
            "index": index,
        },
    )

    truncate_postgresql = PythonOperator(
        task_id="truncate_postgresql",
        python_callable=truncate_table,
        op_kwargs={
            "engine": engine,
        },
    )

    df_to_postgresql = PythonOperator(
        task_id="dataframe_to_postgresql",
        python_callable=dataframe_to_postgresql,
        op_kwargs={
            "engine": engine,
            "dataframe":layanan_pengaduan,
        },
    )

    spreadsheet_to_df >> truncate_postgresql  >> df_to_postgresql