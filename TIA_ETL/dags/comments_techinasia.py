from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
from my_library import my_function as mf
import json
import os

# Constants
RAW_FILE_PATH = 'raw'
STAGING_FILE_PATH = 'staging'
INGEST_TIME = dt.datetime.now().strftime("%Y%m%d")

# Ensure the directories exist
os.makedirs(f"{RAW_FILE_PATH}/comments", exist_ok=True)
os.makedirs(f"{STAGING_FILE_PATH}/comments", exist_ok=True)

# Functions
def fetch_comments():
    engine_postgres = create_engine('postgresql+psycopg2://postgres:postgres@postgres:5432/airflow')
    df = pd.read_sql('SELECT * FROM page', engine_postgres)
    value = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36'}
    
    for id in df['id']:
        url = f"https://www.techinasia.com/wp-json/techinasia/2.0/posts/{id}/comments"
        try:
            response_api = requests.get(url, headers=headers)
            response_api.raise_for_status()  # Raises an HTTPError for bad responses
            try:
                data = response_api.json()
                data = {**data, 'id': id}
                value.append(data)
            except ValueError:
                print(f"Error decoding JSON for id {id}. Response content: {response_api.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed for id {id}. Error: {e}")
    
    # Save the fetched data to a JSON file
    with open(f"{RAW_FILE_PATH}/comments/comments_{INGEST_TIME}.json", "w", encoding="utf-8") as f:
        json.dump(value, f)

def clean_comments():
    with open(f"{RAW_FILE_PATH}/comments/comments_{INGEST_TIME}.json", "r", encoding="utf-8") as f:
        value = json.load(f)
    
    keys_to_keep = {"id", "total", "per_page", "current_page", "comments", "value"}
    filtered_data = [
        mf.enforce_types({key: d.get(key, None) for key in keys_to_keep})
        for d in value if d.get("comments", [])  # Only include if comments is not an empty list
    ]
    try:
        df_comments = pd.DataFrame(filtered_data)
    except Exception as e:
        print(f"Failed to create DataFrame. Error: {e}")
    
    comment = []
    id_list = []
    for i in df_comments['comments']:
        for h in i:
            if h.get('content') is None:
                value = None
                comment.append(value)
                id = h.get('id')
                id_list.append(id)
            else:
                value = mf.clean_comments(h.get('content'))
                comment.append(value)
                id = h.get('id')
                id_list.append(id)
    
    df_comment_list = pd.DataFrame({'id': id_list, "clean_comments": comment})
    df_comment_list = df_comment_list.dropna()
    df_comment_list.to_csv(f"{STAGING_FILE_PATH}/comments/comments_{INGEST_TIME}.csv", index=False)
    print('dataframe count:', df_comment_list.shape[0])

def store_comments():
    df_sql = pd.read_csv(f"{STAGING_FILE_PATH}/comments/comments_{INGEST_TIME}.csv")
    engine_postgres = create_engine('postgresql+psycopg2://postgres:postgres@postgres:5432/airflow')
    df_sql.to_sql('comments', engine_postgres, if_exists='replace', index=False)
    print('Data stored successfully')

# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'comments_techinasia_pipeline',
    default_args=default_args,
    description='comments ingestion pipeline for Tech in Asia',
    schedule_interval='@daily',
)

fetch_comments_task = PythonOperator(
    task_id='fetch_comments',
    python_callable=fetch_comments,
    dag=dag,
)

clean_comments_task = PythonOperator(
    task_id='clean_comments',
    python_callable=clean_comments,
    dag=dag,
)

store_comments_task = PythonOperator(
    task_id='store_comments',
    python_callable=store_comments,
    dag=dag,
)

fetch_comments_task >> clean_comments_task >> store_comments_task
