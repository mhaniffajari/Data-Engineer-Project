from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import polars as pl
import re
import datetime as dt
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from my_library import my_function as mf
import os
import json

# Constants
RAW_FILE_PATH = 'raw'
STAGING_FILE_PATH = 'staging'
INGEST_TIME = dt.datetime.now().strftime("%Y%m%d")
# Ensure the directories exist
os.makedirs(f"{RAW_FILE_PATH}/page", exist_ok=True)
os.makedirs(f"{STAGING_FILE_PATH}/page", exist_ok=True)

# Functions
def fetch_posts():
    value = []
    page = 1
    while True:
        url = f"https://www.techinasia.com/wp-json/techinasia/2.0/posts?page={page}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        data = response.json()
        posts = data.get('posts', [])
        if not posts:
            break
        for post in posts:
            row = {key: post[key] for key in post.keys()}
            value.append(row)
        page += 1
    with open(f"{RAW_FILE_PATH}/page/page_{INGEST_TIME}.json", "w", encoding="utf-8") as f:
        json.dump(value, f)

def clean_data():
    with open(f"{RAW_FILE_PATH}/page/page_{INGEST_TIME}.json", "r", encoding="utf-8") as f:
        value = json.load(f)

    df = pd.DataFrame(value)
    print('Dataframe loaded successfully')
    id = []
    category_name = []
    for i in df['categories']:
        for h in i:
            id_raw = h.get('id')
            id.append(id_raw)
            category_name_raw = h.get('name')
            category_name.append(category_name_raw)
    category_data = {'id':id,"category_name":category_name}
    category_df = pd.DataFrame(category_data)
    id = []
    tags_name = []
    for i in df['tags']:
        for h in i:
            id_raw = h.get('id')
            id.append(id_raw)
            tags_name_raw = h.get('name')
            tags_name.append(tags_name_raw)
    tags_data = {'id':id,"tags_name":tags_name}
    tags_df = pd.DataFrame(tags_data)
    id_list = []
    company_name_list = []
    for i in df['companies']:
        for h in i:
            company_name = h.get('name')
            company_name_list.append(company_name)
            id = h.get('id')
            id_list.append(id)
    company_data = {'id':id_list,"company_name":company_name_list}
    company_df = pd.DataFrame(company_data)
    company_names = []
    id_list = []
    for i in df['companies']:
        if len(i) > 0:
            for h in i:
                company_name = h.get('name')
                company_names.append(company_name)
        else:
            company_names.append("")
    for i in df['id']:
        id_list.append(i)
    company_df = pd.DataFrame({'id':id_list,"company_name":company_names})
    vs_item_row = []
    for i in df['vsitems']:
        if len(i) > 0:   
            for h in i:
                vs_item = mf.remove_html_tags(h)
                vs_item_row.append(vs_item)
        else:
            vs_item_row.append("")
    vs_item_df = pd.DataFrame({'id':id_list,"vs_item_name":vs_item_row})
    external_scripts_row = []
    for i in df['external_scripts']:
        if i is None:
            external_scripts_value = ''
            external_scripts_row.append(external_scripts_value)
        else:
            for h in i:
                external_scripts_value = h
                external_scripts_row.append(external_scripts_value)
    external_scripts_df = pd.DataFrame({'id':id_list,"clean_external_scripts":external_scripts_row})
    print('joined table loaded successfully')
    df = pd.DataFrame(value)
    df['modified_gmt'] = pd.to_datetime(df['modified_gmt'])
    df['date_gmt'] = pd.to_datetime(df['date_gmt'])
    df['content'] = df['content'].apply(lambda s: mf.remove_html_tags(s))
    df['word_count'] = df['content'].apply(mf.count_words)
    df['content'] = df['content'].apply(lambda s: mf.remove_html_tags(s))
    df['word_count'] = df['content'].apply(mf.count_words)
    df['author'] = df['author'].apply(lambda x: x.get('display_name'))
    df['comments'] = df['comments'].apply(lambda x: x.get('comments'))
    df['comments'] = df['comments'].apply(mf.clean_comments_in_row)
    df['featured_image'] = df['featured_image'].apply(lambda x: x.get('source'))
    df['seo_title'] = df['seo'].apply(lambda x: x.get('title'))
    df['seo_description'] = df['seo'].apply(lambda x: x.get('description'))
    df['sponsor'] = df['sponsor'].apply(mf.clean_sponsor_name)
    df = df.merge(category_df, on='id', how='left')
    df = df.merge(tags_df, on='id', how='left')
    df = df.merge(company_df, on='id', how='left')
    df = df.merge(vs_item_df, on='id', how='left')
    df = df.merge(external_scripts_df, on='id', how='left')

    df = df.drop(columns=['seo', 'live_items', 'post_images', 'categories', 'tags', 'companies', 'vsitems', 'short_items', 'external_scripts', 'series_items'])

    df.to_csv(f"{STAGING_FILE_PATH}\page\page_{INGEST_TIME}.csv", index=False)
    print('dataframe count:', df.shape[0])
    print('Data cleaned successfully')

        
def store_data():
    df_sql = pd.read_csv(f"{STAGING_FILE_PATH}\page\page_{INGEST_TIME}.csv")
    engine_postgres = create_engine(f'postgresql+psycopg2://postgres:postgres@postgres:5432/airflow')
    df_sql.to_sql('page', engine_postgres, if_exists='replace', index=False)
    print('Data stored successfully')
    df_postgres = pd.read_sql('SELECT * FROM page', engine_postgres)
    print('Data loaded from Postgres:', df_postgres.shape[0])

# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'page_techinasia_pipeline',
    default_args=default_args,
    description='page ingestion pipeline for Tech in Asia',
    schedule_interval='@daily',
)

fetch_posts_task = PythonOperator(
    task_id='fetch_posts',
    python_callable=fetch_posts,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

store_data_task = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    dag=dag,
)

fetch_posts_task >> clean_data_task >> store_data_task
