import pandas as pd
import gspread
from google.oauth2 import service_account
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from clickhouse_driver import Client

def authenticate_gspread(path_to_key, scope):
    credentials = service_account.Credentials.from_service_account_file(path_to_key, scopes=scope)
    gc = gspread.authorize(credentials)
    return gc

def get_worksheet_data(gc, url, index):
    book = gc.open_by_url(url)
    worksheet = book.get_worksheet(index)
    table = worksheet.get_all_values()
    df = pd.DataFrame(table[1:], columns=table[0])
    return df

def truncate_table(engine, table_name):
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute(f'TRUNCATE TABLE {table_name}')
    session.commit()
    session.close()

def dataframe_to_postgresql(engine, dataframe, table_name):
    conn = engine.connect()
    dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
    conn.close()

def sql_to_dataframe(engine,table_name):
    conn = engine.connect()
    df = pd.read_sql(table_name,con=engine)
    return df

def dataframe_to_clickhouse(account_setting,dataframe,table_name,your_columns):
    client = Client(account_setting)
    client.insert_dataframe(f'INSERT INTO "{TabError}" {your_columns} VALUES', dataframe, settings=dict(use_numpy=True),)