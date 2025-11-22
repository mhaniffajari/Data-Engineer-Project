import pandas as pd

def csv_to_df(path):
    return pd.read_csv(path)

def df_to_csv(df, path):
    df.to_csv(path, index=False)
