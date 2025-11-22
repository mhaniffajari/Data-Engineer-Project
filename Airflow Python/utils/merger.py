import pandas as pd

def merge_by_pk(df_stage, df_target, pk):
    if df_target is None:
        return df_stage.copy()

    df = pd.concat([df_target, df_stage], ignore_index=True)
    df = df.drop_duplicates(subset=[pk], keep="last")
    return df
