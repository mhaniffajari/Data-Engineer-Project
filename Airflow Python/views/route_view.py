# views/route_view.py

import pandas as pd
from utils.file import df_to_csv
import os

def generate_route_view():
    df = pd.read_csv("data target/target/transaksi_bus.csv")

    df_summary = df.groupby(
        ["tanggal_realisasi", "route_code", "route_name", "gate_in_boo"],
        as_index=False
    ).agg(
        jumlah_pelanggan=("customer_id", "count"),
        total_amount=("amount", "sum")
    )

    output_path = "data target/view/summary_route.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df_to_csv(df_summary, output_path)

    return output_path
