import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT_ID = "clever-seat-363006"
BUCKET = "fellowship-777"

dataset = "fraud_dataset_timestamp"
dataset_file = "fraud_dataset_timestamp.csv"
dataset_url = f"https://storage.googleapis.com/fellowship-777/data-lake/fraud_dataset_timestamp.csv"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_local_home = "/home/airflow/gcs/data"
BIGQUERY_DATASET = os.environ.get("fraud_dataset", 'fraud_dataset')

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client(project ='clever-seat-363006')
    bucket = client.bucket(BUCKET)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="fraud_dataset_schema_timestamp_batch",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"data-lake-new/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    GCStoBQ = GCSToBigQueryOperator(
         task_id="GCStoBQ_task",
         bucket = 'fellowship-777',
         source_objects= "data-lake-new/fraud_dataset_timestamp.csv",
         destination_project_dataset_table=  "{}.{}.{}".format(PROJECT_ID,"fraud_dataset_timestamp","fraud_dataset_schema_timestamp_batch"),
         schema_fields=[{"name": "type", "type": "STRING", "mode": "NULLABLE"},
               {"name": "amount", "type": "STRING", "mode": "NULLABLE"},
               {"name": "nameOrig", "type": "STRING", "mode": "NULLABLE"},
               {"name": "oldbalanceOrg", "type": "STRING", "mode": "NULLABLE"},
               {"name": "newbalanceOrig", "type": "STRING", "mode": "NULLABLE"},
               {"name": "nameDest", "type": "STRING", "mode": "NULLABLE"},
               {"name": "oldbalanceDest", "type": "STRING", "mode": "NULLABLE"},
               {"name": "newbalanceDest", "type": "STRING", "mode": "NULLABLE"},
               {"name": "isFraud", "type": "STRING", "mode": "NULLABLE"},
               {"name": "isFlaggedFraud", "type": "STRING", "mode": "NULLABLE"},
               {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
               ],
        skip_leading_rows= 1
    )

    download_dataset_task >> local_to_gcs_task  >> GCStoBQ
