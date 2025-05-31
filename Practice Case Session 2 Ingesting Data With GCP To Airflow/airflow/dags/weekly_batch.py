import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


PROJECT_ID = "clever-seat-363006"
BUCKET = "fellowship-777"

dataset_file = "fraud_dataset.csv"
dataset_url = f"https://storage.cloud.google.com/fellowship-777/{dataset_file}"
# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_local_home = "/opt/airflow/"
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
    dag_id="weekly_batch",
    schedule_interval="@weekly",
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
            "object_name": f"data-lake/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
         task_id="bigquery_external_table_task",
         table_resource={
             "tableReference": {
                 "projectId": PROJECT_ID,
                 "datasetId": BIGQUERY_DATASET,
                 "tableId": "external_table",
             },
             "externalDataConfiguration": {
                 "sourceFormat": "CSV",
                 "sourceUris": [f"gs://{BUCKET}/data-lake/{dataset_file}"],
                 "autodetect" : True
             },
         },
     )

    download_dataset_task >> local_to_gcs_task  >> bigquery_external_table_task