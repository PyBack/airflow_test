import os
from datetime import datetime
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow import DAG
from pathlib import Path

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("cloud-composer-env-id")
PROJECT_ID = os.environ.get("your-project")

DAG_ID = "gcs_upload_download"

BUCKET_NAME = f"your-bucket"
FILE_NAME = "your-object"

PATH_TO_SAVED_FILE = "/home/airflow/gcs/data/<your-object>"
REMOTE_FILE_PATH = "/<your_local directory>/<your-object>"

"""
To transfer GCS object to Cloud Composer local, you can use GCSToLocalFilesystemOperator. 
After locally copied, you may use sftpoperator to transfer to your remote server.
"""

with models.DAG(
        DAG_ID,
        schedule_interval='@once',
        start_date=datetime(2022, 6, 15),
        catchup=False,
        tags=["gcs", "sftp",],
) as dag:
    # [START howto_operator_gcs_download_file_task]
    gcs_download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=FILE_NAME,
        bucket=BUCKET_NAME,
        filename=PATH_TO_SAVED_FILE,
    )
    # [END howto_operator_gcs_download_file_task]

    sftp_put_file = SFTPOperator(
        task_id="test_sftp",
        ssh_conn_id="20220615_connection",
        local_filepath=PATH_TO_SAVED_FILE,
        remote_filepath=REMOTE_FILE_PATH,
        operation="put",
        create_intermediate_dirs=True,
        dag=dag
    )

    gcs_download_file >> sftp_put_file
