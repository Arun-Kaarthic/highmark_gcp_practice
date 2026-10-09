"""Composer DAG: scheduled CPR batch intake through Cloud Run Jobs."""
import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

with DAG(
    dag_id="cpr_batch_to_raw",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["cpr", "batch", "raw"],
) as dag:
    ingest = CloudRunExecuteJobOperator(
        task_id="validate_profile_copy_to_raw",
        project_id=os.environ["PROJECT_ID"],
        region=os.environ["REGION"],
        job_name=os.environ["BATCH_JOB"],
        deferrable=False,
    )
