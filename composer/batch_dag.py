"""CPR Bronze ingestion followed by metadata-driven DQ for every Bronze table."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "gcp-practice-for-highmark"
REGION = "us-central1"
DQ_SQL = (Path(__file__).parent / "bronze_all_tables_dq.sql").read_text(encoding="utf-8")

with DAG(
    dag_id="cpr_batch_to_raw",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["cpr", "batch", "bronze", "dq"],
) as dag:
    ingest = CloudRunExecuteJobOperator(
        task_id="ingest_csv_to_bigquery",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="cpr-gcs-to-bigquery",
        deferrable=False,
    )

    dq_validation = BigQueryInsertJobOperator(
        task_id="bronze_dq_validation",
        project_id=PROJECT_ID,
        location=REGION,
        configuration={"query": {"query": DQ_SQL, "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
    )

    ingest >> dq_validation
