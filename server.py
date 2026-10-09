
import json
import os
import re
import textwrap
import uuid

from fastmcp import FastMCP
from google.cloud import storage

mcp = FastMCP("GCP Pipeline Generator")
gcs = storage.Client()

BUCKET = os.environ["ARTIFACT_BUCKET"]


def safe_name(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,62}", value):
        raise ValueError(
            "Pipeline name must be lowercase snake_case"
        )
    return value


def write_artifact(path: str, content: str):
    bucket = gcs.bucket(BUCKET)
    bucket.blob(path).upload_from_string(
        content,
        content_type="text/plain",
        if_generation_match=0,
    )


@mcp.tool()
def generate_pipeline(
    pipeline_name: str,
    source_gcs_uri: str,
    raw_gcs_uri: str,
    refined_gcs_uri: str,
    project_id: str,
    region: str = "us-central1",
) -> dict:
    """Generate batch CSV pipeline artifacts in GCS.

    Does not execute or deploy any generated code.
    """

    name = safe_name(pipeline_name)

    for uri in (
        source_gcs_uri,
        raw_gcs_uri,
        refined_gcs_uri,
    ):
        if not uri.startswith("gs://"):
            raise ValueError("All storage URIs must use gs://")

    pipeline_id = f"{name}_{uuid.uuid4().hex[:8]}"
    prefix = f"pipelines/{pipeline_id}"

    spec = {
        "pipeline_id": pipeline_id,
        "pipeline_name": name,
        "source": source_gcs_uri,
        "raw": raw_gcs_uri,
        "refined": refined_gcs_uri,
        "project_id": project_id,
        "region": region,
        "status": "GENERATED_NOT_DEPLOYED",
    }

    ingestion = textwrap.dedent(f'''
        import argparse
        from google.cloud import storage

        def split_uri(uri):
            bucket, _, path = uri[5:].partition("/")
            return bucket, path

        def main():
            parser = argparse.ArgumentParser()
            parser.add_argument("--source", required=True)
            parser.add_argument("--target", required=True)
            args = parser.parse_args()

            client = storage.Client()
            sb, sp = split_uri(args.source)
            tb, tp = split_uri(args.target)

            source = client.bucket(sb).blob(sp)
            target = client.bucket(tb).blob(tp)

            client.copy_blob(source, client.bucket(tb), tp)
            print("Raw copy complete:", args.target)

        if __name__ == "__main__":
            main()
    ''').strip() + "\n"

    refine = textwrap.dedent('''
        import argparse
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import (
            col, trim, upper
        )

        def main():
            parser = argparse.ArgumentParser()
            parser.add_argument("--source", required=True)
            parser.add_argument("--target", required=True)
            args = parser.parse_args()

            spark = SparkSession.builder.getOrCreate()

            df = spark.read.option(
                "header", True
            ).csv(args.source)

            required = {
                "provider_id", "npi",
                "provider_name", "specialty"
            }

            missing = required - set(df.columns)
            if missing:
                raise ValueError(
                    f"Missing columns: {sorted(missing)}"
                )

            clean = (
                df.withColumn(
                    "provider_name",
                    upper(trim(col("provider_name")))
                )
                .withColumn(
                    "specialty",
                    trim(col("specialty"))
                )
                .dropDuplicates(["provider_id"])
            )

            clean.write.mode("overwrite").parquet(
                args.target
            )

        if __name__ == "__main__":
            main()
    ''').strip() + "\n"

    dq_sql = textwrap.dedent('''
        -- Replace the placeholder with an actual
        -- BigQuery table containing refined data.
        SELECT
            COUNT(*) AS total_records,
            COUNTIF(npi IS NULL OR TRIM(npi) = '')
                AS missing_npi,
            COUNT(DISTINCT provider_id)
                AS distinct_providers
        FROM `PROJECT.DATASET.PROVIDER_TABLE`;
    ''').strip() + "\n"

    dag = textwrap.dedent(f'''
        from datetime import datetime
        from airflow import DAG
        from airflow.providers.google.cloud.operators.dataproc import (
            DataprocCreateBatchOperator
        )

        PROJECT = {project_id!r}
        REGION = {region!r}
        ARTIFACT_BUCKET = {BUCKET!r}

        with DAG(
            dag_id={name!r},
            start_date=datetime(2026, 1, 1),
            schedule="@daily",
            catchup=False,
        ) as dag:

            refine = DataprocCreateBatchOperator(
                task_id="refine_provider",
                project_id=PROJECT,
                region=REGION,
                batch={{
                    "pyspark_batch": {{
                        "main_python_file_uri":
                            f"gs://{{ARTIFACT_BUCKET}}/"
                            "pipelines/{pipeline_id}/refine.py",
                        "args": [
                            "--source", {raw_gcs_uri!r},
                            "--target", {refined_gcs_uri!r}
                        ],
                    }}
                }},
                batch_id="provider-"
                         "{{{{ ts_nodash | lower }}}}",
            )
    ''').strip() + "\n"

    files = {
        "pipeline_spec.json": json.dumps(spec, indent=2),
        "ingestion.py": ingestion,
        "refine.py": refine,
        "data_quality.sql": dq_sql,
        "pipeline_dag.py": dag,
    }

    for filename, content in files.items():
        write_artifact(
            f"{prefix}/{filename}",
            content,
        )

    return {
        "pipeline_id": pipeline_id,
        "status": "GENERATED_NOT_DEPLOYED",
        "artifact_prefix": f"gs://{BUCKET}/{prefix}/",
        "files": list(files),
        "warnings": [
            "DAG currently contains only the refinement task.",
            "Ingestion must be deployed and orchestrated.",
            "BigQuery DQ query requires a real table.",
            "No tests or infrastructure validation have run.",
            "No MDM integration is configured.",
        ],
    }


@mcp.tool()
def get_pipeline_artifacts(pipeline_id: str) -> dict:
    """List previously generated pipeline artifacts."""
    if not re.fullmatch(
        r"[a-z][a-z0-9_]{2,62}_[0-9a-f]{8}",
        pipeline_id,
    ):
        raise ValueError("Invalid pipeline ID")

    prefix = f"pipelines/{pipeline_id}/"
    blobs = gcs.list_blobs(BUCKET, prefix=prefix)

    return {
        "pipeline_id": pipeline_id,
        "files": [
            f"gs://{BUCKET}/{blob.name}"
            for blob in blobs
        ],
    }


if __name__ == "__main__":
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8080")),
    )
