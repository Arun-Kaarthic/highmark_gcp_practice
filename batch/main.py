"""Cloud Run Job: GCS Landing CSV -> BigQuery Bronze (one table per CSV stem).

Assumptions:
- Files are incremental APPENDS, not full monthly snapshots.
- CSV headers are stable per logical table.
- For production, use an explicit schema registry for IDs/dates and schema changes.
"""
import csv
import hashlib
import logging
import os
import re
from google.cloud import bigquery, storage
from google.api_core.exceptions import Conflict

logging.basicConfig(level=logging.INFO)
PROJECT_ID = os.environ.get('GCP_PROJECT_ID') or os.environ.get('GOOGLE_CLOUD_PROJECT')
LANDING_BUCKET = os.environ['LANDING_BUCKET']
LANDING_PREFIX = os.getenv('LANDING_PREFIX', '')
BQ_DATASET = os.getenv('BQ_DATASET', 'bronze')
BQ_LOCATION = os.getenv('REGION', 'us-central1')


def table_name(object_name):
    stem = object_name.rsplit('/', 1)[-1].rsplit('.', 1)[0]
    safe = re.sub(r'[^a-zA-Z0-9_]', '_', stem).lower().strip('_')
    if not safe:
        raise ValueError(f'Cannot derive table name from {object_name}')
    return ('t_' + safe) if safe[0].isdigit() else safe


def validate_header(blob):
    with blob.open('rt', encoding='utf-8-sig', newline='') as handle:
        header = next(csv.reader(handle), None)
    if not header or any(not h.strip() for h in header) or len(header) != len(set(header)):
        raise ValueError(f'Invalid header: {blob.name}')
    return header


def run():
    if not PROJECT_ID:
        raise ValueError('Set GCP_PROJECT_ID')
    gcs = storage.Client(project=PROJECT_ID)
    bq = bigquery.Client(project=PROJECT_ID)
    dataset = f'{PROJECT_ID}.{BQ_DATASET}'
    # bq.create_dataset(bigquery.Dataset(dataset), exists_ok=True)
    bq.get_dataset(dataset)
    found = loaded = skipped = 0
    errors = []
    for item in gcs.list_blobs(LANDING_BUCKET, prefix=LANDING_PREFIX):
        if not item.name.lower().endswith('.csv'):
            continue
        found += 1
        try:
            # Re-read immutable generation, and use deterministic BigQuery job ID
            blob = gcs.bucket(LANDING_BUCKET).blob(item.name, generation=item.generation)
            validate_header(blob)
            table = f'{dataset}.{table_name(item.name)}'
            LOAD_CONFIG_VERSION = "v3"

            identity = (
                f"{LANDING_BUCKET}/{item.name}"
                f"#{item.generation}#{LOAD_CONFIG_VERSION}"
            )

            digest = hashlib.sha256(identity.encode()).hexdigest()[:32]
            job_id = f"cpr_load_{digest}"
            config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                field_delimiter=",",
                quote_character='"',
                encoding="UTF-8",
                allow_quoted_newlines=True,
                column_name_character_map="V2",
                timestamp_format="DD-MM-YYYY HH24:MI:SS",
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
            # BigQuery load jobs take GCS object paths, not generation-specific URIs.
            # Require source objects to be immutable during processing.
            uri = f'gs://{LANDING_BUCKET}/{item.name}'
            try:
                job = bq.load_table_from_uri(uri, table, job_config=config,
                                             job_id=job_id, location=BQ_LOCATION)
            except Conflict:
                job = bq.get_job(job_id, location=BQ_LOCATION)
            job.result()
            # Reusing a successful deterministic job ID does not append again.
            loaded += 1
            logging.info('LOAD OK %s -> %s job=%s rows=%s', uri, table, job_id, job.output_rows)
        except Exception as exc:
            logging.exception('LOAD FAILED: %s', item.name)
            errors.append(f'{item.name}: {exc}')
    logging.info('CSV found=%d completed_or_existing_jobs=%d failures=%d', found, loaded, len(errors))
    if found == 0:
        raise RuntimeError(f'No CSV files found in gs://{LANDING_BUCKET}/{LANDING_PREFIX}')
    if errors:
        raise RuntimeError('; '.join(errors))

if __name__ == '__main__':
    run()
