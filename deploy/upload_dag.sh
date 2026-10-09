#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_ID:?}" "${REGION:?}" "${COMPOSER_ENV:?}" "${BATCH_JOB:?}"
gcloud composer environments update "$COMPOSER_ENV" --location "$REGION" --update-env-variables="PROJECT_ID=$PROJECT_ID,REGION=$REGION,BATCH_JOB=$BATCH_JOB"
gcloud composer environments storage dags import --environment "$COMPOSER_ENV" --location "$REGION" --source composer/batch_dag.py
