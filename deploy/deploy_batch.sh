#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_ID:?}" "${REGION:?}" "${ARTIFACT_REPO:?}" "${BATCH_JOB:?}" "${LANDING_BUCKET:?}" "${RAW_BUCKET:?}"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$ARTIFACT_REPO/cpr-batch:v1"
gcloud builds submit batch --tag "$IMAGE"
gcloud run jobs deploy "$BATCH_JOB" --region "$REGION" --image "$IMAGE" --service-account "cpr-batch-sa@$PROJECT_ID.iam.gserviceaccount.com" --tasks 1 --max-retries 1 --task-timeout 3600s --memory 2Gi --set-env-vars "LANDING_BUCKET=$LANDING_BUCKET,RAW_BUCKET=$RAW_BUCKET,LANDING_PREFIX=${LANDING_PREFIX:-monthly/}"
echo "Grant Composer SA roles/run.jobsExecutor on this job."
