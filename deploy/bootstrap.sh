#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_ID:?}" "${REGION:?}" "${LANDING_BUCKET:?}" "${RAW_BUCKET:?}" "${CONTROL_BUCKET:?}" "${ARTIFACT_REPO:?}"
gcloud config set project "$PROJECT_ID"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com storage.googleapis.com composer.googleapis.com dataflow.googleapis.com compute.googleapis.com iam.googleapis.com
for BUCKET in "$LANDING_BUCKET" "$RAW_BUCKET" "$CONTROL_BUCKET"; do
  if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
    gcloud storage buckets create "gs://$BUCKET" --location="$REGION" --uniform-bucket-level-access
  fi
done
for NAME in cpr-batch-sa cpr-dataflow-sa; do
  gcloud iam service-accounts describe "$NAME@$PROJECT_ID.iam.gserviceaccount.com" >/dev/null 2>&1 || gcloud iam service-accounts create "$NAME"
done
BATCH_SA="cpr-batch-sa@$PROJECT_ID.iam.gserviceaccount.com"
DATAFLOW_SA="cpr-dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com"
gcloud storage buckets add-iam-policy-binding "gs://$LANDING_BUCKET" --member="serviceAccount:$BATCH_SA" --role=roles/storage.objectViewer
gcloud storage buckets add-iam-policy-binding "gs://$RAW_BUCKET" --member="serviceAccount:$BATCH_SA" --role=roles/storage.objectCreator
gcloud storage buckets add-iam-policy-binding "gs://$RAW_BUCKET" --member="serviceAccount:$BATCH_SA" --role=roles/storage.objectViewer
gcloud storage buckets add-iam-policy-binding "gs://$RAW_BUCKET" --member="serviceAccount:$DATAFLOW_SA" --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding "gs://$CONTROL_BUCKET" --member="serviceAccount:$DATAFLOW_SA" --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$DATAFLOW_SA" --role=roles/dataflow.worker
if ! gcloud artifacts repositories describe "$ARTIFACT_REPO" --location="$REGION" >/dev/null 2>&1; then
  gcloud artifacts repositories create "$ARTIFACT_REPO" --location="$REGION" --repository-format=docker
fi
echo "NOTE: Grant existing Composer environment service account roles/run.jobsExecutor on the deployed Job."
echo "NOTE: Dataflow launch identity needs roles/dataflow.developer and roles/iam.serviceAccountUser on $DATAFLOW_SA."
