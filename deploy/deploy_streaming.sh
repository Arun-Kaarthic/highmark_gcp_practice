#!/usr/bin/env bash
set -euo pipefail
: "${PROJECT_ID:?}" "${REGION:?}" "${RAW_BUCKET:?}" "${CONTROL_BUCKET:?}" "${ARTIFACT_REPO:?}" "${KAFKA_BOOTSTRAP:?}" "${KAFKA_TOPIC:?}"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$ARTIFACT_REPO/cpr-kafka-python:v1"
SPEC="gs://$CONTROL_BUCKET/templates/cpr-kafka-python.json"
gcloud builds submit streaming --tag "$IMAGE"
sed "s|__IMAGE__|$IMAGE|g" streaming/template_spec.json.template > streaming/template_spec.json
gcloud storage cp streaming/template_spec.json "$SPEC"
ARGS=(--project="$PROJECT_ID" --region="$REGION" --template-file-gcs-location="$SPEC" --service-account-email="cpr-dataflow-sa@$PROJECT_ID.iam.gserviceaccount.com" --staging-location="gs://$CONTROL_BUCKET/dataflow/staging" --temp-location="gs://$CONTROL_BUCKET/dataflow/temp" --parameters="kafka_bootstrap=$KAFKA_BOOTSTRAP,kafka_topic=$KAFKA_TOPIC,kafka_group=${KAFKA_GROUP:-cpr-dataflow-raw},raw_prefix=gs://$RAW_BUCKET/cdc,kafka_security_protocol=${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}")
if [[ -n "${DATAFLOW_SUBNETWORK:-}" ]]; then ARGS+=(--subnetwork="$DATAFLOW_SUBNETWORK"); fi
if [[ -n "${KAFKA_USERNAME:-}" || -n "${KAFKA_PASSWORD:-}" ]]; then
  echo 'ERROR: Do not pass Kafka credentials as Flex Template parameters. Implement secret injection before enabling SASL.' >&2
  exit 1
fi
gcloud dataflow flex-template run "cpr-kafka-python-$(date +%s)" "${ARGS[@]}"
