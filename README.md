# CPR → GCP Intake & Ingestion — Python

Two independent pipelines, matching the requested architecture:

- **Batch**: CPR uploads CSV files to GCS Landing → Cloud Composer DAG → Python Cloud Run Job → validates/profiles CSV and copies versioned objects into GCS Raw.
- **Streaming CDC**: CPR Kafka topics → **Python Apache Beam** streaming Dataflow Flex Template → JSONL windows in GCS Raw, with a separate dead-letter path.

## Prerequisites

- Google Cloud Shell with `gcloud` and Bash; existing Cloud Composer environment.
- CPR Kafka brokers reachable from Dataflow workers, with required routing/firewall/VPN/Interconnect.
- For Python Beam `ReadFromKafka`, a **Java 17 runtime is included in the container** for the cross-language Kafka expansion service. The application pipeline is Python, but the Kafka connector uses Java internally.
- Set `KAFKA_SECURITY_PROTOCOL=PLAINTEXT` **only for trusted test Kafka clusters**. Production Kafka TLS/SASL credentials must be securely injected; the sample deployment refuses plaintext password parameters.

## Deploy

```bash
cp .env.example .env
# Edit .env for your project, buckets, Composer and Kafka settings
set -a; source .env; set +a
bash deploy/bootstrap.sh
bash deploy/deploy_batch.sh
bash deploy/upload_dag.sh
bash deploy/deploy_streaming.sh
```

Composer's actual service account must be granted `roles/run.jobsExecutor` on the Cloud Run Job. For launching Dataflow, grant the deployer `roles/dataflow.developer` and `roles/iam.serviceAccountUser` on the Dataflow worker service account; Cloud Build requires Artifact Registry push permissions. Dataflow workers also need access to their staging/temp bucket and the container image.

## Files

- `batch/main.py`: Python CSV validation, profiling, GCS copy and metadata JSON.
- `composer/batch_dag.py`: Airflow batch orchestration.
- `streaming/pipeline.py`: Python Beam Kafka CDC stream → windowed GCS JSONL and invalid-event dead-letter files.
- `streaming/Dockerfile`: Python Flex Template container with Java runtime for KafkaIO expansion.
- `deploy/`: bootstrap and deployment scripts.

## Limitations and production work

- This is a **starter implementation**, not a verified production deployment. It has not been tested against your CPR Kafka cluster or Composer environment.
- Streaming GCS output is **at least once**; downstream Bronze/Silver must deduplicate by a source event ID or Kafka coordinates.
- Kafka source records should contain `table` and `operation` (INSERT/UPDATE/DELETE). Modify `ParseCdc` to match your CPR message contract.
- The Python KafkaIO `ReadFromKafka` transform is a cross-language connector; Java 17 is required in the launcher and may be needed on worker harnesses depending on the Beam expansion setup.
- GCS JSONL is Raw storage; BigQuery table creation is intentionally outside these intake paths.
- For secure Kafka SASL/TLS in production, configure private networking, certificates and Secret Manager integration. Do not pass passwords in CLI arguments or template parameters.
- Cloud Run batch scanning is best for manageable CSV volumes; add sharding, per-source DAGs and audit reconciliation as needed.
