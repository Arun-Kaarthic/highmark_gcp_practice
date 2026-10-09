"""CPR batch adapter: profile landed CSVs and copy immutable source versions into Raw."""
import csv
import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

LANDING_BUCKET = os.environ["LANDING_BUCKET"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
LANDING_PREFIX = os.getenv("LANDING_PREFIX", "monthly/")


def profile_csv(blob):
    # Streaming read: do not load entire file into memory.
    with blob.open("rt", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if not header:
            raise ValueError(f"Empty CSV: {blob.name}")
        if len(header) != len(set(header)) or any(not h.strip() for h in header):
            raise ValueError(f"Invalid/duplicate headers: {blob.name}")
        count = 0
        for row in reader:
            if len(row) != len(header):
                raise ValueError(f"Column mismatch at data row {count + 1}: {blob.name}")
            count += 1
    return {"columns": header, "column_count": len(header), "data_rows": count}


def run():
    client = storage.Client()
    source_bucket = client.bucket(LANDING_BUCKET)
    raw_bucket = client.bucket(RAW_BUCKET)
    errors = []
    processed = 0
    for item in client.list_blobs(LANDING_BUCKET, prefix=LANDING_PREFIX):
        if not item.name.lower().endswith(".csv"):
            continue
        # Pin the source generation for metadata and copy. Versioning or source immutability recommended.
        source = source_bucket.blob(item.name, generation=item.generation)
        version = str(item.generation)
        raw_name = f"batch/{item.name}.generation-{version}.csv"
        target = raw_bucket.blob(raw_name)
        try:
            if target.exists():
                logging.info("Already copied %s", raw_name)
                continue
            stats = profile_csv(source)
            # Server-side copy with destination creation precondition and source generation precondition.
            try:
                source_bucket.copy_blob(
                    source, raw_bucket, new_name=raw_name,
                    if_generation_match=0,
                    if_source_generation_match=int(version),
                )
            except PreconditionFailed:
                if not target.exists():
                    raise
            meta = {
                "source_bucket": LANDING_BUCKET,
                "source_object": item.name,
                "source_generation": version,
                "raw_uri": f"gs://{RAW_BUCKET}/{raw_name}",
                "size_bytes": item.size,
                "content_type": item.content_type,
                "updated": item.updated.isoformat() if item.updated else None,
                "profile": stats,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            audit_name = f"metadata/batch/{hashlib.sha256((item.name + ':' + version).encode()).hexdigest()}.json"
            audit = raw_bucket.blob(audit_name)
            try:
                audit.upload_from_string(json.dumps(meta), content_type="application/json", if_generation_match=0)
            except PreconditionFailed:
                pass
            processed += 1
            logging.info("COPIED %s -> %s rows=%d", item.name, raw_name, stats["data_rows"])
        except Exception as exc:
            logging.exception("Failed processing %s", item.name)
            errors.append({"file": item.name, "error": str(exc)})
    logging.info("Processed %d files; failures=%d", processed, len(errors))
    if errors:
        raise RuntimeError(json.dumps(errors))


if __name__ == "__main__":
    run()
