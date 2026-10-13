-- Automatically validate every BASE TABLE in the Bronze dataset.
-- Null counts are recorded, not treated as failures without business-key rules.
CREATE SCHEMA IF NOT EXISTS `gcp-practice-for-highmark.dq`
OPTIONS(location = 'us-central1');

CREATE TABLE IF NOT EXISTS `gcp-practice-for-highmark.dq.validation_results` (
  run_id STRING,
  table_name STRING,
  column_name STRING,
  check_name STRING,
  checked_rows INT64,
  failed_records INT64,
  status STRING,
  checked_at TIMESTAMP
);

DECLARE dq_run_id STRING DEFAULT GENERATE_UUID();
DECLARE n_tables INT64 DEFAULT 0;
DECLARE n_failed INT64 DEFAULT 0;
DECLARE row_count INT64;
DECLARE null_count INT64;

FOR t IN (
  SELECT table_name
  FROM `gcp-practice-for-highmark.bronze.INFORMATION_SCHEMA.TABLES`
  WHERE table_type = 'BASE TABLE'
  ORDER BY table_name
) DO
  SET n_tables = n_tables + 1;
  EXECUTE IMMEDIATE FORMAT(
    'SELECT COUNT(*) FROM `gcp-practice-for-highmark.bronze.%s`', t.table_name
  ) INTO row_count;

  INSERT INTO `gcp-practice-for-highmark.dq.validation_results`
  VALUES (dq_run_id, t.table_name, NULL, 'NOT_EMPTY', row_count,
          IF(row_count = 0, 1, 0), IF(row_count = 0, 'FAIL', 'PASS'), CURRENT_TIMESTAMP());
  IF row_count = 0 THEN
    SET n_failed = n_failed + 1;
  END IF;

  FOR c IN (
    SELECT column_name
    FROM `gcp-practice-for-highmark.bronze.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = t.table_name
    ORDER BY ordinal_position
  ) DO
    EXECUTE IMMEDIATE FORMAT(
      'SELECT COUNTIF(`%s` IS NULL) FROM `gcp-practice-for-highmark.bronze.%s`',
      REPLACE(c.column_name, '`', ''), t.table_name
    ) INTO null_count;
    INSERT INTO `gcp-practice-for-highmark.dq.validation_results`
    VALUES (dq_run_id, t.table_name, c.column_name, 'NULL_COUNT', row_count,
            null_count, 'INFO', CURRENT_TIMESTAMP());
  END FOR;
END FOR;

ASSERT n_tables > 0 AS 'DQ FAILED: no Bronze tables found';
ASSERT n_failed = 0 AS 'DQ FAILED: one or more Bronze tables are empty; inspect dq.validation_results';
