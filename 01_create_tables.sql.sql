-- Databricks notebook source
-- Weather Pipeline Unity Catalog DDL

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_bronze`;

-- `workspace`.`weather_bronze`.`api_responses`
CREATE TABLE workspace.weather_bronze.api_responses (
  response_id STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  attempt INT,
  ingested_at TIMESTAMP,
  endpoint STRING COLLATE UTF8_BINARY,
  request_json STRING COLLATE UTF8_BINARY,
  http_status INT,
  raw_response STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_config`;

-- `workspace`.`weather_config`.`locations`
CREATE TABLE workspace.weather_config.locations (
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  latitude DOUBLE,
  longitude DOUBLE,
  local_timezone STRING COLLATE UTF8_BINARY,
  enabled BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_demo_bronze`;

-- `workspace`.`weather_demo_bronze`.`api_responses`
CREATE TABLE workspace.weather_demo_bronze.api_responses (
  response_id STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  attempt INT,
  ingested_at TIMESTAMP,
  endpoint STRING COLLATE UTF8_BINARY,
  request_json STRING COLLATE UTF8_BINARY,
  http_status INT,
  raw_response STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_demo_config`;

-- `workspace`.`weather_demo_config`.`locations`
CREATE TABLE workspace.weather_demo_config.locations (
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  latitude DOUBLE,
  longitude DOUBLE,
  local_timezone STRING COLLATE UTF8_BINARY,
  enabled BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_demo_gold`;

-- `workspace`.`weather_demo_gold`.`daily_weather_summary`
CREATE TABLE workspace.weather_demo_gold.daily_weather_summary (
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  weather_date DATE,
  min_temperature_c DOUBLE,
  max_temperature_c DOUBLE,
  avg_temperature_c DOUBLE,
  avg_humidity_pct DOUBLE,
  total_precipitation_mm DOUBLE,
  max_wind_speed_kmh DOUBLE,
  hours_available BIGINT,
  is_complete_day BOOLEAN,
  is_rainy_day BOOLEAN,
  heat_alert BOOLEAN,
  wind_alert BOOLEAN,
  latest_ingested_at TIMESTAMP,
  gold_updated_at TIMESTAMP,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_demo_ops`;

-- `workspace`.`weather_demo_ops`.`api_attempts`
CREATE TABLE workspace.weather_demo_ops.api_attempts (
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  attempt INT,
  source_kind STRING COLLATE UTF8_BINARY,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds DOUBLE,
  http_status INT,
  response_id STRING COLLATE UTF8_BINARY,
  error_message STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_demo_ops`.`location_runs`
CREATE TABLE workspace.weather_demo_ops.location_runs (
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds DOUBLE,
  status STRING COLLATE UTF8_BINARY,
  request_json STRING COLLATE UTF8_BINARY,
  http_status INT,
  attempts INT,
  response_id STRING COLLATE UTF8_BINARY,
  rows_received BIGINT,
  rows_valid BIGINT,
  rows_rejected BIGINT,
  payloads_rejected BIGINT,
  silver_rows_upserted BIGINT,
  gold_rows_upserted BIGINT,
  schema_event_count INT,
  error_message STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_demo_ops`.`pipeline_runs`
CREATE TABLE workspace.weather_demo_ops.pipeline_runs (
  run_id STRING COLLATE UTF8_BINARY,
  run_mode STRING COLLATE UTF8_BINARY,
  scenario STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds DOUBLE,
  status STRING COLLATE UTF8_BINARY,
  parameters_json STRING COLLATE UTF8_BINARY,
  locations_total INT,
  locations_succeeded INT,
  locations_failed INT,
  rows_received BIGINT,
  rows_valid BIGINT,
  rows_rejected BIGINT,
  payloads_rejected BIGINT,
  silver_rows_upserted BIGINT,
  gold_rows_upserted BIGINT,
  error_message STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_demo_ops`.`schema_events`
CREATE TABLE workspace.weather_demo_ops.schema_events (
  event_id STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  response_id STRING COLLATE UTF8_BINARY,
  detected_at TIMESTAMP,
  change_type STRING COLLATE UTF8_BINARY,
  field_name STRING COLLATE UTF8_BINARY,
  details STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_demo_ops`.`v_latest_run`
CREATE VIEW weather_demo_ops.v_latest_run (
  run_id,
  run_mode,
  scenario,
  source_kind,
  started_at,
  ended_at,
  duration_seconds,
  status,
  parameters_json,
  locations_total,
  locations_succeeded,
  locations_failed,
  rows_received,
  rows_valid,
  rows_rejected,
  payloads_rejected,
  silver_rows_upserted,
  gold_rows_upserted,
  error_message)
WITH SCHEMA COMPENSATION
AS SELECT * FROM `workspace`.`weather_demo_ops`.`pipeline_runs`
        QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC, run_id DESC)=1
;

-- `workspace`.`weather_demo_ops`.`v_location_health`
CREATE VIEW weather_demo_ops.v_location_health (
  location_id,
  city_name,
  latest_status,
  latest_attempt_at,
  last_successful_ingestion_at,
  freshness_minutes,
  last_row_ingested_at,
  stored_hourly_rows,
  rows_valid,
  rows_rejected,
  payloads_rejected,
  error_message)
WITH SCHEMA COMPENSATION
AS WITH latest AS (
          SELECT * FROM `workspace`.`weather_demo_ops`.`location_runs`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY started_at DESC, run_id DESC)=1
        ), good AS (
          SELECT location_id, MAX(ended_at) last_successful_ingestion_at
          FROM `workspace`.`weather_demo_ops`.`location_runs`
          WHERE status IN ('SUCCESS','SUCCESS_WITH_WARNINGS') GROUP BY location_id
        ), stored AS (
          SELECT location_id, COUNT(*) stored_hourly_rows,
                 MAX(ingested_at) last_row_ingested_at
          FROM `workspace`.`weather_demo_silver`.`hourly_weather` GROUP BY location_id
        )
        SELECT c.location_id, c.city_name, l.status latest_status, l.ended_at latest_attempt_at,
          g.last_successful_ingestion_at,
          TIMESTAMPDIFF(MINUTE,g.last_successful_ingestion_at,CURRENT_TIMESTAMP()) freshness_minutes,
          s.last_row_ingested_at, COALESCE(s.stored_hourly_rows,0) stored_hourly_rows,
          l.rows_valid, l.rows_rejected, l.payloads_rejected, l.error_message
        FROM `workspace`.`weather_demo_config`.`locations` c
        LEFT JOIN latest l ON c.location_id=l.location_id
        LEFT JOIN good g ON c.location_id=g.location_id
        LEFT JOIN stored s ON c.location_id=s.location_id WHERE c.enabled=true
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_demo_silver`;

-- `workspace`.`weather_demo_silver`.`hourly_weather`
CREATE TABLE workspace.weather_demo_silver.hourly_weather (
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  weather_timestamp TIMESTAMP,
  weather_date DATE,
  temperature_c DOUBLE,
  humidity_pct DOUBLE,
  precipitation_mm DOUBLE,
  wind_speed_kmh DOUBLE,
  source_response_id STRING COLLATE UTF8_BINARY,
  source_run_id STRING COLLATE UTF8_BINARY,
  ingested_at TIMESTAMP,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_demo_silver`.`quarantine`
CREATE TABLE workspace.weather_demo_silver.quarantine (
  quarantine_id STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  response_id STRING COLLATE UTF8_BINARY,
  rejected_at TIMESTAMP,
  record_scope STRING COLLATE UTF8_BINARY,
  row_index INT,
  raw_record STRING COLLATE UTF8_BINARY,
  reason STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_gold`;

-- `workspace`.`weather_gold`.`daily_weather_summary`
CREATE TABLE workspace.weather_gold.daily_weather_summary (
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  weather_date DATE,
  min_temperature_c DOUBLE,
  max_temperature_c DOUBLE,
  avg_temperature_c DOUBLE,
  avg_humidity_pct DOUBLE,
  total_precipitation_mm DOUBLE,
  max_wind_speed_kmh DOUBLE,
  hours_available BIGINT,
  is_complete_day BOOLEAN,
  is_rainy_day BOOLEAN,
  heat_alert BOOLEAN,
  wind_alert BOOLEAN,
  latest_ingested_at TIMESTAMP,
  gold_updated_at TIMESTAMP,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_ops`;

-- `workspace`.`weather_ops`.`api_attempts`
CREATE TABLE workspace.weather_ops.api_attempts (
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  attempt INT,
  source_kind STRING COLLATE UTF8_BINARY,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds DOUBLE,
  http_status INT,
  response_id STRING COLLATE UTF8_BINARY,
  error_message STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_ops`.`location_runs`
CREATE TABLE workspace.weather_ops.location_runs (
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds DOUBLE,
  status STRING COLLATE UTF8_BINARY,
  request_json STRING COLLATE UTF8_BINARY,
  http_status INT,
  attempts INT,
  response_id STRING COLLATE UTF8_BINARY,
  rows_received BIGINT,
  rows_valid BIGINT,
  rows_rejected BIGINT,
  payloads_rejected BIGINT,
  silver_rows_upserted BIGINT,
  gold_rows_upserted BIGINT,
  schema_event_count INT,
  error_message STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0',
  'delta.workloadBasedColumns.deltaFileStatistics' = '`rows_rejected`,`payloads_rejected`')
;

-- `workspace`.`weather_ops`.`pipeline_runs`
CREATE TABLE workspace.weather_ops.pipeline_runs (
  run_id STRING COLLATE UTF8_BINARY,
  run_mode STRING COLLATE UTF8_BINARY,
  scenario STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds DOUBLE,
  status STRING COLLATE UTF8_BINARY,
  parameters_json STRING COLLATE UTF8_BINARY,
  locations_total INT,
  locations_succeeded INT,
  locations_failed INT,
  rows_received BIGINT,
  rows_valid BIGINT,
  rows_rejected BIGINT,
  payloads_rejected BIGINT,
  silver_rows_upserted BIGINT,
  gold_rows_upserted BIGINT,
  error_message STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_ops`.`schema_events`
CREATE TABLE workspace.weather_ops.schema_events (
  event_id STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  response_id STRING COLLATE UTF8_BINARY,
  detected_at TIMESTAMP,
  change_type STRING COLLATE UTF8_BINARY,
  field_name STRING COLLATE UTF8_BINARY,
  details STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;

-- `workspace`.`weather_ops`.`v_latest_run`
CREATE VIEW weather_ops.v_latest_run (
  run_id,
  run_mode,
  scenario,
  source_kind,
  started_at,
  ended_at,
  duration_seconds,
  status,
  parameters_json,
  locations_total,
  locations_succeeded,
  locations_failed,
  rows_received,
  rows_valid,
  rows_rejected,
  payloads_rejected,
  silver_rows_upserted,
  gold_rows_upserted,
  error_message)
WITH SCHEMA COMPENSATION
AS SELECT * FROM `workspace`.`weather_ops`.`pipeline_runs`
        QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC, run_id DESC)=1
;

-- `workspace`.`weather_ops`.`v_location_health`
CREATE VIEW weather_ops.v_location_health (
  location_id,
  city_name,
  latest_status,
  latest_attempt_at,
  last_successful_ingestion_at,
  freshness_minutes,
  last_row_ingested_at,
  stored_hourly_rows,
  rows_valid,
  rows_rejected,
  payloads_rejected,
  error_message)
WITH SCHEMA COMPENSATION
AS WITH latest AS (
          SELECT * FROM `workspace`.`weather_ops`.`location_runs`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY started_at DESC, run_id DESC)=1
        ), good AS (
          SELECT location_id, MAX(ended_at) last_successful_ingestion_at
          FROM `workspace`.`weather_ops`.`location_runs`
          WHERE status IN ('SUCCESS','SUCCESS_WITH_WARNINGS') GROUP BY location_id
        ), stored AS (
          SELECT location_id, COUNT(*) stored_hourly_rows,
                 MAX(ingested_at) last_row_ingested_at
          FROM `workspace`.`weather_silver`.`hourly_weather` GROUP BY location_id
        )
        SELECT c.location_id, c.city_name, l.status latest_status, l.ended_at latest_attempt_at,
          g.last_successful_ingestion_at,
          TIMESTAMPDIFF(MINUTE,g.last_successful_ingestion_at,CURRENT_TIMESTAMP()) freshness_minutes,
          s.last_row_ingested_at, COALESCE(s.stored_hourly_rows,0) stored_hourly_rows,
          l.rows_valid, l.rows_rejected, l.payloads_rejected, l.error_message
        FROM `workspace`.`weather_config`.`locations` c
        LEFT JOIN latest l ON c.location_id=l.location_id
        LEFT JOIN good g ON c.location_id=g.location_id
        LEFT JOIN stored s ON c.location_id=s.location_id WHERE c.enabled=true
;

CREATE SCHEMA IF NOT EXISTS `workspace`.`weather_silver`;

-- `workspace`.`weather_silver`.`hourly_weather`
CREATE TABLE workspace.weather_silver.hourly_weather (
  location_id STRING COLLATE UTF8_BINARY,
  city_name STRING COLLATE UTF8_BINARY,
  weather_timestamp TIMESTAMP,
  weather_date DATE,
  temperature_c DOUBLE,
  humidity_pct DOUBLE,
  precipitation_mm DOUBLE,
  wind_speed_kmh DOUBLE,
  source_response_id STRING COLLATE UTF8_BINARY,
  source_run_id STRING COLLATE UTF8_BINARY,
  ingested_at TIMESTAMP,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0',
  'delta.workloadBasedColumns.deltaFileStatistics' = '`city_name`,`weather_timestamp`,`location_id`,`weather_date`,`humidity_pct`')
;

-- `workspace`.`weather_silver`.`quarantine`
CREATE TABLE workspace.weather_silver.quarantine (
  quarantine_id STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  location_id STRING COLLATE UTF8_BINARY,
  response_id STRING COLLATE UTF8_BINARY,
  rejected_at TIMESTAMP,
  record_scope STRING COLLATE UTF8_BINARY,
  row_index INT,
  raw_record STRING COLLATE UTF8_BINARY,
  reason STRING COLLATE UTF8_BINARY,
  source_kind STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0')
;
