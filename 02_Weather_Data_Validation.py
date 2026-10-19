# Databricks notebook source
# MAGIC %md
# MAGIC # Weather project: SQL validation
# MAGIC
# MAGIC 1. Keep `catalog` blank to use the current catalog, or enter the catalog printed by the pipeline.
# MAGIC 2. Select `run_mode=live` for real weather or `demo` for fixture evidence.
# MAGIC 3. Leave `validation_run_id` blank to select the latest run, or paste a specific run ID from integration results.
# MAGIC 4. Leave `window_start` and `window_end` blank to use that selected run's requested window.
# MAGIC 5. Click **Run All**. Each section displays the SQL, results and expected interpretation.

# COMMAND ----------

import json
import re
from datetime import datetime

controls = {"catalog": "", "run_mode": "live", "validation_run_id": "",
            "window_start": "", "window_end": "", "freshness_limit_minutes": "1440"}
existing = dbutils.widgets.getAll()
for name, default in controls.items():
    if name not in existing:
        if name == "run_mode":
            dbutils.widgets.dropdown(name, default, ["live", "demo"])
        else:
            dbutils.widgets.text(name, default)
CATALOG = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
MODE = dbutils.widgets.get("run_mode")
if MODE not in ("live", "demo"):
    raise ValueError("Choose live or demo")
PREFIX = "weather" if MODE == "live" else "weather_demo"
FRESHNESS_LIMIT = int(dbutils.widgets.get("freshness_limit_minutes"))
if FRESHNESS_LIMIT <= 0:
    raise ValueError("freshness_limit_minutes must be positive")
spark.conf.set("spark.sql.session.timeZone", "UTC")
def quoted_identifier(value):
    return "`" + value.replace("`", "``") + "`"
def literal(value):
    return "'" + value.replace("'", "''") + "'"
def table(layer, name):
    return f"{quoted_identifier(CATALOG)}.{quoted_identifier(PREFIX + '_' + layer)}.{quoted_identifier(name)}"
T = {"config": table("config", "locations"), "bronze": table("bronze", "api_responses"),
     "silver": table("silver", "hourly_weather"), "quarantine": table("silver", "quarantine"),
     "gold": table("gold", "daily_weather_summary"), "runs": table("ops", "pipeline_runs"),
     "locations": table("ops", "location_runs"), "attempts": table("ops", "api_attempts"),
     "events": table("ops", "schema_events")}
missing = [name for name in T.values() if not spark.catalog.tableExists(name)]
if missing:
    raise RuntimeError("Missing tables: " + ", ".join(missing) +
                       ". Check catalog/mode and run the pipeline in that mode first.")
requested_run = dbutils.widgets.get("validation_run_id").strip()
where = " WHERE run_id = " + literal(requested_run) if requested_run else ""
selected = spark.sql(f"SELECT * FROM {T['runs']}{where} ORDER BY started_at DESC, run_id DESC LIMIT 1").collect()
if not selected:
    raise RuntimeError("No matching audit run. Run the pipeline first or check validation_run_id.")
RUN = selected[0].asDict()
RUN_ID = RUN["run_id"]
RID = literal(RUN_ID)
params = json.loads(RUN["parameters_json"])
WSTART = dbutils.widgets.get("window_start").strip() or params["start_date"]
WEND = dbutils.widgets.get("window_end").strip() or params["end_date"]
for value in (WSTART, WEND):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Window dates must be YYYY-MM-DD")
    datetime.strptime(value, "%Y-%m-%d")
if WEND < WSTART:
    raise ValueError("window_end must be on or after window_start")
EXPECTED_HOURS = ((datetime.fromisoformat(WEND)-datetime.fromisoformat(WSTART)).days + 1) * 24
print(f"Catalog: {CATALOG} | mode: {MODE} | selected run: {RUN_ID}")
print(f"Run status: {RUN['status']} | scenario: {RUN['scenario']} | started: {RUN['started_at']}")
print(f"Validation UTC window: {WSTART} through {WEND} | {EXPECTED_HOURS} hours/location")
print("History/quality/uniqueness checks inspect all stored data; run evidence checks inspect the selected run.")
CHECKS = []

def run_sql(title, sql):
    print("\n" + title + "\n" + sql.strip() + ";")
    display(spark.sql(sql))

def sql_check(name, violations_sql, review=False):
    print("\n" + name + "\n" + violations_sql.strip() + ";")
    frame = spark.sql(violations_sql)
    count = frame.count()
    result = "PASS" if count == 0 else ("REVIEW" if review else "FAIL")
    CHECKS.append({"check": name, "result": result, "violations": count,
                   "selected_run_id": RUN_ID, "mode": MODE})
    print(f"{result}: {count} exception row(s)")
    if count:
        display(frame.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tables and schema
# MAGIC Expected: explicit timestamp/date and double weather columns. These commands inspect metadata only.

# COMMAND ----------

run_sql("Silver schema", f"DESCRIBE TABLE {T['silver']}")
run_sql("Gold schema", f"DESCRIBE TABLE {T['gold']}")
run_sql("Configured locations", f"SELECT * FROM {T['config']} ORDER BY location_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Original responses and structured data
# MAGIC Bronze is shown for the selected run; silver/gold show the selected date window.
# MAGIC Older accepted silver rows may remain after a later failed pull. Inspect source_run_id for provenance.

# COMMAND ----------

run_sql("Bronze responses", f"""SELECT run_id, response_id, location_id, attempt, ingested_at,
    http_status, source_kind, request_json, raw_response
    FROM {T['bronze']} WHERE run_id={RID} ORDER BY location_id, attempt LIMIT 20""")
run_sql("Silver hourly preview", f"""SELECT city_name, weather_timestamp, temperature_c,
    humidity_pct, precipitation_mm, wind_speed_kmh, source_run_id, source_response_id, ingested_at
    FROM {T['silver']} WHERE weather_date BETWEEN DATE '{WSTART}' AND DATE '{WEND}'
    ORDER BY city_name, weather_timestamp LIMIT 30""")
run_sql("Gold daily results", f"""SELECT * FROM {T['gold']}
    WHERE weather_date BETWEEN DATE '{WSTART}' AND DATE '{WEND}'
    ORDER BY weather_date, city_name""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run history and selected location results
# MAGIC Clean three-day/five-city run: five successful locations, 360 valid hourly rows and 15 daily keys upserted.
# MAGIC Upsert counts are processed keys, not net-new inserts. Deliberate failures legitimately change these counts.

# COMMAND ----------

run_sql("Run history", f"""SELECT run_id, started_at, status, scenario, duration_seconds,
    locations_total, locations_succeeded, locations_failed, rows_received, rows_valid,
    rows_rejected, payloads_rejected, silver_rows_upserted, gold_rows_upserted, error_message
    FROM {T['runs']} ORDER BY started_at DESC LIMIT 30""")
run_sql("Selected run", f"SELECT * FROM {T['runs']} WHERE run_id={RID}")
run_sql("Selected location outcomes", f"""SELECT city_name, status, http_status, attempts,
    rows_received, rows_valid, rows_rejected, payloads_rejected,
    silver_rows_upserted, gold_rows_upserted, schema_event_count, error_message
    FROM {T['locations']} WHERE run_id={RID} ORDER BY city_name""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Window coverage, including missing hours
# MAGIC A complete UTC day has 24 unique hours. The coverage check generates every expected city/hour and finds missing keys.
# MAGIC REVIEW can be expected after failure or quarantine scenarios. Coverage reflects current stored data, not proof that the
# MAGIC selected run loaded all those rows. Enabled city configuration is current and may differ from historical configuration.

# COMMAND ----------

run_sql("Coverage by city/day", f"""SELECT city_name, weather_date, COUNT(*) hourly_rows,
    MIN(weather_timestamp) first_hour, MAX(weather_timestamp) last_hour
    FROM {T['silver']} WHERE weather_date BETWEEN DATE '{WSTART}' AND DATE '{WEND}'
    GROUP BY city_name, weather_date ORDER BY city_name, weather_date""")
sql_check("Missing expected city/hour keys", f"""WITH hours AS (
    SELECT EXPLODE(SEQUENCE(TIMESTAMP '{WSTART} 00:00:00',
        TIMESTAMP '{WEND} 23:00:00', INTERVAL 1 HOUR)) weather_timestamp
    ), expected AS (
    SELECT c.location_id, c.city_name, h.weather_timestamp
    FROM {T['config']} c CROSS JOIN hours h WHERE c.enabled=true
    )
    SELECT e.* FROM expected e LEFT ANTI JOIN {T['silver']} s
    ON e.location_id=s.location_id AND e.weather_timestamp=s.weather_timestamp""", review=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Duplicate business keys
# MAGIC Expected: PASS with zero duplicate groups. Applies to all dates in the selected live/demo tables.
# MAGIC Silver key=location/hour; gold key=location/day. This checks stored uniqueness, not pre/post key-set equality;
# MAGIC the pipeline integration suite performs the controlled rerun key-set test.

# COMMAND ----------

sql_check("Silver unique keys", f"""SELECT location_id, weather_timestamp, COUNT(*) duplicate_count
    FROM {T['silver']} GROUP BY location_id, weather_timestamp HAVING COUNT(*)>1""")
sql_check("Gold unique keys", f"""SELECT location_id, weather_date, COUNT(*) duplicate_count
    FROM {T['gold']} GROUP BY location_id, weather_date HAVING COUNT(*)>1""")
run_sql("Bronze append history", f"""SELECT run_id, location_id, COUNT(*) responses_received,
    MIN(ingested_at) first_response_at, MAX(ingested_at) last_response_at
    FROM {T['bronze']} GROUP BY run_id, location_id ORDER BY last_response_at DESC LIMIT 40""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Stored silver quality and timestamp rules
# MAGIC Expected: zero invalid records, even after fault tests. Invalid input belongs in quarantine.
# MAGIC Checks finite numbers, ranges, required values, UTC hour alignment and weather_date consistency.

# COMMAND ----------

sql_check("Silver value quality", f"""SELECT * FROM {T['silver']}
    WHERE location_id IS NULL OR city_name IS NULL OR weather_timestamp IS NULL
    OR weather_date IS NULL OR temperature_c IS NULL OR humidity_pct IS NULL
    OR precipitation_mm IS NULL OR wind_speed_kmh IS NULL
    OR temperature_c < -100 OR temperature_c > 70 OR humidity_pct < 0 OR humidity_pct > 100
    OR precipitation_mm < 0 OR wind_speed_kmh < 0
    OR ISNAN(temperature_c) OR ISNAN(humidity_pct) OR ISNAN(precipitation_mm) OR ISNAN(wind_speed_kmh)
    OR ABS(precipitation_mm)=CAST('Infinity' AS DOUBLE)
    OR ABS(wind_speed_kmh)=CAST('Infinity' AS DOUBLE)""")
sql_check("Hourly timestamp/date consistency", f"""SELECT * FROM {T['silver']}
    WHERE NOT (weather_timestamp <=> DATE_TRUNC('HOUR',weather_timestamp))
       OR NOT (weather_date <=> CAST(weather_timestamp AS DATE))""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver-to-bronze lineage
# MAGIC Expected: every stored silver record refers to an existing successful bronze response for the same city and run.
# MAGIC Duplicate bronze response IDs also fail; raw bodies remain unchanged in bronze.

# COMMAND ----------

sql_check("Unique bronze response IDs", f"""SELECT response_id, COUNT(*) duplicate_count
    FROM {T['bronze']} GROUP BY response_id HAVING response_id IS NULL OR COUNT(*)>1""")
sql_check("Silver source linkage", f"""SELECT s.location_id, s.weather_timestamp,
    s.source_response_id, s.source_run_id, b.run_id bronze_run_id, b.http_status
    FROM {T['silver']} s LEFT JOIN {T['bronze']} b ON s.source_response_id=b.response_id
    WHERE b.response_id IS NULL OR b.http_status < 200 OR b.http_status >= 300
       OR NOT (s.location_id <=> b.location_id) OR NOT (s.source_run_id <=> b.run_id)
       OR NOT (s.source_kind <=> b.source_kind)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold reconciliation against silver
# MAGIC Independently recomputes all daily metrics and alert/completeness flags from silver.
# MAGIC Expected: PASS with zero mismatches. A crashed run between silver and gold writes can legitimately expose stale gold;
# MAGIC repair through a same-window successful rerun. Floating-point comparisons use a 0.000001 tolerance.

# COMMAND ----------

sql_check("Gold matches silver aggregations", f"""WITH expected AS (
    SELECT location_id, weather_date, COUNT(*) hours_available,
      MIN(temperature_c) min_temperature_c, MAX(temperature_c) max_temperature_c,
      AVG(temperature_c) avg_temperature_c, AVG(humidity_pct) avg_humidity_pct,
      SUM(precipitation_mm) total_precipitation_mm, MAX(wind_speed_kmh) max_wind_speed_kmh,
      COUNT(*)=24 is_complete_day, SUM(precipitation_mm)>0 is_rainy_day,
      MAX(temperature_c)>=35 heat_alert, MAX(wind_speed_kmh)>=40 wind_alert
    FROM {T['silver']} GROUP BY location_id, weather_date
    )
    SELECT COALESCE(e.location_id,g.location_id) location_id,
      COALESCE(e.weather_date,g.weather_date) weather_date,
      e.hours_available expected_hours, g.hours_available actual_hours,
      e.avg_temperature_c expected_temperature, g.avg_temperature_c actual_temperature,
      e.total_precipitation_mm expected_precipitation, g.total_precipitation_mm actual_precipitation
    FROM expected e FULL OUTER JOIN {T['gold']} g
      ON e.location_id=g.location_id AND e.weather_date=g.weather_date
    WHERE e.location_id IS NULL OR g.location_id IS NULL
      OR NOT (e.hours_available <=> g.hours_available)
      OR NOT (e.min_temperature_c <=> g.min_temperature_c)
      OR NOT (e.max_temperature_c <=> g.max_temperature_c)
      OR g.avg_temperature_c IS NULL OR ISNAN(g.avg_temperature_c)
      OR ABS(e.avg_temperature_c-g.avg_temperature_c)>0.000001
      OR g.avg_humidity_pct IS NULL OR ISNAN(g.avg_humidity_pct)
      OR ABS(e.avg_humidity_pct-g.avg_humidity_pct)>0.000001
      OR g.total_precipitation_mm IS NULL OR ISNAN(g.total_precipitation_mm)
      OR ABS(e.total_precipitation_mm-g.total_precipitation_mm)>0.000001
      OR NOT (e.max_wind_speed_kmh <=> g.max_wind_speed_kmh)
      OR NOT (e.is_complete_day <=> g.is_complete_day)
      OR NOT (e.is_rainy_day <=> g.is_rainy_day)
      OR NOT (e.heat_alert <=> g.heat_alert)
      OR NOT (e.wind_alert <=> g.wind_alert)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Quarantine, schema changes and retry evidence
# MAGIC Filtered to your selected run. Empty results are expected for a clean run. Use demo mode and paste the integration
# MAGIC scenario's run ID to investigate an earlier failure test. Timeout attempts have no HTTP response status.

# COMMAND ----------

run_sql("Selected rejected records", f"""SELECT location_id, record_scope, row_index, reason,
    raw_record, response_id FROM {T['quarantine']} WHERE run_id={RID}
    ORDER BY location_id, row_index""")
run_sql("Selected schema events", f"""SELECT location_id, change_type, field_name, details,
    response_id FROM {T['events']} WHERE run_id={RID} ORDER BY location_id, field_name""")
run_sql("Selected HTTP attempts", f"""SELECT location_id, attempt, http_status,
    duration_seconds, error_message, response_id FROM {T['attempts']} WHERE run_id={RID}
    ORDER BY location_id, attempt""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Audit counts vs persisted rejection evidence
# MAGIC Expected: audit rejection/schema counts agree with their detailed records. REVIEW can occur for an interrupted or
# MAGIC failed persistence sequence: investigate rather than equating an incomplete audit with a clean run.
# MAGIC The rows_received arithmetic check checks accounting only, not raw array cardinality.

# COMMAND ----------

sql_check("Location audit count arithmetic", f"""SELECT location_id, status, rows_received,
    rows_valid, rows_rejected FROM {T['locations']} WHERE run_id={RID}
    AND NOT (rows_received <=> rows_valid+rows_rejected)""")
sql_check("Audit vs quarantine/schema detail counts", f"""WITH q AS (
      SELECT location_id, SUM(CASE WHEN record_scope='ROW' THEN 1 ELSE 0 END) row_rejections,
        SUM(CASE WHEN record_scope='PAYLOAD' THEN 1 ELSE 0 END) payload_rejections
      FROM {T['quarantine']} WHERE run_id={RID} GROUP BY location_id
    ), e AS (
      SELECT location_id, COUNT(*) event_count FROM {T['events']}
      WHERE run_id={RID} GROUP BY location_id
    )
    SELECT l.location_id, l.status, l.rows_rejected, COALESCE(q.row_rejections,0) detail_rows_rejected,
      l.payloads_rejected, COALESCE(q.payload_rejections,0) detail_payloads_rejected,
      l.schema_event_count, COALESCE(e.event_count,0) detail_schema_events
    FROM {T['locations']} l LEFT JOIN q ON l.location_id=q.location_id
    LEFT JOIN e ON l.location_id=e.location_id WHERE l.run_id={RID}
    AND (NOT (l.rows_rejected <=> COALESCE(q.row_rejections,0))
      OR NOT (l.payloads_rejected <=> COALESCE(q.payload_rejections,0))
      OR NOT (l.schema_event_count <=> COALESCE(e.event_count,0)))""", review=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Current ingestion freshness
# MAGIC This checks current health across all runs in the chosen mode, not historical health at the selected run time.
# MAGIC A 1440-minute threshold is an example for daily ingestion; change the control to your agreed SLA.
# MAGIC No successful ingestion or stale ingestion is REVIEW. Historical demo fixtures can become stale normally.

# COMMAND ----------

freshness_sql = f"""WITH latest AS (
      SELECT * FROM {T['locations']}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY started_at DESC, run_id DESC)=1
    ), good AS (
      SELECT location_id, MAX(ended_at) last_successful_ingestion_at FROM {T['locations']}
      WHERE status IN ('SUCCESS','SUCCESS_WITH_WARNINGS') GROUP BY location_id
    )
    SELECT c.location_id, c.city_name, l.status latest_status, l.ended_at latest_attempt_at,
      g.last_successful_ingestion_at,
      TIMESTAMPDIFF(MINUTE,g.last_successful_ingestion_at,CURRENT_TIMESTAMP()) freshness_minutes,
      l.error_message
    FROM {T['config']} c LEFT JOIN latest l ON c.location_id=l.location_id
    LEFT JOIN good g ON c.location_id=g.location_id WHERE c.enabled=true"""
run_sql("Current location health", freshness_sql)
sql_check("Ingestion freshness", f"""SELECT * FROM ({freshness_sql}) health
    WHERE last_successful_ingestion_at IS NULL OR freshness_minutes>{FRESHNESS_LIMIT}""", review=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validation summary and interview explanation
# MAGIC A clean complete run should show PASS for every integrity check. REVIEW needs context, especially after deliberate
# MAGIC faults. FAIL indicates stored integrity/reconciliation violations. This notebook does not automatically fail a job;
# MAGIC inspect the summary and exception rows.
# MAGIC
# MAGIC Suggested explanation: "I use independent SQL checks for unique business keys, hourly coverage, value ranges,
# MAGIC bronze lineage and daily aggregation reconciliation. Audit and quarantine queries explain deliberately failed inputs.
# MAGIC A same-window rerun preserves unique keys while bronze retains each pull."
# MAGIC
# MAGIC **Testing note:** this file has syntax and SQL parse checks; execution against your Databricks tables is performed
# MAGIC when you import and run it. It does not require importing the original pipeline's Python definitions.

# COMMAND ----------

display(spark.createDataFrame(CHECKS))
failed = [row for row in CHECKS if row["result"]=="FAIL"]
review = [row for row in CHECKS if row["result"]=="REVIEW"]
print(f"Selected run: {RUN_ID} | integrity failures: {len(failed)} | checks to review: {len(review)}")
print("Read-only validation completed. No API calls or persisted table changes were made.")