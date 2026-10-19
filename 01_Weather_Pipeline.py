# Databricks notebook source
# MAGIC %pip install requests==2.32.5

# COMMAND ----------

import json
import re
import uuid
from datetime import datetime, timedelta
import requests
from pyspark.sql import functions as F

# Preserve existing widget selections when the notebook is rerun.
defaults = {"catalog": "", "run_mode": "live", "scenario": "none",
            "start_date": "", "forecast_days": "3", "run_tests": "false", "run_now": "true"}
existing = dbutils.widgets.getAll()
for name, value in defaults.items():
    if name not in existing:
        if name == "run_mode":
            dbutils.widgets.dropdown(name, value, ["live", "demo"])
        elif name == "scenario":
            dbutils.widgets.dropdown(name, value, ["none", "bad_request", "edge_mix", "structural_mix", "retry_mix"])
        elif name in ("run_tests", "run_now"):
            dbutils.widgets.dropdown(name, value, ["false", "true"])
        else:
            dbutils.widgets.text(name, value)
CATALOG = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError("Use a catalog with a simple name: letters, numbers and underscores")
RUN_MODE = dbutils.widgets.get("run_mode")
SCENARIO = dbutils.widgets.get("scenario")
FORECAST_DAYS = int(dbutils.widgets.get("forecast_days"))
if not 3 <= FORECAST_DAYS <= 7:
    raise ValueError("Assignment requires forecast_days between 3 and 7")
START_DATE = dbutils.widgets.get("start_date").strip() or datetime.utcnow().date().isoformat()
datetime.strptime(START_DATE, "%Y-%m-%d")
END_DATE = (datetime.fromisoformat(START_DATE) + timedelta(days=FORECAST_DAYS - 1)).date().isoformat()
spark.conf.set("spark.sql.session.timeZone", "UTC")
print(f"Catalog={CATALOG}; mode={RUN_MODE}; scenario={SCENARIO}; UTC window={START_DATE} through {END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validation and retry helpers

# COMMAND ----------

"""Pure Python weather validation and HTTP retry logic (no Spark dependency)."""
import json
import math
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

FIELDS = ("temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m")
UNITS = {"time": "iso8601", "temperature_2m": "°C", "relative_humidity_2m": "%",
         "precipitation": "mm", "wind_speed_10m": "km/h"}


def utcnow():
    # Spark TimestampType accepts Python datetime; session is explicitly UTC.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def as_json(value):
    return json.dumps(value, ensure_ascii=False, default=str, allow_nan=False)


class FetchFailure(Exception):
    def __init__(self, message, attempts, http_status=None):
        super().__init__(message)
        self.attempts = attempts
        self.http_status = http_status


def retry_delay(headers, attempt, now=None):
    """Honor Retry-After seconds or HTTP-date, capped for a bounded demo."""
    raw = headers.get("Retry-After")
    if raw is not None:
        try:
            return min(30.0, max(0.0, float(raw)))
        except (ValueError, TypeError):
            try:
                deadline = parsedate_to_datetime(raw)
                now = now or datetime.now(timezone.utc)
                return min(30.0, max(0.0, (deadline - now).total_seconds()))
            except (ValueError, TypeError, OverflowError):
                pass
    return min(30.0, 2.0 ** (attempt - 1))


def fetch_with_retries(request, on_attempt, sleep=time.sleep, max_attempts=3):
    """request() returns a requests-like response. Persist each attempt BEFORE retry.

    Retry transport failures, 429 and 5xx; fail immediately on other non-2xx.
    Malformed successful JSON is handled downstream, preserving its original body.
    """
    import requests
    for attempt in range(1, max_attempts + 1):
        started = utcnow()
        response = None
        try:
            response = request()
        except (requests.Timeout, requests.ConnectionError) as exc:
            message = f"{type(exc).__name__}: {exc}"
            on_attempt(attempt, started, utcnow(), None, None, message)
            if attempt == max_attempts:
                raise FetchFailure(message, attempt) from exc
            sleep(retry_delay({}, attempt))
            continue
        except requests.RequestException as exc:
            message = f"{type(exc).__name__}: {exc}"
            on_attempt(attempt, started, utcnow(), None, None, message)
            raise FetchFailure(message, attempt) from exc
        status = response.status_code
        error = None if 200 <= status < 300 else f"HTTP {status}: {response.text[:500]}"
        on_attempt(attempt, started, utcnow(), status, response.text, error)
        if 200 <= status < 300:
            return response.text, status, attempt
        if (status == 429 or 500 <= status < 600) and attempt < max_attempts:
            sleep(retry_delay(response.headers, attempt))
            continue
        raise FetchFailure(error, attempt, status)
    raise AssertionError("Unreachable retry state")


def validate_payload(raw_text, location, start_date, end_date):
    """Returns valid hourly dictionaries, quarantine dictionaries and schema events.

    Strict contract: required arrays and exact units; UTC hourly keys; finite numeric
    values. Numeric strings are converted and logged. Extra fields are retained in
    bronze and logged, but do not automatically widen the silver schema.
    Array-length mismatch rejects the entire payload: never silently zip arrays.
    """
    valid, rejected, events = [], [], []
    lower = datetime.fromisoformat(start_date)
    upper = datetime.fromisoformat(end_date) + timedelta(days=1)
    expected = int((upper - lower).total_seconds() // 3600)

    def event(kind, field, detail):
        events.append({"change_type": kind, "field_name": field, "details": detail})

    def payload_reject(reason):
        rejected.append({"record_scope": "PAYLOAD", "row_index": None,
                         "raw_record": raw_text, "reason": reason})
        return valid, rejected, events

    try:
        def reject_constant(value):
            raise ValueError(f"Non-standard JSON constant: {value}")
        payload = json.loads(raw_text, parse_constant=reject_constant)
    except (ValueError, TypeError) as exc:
        return payload_reject(f"INVALID_JSON: {exc}")
    if not isinstance(payload, dict):
        return payload_reject("INVALID_ROOT: expected an object")
    known_root = {"latitude", "longitude", "generationtime_ms", "utc_offset_seconds",
                  "timezone", "timezone_abbreviation", "elevation", "hourly_units", "hourly"}
    for name in sorted(set(payload) - known_root):
        event("NEW_FIELD", name, "Preserved in bronze; ignored in silver")
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        event("MISSING_OR_CHANGED_FIELD", "hourly", "Required object missing or wrong type")
        return payload_reject("MISSING_OR_INVALID_HOURLY")
    required = ("time",) + FIELDS
    for name in sorted(set(hourly) - set(required)):
        event("NEW_FIELD", f"hourly.{name}", "Preserved in bronze; ignored in silver")
    bad_shape = []
    for name in required:
        if name not in hourly:
            event("MISSING_FIELD", f"hourly.{name}", "Required array missing")
            bad_shape.append(f"MISSING_FIELD:{name}")
        elif not isinstance(hourly[name], list):
            event("TYPE_CHANGED", f"hourly.{name}", "Expected an array")
            bad_shape.append(f"NOT_ARRAY:{name}")
    if bad_shape:
        return payload_reject("; ".join(bad_shape))
    lengths = {name: len(hourly[name]) for name in required}
    if len(set(lengths.values())) != 1:
        return payload_reject(f"ARRAY_LENGTH_MISMATCH: {lengths}")
    if not lengths["time"]:
        return payload_reject("EMPTY_HOURLY_ARRAYS")
    units = payload.get("hourly_units")
    if not isinstance(units, dict):
        event("MISSING_OR_CHANGED_FIELD", "hourly_units", "Required units object absent")
        return payload_reject("MISSING_UNITS")
    bad_units = [name for name, value in UNITS.items() if units.get(name) != value]
    if bad_units:
        for name in bad_units:
            event("UNIT_CHANGED", name, f"Expected {UNITS[name]!r}, got {units.get(name)!r}")
        return payload_reject(f"UNIT_MISMATCH: {bad_units}")
    if payload.get("utc_offset_seconds") != 0:
        return payload_reject("NON_UTC_RESPONSE: this project requests UTC")
    seen = set()
    converted, changed = {}, {}
    for index, raw_timestamp in enumerate(hourly["time"]):
        record = {name: hourly[name][index] for name in required}
        reasons, values = [], {}
        timestamp = None
        try:
            if not isinstance(raw_timestamp, str):
                raise ValueError("not a string")
            timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
            if timestamp.tzinfo is not None:
                timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
            if timestamp.minute or timestamp.second or timestamp.microsecond:
                reasons.append("NOT_HOURLY_TIMESTAMP")
            if not lower <= timestamp < upper:
                reasons.append("TIMESTAMP_OUTSIDE_REQUEST_WINDOW")
            if timestamp in seen:
                reasons.append("DUPLICATE_TIMESTAMP")
            seen.add(timestamp)
        except (ValueError, TypeError, OverflowError):
            reasons.append("INVALID_TIMESTAMP")
        for name in FIELDS:
            value = record[name]
            if value is None:
                reasons.append(f"NULL:{name}")
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float, str)):
                reasons.append(f"INVALID_TYPE:{name}")
                changed[name] = changed.get(name, 0) + 1
                continue
            try:
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError("non-finite")
            except (ValueError, TypeError, OverflowError):
                reasons.append(f"NOT_FINITE_NUMERIC:{name}")
                changed[name] = changed.get(name, 0) + 1
                continue
            if isinstance(value, str):
                converted[name] = converted.get(name, 0) + 1
            values[name] = number
            if name == "relative_humidity_2m" and not 0 <= number <= 100:
                reasons.append("HUMIDITY_OUT_OF_RANGE")
            elif name in ("precipitation", "wind_speed_10m") and number < 0:
                reasons.append(f"NEGATIVE:{name}")
            elif name == "temperature_2m" and not -100 <= number <= 70:
                reasons.append("TEMPERATURE_OUT_OF_RANGE")
        if reasons:
            rejected.append({"record_scope": "ROW", "row_index": index,
                             "raw_record": as_json(record), "reason": "; ".join(reasons)})
        else:
            valid.append({"location_id": location["location_id"], "city_name": location["city_name"],
                          "weather_timestamp": timestamp, "weather_date": timestamp.date(),
                          "temperature_c": values["temperature_2m"],
                          "humidity_pct": values["relative_humidity_2m"],
                          "precipitation_mm": values["precipitation"],
                          "wind_speed_kmh": values["wind_speed_10m"]})
    for name, count in converted.items():
        event("TYPE_COERCED", f"hourly.{name}", f"Converted {count} numeric strings to double")
    for name, count in changed.items():
        event("TYPE_CHANGED", f"hourly.{name}", f"Rejected {count} incompatible values")
    if len(seen) != expected or len(hourly["time"]) != expected:
        event("INCOMPLETE_WINDOW", "hourly.time",
              f"Expected {expected} hours; received {len(hourly['time'])}; distinct parsed {len(seen)}")
    return valid, rejected, events


class FixtureResponse:
    def __init__(self, status, text, headers=None):
        self.status_code, self.text, self.headers = status, text, headers or {}


def fixture_payload(location, start_date, end_date):
    start = datetime.fromisoformat(start_date)
    hours = int(((datetime.fromisoformat(end_date) + timedelta(days=1)) - start).total_seconds() / 3600)
    hourly = {"time": [(start + timedelta(hours=i)).isoformat(timespec="minutes") for i in range(hours)]}
    hourly.update({"temperature_2m": [25.0 + (i % 12) / 2 for i in range(hours)],
                   "relative_humidity_2m": [60.0 for _ in range(hours)],
                   "precipitation": [1.5 if i % 8 == 0 else 0.0 for i in range(hours)],
                   "wind_speed_10m": [12.0 for _ in range(hours)]})
    return {"latitude": location["latitude"], "longitude": location["longitude"],
            "utc_offset_seconds": 0, "timezone": "GMT", "hourly_units": UNITS.copy(), "hourly": hourly}


def fixture_request(location, start_date, end_date, fault="none"):
    """Controlled test transport. No real API requests, no real rate-limit abuse."""
    import requests
    payload = fixture_payload(location, start_date, end_date)
    h = payload["hourly"]
    if fault == "edge_mix":
        h["relative_humidity_2m"][0] = 150
        h["temperature_2m"][1] = None
        h["time"][2] = "invalid-timestamp"
        h["time"][3] = h["time"][4]
        h["temperature_2m"][5] = "28.5"
        h["cloud_cover"] = [50 for _ in h["time"]]
    elif fault == "array_mismatch":
        h["precipitation"].pop()
    elif fault == "missing_field":
        del h["wind_speed_10m"]
    elif fault == "changed_type":
        h["temperature_2m"][0] = "hot"
        h["temperature_2m"][1] = "27.5"
    elif fault == "empty":
        payload["hourly"] = {name: [] for name in ("time",) + FIELDS}
    elif fault == "cutoff":
        payload["hourly"] = {name: values[:24] for name, values in h.items()}
    elif fault == "unit_change":
        payload["hourly_units"]["wind_speed_10m"] = "m/s"
    raw = as_json(payload)
    if fault == "truncated_json":
        raw = raw[:len(raw) // 2]
    calls = 0

    def request():
        nonlocal calls
        calls += 1
        if fault == "bad_request":
            return FixtureResponse(400, '{"error":true,"reason":"Deliberate bad parameter"}')
        if fault == "timeout" or (fault == "timeout_once" and calls == 1):
            raise requests.Timeout("Deliberate fixture timeout")
        if fault == "rate_limit_once" and calls == 1:
            return FixtureResponse(429, '{"error":"rate limit fixture"}', {"Retry-After": "0"})
        if fault == "server_error_once" and calls == 1:
            return FixtureResponse(503, '{"error":"server error fixture"}')
        if fault == "server_error":
            return FixtureResponse(503, '{"error":"persistent server error fixture"}')
        return FixtureResponse(200, raw)
    return request

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta table definitions

# COMMAND ----------

DDL = {
    "config.locations": """location_id STRING, city_name STRING, latitude DOUBLE,
        longitude DOUBLE, local_timezone STRING, enabled BOOLEAN""",
    "bronze.api_responses": """response_id STRING, run_id STRING, location_id STRING,
        city_name STRING, attempt INT, ingested_at TIMESTAMP, endpoint STRING,
        request_json STRING, http_status INT, raw_response STRING, source_kind STRING""",
    "silver.hourly_weather": """location_id STRING, city_name STRING, weather_timestamp TIMESTAMP,
        weather_date DATE, temperature_c DOUBLE, humidity_pct DOUBLE, precipitation_mm DOUBLE,
        wind_speed_kmh DOUBLE, source_response_id STRING, source_run_id STRING,
        ingested_at TIMESTAMP, source_kind STRING""",
    "silver.quarantine": """quarantine_id STRING, run_id STRING, location_id STRING,
        response_id STRING, rejected_at TIMESTAMP, record_scope STRING,
        row_index INT, raw_record STRING, reason STRING, source_kind STRING""",
    "gold.daily_weather_summary": """location_id STRING, city_name STRING, weather_date DATE,
        min_temperature_c DOUBLE, max_temperature_c DOUBLE, avg_temperature_c DOUBLE,
        avg_humidity_pct DOUBLE, total_precipitation_mm DOUBLE, max_wind_speed_kmh DOUBLE,
        hours_available BIGINT, is_complete_day BOOLEAN, is_rainy_day BOOLEAN,
        heat_alert BOOLEAN, wind_alert BOOLEAN, latest_ingested_at TIMESTAMP,
        gold_updated_at TIMESTAMP, source_kind STRING""",
    "ops.pipeline_runs": """run_id STRING, run_mode STRING, scenario STRING, source_kind STRING,
        started_at TIMESTAMP, ended_at TIMESTAMP, duration_seconds DOUBLE, status STRING,
        parameters_json STRING, locations_total INT, locations_succeeded INT, locations_failed INT,
        rows_received BIGINT, rows_valid BIGINT, rows_rejected BIGINT,
        payloads_rejected BIGINT, silver_rows_upserted BIGINT, gold_rows_upserted BIGINT,
        error_message STRING""",
    "ops.location_runs": """run_id STRING, location_id STRING, city_name STRING, source_kind STRING,
        started_at TIMESTAMP, ended_at TIMESTAMP, duration_seconds DOUBLE, status STRING,
        request_json STRING, http_status INT, attempts INT, response_id STRING,
        rows_received BIGINT, rows_valid BIGINT, rows_rejected BIGINT, payloads_rejected BIGINT,
        silver_rows_upserted BIGINT, gold_rows_upserted BIGINT,
        schema_event_count INT, error_message STRING""",
    "ops.api_attempts": """run_id STRING, location_id STRING, attempt INT, source_kind STRING,
        started_at TIMESTAMP, ended_at TIMESTAMP, duration_seconds DOUBLE, http_status INT,
        response_id STRING, error_message STRING""",
    "ops.schema_events": """event_id STRING, run_id STRING, location_id STRING,
        response_id STRING, detected_at TIMESTAMP, change_type STRING,
        field_name STRING, details STRING, source_kind STRING""",
}


def table(mode, name):
    layer, entity = name.split(".")
    prefix = "weather" if mode == "live" else "weather_demo"
    return f"`{CATALOG}`.`{prefix}_{layer}`.`{entity}`"


def append_rows(mode, name, rows):
    if rows:
        target = table(mode, name)
        # Explicit table schema prevents failures with all-null fields.
        spark.createDataFrame(rows, schema=spark.table(target).schema).write.mode("append").saveAsTable(target)


def merge_rows(mode, name, rows, keys, newest_guard=False):
    if not rows:
        return
    target = table(mode, name)
    source = spark.createDataFrame(rows, schema=spark.table(target).schema)
    view = "src_" + uuid.uuid4().hex
    source.createOrReplaceTempView(view)
    condition = " AND ".join(f"t.`{key}` = s.`{key}`" for key in keys)
    guard = " AND s.ingested_at >= t.ingested_at" if newest_guard else ""
    try:
        spark.sql(f"""MERGE INTO {target} t USING {view} s ON {condition}
            WHEN MATCHED{guard} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
    finally:
        spark.catalog.dropTempView(view)


def setup(mode):
    prefix = "weather" if mode == "live" else "weather_demo"
    for layer in ("config", "bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{prefix}_{layer}`")
    for name, columns in DDL.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table(mode, name)} ({columns}) USING DELTA")
    # Seed only missing configuration rows; preserve changes made by the user.
    seeds = [
        {"location_id": "SG", "city_name": "Singapore", "latitude": 1.3521, "longitude": 103.8198,
         "local_timezone": "Asia/Singapore", "enabled": True},
        {"location_id": "DEL", "city_name": "Delhi", "latitude": 28.6139, "longitude": 77.2090,
         "local_timezone": "Asia/Kolkata", "enabled": True},
        {"location_id": "HYD", "city_name": "Hyderabad", "latitude": 17.3850, "longitude": 78.4867,
         "local_timezone": "Asia/Kolkata", "enabled": True},
        {"location_id": "LON", "city_name": "London", "latitude": 51.5074, "longitude": -0.1278,
         "local_timezone": "Europe/London", "enabled": True},
        {"location_id": "SYD", "city_name": "Sydney", "latitude": -33.8688, "longitude": 151.2093,
         "local_timezone": "Australia/Sydney", "enabled": True},
    ]
    target = table(mode, "config.locations")
    seed_df = spark.createDataFrame(seeds, schema=spark.table(target).schema)
    seed_df.createOrReplaceTempView("weather_location_seeds")
    spark.sql(f"""MERGE INTO {target} t USING weather_location_seeds s
        ON t.location_id = s.location_id WHEN NOT MATCHED THEN INSERT *""")
    spark.catalog.dropTempView("weather_location_seeds")


def refresh_gold(mode, location_id, start_date, end_date):
    silver, gold = table(mode, "silver.hourly_weather"), table(mode, "gold.daily_weather_summary")
    # Inputs have already been constrained/validated; escape the configurable ID.
    city_key = location_id.replace("'", "''")
    daily = spark.sql(f"""SELECT location_id, city_name, weather_date,
        MIN(temperature_c) min_temperature_c, MAX(temperature_c) max_temperature_c,
        AVG(temperature_c) avg_temperature_c, AVG(humidity_pct) avg_humidity_pct,
        SUM(precipitation_mm) total_precipitation_mm, MAX(wind_speed_kmh) max_wind_speed_kmh,
        COUNT(*) hours_available, COUNT(*) = 24 is_complete_day,
        SUM(precipitation_mm) > 0 is_rainy_day,
        MAX(temperature_c) >= 35 heat_alert, MAX(wind_speed_kmh) >= 40 wind_alert,
        MAX(ingested_at) latest_ingested_at, CURRENT_TIMESTAMP() gold_updated_at,
        MAX(source_kind) source_kind
        FROM {silver} WHERE location_id = '{city_key}'
        AND weather_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY location_id, city_name, weather_date""")
    count = daily.count()
    view = "gold_src_" + uuid.uuid4().hex
    daily.createOrReplaceTempView(view)
    try:
        spark.sql(f"""MERGE INTO {gold} t USING {view} s
            ON t.location_id=s.location_id AND t.weather_date=s.weather_date
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    finally:
        spark.catalog.dropTempView(view)
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pipeline orchestration

# COMMAND ----------

SCENARIO_FAULTS = {
    "none": {},
    "bad_request": {"SG": "bad_request"},
    "edge_mix": {"SG": "edge_mix", "DEL": "bad_request"},
    "structural_mix": {"SG": "array_mismatch", "DEL": "missing_field", "HYD": "truncated_json"},
    "retry_mix": {"SG": "rate_limit_once", "DEL": "server_error_once", "HYD": "timeout_once", "LON": "timeout"},
}


class AuditPersistenceError(RuntimeError):
    pass


def process_location(mode, run_id, location, start_date, end_date, fault):
    started = utcnow()
    source_kind = "LIVE_API" if mode == "live" else "SIMULATED_FIXTURE"
    params = {"latitude": location["latitude"], "longitude": location["longitude"],
              "hourly": ",".join(FIELDS), "start_date": start_date, "end_date": end_date,
              "timezone": "UTC", "temperature_unit": "celsius", "wind_speed_unit": "kmh",
              "precipitation_unit": "mm"}
    endpoint = "https://api.open-meteo.com/v1/forecast"
    result = {"run_id": run_id, "location_id": location["location_id"], "city_name": location["city_name"],
              "source_kind": source_kind, "started_at": started, "ended_at": None,
              "duration_seconds": None, "status": "RUNNING", "request_json": as_json(params),
              "http_status": None, "attempts": 0, "response_id": None,
              "rows_received": 0, "rows_valid": 0, "rows_rejected": 0, "payloads_rejected": 0,
              "silver_rows_upserted": 0, "gold_rows_upserted": 0,
              "schema_event_count": 0, "error_message": None}
    merge_rows(mode, "ops.location_runs", [result], ["run_id", "location_id"])

    def on_attempt(attempt, attempt_start, attempt_end, status, raw, error):
        result.update({"attempts": attempt, "http_status": status})
        response_id = uuid.uuid4().hex if raw is not None else None
        result["response_id"] = response_id
        # Never update an existing bronze response, even for the same hour/window.
        try:
            if raw is not None:
                append_rows(mode, "bronze.api_responses", [{"response_id": response_id,
                    "run_id": run_id, "location_id": location["location_id"], "city_name": location["city_name"],
                    "attempt": attempt, "ingested_at": attempt_end, "endpoint": endpoint,
                    "request_json": as_json(params), "http_status": status,
                    "raw_response": raw, "source_kind": source_kind}])
            append_rows(mode, "ops.api_attempts", [{"run_id": run_id, "location_id": location["location_id"],
                "attempt": attempt, "source_kind": source_kind, "started_at": attempt_start,
                "ended_at": attempt_end, "duration_seconds": (attempt_end-attempt_start).total_seconds(),
                "http_status": status, "response_id": response_id, "error_message": error}])
        except Exception as exc:
            raise AuditPersistenceError(f"Cannot persist API attempt/bronze: {exc}") from exc

    session = None
    try:
        if mode == "live":
            session = requests.Session()
            request = lambda: session.get(endpoint, params=params, timeout=(10, 30))
            raw, status, attempts = fetch_with_retries(request, on_attempt)
        else:
            request = fixture_request(location, start_date, end_date, fault)
            raw, status, attempts = fetch_with_retries(request, on_attempt, sleep=lambda seconds: None)
        ingested_at = utcnow()
        valid, rejected, events = validate_payload(raw, location, start_date, end_date)
        result["rows_rejected"] = sum(row["record_scope"] == "ROW" for row in rejected)
        result["payloads_rejected"] = sum(row["record_scope"] == "PAYLOAD" for row in rejected)
        result["rows_received"] = len(valid) + result["rows_rejected"]
        result["rows_valid"] = len(valid)
        result["schema_event_count"] = len(events)
        append_rows(mode, "silver.quarantine", [dict(row,
            quarantine_id=uuid.uuid4().hex, run_id=run_id, location_id=location["location_id"],
            response_id=result["response_id"], rejected_at=ingested_at, source_kind=source_kind) for row in rejected])
        append_rows(mode, "ops.schema_events", [dict(event,
            event_id=uuid.uuid4().hex, run_id=run_id, location_id=location["location_id"],
            response_id=result["response_id"], detected_at=ingested_at, source_kind=source_kind) for event in events])
        if not valid:
            result["status"] = "FAILED"
            result["error_message"] = "; ".join(row["reason"] for row in rejected[:3]) or "NO_VALID_ROWS"
        else:
            rows = [dict(row, source_response_id=result["response_id"], source_run_id=run_id,
                         ingested_at=ingested_at, source_kind=source_kind) for row in valid]
            merge_rows(mode, "silver.hourly_weather", rows,
                       ["location_id", "weather_timestamp"], newest_guard=True)
            # Upserted counts are successful input keys, not net-new inserts.
            result["silver_rows_upserted"] = len(rows)
            result["gold_rows_upserted"] = refresh_gold(mode, location["location_id"], start_date, end_date)
            result["status"] = "SUCCESS_WITH_WARNINGS" if rejected or events else "SUCCESS"
            if rejected:
                result["error_message"] = f"Quarantined {result['rows_rejected']} hourly rows"
    except AuditPersistenceError:
        raise
    except Exception as exc:
        result["status"] = "FAILED"
        result["error_message"] = f"{type(exc).__name__}: {exc}"[:4000]
    finally:
        if session:
            session.close()
    ended = utcnow()
    result.update({"ended_at": ended, "duration_seconds": (ended-started).total_seconds()})
    # Do not silently lose final auditing if the write itself fails.
    merge_rows(mode, "ops.location_runs", [result], ["run_id", "location_id"])
    return result


def run_pipeline(mode="live", scenario="none", start_date=None, days=3, faults=None):
    if mode not in ("live", "demo"):
        raise ValueError("mode must be live or demo")
    if scenario not in SCENARIO_FAULTS and faults is None:
        raise ValueError("Unknown scenario")
    if mode == "live" and (scenario != "none" or faults):
        raise ValueError("Fault injection is allowed only in demo mode")
    if not 3 <= days <= 7:
        raise ValueError("days must be between 3 and 7")
    start_date = start_date or utcnow().date().isoformat()
    datetime.strptime(start_date, "%Y-%m-%d")
    end_date = (datetime.fromisoformat(start_date) + timedelta(days=days-1)).date().isoformat()
    setup(mode)
    locations = [row.asDict() for row in spark.table(table(mode, "config.locations"))
                 .where("enabled = true").orderBy("location_id").collect()]
    if len(locations) < 5:
        raise ValueError("Keep at least five enabled locations for this assignment")
    ids = [row["location_id"] for row in locations]
    if len(ids) != len(set(ids)) or any(not value for value in ids):
        raise ValueError("Configuration location_id must be unique and nonempty")
    for row in locations:
        if not row["city_name"] or row["latitude"] is None or row["longitude"] is None:
            raise ValueError("Configuration city and coordinates are required")
        if not -90 <= row["latitude"] <= 90 or not -180 <= row["longitude"] <= 180:
            raise ValueError("Configuration coordinates are out of range")
    fault_map = faults if faults is not None else SCENARIO_FAULTS[scenario]
    if set(fault_map) - set(ids):
        raise ValueError("Fault configuration contains unknown location IDs")
    run_id, started = uuid.uuid4().hex, utcnow()
    run = {"run_id": run_id, "run_mode": mode, "scenario": scenario,
        "source_kind": "LIVE_API" if mode == "live" else "SIMULATED_FIXTURE",
        "started_at": started, "ended_at": None, "duration_seconds": None, "status": "RUNNING",
        "parameters_json": as_json({"start_date": start_date, "end_date": end_date,
            "timezone": "UTC", "days": days, "fields": FIELDS, "faults": fault_map,
            "heat_threshold_c": 35, "wind_threshold_kmh": 40}),
        "locations_total": len(locations), "locations_succeeded": 0, "locations_failed": 0,
        "rows_received": 0, "rows_valid": 0, "rows_rejected": 0, "payloads_rejected": 0,
        "silver_rows_upserted": 0, "gold_rows_upserted": 0, "error_message": None}
    merge_rows(mode, "ops.pipeline_runs", [run], ["run_id"])
    results = []
    fatal_error = None
    try:
        for location in locations:
            result = process_location(mode, run_id, location, start_date, end_date,
                                      fault_map.get(location["location_id"], "none"))
            results.append(result)
            print(f"{result['city_name']}: {result['status']} | valid={result['rows_valid']} "
                  f"rejected={result['rows_rejected']} payloads_rejected={result['payloads_rejected']} "
                  f"attempts={result['attempts']}")
    except Exception as exc:
        fatal_error = f"{type(exc).__name__}: {exc}"[:4000]
    for name in ("rows_received", "rows_valid", "rows_rejected", "payloads_rejected",
                 "silver_rows_upserted", "gold_rows_upserted"):
        run[name] = sum(row[name] for row in results)
    run["locations_failed"] = sum(row["status"] == "FAILED" for row in results)
    run["locations_succeeded"] = len(results) - run["locations_failed"]
    if fatal_error or run["locations_failed"] == len(locations):
        run["status"] = "FAILED"
    elif run["locations_failed"]:
        run["status"] = "PARTIAL_SUCCESS"
    elif any(row["status"] == "SUCCESS_WITH_WARNINGS" for row in results):
        run["status"] = "SUCCESS_WITH_WARNINGS"
    else:
        run["status"] = "SUCCESS"
    ended = utcnow()
    run.update({"ended_at": ended, "duration_seconds": (ended-started).total_seconds(),
                "error_message": fatal_error or (f"{run['locations_failed']} location(s) failed" if run["locations_failed"] else None)})
    merge_rows(mode, "ops.pipeline_runs", [run], ["run_id"])
    print(f"Run {run_id}: {run['status']} | {run['duration_seconds']:.1f}s")
    if fatal_error:
        raise RuntimeError(f"Run {run_id} aborted: {fatal_error}")
    return run

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run the selected pipeline

# COMMAND ----------

LAST_RUN = None
if dbutils.widgets.get("run_now") == "true":
    LAST_RUN = run_pipeline(RUN_MODE, SCENARIO, START_DATE, FORECAST_DAYS)
    display(spark.table(table(RUN_MODE, "ops.location_runs")).where(F.col("run_id") == LAST_RUN["run_id"]))
else:
    setup(RUN_MODE)
    print("Definitions and tables ready; ingestion skipped because run_now=false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Views for dashboard health and freshness

# COMMAND ----------

def create_monitoring_views(mode):
    prefix = "weather" if mode == "live" else "weather_demo"
    ops = f"`{CATALOG}`.`{prefix}_ops`"
    spark.sql(f"""CREATE OR REPLACE VIEW {ops}.v_latest_run AS
        SELECT * FROM {table(mode, 'ops.pipeline_runs')}
        QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC, run_id DESC)=1""")
    spark.sql(f"""CREATE OR REPLACE VIEW {ops}.v_location_health AS
        WITH latest AS (
          SELECT * FROM {table(mode, 'ops.location_runs')}
          QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY started_at DESC, run_id DESC)=1
        ), good AS (
          SELECT location_id, MAX(ended_at) last_successful_ingestion_at
          FROM {table(mode, 'ops.location_runs')}
          WHERE status IN ('SUCCESS','SUCCESS_WITH_WARNINGS') GROUP BY location_id
        ), stored AS (
          SELECT location_id, COUNT(*) stored_hourly_rows,
                 MAX(ingested_at) last_row_ingested_at
          FROM {table(mode, 'silver.hourly_weather')} GROUP BY location_id
        )
        SELECT c.location_id, c.city_name, l.status latest_status, l.ended_at latest_attempt_at,
          g.last_successful_ingestion_at,
          TIMESTAMPDIFF(MINUTE,g.last_successful_ingestion_at,CURRENT_TIMESTAMP()) freshness_minutes,
          s.last_row_ingested_at, COALESCE(s.stored_hourly_rows,0) stored_hourly_rows,
          l.rows_valid, l.rows_rejected, l.payloads_rejected, l.error_message
        FROM {table(mode, 'config.locations')} c
        LEFT JOIN latest l ON c.location_id=l.location_id
        LEFT JOIN good g ON c.location_id=g.location_id
        LEFT JOIN stored s ON c.location_id=s.location_id WHERE c.enabled=true""")

create_monitoring_views(RUN_MODE)
print("Monitoring views created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify keys and outputs.

# COMMAND ----------

def verify_keys(mode):
    for name, keys in (("silver.hourly_weather", "location_id, weather_timestamp"),
                       ("gold.daily_weather_summary", "location_id, weather_date")):
        duplicate_groups = spark.sql(f"""SELECT {keys}, COUNT(*) n FROM {table(mode, name)}
            GROUP BY {keys} HAVING COUNT(*) > 1""").count()
        assert duplicate_groups == 0, f"Duplicate keys in {name}"
        print(f"PASS: unique keys in {name}")

verify_keys(RUN_MODE)
display(spark.table(table(RUN_MODE, "gold.daily_weather_summary")).orderBy("weather_date", "city_name"))
display(spark.table(f"`{CATALOG}`.`{'weather' if RUN_MODE == 'live' else 'weather_demo'}_ops`.v_location_health"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optional deterministic integration tests

# COMMAND ----------

TEST_RESULTS = []
if dbutils.widgets.get("run_tests") == "true":
    test_start = "2030-01-01"
    first = run_pipeline("demo", "none", test_start, 3)
    assert first["status"] == "SUCCESS" and first["rows_valid"] == 360

    def keyset(name, columns):
        return {tuple(row) for row in spark.table(table("demo", name)).select(*columns).collect()}

    silver_before = keyset("silver.hourly_weather", ["location_id", "weather_timestamp"])
    gold_before = keyset("gold.daily_weather_summary", ["location_id", "weather_date"])
    bronze_before = spark.table(table("demo", "bronze.api_responses")).count()
    second = run_pipeline("demo", "none", test_start, 3)
    assert second["status"] == "SUCCESS"
    assert silver_before == keyset("silver.hourly_weather", ["location_id", "weather_timestamp"])
    assert gold_before == keyset("gold.daily_weather_summary", ["location_id", "weather_date"])
    assert spark.table(table("demo", "bronze.api_responses")).count() == bronze_before + 5
    TEST_RESULTS.append({"test": "same-window rerun", "result": "PASS", "run_id": second["run_id"]})

    edge = run_pipeline("demo", "edge_mix", test_start, 3)
    assert edge["status"] == "PARTIAL_SUCCESS" and edge["locations_failed"] == 1
    assert edge["rows_rejected"] == 4 and edge["rows_valid"] == 284
    TEST_RESULTS.append({"test": "bad request + invalid humidity/null/time/duplicate/new field/string", "result": "PASS", "run_id": edge["run_id"]})

    structural = run_pipeline("demo", "structural_mix", test_start, 3)
    assert structural["locations_failed"] == 3 and structural["payloads_rejected"] == 3
    TEST_RESULTS.append({"test": "array mismatch + missing field + truncated JSON", "result": "PASS", "run_id": structural["run_id"]})

    retries = run_pipeline("demo", "retry_mix", test_start, 3)
    assert retries["locations_failed"] == 1 and retries["locations_succeeded"] == 4
    retry_rows = {row.location_id: row.asDict() for row in spark.table(table("demo", "ops.location_runs"))
                  .where(F.col("run_id") == retries["run_id"]).collect()}
    assert [retry_rows[key]["attempts"] for key in ("SG", "DEL", "HYD", "LON", "SYD")] == [2, 2, 2, 3, 1]
    TEST_RESULTS.append({"test": "429 + 503 + timeout retries + isolated persistent timeout", "result": "PASS", "run_id": retries["run_id"]})

    additional = run_pipeline("demo", "extra_edges", test_start, 3,
        faults={"SG": "empty", "DEL": "changed_type", "HYD": "cutoff", "LON": "unit_change", "SYD": "server_error"})
    assert additional["locations_failed"] == 3 and additional["rows_rejected"] == 1
    assert additional["payloads_rejected"] == 2 and additional["rows_valid"] == 95
    TEST_RESULTS.append({"test": "empty + changed type + short response + units + persistent 503", "result": "PASS", "run_id": additional["run_id"]})
    verify_keys("demo")
    create_monitoring_views("demo")
    display(spark.createDataFrame(TEST_RESULTS))
    print("All deterministic Delta integration tests passed.")
else:
    print("Integration tests skipped. Set run_tests=true when ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Dashboard SQL

# COMMAND ----------

def dashboard_queries(mode):
    prefix = "weather" if mode == "live" else "weather_demo"
    ops = f"`{CATALOG}`.`{prefix}_ops`"
    return {
        "Daily temperature": f"SELECT city_name, weather_date, avg_temperature_c, min_temperature_c, max_temperature_c FROM {table(mode, 'gold.daily_weather_summary')} ORDER BY weather_date, city_name",
        "Daily precipitation": f"SELECT city_name, weather_date, total_precipitation_mm, is_complete_day FROM {table(mode, 'gold.daily_weather_summary')} ORDER BY weather_date, city_name",
        "Weather alerts": f"SELECT city_name, weather_date, max_temperature_c, max_wind_speed_kmh, heat_alert, wind_alert, hours_available, is_complete_day FROM {table(mode, 'gold.daily_weather_summary')} ORDER BY weather_date, city_name",
        "Latest run": f"SELECT status, duration_seconds, locations_succeeded, locations_failed, rows_received, rows_valid, rows_rejected, payloads_rejected, silver_rows_upserted, gold_rows_upserted FROM {ops}.v_latest_run",
        "Run history": f"SELECT started_at, status, scenario, duration_seconds, locations_succeeded, locations_failed, rows_valid, rows_rejected, payloads_rejected FROM {table(mode, 'ops.pipeline_runs')} ORDER BY started_at DESC LIMIT 50",
        "Location health": f"SELECT * FROM {ops}.v_location_health ORDER BY city_name",
        "Latest location results": f"SELECT l.* FROM {table(mode, 'ops.location_runs')} l JOIN {ops}.v_latest_run r ON l.run_id=r.run_id ORDER BY l.city_name",
        "Latest quarantine": f"SELECT q.* FROM {table(mode, 'silver.quarantine')} q JOIN {ops}.v_latest_run r ON q.run_id=r.run_id ORDER BY q.location_id, q.row_index",
        "Latest schema events": f"SELECT e.* FROM {table(mode, 'ops.schema_events')} e JOIN {ops}.v_latest_run r ON e.run_id=r.run_id ORDER BY e.location_id, e.field_name",
        "Latest API attempts": f"SELECT a.* FROM {table(mode, 'ops.api_attempts')} a JOIN {ops}.v_latest_run r ON a.run_id=r.run_id ORDER BY a.location_id, a.attempt",
    }

for mode in (RUN_MODE,):
    for title, query in dashboard_queries(mode).items():
        print(f"\n-- {title}\n{query};")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Scheduled-job status
# MAGIC After finishing monitoring outputs, surface a failed/partial LIVE run as a notebook
# MAGIC error so Jobs failure notifications can work.

# COMMAND ----------

if LAST_RUN and RUN_MODE == "live" and LAST_RUN["status"] in ("FAILED", "PARTIAL_SUCCESS"):
    raise RuntimeError(f"Live run {LAST_RUN['run_id']} finished {LAST_RUN['status']}; inspect weather_ops audit tables")