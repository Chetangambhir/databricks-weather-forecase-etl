# Databricks notebook source
spark.sql("SELECT current_catalog(), current_schema()").show()

# COMMAND ----------

import requests

params = {
    "latitude": 1.3521,
    "longitude": 103.8198,
    "hourly": (
        "temperature_2m,relative_humidity_2m,"
        "precipitation,wind_speed_10m"
    ),
    "forecast_days": 3,
    "timezone": "UTC",
}

response = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params=params,
    timeout=(10, 30),
)
response.raise_for_status()

payload = response.json()
print("HTTP status:", response.status_code)
print("Hourly rows:", len(payload["hourly"]["time"]))
print("Units:", payload["hourly_units"])