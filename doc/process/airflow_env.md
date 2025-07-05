# 🌬️ Airflow Environment Configuration — Toetrandro ETL

This document describes how the Airflow environment is configured for the `toetrandro-etl` project. All runtime credentials and configuration values are managed using **Airflow Variables**, with no `.env` file or shell environment variables required.

---

## 📦 Configuration Overview

The pipeline relies on two key Airflow Variables:

1. `OPENWEATHER_API_KEY` — used to authenticate with the OpenWeather API  
2. `toetrandro_db_config` — used to connect to the PostgreSQL database for data migration

These variables are securely stored and accessed at runtime by the DAG tasks.

---

## 🔐 Setting Up the Variables

You can set the variables using the Airflow CLI:

### 1. OpenWeather API Key

```bash
airflow variables set OPENWEATHER_API_KEY your_api_key_here
```

This key is used by the `extract_weather_data` task to fetch real-time weather data from the OpenWeather API.

---

### 2. PostgreSQL Database Configuration

```bash
airflow variables set toetrandro_db_config '{
  "dbname": "your_db_name",
  "user": "you_username",
  "password": "your_password",
  "host": "localhost",
  "port": 5432
}'
```

This JSON object is used by the `migrate_data_to_postgres` task to connect to the PostgreSQL database and load the final dataset.

---

## ✅ Summary

| Variable Name         | Purpose                          | Format     |
|-----------------------|----------------------------------|------------|
| `OPENWEATHER_API_KEY` | API key for weather extraction   | String     |
| `toetrandro_db_config`| DB credentials for migration     | JSON object|

This setup ensures that sensitive credentials are securely managed and easily accessible within the Airflow runtime environment.
