# 🌐 Data Sources — Toetrandro ETL

This document describes the external data sources used in the `toetrandro-etl` pipeline. The project integrates both real-time and historical weather data to generate climate-based travel recommendations.

---

## 📡 1. Real-Time Forecast Data — OpenWeather API

### 🔗 API Endpoint

```
https://api.openweathermap.org/data/2.5/forecast
```

### 🧾 Description

This API provides 5-day weather forecasts in 3-hour intervals for a given city. It is used in the `extract_weather_data` task to fetch up-to-date weather conditions.

### 🔐 Authentication

- Requires an API key (stored in Airflow Variable `OPENWEATHER_API_KEY`)
- Passed as a query parameter: `?appid=YOUR_API_KEY`

### 📥 Parameters Used

| Parameter | Description                 |
|----------|-----------------------------|
| `q`      | City name (e.g., `Paris`)   |
| `units`  | Metric system (`metric`)    |
| `appid`  | API key                     |

### 📊 Key Fields Extracted

- `dt_txt` — Forecast timestamp
- `main.temp`, `main.temp_min`, `main.temp_max`, `main.feels_like`
- `main.pressure`, `main.humidity`
- `wind.speed`, `wind.deg`, `wind.gust`
- `clouds.all`
- `weather.main`, `weather.description`

### 🧠 Usage in Pipeline

- Stored in `data/raw/`
- Cleaned and enriched in `transform_enriched_data`
- Used to compute `comfort_score` and `is_ideal_day`

---

## 🗃️ 2. Historical Weather Data — Open-Meteo API

### 🔗 API Reference

[Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)

### 🧾 Description

This API provides historical daily weather summaries for multiple cities from 2020 onward. It is used to build a long-term climate baseline for each city.

### 📥 Sample Query

```
https://archive-api.open-meteo.com/v1/archive
?latitude=...
&longitude=...
&start_date=2020-01-01
&daily=temperature_2m_mean,wind_speed_10m_max,...
&timezone=auto
```

### 📊 Key Fields Extracted

| Field                         | Description                            |
|------------------------------|----------------------------------------|
| `temperature_2m_mean`        | Daily average temperature              |
| `temperature_2m_max/min`     | Daily max/min temperature              |
| `apparent_temperature_*`     | Feels-like temperature metrics         |
| `wind_speed_10m_max`         | Max wind speed                         |
| `wind_gusts_10m_max`         | Max wind gusts                         |
| `precipitation_sum`          | Total daily precipitation              |
| `sunshine_duration`          | Total sunshine hours                   |
| `sunrise`, `sunset`          | Timestamps for daylight calculation    |
| `weather_code`               | Encoded weather condition              |

### 🧠 Usage in Pipeline

- Downloaded manually or via script into `data/raw/`
- Merged with real-time data in `merge_processed_files`
- Used to compute historical comfort trends and seasonal patterns

---

## 🧪 Data Quality & Harmonization

- Units are standardized (e.g., °C, m/s)
- Timestamps are normalized to local timezones
- Missing values are handled during transformation
- Fields from both APIs are aligned into a unified schema

---

## 🧠 Summary

| Source       | Type       | Frequency | Used In Task             |
|--------------|------------|-----------|---------------------------|
| OpenWeather  | Real-time  | Daily     | `extract_weather_data`    |
| Open-Meteo   | Historical | One-time  | `merge_processed_files`   |

Together, these sources provide a robust foundation for climate-aware travel recommendations.
