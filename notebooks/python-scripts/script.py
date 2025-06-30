import pandas as pd
from datetime import datetime


locations = pd.read_csv("locations.csv")
weather = pd.read_csv("../data/merge/all_weather_data.csv")


df = weather.merge(locations, on="location_id")

df["temp_C"] = df["temperature_2m_mean (°C)"]
df["temp_min_C"] = df["temperature_2m_min (°C)"]
df["temp_max_C"] = df["temperature_2m_max (°C)"]
df["feels_like_C"] = df["apparent_temperature_mean (°C)"]
df["wind_speed"] = df["wind_speed_10m_max (m/s)"]
df["wind_gust"] = df["wind_gusts_10m_max (m/s)"]
df["wind_deg"] = df["wind_direction_10m_dominant (°)"]
df["rain_1d"] = df["rain_sum (mm)"]
df["humidity"] = None
df["cloudiness"] = None
df["precipitation_prob"] = None
df["weather_main"] = None
df["weather_description"] = None
df["city"] = df["timezone"].str.split("/").str[-1].str.replace("_", " ")
df["timestamp"] = pd.to_datetime(df["time"])
df["extracted_at"] = datetime.now()


df["is_ideal_temp"] = df["temp_C"].between(22, 28)
df["is_low_rain"] = df["rain_1d"] == 0
df["is_low_wind"] = df["wind_speed"] < 5
df["is_ideal_humidity"] = False
df["comfort_score"] = (
    df["is_ideal_temp"].astype(int) * 0.4 +
    df["is_low_rain"].astype(int) * 0.3 +
    df["is_low_wind"].astype(int) * 0.2
)
df["is_ideal_day"] = df["is_ideal_temp"] & df["is_low_rain"] & df["is_low_wind"]
df["month"] = df["timestamp"].dt.month_name()
df["year"] = df["timestamp"].dt.year
df["day_of_week"] = df["timestamp"].dt.day_name()
df["summary"] = None


final_cols = [
    "city", "timestamp", "sunrise", "sunset", "temp_C", "temp_min_C", "temp_max_C",
    "feels_like_C", "pressure", "humidity", "wind_speed", "wind_deg", "wind_gust",
    "cloudiness", "precipitation_prob", "rain_1d", "weather_main", "weather_description",
    "summary", "extracted_at", "is_ideal_temp", "is_low_rain", "is_low_wind",
    "is_ideal_humidity", "comfort_score", "is_ideal_day", "month", "year", "day_of_week"
]

df_final = df[final_cols]
