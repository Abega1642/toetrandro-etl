INSERT INTO dim_city (city_name)
SELECT DISTINCT city FROM staging_ready_data;

INSERT INTO dim_date (date_value, year, month, day_of_week)
SELECT DISTINCT DATE(timestamp), year, month, day_of_week
FROM staging_ready_data;

INSERT INTO dim_weather (weather_main, weather_description)
SELECT DISTINCT weather_main, weather_description
FROM staging_ready_data;

INSERT INTO weather_facts (
    city_id, date_id, weather_id,
    sunrise, sunset, temp_C, temp_min_C, temp_max_C, feels_like_C,
    pressure, humidity, wind_speed, wind_deg, wind_gust, cloudiness,
    precipitation_prob, rain_1d, summary, extracted_at,
    is_ideal_temp, is_low_rain, is_low_wind, is_ideal_humidity,
    comfort_score, is_ideal_day
)
SELECT
    c.city_id,
    d.date_id,
    w.weather_id,
    s.sunrise, s.sunset, s.temp_C, s.temp_min_C, s.temp_max_C, s.feels_like_C,
    s.pressure, s.humidity, s.wind_speed, s.wind_deg, s.wind_gust, s.cloudiness,
    s.precipitation_prob, s.rain_1d, s.summary, s.extracted_at,
    s.is_ideal_temp, s.is_low_rain, s.is_low_wind, s.is_ideal_humidity,
    s.comfort_score, s.is_ideal_day
FROM staging_ready_data s
JOIN dim_city c ON s.city = c.city_name
JOIN dim_date d ON DATE(s.timestamp) = d.date_value
LEFT JOIN dim_weather w ON s.weather_main = w.weather_main AND s.weather_description = w.weather_description;