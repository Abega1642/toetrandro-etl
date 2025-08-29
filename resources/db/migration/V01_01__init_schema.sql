CREATE TABLE staging_ready_data (
    city TEXT,
    timestamp TIMESTAMP,
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    temp_C REAL,
    temp_min_C REAL,
    temp_max_C REAL,
    feels_like_C REAL,
    pressure REAL,
    humidity REAL,
    wind_speed REAL,
    wind_deg REAL,
    wind_gust REAL,
    cloudiness REAL,
    precipitation_prob REAL,
    rain_1d REAL,
    weather_main TEXT,
    weather_description TEXT,
    summary TEXT,
    extracted_at TIMESTAMP,
    is_ideal_temp BOOLEAN,
    is_low_rain BOOLEAN,
    is_low_wind BOOLEAN,
    is_ideal_humidity BOOLEAN,
    comfort_score REAL,
    is_ideal_day BOOLEAN,
    month TEXT,
    year INTEGER,
    day_of_week TEXT
);

COPY staging_ready_data
FROM 'ready_data.csv'
DELIMITER ','
CSV HEADER;