DROP TABLE IF EXISTS staging_ready_data;

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

psql -U toetrandro_admin -d toetrandro_db -c "\COPY staging_ready_data FROM '../data/merged/ready_data.csv' DELIMITER ',' CSV HEADER;"


CREATE TABLE dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name TEXT UNIQUE
);

INSERT INTO dim_city (city_name)
SELECT DISTINCT city FROM staging_ready_data;


CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE UNIQUE,
    year INTEGER,
    month TEXT,
    day_of_week TEXT
);

INSERT INTO dim_date (date_value, year, month, day_of_week)
SELECT DISTINCT DATE(timestamp), year, month, day_of_week
FROM staging_ready_data;


CREATE TABLE dim_weather (
    weather_id SERIAL PRIMARY KEY,
    weather_main TEXT,
    weather_description TEXT,
    UNIQUE(weather_main, weather_description)
);

INSERT INTO dim_weather (weather_main, weather_description)
SELECT DISTINCT weather_main, weather_description
FROM staging_ready_data;


CREATE TABLE weather_facts (
    fact_id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES dim_city(city_id),
    date_id INTEGER REFERENCES dim_date(date_id),
    weather_id INTEGER REFERENCES dim_weather(weather_id),
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
    summary TEXT,
    extracted_at TIMESTAMP,
    is_ideal_temp BOOLEAN,
    is_low_rain BOOLEAN,
    is_low_wind BOOLEAN,
    is_ideal_humidity BOOLEAN,
    comfort_score REAL,
    is_ideal_day BOOLEAN
);


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


-- Link weather_facts.city_id → dim_city.city_id
ALTER TABLE weather_facts
ADD CONSTRAINT fk_city
FOREIGN KEY (city_id)
REFERENCES dim_city(city_id);

-- Link weather_facts.date_id → dim_date.date_id
ALTER TABLE weather_facts
ADD CONSTRAINT fk_date
FOREIGN KEY (date_id)
REFERENCES dim_date(date_id);

-- Link weather_facts.weather_id → dim_weather.weather_id
ALTER TABLE weather_facts
ADD CONSTRAINT fk_weather
FOREIGN KEY (weather_id)
REFERENCES dim_weather(weather_id);
