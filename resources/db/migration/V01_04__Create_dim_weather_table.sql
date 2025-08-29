CREATE TABLE dim_weather (
    weather_id SERIAL PRIMARY KEY,
    weather_main TEXT,
    weather_description TEXT,
    UNIQUE(weather_main, weather_description)
);