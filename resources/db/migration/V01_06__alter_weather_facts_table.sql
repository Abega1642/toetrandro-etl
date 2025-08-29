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