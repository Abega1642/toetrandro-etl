CREATE TABLE dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name TEXT UNIQUE
);