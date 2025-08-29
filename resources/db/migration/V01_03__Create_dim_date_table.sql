CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE UNIQUE,
    year INTEGER,
    month TEXT,
    day_of_week TEXT
);