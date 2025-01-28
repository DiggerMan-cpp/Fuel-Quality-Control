CREATE TABLE fuel_quality (
    id SERIAL PRIMARY KEY,
    station_name VARCHAR(255) NOT NULL,
    fuel_type VARCHAR(50) NOT NULL,
    is_quality_ok BOOLEAN NOT NULL
);