-- MySQL staging database schema
DROP TABLE IF EXISTS staging;

CREATE TABLE staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    destination_name VARCHAR(255) NOT NULL,
    departure_date_and_time TIMESTAMP NOT NULL,
    departure_datetime TIMESTAMP NOT NULL,
    arrival_date_and_time TIMESTAMP NOT NULL,
    arrival_datetime TIMESTAMP NOT NULL,
    duration_hrs DOUBLE(10, 2) NOT NULL,
    stopovers VARCHAR(255) NOT NULL,
    aircraft_type VARCHAR(255) NOT NULL,
    class VARCHAR(255) NOT NULL,
    booking_source VARCHAR(255) NOT NULL,
    base_fare_bdt DOUBLE(10, 2) NOT NULL,
    tax_and_surcharge_bdt DOUBLE(10, 2) NOT NULL,
    total_fare_bdt DOUBLE NOT NULL,
    seasonality VARCHAR(255) NOT NULL,
    days_before_departure INT NOT NULL
)