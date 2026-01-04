-- MySQL staging database schema
DROP TABLE IF EXISTS staging_prices;

CREATE TABLE staging_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(255) NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_name VARCHAR(255),
    destination VARCHAR(255) NOT NULL,
    destination_name VARCHAR(255),
    departure_date_and_time TIMESTAMP NOT NULL,
    arrival_date_and_time TIMESTAMP NOT NULL,
    duration_hrs DOUBLE(10, 2),
    stopovers VARCHAR(255),
    aircraft_type VARCHAR(255),
    class VARCHAR(255),
    booking_source VARCHAR(255),
    base_fare_bdt DOUBLE(10, 2) NOT NULL,
    tax_and_surcharge_bdt DOUBLE(10, 2) NOT NULL,
    total_fare_bdt DOUBLE(10, 2) NOT NULL,
    seasonality VARCHAR(255),
    days_before_departure INT
)