-- PostgreSQL Analytics Database Schema

DROP TABLE IF EXISTS kpi_seasonal_variation CASCADE;
DROP TABLE IF EXISTS kpi_popular_routes CASCADE;
DROP TABLE IF EXISTS kpi_airline_bookings CASCADE;
DROP TABLE IF EXISTS kpi_airline_fares CASCADE;
DROP TABLE IF EXISTS flight_prices CASCADE;

-- flight prices table
CREATE TABLE flight_prices (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source VARCHAR(50) NOT NULL,
    source_name VARCHAR(150),
    destination VARCHAR(50) NOT NULL,
    destination_name VARCHAR(150),
    departure_date_and_time TIMESTAMP NOT NULL,
    arrival_date_and_time TIMESTAMP,
    duration_hrs DECIMAL(5, 2),
    stopovers VARCHAR(255),
    aircraft_type VARCHAR(100),
    class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12, 2) NOT NULL,
    tax_and_surcharge_bdt DECIMAL(12, 2) NOT NULL,
    total_fare_bdt DECIMAL(12, 2) NOT NULL,
    seasonality VARCHAR(50),
    days_before_departure INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Average Fare by Airline
CREATE TABLE kpi_airline_fares (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL UNIQUE,
    average_base_fare DECIMAL(12, 2),
    average_tax_surcharge DECIMAL(12, 2),
    average_total_fare DECIMAL(12, 2),
    min_fare DECIMAL(12, 2),
    max_fare DECIMAL(12, 2),
    booking_count INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--  Seasonal Fare Variation
CREATE TABLE kpi_seasonal_variation (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    seasonality VARCHAR(50) NOT NULL,
    average_fare DECIMAL(12, 2),
    booking_count INT,
    fare_trend VARCHAR(50), -- 'Peak' or 'Non-Peak'
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(airline, seasonality)
);

--  Most Popular Routes
CREATE TABLE kpi_popular_routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    source_name VARCHAR(150),
    destination VARCHAR(50) NOT NULL,
    destination_name VARCHAR(150),
    booking_count INT NOT NULL,
    average_fare DECIMAL(12, 2),
    route_rank INT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, destination)
);

--  Booking Count by Airline
CREATE TABLE kpi_airline_bookings (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL UNIQUE,
    total_bookings INT,
    peak_season_bookings INT,
    non_peak_bookings INT,
    booking_share_percentage DECIMAL(5, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create view for peak season analysis
CREATE VIEW v_peak_season_summary AS
SELECT 
    airline,
    seasonality,
    COUNT(*) as booking_count,
    AVG(total_fare_bdt) as avg_fare,
    MIN(total_fare_bdt) as min_fare,
    MAX(total_fare_bdt) as max_fare
FROM flight_prices
GROUP BY airline, seasonality
ORDER BY airline, seasonality;

-- Create view for route analysis
CREATE VIEW v_route_analysis AS
SELECT 
    source,
    source_name,
    destination,
    destination_name,
    COUNT(*) as booking_count,
    AVG(total_fare_bdt) as avg_fare,
    MIN(total_fare_bdt) as min_fare,
    MAX(total_fare_bdt) as max_fare
FROM flight_prices
GROUP BY source, source_name, destination, destination_name
ORDER BY booking_count DESC;

-- Create view for airline summary
CREATE VIEW v_airline_summary AS
SELECT 
    airline,
    COUNT(*) as total_bookings,
    AVG(total_fare_bdt) as avg_fare,
    AVG(base_fare_bdt) as avg_base_fare,
    AVG(tax_and_surcharge_bdt) as avg_tax,
    MIN(total_fare_bdt) as min_fare,
    MAX(total_fare_bdt) as max_fare
FROM flight_prices
GROUP BY airline
ORDER BY total_bookings DESC;
