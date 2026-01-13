# KPI Definitions and Computation

This document defines the Key Performance Indicators (KPIs) computed in the pipeline.

## KPI Logic

The KPIs are computed using Spark on the cleaned dataset to derive business insights.

### 1. Airline Fare Analysis (`kpi_airline_fares`)
- **Description**: This KPI provides a summary of fare structures for each airline.
- **Computation**:
  - `average_base_fare`: Average of `base_fare_bdt`.
  - `average_tax_surcharge`: Average of `tax_and_surcharge_bdt`.
  - `average_total_fare`: Average of `total_fare_bdt`.
  - `min_fare` & `max_fare`: The minimum and maximum `total_fare_bdt`.
  - `booking_count`: Total number of flights recorded for the airline.

### 2. Seasonal Fare Variation (`kpi_seasonal_variation`)
- **Description**: Compares the average fares during peak and non-peak seasons for each airline.
- **Computation**:
  - The `seasonality` column is used to classify bookings into 'Peak' or 'Non-Peak'.
  - The average `total_fare_bdt` is then calculated for each airline and season type.

### 3. Popular Routes (`kpi_popular_routes`)
- **Description**: Identifies the top 50 most frequently booked flight routes.
- **Computation**:
  - Bookings are grouped by `source` and `destination`.
  - The `booking_count` and `average_fare` are calculated for each route.
  - A `dense_rank` is applied to identify the top routes based on booking count.

### 4. Airline Booking Share (`kpi_airline_bookings`)
- **Description**: Calculates the total booking count and market share for each airline, with a breakdown for peak and non-peak seasons.
- **Computation**:
  - `total_bookings`: The total number of flights for each airline.
  - `peak_season_bookings`: The count of flights during 'Peak' seasons.
  - `non_peak_bookings`: The count of flights during 'Non-Peak' seasons.
  - `booking_share_percentage`: The airline's percentage of all bookings in the dataset.
