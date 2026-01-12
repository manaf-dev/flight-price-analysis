"""Data transformation module for Flight Price ETL"""

import logging

from pyspark.sql.functions import col, monotonically_increasing_id, to_timestamp

from .computes import (
    compute_kpi_airline_bookings,
    compute_kpi_airline_fares,
    compute_kpi_popular_routes,
    compute_kpi_seasonal_variation,
)

logger = logging.getLogger(__name__)


def clean_data(df):
    """
    Validate and clean the data.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    initial_count = df.count()

    # Handle missing and null values - drop rows with nulls in critical columns
    df = df.na.drop(subset=['airline', 'source', 'destination', 'base_fare_bdt'])

    # Validate fare values (must be non-negative)
    df = df.filter(col('base_fare_bdt') >= 0)
    df = df.filter(col('tax_and_surcharge_bdt') >= 0)
    df = df.filter(col('total_fare_bdt') > 0)

    # ensure consistency
    df = df.withColumn(
        'total_fare_bdt',
        col('base_fare_bdt') + col('tax_and_surcharge_bdt')
    )

    # Validate non-empty strings for categorical fields
    df = df.filter(col('airline').isNotNull() & (col('airline') != ''))
    df = df.filter(col('source').isNotNull() & (col('source') != ''))
    df = df.filter(col('destination').isNotNull() & (col('destination') != ''))

    # Validate departure and arrival dates
    df = df.filter(col('departure_date_and_time').isNotNull())
    df = df.filter(col('arrival_date_and_time').isNotNull())
    df = df.filter(col('departure_date_and_time') < col('arrival_date_and_time'))

    # Drop duplicates
    df = df.dropDuplicates([
        'airline', 'source', 'destination', 
        'departure_date_and_time', 'base_fare_bdt'
    ])

    final_count = df.count()
    removed_count = initial_count - final_count

    logger.info(f"Data validation complete. Removed {removed_count} invalid records.")

    return df


def transform(df):
    """
    Main transformation pipeline with validations and KPI computations.

    Args:
        df: Input DataFrame

    Returns:
        Tuple of (dim_airline, dim_route, dim_class, dim_booking_source, fact_flight_prices,
                  kpi_airline_fares, kpi_seasonal_variation, kpi_popular_routes, kpi_airline_bookings)
    """

    # Parse timestamp columns
    df = df.withColumn(
        'departure_date_and_time', 
        to_timestamp(col('departure_date_and_time'), 'yyyy-MM-dd HH:mm:ss')
    )
    df = df.withColumn(
        'arrival_date_and_time', 
        to_timestamp(col('arrival_date_and_time'), 'yyyy-MM-dd HH:mm:ss')
    )

    # Validate and clean data
    df = clean_data(df)

    # Create Dimension DataFrames ---

    # dim_airline
    dim_airline = (
        df.select("airline").distinct().withColumnRenamed("airline", "airline_name")
    )
    dim_airline = dim_airline.withColumn(
        "airline_id", monotonically_increasing_id()
    ).cache()

    # dim_route
    dim_route = df.select(
        "source", "source_name", "destination", "destination_name"
    ).distinct()
    dim_route = (
        dim_route.withColumnRenamed("source", "source_airport_code")
        .withColumnRenamed("source_name", "source_airport_name")
        .withColumnRenamed("destination", "destination_airport_code")
        .withColumnRenamed("destination_name", "destination_airport_name")
    )
    dim_route = dim_route.withColumn("route_id", monotonically_increasing_id()).cache()

    # dim_class
    dim_class = (
        df.select("class").distinct().na.drop().withColumnRenamed("class", "class_name")
    )
    dim_class = dim_class.withColumn("class_id", monotonically_increasing_id()).cache()

    # dim_booking_source
    dim_booking_source = (
        df.select("booking_source")
        .distinct()
        .na.drop()
        .withColumnRenamed("booking_source", "booking_source_name")
    )
    dim_booking_source = dim_booking_source.withColumn(
        "booking_source_id", monotonically_increasing_id()
    ).cache()

    # Create Fact DataFrame

    fact_flight_prices = (
        df.join(dim_airline, df.airline == dim_airline.airline_name, "inner")
        .join(
            dim_route,
            (df.source == dim_route.source_airport_code)
            & (df.destination == dim_route.destination_airport_code),
            "inner",
        )
        .join(dim_class, col("class") == dim_class.class_name, "inner")
        .join(
            dim_booking_source,
            df.booking_source == dim_booking_source.booking_source_name,
            "inner",
        )
        .select(
            "airline_id",
            "route_id",
            "class_id",
            "booking_source_id",
            col("departure_date_and_time"),
            col("arrival_date_and_time"),
            col("duration_hrs"),
            col("stopovers"),
            col("aircraft_type"),
            col("base_fare_bdt"),
            col("tax_and_surcharge_bdt"),
            col("total_fare_bdt"),
            col("seasonality"),
            col("days_before_departure"),
        )
    )

    row_count = fact_flight_prices.count()
    logger.info(
        f"Transformation complete. Final record count for fact table: {row_count}"
    )

    # --- Compute KPIs (on the original cleaned df) ---
    kpi_airline_fares = compute_kpi_airline_fares(df)
    kpi_seasonal_variation = compute_kpi_seasonal_variation(df)
    kpi_popular_routes = compute_kpi_popular_routes(df)
    kpi_airline_bookings = compute_kpi_airline_bookings(df)

    logger.info("All KPIs computed successfully")

    return (
        dim_airline,
        dim_route,
        dim_class,
        dim_booking_source,
        fact_flight_prices,
        kpi_airline_fares,
        kpi_seasonal_variation,
        kpi_popular_routes,
        kpi_airline_bookings,
    )
