"""Data transformation module for Flight Price ETL"""

import logging

from pyspark.sql.functions import col, to_timestamp

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



def transform(spark, df):
    """
    Main transformation pipeline with validations and KPI computations.
    
    Args:
        spark: SparkSession
        df: Input DataFrame
    
    Returns:
        Tuple of (transformed_df, kpi_airline_fares, kpi_seasonal_variation, kpi_popular_routes, kpi_airline_bookings)
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
    
    row_count = df.count()
    logger.info(f"Transformation complete. Final record count: {row_count}")
    
    # Compute KPIs
    kpi_airline_fares = compute_kpi_airline_fares(spark, df)
    kpi_seasonal_variation = compute_kpi_seasonal_variation(spark, df)
    kpi_popular_routes = compute_kpi_popular_routes(spark, df)
    kpi_airline_bookings = compute_kpi_airline_bookings(spark, df)
    
    logger.info("All KPIs computed successfully")
    
    return df, kpi_airline_fares, kpi_seasonal_variation, kpi_popular_routes, kpi_airline_bookings
