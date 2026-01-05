import logging

from pyspark.sql.functions import (
    avg,
    col,
    count,
    dense_rank,
    lit,
    max,
    min,
    round,
    when,
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# Define peak seasons
PEAK_SEASONS = ["Eid", "Hajj", "Winter Holidays"]


def compute_kpi_airline_fares(df):
    """
    Compute KPI: Average Fare by Airline
    
    Args:
        df: Transformed DataFrame
    
    Returns:
        KPI DataFrame
    """
    logger.info("Computing KPI: Average Fare by Airline")
    
    kpi_df = df.groupBy('airline').agg(
        round(avg('base_fare_bdt'), 2).alias('average_base_fare'),
        round(avg('tax_and_surcharge_bdt'), 2).alias('average_tax_surcharge'),
        round(avg('total_fare_bdt'), 2).alias('average_total_fare'),
        round(min('total_fare_bdt'), 2).alias('min_fare'),
        round(max('total_fare_bdt'), 2).alias('max_fare'),
        count('*').alias('booking_count')
    ).orderBy(col('booking_count').desc())
    
    return kpi_df


def compute_kpi_seasonal_variation(df):
    """
    Compute KPI: Seasonal Fare Variation (Peak vs Non-Peak)
    
    Args:
        df: Transformed DataFrame
    
    Returns:
        KPI DataFrame
    """
    logger.info("Computing KPI: Seasonal Fare Variation")
    
    # Classify as peak or non-peak season
    df_seasonal = df.withColumn(
        'fare_trend',
        when(col('seasonality').isin(PEAK_SEASONS), 'Peak').otherwise('Non-Peak')
    )
    
    kpi_df = df_seasonal.groupBy('airline', 'seasonality', 'fare_trend').agg(
        round(avg('total_fare_bdt'), 2).alias('average_fare'),
        count('*').alias('booking_count')
    ).orderBy('airline', 'seasonality')
    
    return kpi_df


def compute_kpi_popular_routes(df):
    """
    Compute KPI: Most Popular Routes
    
    Args:
        df: Transformed DataFrame
    
    Returns:
        Top routes DataFrame
    """
    logger.info("Computing KPI: Most Popular Routes")
    
    window_spec = Window.orderBy(col('booking_count').desc())
    
    kpi_df = df.groupBy(
        'source', 'source_name', 
        'destination', 'destination_name'
    ).agg(
        count('*').alias('booking_count'),
        round(avg('total_fare_bdt'), 2).alias('average_fare')
    ).withColumn(
        'route_rank',
        dense_rank().over(window_spec)
    ).filter(col('route_rank') <= 50)  # Top 50 routes
    
    return kpi_df


def compute_kpi_airline_bookings(df):
    """
    Compute KPI: Booking Count by Airline

    Args:
        df: Transformed DataFrame

    Returns:peak_season_bookings
        KPI DataFrame
    """
    logger.info("Computing KPI: Booking Count by Airline")

    # Classify as peak or non-peak
    df_seasonal = df.withColumn(
        'season_type',
        when(col('seasonality').isin(PEAK_SEASONS), 'peak').otherwise('non_peak')
    )

    total_bookings = df.count()

    kpi_df = (
        df_seasonal.groupBy("airline")
        .pivot("season_type")
        .agg(count("departure_date_and_time").alias("bookings"))
        .fillna(0)
    )

    # Rename columns if they exist
    if 'peak' in kpi_df.columns:
        kpi_df = kpi_df.withColumnRenamed('peak', 'peak_season_bookings')
    if 'non_peak' in kpi_df.columns:
        kpi_df = kpi_df.withColumnRenamed('non_peak', 'non_peak_bookings')

    # Calculate total and percentage
    kpi_df = kpi_df.withColumn(
        'total_bookings',
        col('peak_season_bookings') + col('non_peak_bookings')
    ).withColumn(
        'booking_share_percentage',
        round((col('total_bookings') / lit(total_bookings)) * 100, 2)
    ).select(
        'airline',
        'total_bookings',
        'peak_season_bookings',
        'non_peak_bookings',
        'booking_share_percentage'
    )

    return kpi_df
