"""
Flight Price Analysis ETL Pipeline

This module orchestrates the extraction, transformation, and loading of flight price data.
Pipeline: CSV > MySQL (Staging) > Transform > PostgreSQL (Analytics)
"""

import logging

from extract import extract_data
from loaders import load_to_mysql, load_to_postgres
from pyspark.sql import SparkSession
from schema import FLIGHT_PRICE_SCHEMA
from tranformers.transform import transform
from tranformers.validations import clean_column_names, validate_required_columns

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_spark():
    spark = (
        SparkSession.builder.appName("flight-price-analysis")
        .master("local[*]")
        .config(
            "spark.jars",
            "/opt/jars/mysql-connector-j-9.5.0.jar," "/opt/jars/postgresql-42.7.6.jar",
        )
        .getOrCreate()
    )
    logger.info("Spark session initialized")
    return spark


def main():
    """Main ETL pipeline orchestrator."""

    csv_path = '/opt/data/Flight_Price_Dataset_of_Bangladesh.csv'

    try:
        logger.info("Starting Flight Price Analysis ETL Pipeline")
        spark = initialize_spark()

        # Extract
        df_raw = extract_data(spark, csv_path, FLIGHT_PRICE_SCHEMA)

        # Clean column names
        df_raw = clean_column_names(df_raw)

        # Validate required columns
        df_raw, is_valid = validate_required_columns(df_raw)
        if not is_valid:
            spark.stop()

        # Load to MySQL (staging)
        staging_loaded = load_to_mysql(spark, df_raw)
        if not staging_loaded:
            spark.stop()

        # Transform
        df_transformed, kpi_fares, kpi_seasonal, kpi_routes, kpi_bookings = transform(spark, df_raw)

        # Load to PostgreSQL (analytics)

        # Load to PostgreSQL (analytics)
        load_to_postgres(df_transformed, config={"table_name": "flight_prices"})

        # Airline Fares
        load_to_postgres(kpi_fares, config={"table_name": "kpi_airline_fares"})

        # Seasonal Variation
        load_to_postgres(kpi_seasonal, config={"table_name": "kpi_seasonal_variation"})

        # Popular Routes
        load_to_postgres(kpi_routes, config={"table_name": "kpi_popular_routes"})

        # Airline Bookings
        load_to_postgres(kpi_bookings, config={"table_name": "kpi_airline_bookings"})

        logger.info("ETL Pipeline completed successfully!")

        spark.stop()
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise


if __name__ == '__main__':
    main()
