"""
Flight Price Analysis ETL Pipeline

This module orchestrates the extraction, transformation, and loading of flight price data.
It uses modular components for each ETL stage.
"""

import logging

from extract import extract_data
from loaders import load_to_mysql
from pyspark.sql import SparkSession
from schema import FLIGHT_PRICE_SCHEMA
from transform import clean_column_names, transform

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_spark():
    """Initialize and return a Spark session."""
    spark = SparkSession.builder \
        .appName('flight-price-analysis') \
        .config("spark.jars", "/opt/jars/mysql-connector-j-9.5.0.jar") \
        .master('local[*]') \
        .getOrCreate()
    logger.info("Spark session initialized")
    return spark


def main():
    """Main ETL pipeline orchestrator."""
    csv_path = '/opt/data/Flight_Price_Dataset_of_Bangladesh.csv'
    try:
        logger.info("Starting flight price ETL pipeline")
        spark = initialize_spark()
        
        # Extract
        df = extract_data(spark, csv_path, FLIGHT_PRICE_SCHEMA)
        
        # Clean column names
        df = clean_column_names(df)
        
        # Load to MySQL (staging)
        load_staging = load_to_mysql(spark, df)

        # Transform
        df = transform(spark, df)
        
        
        logger.info("ETL pipeline completed successfully")
        spark.stop()
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise


if __name__ == '__main__':
    main()
