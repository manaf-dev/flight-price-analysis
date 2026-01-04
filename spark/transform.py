"""Data transformation module for Flight Price ETL"""

import logging

from pyspark.sql.functions import col, to_timestamp

logger = logging.getLogger(__name__)



def clean_column_names(df):
    """
    Clean column names by removing leading/trailing spaces, 
    replacing spaces with underscores, and converting to lowercase.
    """
    logger.info("Cleaning column names")
    for old_col in df.columns:
        new_col = old_col.strip() \
            .replace(' ', '_') \
            .replace('(', '') \
            .replace(')', '') \
            .replace('&', 'and') \
            .lower()
        
        df = df.withColumnRenamed(old_col, new_col)
    return df


def transform(spark, df):
    """
    Transform and clean flight price data.
    
    Args:
        spark: SparkSession instance
        df: Input PySpark DataFrame
    
    Returns:
        Transformed PySpark DataFrame
    """
    logger.info("Starting data transformation")
    
    # Handle missing and null values
    df = df.na.drop()

    # Parse timestamp columns
    df = df.withColumn('departure_datetime', to_timestamp(col('departure_date_and_time'), 'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('arrival_datetime', to_timestamp(col('arrival_date_and_time'), 'yyyy-MM-dd HH:mm:ss'))

    # Validate fare values
    df = df.filter(col('base_fare_bdt') >= 0)
    df = df.filter(col('tax_and_surcharge_bdt') >= 0)
    df = df.filter(col('total_fare_bdt') >= 0)

    # Validate non-empty strings
    df = df.filter(col('airline').isNotNull())
    df = df.filter(col('source').isNotNull())
    df = df.filter(col('source_name').isNotNull())
    df = df.filter(col('destination').isNotNull())

    # Drop duplicates
    df = df.dropDuplicates(['airline', 'source', 'destination', 'departure_datetime', 'base_fare_bdt'])

    row_count = df.count()
    logger.info(f"Transformation complete. Rows after transformation: {row_count}")
    return df
