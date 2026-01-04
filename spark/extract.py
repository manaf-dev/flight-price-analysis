"""Data extraction module for Flight Price ETL"""

import logging

logger = logging.getLogger(__name__)


def extract_data(spark, file_path, schema):
    """
    Extract flight price data from CSV file.
    
    Args:
        spark: SparkSession instance
        file_path: Path to the CSV file
        schema: PySpark StructType schema for the data
    
    Returns:
        PySpark DataFrame with extracted data
    """
    logger.info(f"Extracting data from {file_path}")
    try:
        df = spark.read.csv(file_path, header=True, schema=schema)
        row_count = df.count()
        logger.info(f"Extracted {row_count} rows")
        return df
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise
