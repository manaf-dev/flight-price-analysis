"""PostgreSQL loader module for Flight Price ETL"""

import logging

logger = logging.getLogger(__name__)


def load_to_postgres(df):
    """
    Load DataFrame to PostgreSQL database.
    
    Args:
        df: Input PySpark DataFrame
        
    Returns:
        Boolean indicating success
    """
    try:
        logger.info("Starting PostgreSQL load operation")
        
        pg_host = 'postgres_analytics'
        pg_port = '5432'
        pg_database = 'analytics_db'
        pg_user = 'analytics_user'
        pg_password = 'analytics_password'
        pg_table = 'flight_prices'
        
        # Build JDBC URL for PostgreSQL
        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
        
        # Write DataFrame to PostgreSQL
        df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", jdbc_url) \
            .option("dbtable", pg_table) \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .save()
        
        row_count = df.count()
        logger.info(f"Successfully loaded {row_count} rows to PostgreSQL table: {pg_table}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise
