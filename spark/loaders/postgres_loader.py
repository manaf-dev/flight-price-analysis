"""PostgreSQL loader module for Flight Price ETL"""

import logging

logger = logging.getLogger(__name__)


def load_to_postgres(df, config):
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
        pg_table = config.get("table_name")

        # Build JDBC URL for PostgreSQL
        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
        jdbc_properties = {
            "user": pg_user,
            "password": pg_password,
            "driver": "org.postgresql.Driver",
        }
        df.write.jdbc(
            url=jdbc_url, table=pg_table, mode="overwrite", properties=jdbc_properties
        )

        row_count = df.count()
        logger.info(f"Successfully loaded {row_count} rows to PostgreSQL table: {pg_table}")
        return True

    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise
