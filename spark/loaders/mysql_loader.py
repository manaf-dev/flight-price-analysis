"""MySQL loader module for Flight Price ETL"""

import logging

logger = logging.getLogger(__name__)


def load_to_mysql(spark, df):
    """
    Load DataFrame to MySQL database.

    Args:
        spark: SparkSession instance
        df: Input PySpark DataFrame

    Returns:
        Boolean indicating success
    """
    try:
        logger.info("Starting MySQL load operation")

        mysql_host = "mysql"
        mysql_port = "3306"
        mysql_database = "mysql_db"
        mysql_user = "mysql_user"
        mysql_password = "mysql_pass"
        mysql_table = "raw_prices"

        # Build JDBC URL
        jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}?allowPublicKeyRetrieval=true&useSSL=false"

        # Write DataFrame to MySQL
        df.write.format("jdbc").mode("overwrite").option("url", jdbc_url).option(
            "dbtable", mysql_table
        ).option("user", mysql_user).option("password", mysql_password).option(
            "driver", "com.mysql.cj.jdbc.Driver"
        ).option(
            "batchsize", "10000"
        ).save()

        row_count = df.count()
        logger.info(
            f"Successfully loaded {row_count} rows to MySQL table: {mysql_table}"
        )
        return True

    except Exception as e:
        logger.error(f"Error loading data to MySQL: {str(e)}")
        raise
