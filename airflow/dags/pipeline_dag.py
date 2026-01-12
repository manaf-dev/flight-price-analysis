import logging
import subprocess
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow import DAG

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "flight_price_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for flight price analysis",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "spark", "flight-analysis"],
)


def check_csv_exists(**context):
    """Check if the CSV file exists."""
    import os

    csv_path = "/opt/data/Flight_Price_Dataset_of_Bangladesh.csv"

    logger.info(f"Checking if file exists: {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    file_size = os.path.getsize(csv_path)
    logger.info(f"CSV file found! Size: {file_size} bytes")

    return csv_path


def run_spark_etl_python(**context):
    """
    Run Spark ETL by executing spark-submit on spark-master container.
    This uses Python subprocess to run docker exec command.
    """
    logger.info("Submitting Spark job to spark-master container...")

    cmd = [
        "docker",
        "exec",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--executor-memory",
        "2g",
        "--driver-memory",
        "1g",
        "--conf",
        "spark.sql.warehouse.dir=/tmp/spark-warehouse",
        "--jars",
        "/opt/jars/mysql-connector-j-9.5.0.jar,/opt/jars/postgresql-42.7.6.jar",
        "/opt/jobs/spark_etl.py",
    ]

    try:
        logger.info(f"Executing: {' '.join(cmd)}")

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=900  # 15 minutes timeout
        )

        if result.returncode == 0:
            logger.info("Spark job completed successfully!")
            return True
        else:
            logger.error(f"Spark job failed with return code {result.returncode}")
            raise Exception(f"Spark job failed with return code {result.returncode}")

    except subprocess.TimeoutExpired:
        logger.error("Spark job timed out after 15 minutes")
        raise
    except Exception as e:
        logger.error(f"Error running Spark job: {str(e)}")
        raise


def validate_mysql_data(**context):
    """Validate MySQL staging data."""
    logger.info("Validating MySQL staging data...")

    mysql_hook = MySqlHook(mysql_conn_id="mysql_staging")

    # Check table exists
    table_check = mysql_hook.get_records("SHOW TABLES LIKE 'staging_prices'")
    if not table_check:
        raise ValueError("Table 'staging_prices' does not exist in MySQL")

    row_count = mysql_hook.get_first("SELECT COUNT(*) FROM staging_prices")[0]
    logger.info(f" MySQL staging contains {row_count} records")

    if row_count == 0:
        raise ValueError("MySQL staging table is empty")

    context["task_instance"].xcom_push(key="mysql_row_count", value=row_count)
    return row_count


def validate_postgres_data(**context):
    """Validate PostgreSQL analytics data."""
    logger.info("Validating PostgreSQL analytics data...")

    postgres_hook = PostgresHook(postgres_conn_id="postgres_analytics")

    tables = [
        "dim_airline",
        "dim_route",
        "dim_class",
        "dim_booking_source",
        "fact_flight_prices",
        "kpi_airline_fares",
        "kpi_seasonal_variation",
        "kpi_popular_routes",
        "kpi_airline_bookings",
    ]

    results = {}
    all_tables_exist = True

    for table in tables:
        # Check existence
        exists_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = '{table}'
            );
        """
        exists = postgres_hook.get_first(exists_query)[0]

        if not exists:
            logger.warning(f"Table '{table}' does not exist in PostgreSQL")
            all_tables_exist = False
            continue

        # Check row count
        row_count = postgres_hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        results[table] = row_count
        logger.info(f"Table '{table}': {row_count} records")

    if not all_tables_exist:
        raise ValueError("One or more analytics tables were not created.")

    if results.get("fact_flight_prices", 0) == 0:
        raise ValueError("Main 'fact_flight_prices' table is empty")

    return results


# Task 1: Check CSV file exists
check_csv = PythonOperator(
    task_id="check_csv_file",
    python_callable=check_csv_exists,
    provide_context=True,
    dag=dag,
)

# Task 2: Run Spark ETL using Python subprocess with docker exec
run_spark = PythonOperator(
    task_id="run_spark_etl",
    python_callable=run_spark_etl_python,
    provide_context=True,
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

# Task 3: Validate MySQL
validate_mysql = PythonOperator(
    task_id="validate_mysql_staging",
    python_callable=validate_mysql_data,
    provide_context=True,
    dag=dag,
)

# Task 4: Validate PostgreSQL
validate_postgres = PythonOperator(
    task_id="validate_postgres_analytics",
    python_callable=validate_postgres_data,
    provide_context=True,
    dag=dag,
)


# Define task dependencies
check_csv >> run_spark >> validate_mysql >> validate_postgres
