import logging
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from python_callables import (
    CSV_PATH,
    cleanup_temp_files,
    extract_and_clean,
    load_all_analytics,
    load_staging,
    transform_data,
)

from airflow import DAG

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "flight_price_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for flight price analysis",
    schedule_interval=None,
    catchup=False,
    tags=["etl", "spark", "flight-analysis"],
)


# Task 1: Sense CSV file
sense_csv = FileSensor(
    task_id="sense_csv_file",
    filepath=CSV_PATH,
    fs_conn_id="fs_default",
    poke_interval=10,
    timeout=600,
    mode="poke",
    dag=dag,
)

# Task 2: Extract and clean data
extract_task = PythonOperator(
    task_id="extract_and_clean",
    python_callable=extract_and_clean,
    provide_context=True,
    dag=dag,
)

# Task 3: Load to MySQL staging
load_staging_task = PythonOperator(
    task_id="load_to_mysql_staging",
    python_callable=load_staging,
    provide_context=True,
    dag=dag,
)

# Task 4: Transform data
transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 5: Load analytics tables to PostgreSQL
# analytics_tables = [
#     "dim_airline",
#     "dim_route",
#     "dim_class",
#     "dim_booking_source",
#     "fact_flight_prices",
#     "kpi_airline_fares",
#     "kpi_seasonal_variation",
#     "kpi_popular_routes",
#     "kpi_airline_bookings",
# ]

load_analytics_task = PythonOperator(
    task_id="load_analytics_to_postgres",
    python_callable=load_all_analytics,
    dag=dag,
)

# Task 6: Cleanup temporary files
cleanup_task = PythonOperator(
    task_id="cleanup_temp_files",
    python_callable=cleanup_temp_files,
    provide_context=True,
    trigger_rule="all_done",  # Run even if upstream tasks fail
    dag=dag,
)

# Define task dependencies
(sense_csv >> extract_task >> [load_staging_task, transform_task])

transform_task >> load_analytics_task >> cleanup_task
