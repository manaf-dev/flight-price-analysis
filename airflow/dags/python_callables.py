import logging
import os

from extract import extract_data
from loaders.mysql_loader import load_to_mysql
from loaders.postgres_loader import load_to_postgres
from pyspark.sql import SparkSession
from schema import FLIGHT_PRICE_SCHEMA
from tranformers.transform import transform
from tranformers.validations import clean_column_names, validate_required_columns

logger = logging.getLogger(__name__)

CSV_PATH = "/opt/data/Flight_Price_Dataset_of_Bangladesh.csv"
TEMP_DIR = "/tmp/flight_data"
RAW_DATA_PATH = f"{TEMP_DIR}/raw_data.parquet"
TRANSFORMED_DATA_PATH = f"{TEMP_DIR}/transformed_data"


def initialize_spark():
    """Initializes a Spark session."""
    return (
        SparkSession.builder.appName("flight-price-analysis")
        .master("local[*]")
        .config(
            "spark.jars",
            "/opt/jars/mysql-connector-j-9.5.0.jar,/opt/jars/postgresql-42.7.6.jar",
        )
        .getOrCreate()
    )


def extract_and_clean(**context):
    """Extracts data from CSV, cleans it, and saves it as a parquet file."""
    spark = initialize_spark()
    os.makedirs(TEMP_DIR, exist_ok=True)

    df_raw = extract_data(spark, CSV_PATH, FLIGHT_PRICE_SCHEMA)
    df_raw = clean_column_names(df_raw)
    df_raw, is_valid = validate_required_columns(df_raw)

    if not is_valid:
        raise ValueError("Source data validation failed.")

    df_raw.write.mode("overwrite").parquet(RAW_DATA_PATH)


def load_staging(**context):
    """Loads the raw data into MySQL."""
    spark = initialize_spark()
    df_raw = spark.read.parquet(RAW_DATA_PATH)
    load_to_mysql(spark, df_raw)


def transform_data(**context):
    """Transforms the raw data and saves the results as parquet files."""
    spark = initialize_spark()
    df_raw = spark.read.parquet(RAW_DATA_PATH)

    (
        dim_airline,
        dim_route,
        dim_class,
        dim_booking_source,
        fact_flight_prices,
        kpi_fares,
        kpi_seasonal,
        kpi_routes,
        kpi_bookings,
    ) = transform(df_raw)

    tables = {
        "dim_airline": dim_airline,
        "dim_route": dim_route,
        "dim_class": dim_class,
        "dim_booking_source": dim_booking_source,
        "fact_flight_prices": fact_flight_prices,
        "kpi_airline_fares": kpi_fares,
        "kpi_seasonal_variation": kpi_seasonal,
        "kpi_popular_routes": kpi_routes,
        "kpi_airline_bookings": kpi_bookings,
    }

    for name, df in tables.items():
        df.write.mode("overwrite").parquet(f"{TRANSFORMED_DATA_PATH}/{name}.parquet")


def load_all_analytics(**context):
    """Loads a transformed table into PostgreSQL."""
    # table_name = context["params"]["table_name"]
    spark = initialize_spark()

    analytics_tables = [
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

    for table_name in analytics_tables:
        try:
            logger.info(f"Loading table: {table_name}")
            df = spark.read.parquet(f"{TRANSFORMED_DATA_PATH}/{table_name}.parquet")
            load_to_postgres(df, config={"table_name": table_name})
            logger.info(f"Successfully loaded table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to load table {table_name}: {str(e)}")
            raise


def cleanup_temp_files(**context):
    """Removes temporary directories."""
    import shutil

    shutil.rmtree(TEMP_DIR)
