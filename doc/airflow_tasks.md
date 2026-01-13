# Airflow DAG and Task Descriptions

This document details the tasks within the `flight_price_etl_pipeline` Airflow DAG.

## DAG: `flight_price_etl_pipeline`

This DAG orchestrates the entire ETL process. It is designed to run once the source CSV data is available.

### Task Descriptions

1.  **`sense_csv_file` (FileSensor)**:
    This task is the entry point of the pipeline. It waits for the source dataset `Flight_Price_Dataset_of_Bangladesh.csv` to become available in the `/opt/data` directory before allowing the DAG to proceed.

2.  **`extract_and_clean` (PythonOperator)**:
    This task initiates a Spark job to read the source CSV. It performs initial validation, cleans column names, and saves the cleaned dataset as an intermediate Parquet file.

3.  **`load_to_mysql_staging` (PythonOperator)**:
    This task loads the cleaned, raw data from the intermediate Parquet file into a staging table in MySQL. This serves as a durable copy of the source-of-truth data for the run.

4.  **`transform_data` (PythonOperator)**:
    Running in parallel with the staging load, this task executes the main transformation logic in Spark. It creates the star schema (dimensions and facts) and computes all business KPIs. The resulting tables are saved as separate Parquet files in a temporary directory.

5.  **`load_analytics_to_postgres` (PythonOperator)**:
    After the transformation is complete, this task iterates through the transformed Parquet files (dimensions, facts, KPIs) and loads each one into its corresponding table in the PostgreSQL analytics database.

6.  **`cleanup_temp_files` (PythonOperator)**:
    This final task ensures that all intermediate Parquet files generated during the run are deleted, freeing up temporary storage space. It is configured to run regardless of the success or failure of upstream tasks.
