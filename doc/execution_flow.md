# Pipeline Architecture and Execution Flow

This document outlines the architecture of the flight price analysis ETL pipeline.

## System Architecture

The pipeline is orchestrated using Apache Airflow and processes data using Apache Spark. The entire environment is containerized with Docker, ensuring portability and consistent execution.

The key components are:
- **Apache Airflow**: Serves as the orchestrator, managing the sequence of tasks, dependencies, and retries.
- **Apache Spark**: Acts as the processing engine for all data extraction, transformation, and loading (ETL) tasks.
- **MySQL**: A relational database used as a staging area for raw, validated data.
- **PostgreSQL**: A relational database serving as the analytical data warehouse for storing transformed dimensions, facts, and KPIs.

## Execution Flow

1.  **Data Ingestion**: The pipeline begins by sensing a CSV file in the designated data directory.
2.  **Extraction and Validation**: Once the file is present, a Spark job is triggered. It extracts the data, validates its schema, cleans column names, and saves the result as a Parquet file in a temporary location.
3.  **Staging Load**: The validated raw data is loaded into a `raw_prices` table in the MySQL staging database. This provides a checkpoint for the raw data.
4.  **Transformation**: In parallel with the staging load, another Spark job transforms the raw data. This involves data cleaning, creating a star schema (dimension and fact tables), and computing key performance indicators (KPIs).
5.  **Analytics Load**: The transformed tables (dimensions, facts, and KPIs) are loaded into the PostgreSQL analytics database.
6.  **Cleanup**: Finally, the temporary Parquet files created during the process are removed.
