# Flight Price Analysis ETL Pipeline

This project implements an Extract, Transform, Load (ETL) pipeline for analyzing flight price data. It uses Apache Airflow for orchestration, Apache Spark for data processing, and stores processed data in MySQL and PostgreSQL databases.

## Features

- **Data Extraction:** Extracts flight price data from CSV files.
- **Data Cleaning & Transformation:** Utilizes Apache Spark to clean, validate, and transform raw flight data into a structured format suitable for analysis.
- **Data Loading:** Loads unprocessed data into a MySQL staging database and a transformed data into PostgreSQL analytics database.
- **Workflow Orchestration:** Manages and schedules the entire ETL process using Apache Airflow.
- **Containerized Environment:** Provides a containerized development and production environment using Docker and Docker Compose.

## Technologies Used

- Apache Airflow
- Apache Spark
- Python
- MySQL
- PostgreSQL
- Docker

## Project Structure

- `airflow/`: Contains Airflow DAGs, logs, and plugins.
- `data/`: Stores raw input data, e.g., `Flight_Price_Dataset_of_Bangladesh.csv`.
- `etl/`: Custom ETL scripts (if any, though Spark handles much of this).
- `jars/`: JDBC connectors for Spark to connect with databases.
- `spark/`: Spark scripts for extraction, transformation, and loading.
- `sql/`: SQL scripts for database initialization or schema setup.
- `Dockerfile`: Defines the Docker image for the project.
- `docker-compose.yml`: Orchestrates multi-container Docker applications (Airflow, Spark, Databases).
- `requirements.txt`: Project dependencies.

## Setup

To set up and run this project, ensure you have Docker and Docker Compose installed on your system.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/manaf-dev/flight-price-analysis.git
    cd flight-price-analysis
    ```

2.  **Build and run the Docker containers:**
    ```bash
    docker-compose up --build -d
    ```
    This command will build the necessary Docker images and start services for Airflow, Spark, MySQL, and PostgreSQL in detached mode.


## Usage

1.  **Access Airflow UI:**
    Once the Docker containers are running, you can access the Airflow UI, typically at `http://localhost:8080`.

2.  **Trigger the ETL DAG:**
    In the Airflow UI, navigate to the `flight_price_etl_pipeline` DAG. Unpause it and trigger a new run.

3.  **Monitor Progress:**
    Monitor the progress of the DAG run in the Airflow UI. Logs for each task will be available.

4.  **Verify Data:**
    After a successful DAG run, connect to your MySQL and PostgreSQL databases to verify that the data has been loaded correctly into the respective staging and analytics tables.

