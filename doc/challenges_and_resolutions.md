# Challenges and Resolutions

During the development of this pipeline, I encountered several technical challenges. This document outlines the key issues and their resolutions.

## Challenge 1: Integrating Spark with Airflow in Docker

The primary challenge was enabling Airflow to submit jobs to Spark. Since both were running in separate Docker containers, they needed a way to communicate and share necessary files.

**Resolution**:

1.  **Shared Docker Network**: All services (Airflow, Spark, databases) were placed on the same Docker bridge network (`flight_network`), allowing them to resolve each other by container name.

2.  **Volume Mounts**: I used Docker volumes to mount the Spark job scripts (`/spark`), data (`/data`), and JDBC driver JARs (`/jars`) directly into the Airflow scheduler and webserver containers. This gave Airflow's Python environment access to everything needed to construct and submit a Spark job.

3.  **Local Spark Master**: For this project's scale, I configured Spark to run in `local[*]` mode within the Airflow containers. The Spark connection in Airflow (`AIRFLOW_CONN_SPARK_DEFAULT`) was set to `local[*]`, which simplifies the setup by not requiring a separate Spark cluster.

## Challenge 2: Spark Connecting to Databases

The Spark jobs, running within the Airflow containers, needed to connect to the MySQL and PostgreSQL databases.

**Resolution**:

1.  **JDBC Drivers**: The necessary MySQL and PostgreSQL JDBC `.jar` files were added to the project's `/jars` directory.

2.  **Spark Session Configuration**: When initializing the SparkSession in the `python_callables.py` script, I configured the `spark.jars` property to point to the paths of the mounted JAR files. This makes the drivers available on Spark's classpath, enabling it to establish JDBC connections.

3.  **Database Hostnames**: The JDBC connection strings use the Docker container names (`mysql`, `postgres_analytics`) as hostnames, which is possible because all containers are on the same network.
