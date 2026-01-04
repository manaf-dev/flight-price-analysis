import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCHEMA = StructType([
    StructField('Airline', StringType(), True), 
    StructField('Source', StringType(), True), 
    StructField('Source Name', StringType(), True), 
    StructField('Destination', StringType(), True), 
    StructField('Destination Name', StringType(), True), 
    StructField('Departure Date & Time', TimestampType(), True), 
    StructField('Arrival Date & Time', TimestampType(), True), 
    StructField('Duration (hrs)', DoubleType(), True), 
    StructField('Stopovers', StringType(), True), 
    StructField('Aircraft Type', StringType(), True), 
    StructField('Class', StringType(), True), 
    StructField('Booking Source', StringType(), True), 
    StructField('Base Fare (BDT)', DoubleType(), True), 
    StructField('Tax & Surcharge (BDT)', DoubleType(), True), 
    StructField('Total Fare (BDT)', DoubleType(), True), 
    StructField('Seasonality', StringType(), True), 
    StructField('Days Before Departure', IntegerType(), True)
])


def initialize_spark():
    spark = SparkSession.builder \
        .appName('flight-price-analysis') \
        .config("spark.jars", "/opt/jars/mysql-connector-j-9.5.0.jar") \
        .master('local[*]') \
        .getOrCreate()
    logger.info("Spark session initialized")
    return spark

    

def extract_data(spark, file_path):
    logger.info(f"Extracting data from {file_path}")
    try:
        df = spark.read.csv(file_path, header=True, schema=SCHEMA)
        logger.info(f"Extracted {df.count()} rows")
        return df
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise



def transform(spark, df):
    logger.info("Starting data transformation")
    
    # clean column names
    for old_col in df.columns:
        new_col = old_col.strip() \
            .replace(' ', '_') \
            .replace('(', '') \
            .replace(')', '') \
            .replace('&', 'and') \
            .lower()
        
        df = df.withColumnRenamed(old_col, new_col)

    # handle missing and null values
    df = df.na.drop()

    # parse timestamp columns
    df = df.withColumn('departure_datetime', to_timestamp(col('departure_date_and_time'), 'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('arrival_datetime', to_timestamp(col('arrival_date_and_time'), 'yyyy-MM-dd HH:mm:ss'))

    # validate fare values
    df = df.filter(col('base_fare_bdt') >= 0)
    df = df.filter(col('tax_and_surcharge_bdt') >= 0)
    df = df.filter(col('total_fare_bdt') >= 0)

    # validate non-empty strings
    df = df.filter(col('airline').isNotNull())
    df = df.filter(col('source').isNotNull())
    df = df.filter(col('source_name').isNotNull())
    df = df.filter(col('destination').isNotNull())

    # drop duplicates
    df = df.dropDuplicates(['airline', 'source', 'destination', 'departure_datetime', 'base_fare_bdt'])

    logger.info(f"Transformation complete. Rows after transformation: {df.count()}")
    return df


def load_to_mysql(spark, df):
    """Load DataFrame to MySQL database"""
    try:
        logger.info("Starting MySQL load operation")
        
        # Use the container name from docker-compose network
        mysql_host = 'mysql'  # Docker container name accessible via service name
        mysql_port = '3306'
        mysql_database = 'staging_db'
        mysql_user = 'staging_user'
        mysql_password = 'staging_password'
        mysql_table = 'staging_prices'
        
        # Build JDBC URL
        jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}?allowPublicKeyRetrieval=true&useSSL=false"
        
        
        logger.info(f"Connecting to MySQL at {mysql_host}:{mysql_port}/{mysql_database}")
        logger.info(f"Writing to table: {mysql_table}")
        
        # Write DataFrame to MySQL
        df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", jdbc_url) \
            .option("dbtable", mysql_table) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("batchsize", "10000") \
            .save()
        
        logger.info(f"Successfully loaded {df.count()} rows to MySQL table: {mysql_table}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading data to MySQL: {str(e)}")
        raise





def main():
    csv_path = '/opt/data/Flight_Price_Dataset_of_Bangladesh.csv'
    try:
        logger.info("Starting flight price ETL pipeline")
        spark = initialize_spark()
        
        df = extract_data(spark, csv_path)
        df = transform(spark, df)
        load_to_mysql(spark, df)
        
        logger.info("ETL pipeline completed successfully")
        spark.stop()
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise


if __name__ == '__main__':
    main()
    
