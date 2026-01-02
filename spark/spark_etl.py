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
    spark = SparkSession.builder.appName('flight-price-analysis').getOrCreate()
    return spark
    

def extract_data(spark, file_path):
    return spark.read.csv(file_path, header=True, schema=SCHEMA)


def transform(spark, df):
    # clean column names
    for old_col in df.columns:
        new_col = old_col.strip() \
            .replace(' ', '_') \
            .replace('(', '_') \
            .replace(')', '_') \
            .replace('&', 'and') \
            .lower()
        
        df = df.withColumnRenamed(old_col, new_col)

    # hanle missing and null values
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

    return df





def main():
    csv_path = './data/Flight_Price_Dataset_of_Bangladesh.csv'
    spark = initialize_spark()
    df = extract_data(spark, csv_path)
    df = transform(spark, df)
