"""Data schema definitions for Flight Price ETL"""

from pyspark.sql.types import (
                               DoubleType,
                               IntegerType,
                               StringType,
                               StructField,
                               StructType,
                               TimestampType,
)

FLIGHT_PRICE_SCHEMA = StructType([
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
