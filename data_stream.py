import logging
import json
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date

# TODO Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType()),
    StructField('original_crime_type_name', StringType()),
    StructField('report_date', StringType()),
    StructField('call_date', StringType()),
    StructField('offense_date', StringType()),
    StructField('call_time', StringType()),
    StructField('call_date_time', StringType()),
    StructField('disposition', StringType()),
    StructField('address', StringType()),
    StructField('city', StringType()),
    StructField('state', StringType()),
    StructField('agency_id', StringType()),
    StructField('address_type', StringType()),
    StructField('common_location', StringType()),
    
])

# TODO create a spark udf to convert time to YYYYmmDDhh format
@psf.udf(StringType())
def udf_convert_time(timestamp):
    return timestamp.strftime('%Y%m%d%H')


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "udacity.sfcrime") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 200) \
    .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")

    service_table.printSchema()

    distinct_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    # TODO use udf to convert timestamp to right format on a call_date_time column
    converted_df = distinct_table.select('original_crime_type_name', udf_convert_time(psf.col('call_datetime')).alias("datime_string"))

    # TODO get different types of original_crime_type_name in 60 minutes interval
    counts_df = distinct_table.withWatermark("call_datetime", "10 minutes")\
        .groupBy('original_crime_type_name', psf.window('call_datetime', '60 minutes')).count()

    

    # TODO apply aggregations using windows function to see how many calls occurred in 2 day span
    calls_per_2_days =  distinct_table.withWatermark("call_datetime", "10 minutes")\
        .groupBy(psf.window('call_datetime', '2 days')).count()
    

    # TODO write output stream
    query = counts_df  \
     .writeStream \
     .outputMode("complete") \
     .format("console") \
     .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()
    query.recentProgress() 


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark =  SparkSession.builder \
     .master("local[4]") \
     .appName("SF Crime") \
     .getOrCreate()

    print('spark', spark)

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()



