import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Create a schema for incoming resources (from a data sample)

data_sample_dict = {
        "crime_id": "183653746",
        "original_crime_type_name": "Passing Call",
        "report_date": "2018-12-31T00:00:00.000",
        "call_date": "2018-12-31T00:00:00.000",
        "offense_date": "2018-12-31T00:00:00.000",
        "call_time": "23:49",
        "call_date_time": "2018-12-31T23:49:00.000",
        "disposition": "HAN",
        "address": "3300 Block Of 20th Av",
        "city": "San Francisco",
        "state": "CA",
        "agency_id": "1",
        "address_type": "Common Location",
        "common_location": "Stonestown Galleria, Sf"
    }

schema = StructType([
    StructField(key, StringType(), True) for key in data_sample_dict
])

def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "sf.police_call_for_service")\
        .option("startingOffsets", "earliest")\
        .option("maxOffsetsPerTrigger", 200)\
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # select original_crime_type_name and disposition
    type_disposition_table = service_table.select("original_crime_type_name", "disposition") # .distinct()

    # Count the number of original crime type
    agg_df = type_disposition_table.groupBy("original_crime_type_name", "disposition").count()

    # # write output stream
    # query = agg_df.writeStream.format("console").outputMode("complete").start()
    # # attach a ProgressReporter
    # query.awaitTermination()

    radio_code_json_filepath = "../data/radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)

    # rename disposition_code column to disposition that the column names match on radio_code_df and agg_df
    # so we can join on the disposition code
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # Join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df["disposition"]==radio_code_df["disposition"]).\
        select("original_crime_type_name", "description", "count").\
        writeStream.format("console").outputMode("complete").start()
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
