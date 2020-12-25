import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Create a schema for incoming resources (from a data sample)
SCHEMA = StructType([
    # StructField(key, StringType(), True) for key in data_sample_dict
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "sf.police_call_for_service")\
        .option("startingOffsets", "earliest")\
        .option("fetchOffset.numRetries", 2)\
        .option("fetchOffset.retryIntervalMs", 600)\
        .option("maxOffsetsPerTrigger", 600)\
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), SCHEMA).alias("DF"))\
        .select("DF.*")

    # select original_crime_type_name and disposition
    type_disposition_table = service_table.select("original_crime_type_name", "disposition", psf.to_timestamp("call_date_time").alias("call_date_time"))

    # Count the number of original crime type
    window_size_in_minutes = 60
    delay_in_minutes = 10
    agg_df = type_disposition_table\
        .withWatermark("call_date_time", f"{window_size_in_minutes+delay_in_minutes} minutes")\
        .groupBy(psf.window(type_disposition_table.call_date_time, f"{window_size_in_minutes} minutes"), "original_crime_type_name", "disposition")\
        .count()

    # # write output stream
    # query = agg_df.writeStream.format("console").trigger(processingTime='5 seconds').outputMode("complete").start()
    # # attach a ProgressReporter
    # query.awaitTermination()

    radio_code_json_filepath = "../data/radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)

    # rename disposition_code column to disposition that the column names match on radio_code_df and agg_df
    # so we can join on the disposition code
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # Join on disposition column
    join_query = agg_df.join(radio_code_df.alias("radio"), agg_df["disposition"]==radio_code_df["disposition"])\
        .select("window", "original_crime_type_name", "description", "count", "radio.disposition")\
        .orderBy(psf.col("count").desc())\
        .writeStream.format("console").trigger(processingTime='5 seconds').outputMode("complete").start()
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
