from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import configparser
import boto3
import pandas as pd
# For sum, udf, agg, etc
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
import datetime
import json
from datetime import datetime
import re
import sparknlp


def main():


    # set up sparkSession for structured streams and Spark SQL
    # Just learned we can specify the spark submit command line args in this config here
    # TOPICS ARE CASE SENSITIVE
    topics = "AskReddit, MachineLearning, wallstreetbets"

    spark = SparkSession \
        .builder \
        .master("local[16]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .appName("advanced_analytics_nlp") \
        .getOrCreate()


    dataflow = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.1.1:9092") \
        .option("subscribe", topics) \
        .load()

    ask_reddit_data = dataflow \
        .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)", "CAST(key as STRING)")

    # Transformation and Action on the Discretized Stream
    # Currently writign to console, store window'd results in S3, not Cassandra
    redditDataQuery = ask_reddit_data \
        .select("topic", "timestamp", "value") \
        .withColumnRenamed("value", "Data") \
        .groupBy(F.window("timestamp", "2 seconds"), "topic") \
        .count() \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false")

    # begin the data output
    redditDataQuery \
        .start() \
        .awaitTermination()

    # close connection
    spark \
        .stop()



    return



if __name__ == "__main__":
    main()