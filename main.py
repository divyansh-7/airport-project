import os
import sys

if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col

from src.readers.msk_reader import get_kafka_stream
from src.transformers.baggage_joiner import enrich_and_join_telemetry
from src.writers.s3_writer import write_partitioned_stream
from src.transformers.common_transforms import add_hive_partitions

def main():
    spark_version = pyspark.__version__
    
    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    print("Initializing Enterprise Airline Telemetry Pipeline with DLQ...")
    
    spark = SparkSession.builder \
        .appName("RealTimeBaggageTelemetry") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("fs.s3a.committer.name", "directory") 

    s3_base_path = "s3a://airline-telemetry-bronze-941243735155-us-east-1-an/"

    # 1. Read Streams (Unpacking the Good and Bad Data)
    good_bag_df, bad_bag_df = get_kafka_stream(spark, "airline.telemetry.baggage", "BAGGAGE")
    good_pax_df, bad_pax_df = get_kafka_stream(spark, "airline.telemetry.passenger", "PASSENGER")

    # 2. Transform and Join ONLY the Good Data
    final_enriched_df = enrich_and_join_telemetry(good_bag_df, good_pax_df)

    # 3. Apply Hive Partitions (1 line per stream instead of 4!)
    partitioned_df = add_hive_partitions(final_enriched_df, "baggage_time")
    dlq_bag_partitioned = add_hive_partitions(bad_bag_df, "event_timestamp")
    dlq_pax_partitioned = add_hive_partitions(bad_pax_df, "event_timestamp")

    print("Starting Main Stream and DLQ Streams...")

    # The 30 lines of repetitive code are now just 3 clean function calls!
    main_query = write_partitioned_stream(partitioned_df, s3_base_path, "valid_data", "main_query")
    dlq_bag_query = write_partitioned_stream(dlq_bag_partitioned, s3_base_path, "dlq/baggage", "dlq_bag")
    dlq_pax_query = write_partitioned_stream(dlq_pax_partitioned, s3_base_path, "dlq/passenger", "dlq_pax")

    # Monitor all streams
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()