from pyspark.sql.functions import col, from_json, length, trim, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def get_kafka_stream(spark, topic_name, schema_type):
    """Reads a stream from Kafka, applies schema, and splits Good Data from DLQ Data."""
    
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Define schemas dynamically 
    if schema_type == "BAGGAGE":
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("airline_code", StringType(), True),
            StructField("bag_tag_number", StringType(), True),
            StructField("pnr_locator", StringType(), True),
            StructField("flight_number", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("location_id", StringType(), True)
        ])
    elif schema_type == "PASSENGER":
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("airline_code", StringType(), True),
            StructField("pnr_locator", StringType(), True),
            StructField("flight_number", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("gate_id", StringType(), True)
        ])

    # Parse JSON
    parsed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*") \
        .withColumn("pnr_locator", trim(col("pnr_locator")))

    # 1. Define the Data Contract
    valid_contract = (
        col("pnr_locator").isNotNull() & 
        (col("pnr_locator") != "") & 
        (col("pnr_locator") != "unknown") &
        (length(col("pnr_locator")) == 6)
    )

    # 2. The Golden Path (Passes the contract)
    good_df = parsed_df.filter(valid_contract)

    # 3. The DLQ Path (Fails the contract - Note the ~ inverse operator)
    # We inject a hardcoded column so the analysts know WHY it's in the DLQ
    bad_df = parsed_df.filter(~valid_contract) \
        .withColumn("error_reason", lit("FAILED_PNR_CONTRACT"))

    # Return BOTH streams
    return good_df, bad_df