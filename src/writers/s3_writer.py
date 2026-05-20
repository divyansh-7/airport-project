def write_partitioned_stream(df, base_s3_path, target_folder, checkpoint_name):
    """
    A reusable function to write any partitioned PySpark streaming DataFrame to S3.
    """
    full_s3_path = f"{base_s3_path}{target_folder}/"
    checkpoint_path = f"./spark_checkpoints_aws/{checkpoint_name}"

    return df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", full_s3_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="60 seconds") \
        .start()