from pyspark.sql.functions import year, month, dayofmonth, col

def add_hive_partitions(df, timestamp_column_name):
    """
    Dynamically adds year, month, and day columns for S3 partitioning
    based on the provided timestamp column.
    """
    return df \
        .withColumn("year", year(col(timestamp_column_name))) \
        .withColumn("month", month(col(timestamp_column_name))) \
        .withColumn("day", dayofmonth(col(timestamp_column_name)))