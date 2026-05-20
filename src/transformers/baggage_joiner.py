from pyspark.sql.functions import col, expr

def enrich_and_join_telemetry(baggage_df, passenger_df):
    """Applies watermarks, deduplicates, and joins the two B2B telemetry streams."""
    
    # 1. Clean Baggage Stream
    clean_baggage = baggage_df \
        .withWatermark("event_timestamp", "2 hours") \
        .dropDuplicates(["event_id", "event_timestamp"]) \
        .withColumnRenamed("event_timestamp", "baggage_time")

    # 2. Clean Passenger Stream
    clean_passenger = passenger_df \
        .withWatermark("event_timestamp", "2 hours") \
        .dropDuplicates(["event_id", "event_timestamp"]) \
        .withColumnRenamed("event_timestamp", "passenger_time")

    # 3. Stateful Time-Window Join
    joined_df = clean_baggage.alias("bag").join(
        clean_passenger.alias("pax"),
        expr("""
            bag.pnr_locator = pax.pnr_locator AND
            bag.flight_number = pax.flight_number AND
            bag.baggage_time >= pax.passenger_time - interval 3 hours AND
            bag.baggage_time <= pax.passenger_time + interval 1 hour
        """),
        "inner"
    )

    # 4. Final Projection
    return joined_df.select(
        col("pax.pnr_locator"),
        col("pax.flight_number"),
        col("bag.bag_tag_number"),
        col("bag.location_id").alias("baggage_location"),
        col("pax.gate_id").alias("passenger_gate"),
        col("bag.baggage_time"),
        col("pax.passenger_time")
    )