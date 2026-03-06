from pyspark import pipelines as dp
from pyspark import functions as F


@dp.view(
    name = "Trips_silver_staging",
    comment = "Transformed trips data ready for CDC upsert"
    table_properties = {
        
    }
)
@dp.expect("valid_data", "year(business_date) >= 2020")
@dp.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
def trips_silver():
    df_bronze = spark.readStream.table("transportation.bronze.trips")

    df_silver = df_bronze.select(
        F.col("trip_id").alias("id")
        F.col("date").cast("date").alias("date")
        F.col("city_id").alias("city_id")
        F.col("passenger_type").alias("passenger_category")
        F.col("distance_travelled_km").alias("distance_kms")
        F.col("fare_amount").alias("sales_amt")
        F.col("passenger_rating").alias("passenger_rating")
        F.col("driver_rating").alias("driver_rating")
        F.col("ingest_date").alias("bronze_ingest_timestamp")
    )


    df_silver = df_silver.withColumn("silver_processed_timestamp", F.current_timestamp())

    return df_silver

dp.create_streaming_table(
    name = "transportation.silver.trips",
    comment = "Cleaned and Validate others with CDC upsert capabilities",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)

dp.create_auto_cdc_flow(
    target = "transportation.silver.trips",
    source = "Trips_silver_staging",
    keys = ["id"],
    sequence_by = F.col("silver_processed_timestamp"),
    stored_as_scd_type = 1,
    excep_column_list = []
)
