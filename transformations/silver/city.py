from pyspark import pipelines as dp
from pyspark.sql import functions as F



@dp.materialized_view(
    name = "transportation.silver.city",
    comment = "Cleaned and Standardized City Data",
    table_properties={
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)
def city_silver():
    df_bronze = spark.read.table("transportation.bronze.city")

    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingest_date").alias("bronze_ingestion_datetime")
    )

    df_silver = df_silver.withColumn("silve_ingestion_datetime", F.current_timestamp())

    return df_silver
