from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_PATH="s3://amzn-s3-nedio-transportation/nedio-data-store/trips"

@dp.table(
    name = "transportation.bronze.trips",
    comment = "Trips Raw data processing",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "format":"csv",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)
def orders_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode","rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
        )
    

    # Rename la colonne de la problématique.

    df = (
        df.withColumnRenamed("distance_travelled(km)", "distance_travelled_km")
    )


    df = df.withColumn("file_name", F.col("_metadata.file_name")).withColumn("ingest_date", F.current_timestamp())

    return df



