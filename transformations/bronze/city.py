from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# configuration
SOURCE_PATH = "s3://amzn-s3-nedio-transportation/nedio-data-store/city"



@dp.materialized_view(
    name = "transportation.bronze.city",
    comment = "City Raw data processing",
    table_properties={
        "quality":"bronze",
        "layer":"bronze",
        "format":"csv",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)

def city_bronze():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("mergeSchema", "true")
        .option("columnNameOfCorruptRecord", "corrupt_record")
        .load(SOURCE_PATH)
    )
    
    df = (
        df.withColumn("file_name", col("_metadata.file_name"))
          .withColumn("ingest_date", current_timestamp())
    )

    return df



   