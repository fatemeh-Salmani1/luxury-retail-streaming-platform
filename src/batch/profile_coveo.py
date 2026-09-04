from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

BROWSING_PATH = "data/raw/coveo/train/browsing_train.csv"


BROWSING_SCHEMA = StructType(
    [
        StructField("session_id_hash", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("product_action", StringType(), True),
        StructField("product_sku_hash", StringType(), True),
        StructField("server_timestamp_epoch_ms", LongType(), False),
        StructField("hashed_url", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("ProfileCoveoBrowsingData")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    browsing = (
        spark.read.option("header", True).schema(BROWSING_SCHEMA).csv(BROWSING_PATH)
    )

    print("\nDataset schema:")
    browsing.printSchema()

    print("\nDataset metrics:")
    browsing.agg(
        F.count("*").alias("total_events"),
        F.approx_count_distinct("session_id_hash").alias("approximate_sessions"),
        F.approx_count_distinct("product_sku_hash").alias("approximate_products"),
        F.min("server_timestamp_epoch_ms").alias("minimum_timestamp_ms"),
        F.max("server_timestamp_epoch_ms").alias("maximum_timestamp_ms"),
        F.sum(
            F.when(
                F.col("product_action").isNull() | (F.trim("product_action") == ""),
                1,
            ).otherwise(0)
        ).alias("events_without_product_action"),
        F.sum(
            F.when(
                F.col("product_sku_hash").isNull() | (F.trim("product_sku_hash") == ""),
                1,
            ).otherwise(0)
        ).alias("events_without_product"),
    ).show(truncate=False)

    print("\nEvent types:")
    browsing.groupBy("event_type").count().orderBy(F.desc("count")).show(truncate=False)

    print("\nProduct actions:")
    browsing.groupBy("product_action").count().orderBy(F.desc("count")).show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()
