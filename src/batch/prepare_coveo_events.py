from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.batch.profile_coveo import BROWSING_SCHEMA

BROWSING_PATH = "data/raw/coveo/train/browsing_train.csv"
CATALOG_PATH = "data/raw/coveo/train/sku_to_content.csv"
OUTPUT_PATH = "data/processed/coveo/prepared_events"


CATALOG_SCHEMA = StructType(
    [
        StructField("product_sku_hash", StringType(), False),
        StructField("description_vector", StringType(), True),
        StructField("category_hash", StringType(), True),
        StructField("image_vector", StringType(), True),
        StructField("price_bucket", DoubleType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("PrepareCoveoRetailEvents")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    browsing = (
        spark.read.option("header", True).schema(BROWSING_SCHEMA).csv(BROWSING_PATH)
    )

    catalog = (
        spark.read.option("header", True)
        .schema(CATALOG_SCHEMA)
        .csv(CATALOG_PATH)
        .select(
            "product_sku_hash",
            "category_hash",
            F.col("price_bucket").cast(IntegerType()).alias("price_bucket"),
        )
        .dropDuplicates(["product_sku_hash"])
    )

    product_events = browsing.filter(
        F.col("product_action").isin(
            "detail",
            "add",
            "remove",
            "purchase",
        )
    )

    mapped_event_type = (
        F.when(F.col("product_action") == "detail", "product_view")
        .when(F.col("product_action") == "add", "add_to_cart")
        .when(F.col("product_action") == "remove", "remove_from_cart")
        .when(F.col("product_action") == "purchase", "purchase")
    )

    enriched_events = (
        product_events.join(
            F.broadcast(catalog),
            on="product_sku_hash",
            how="left",
        )
        .withColumn("event_type_mapped", mapped_event_type)
        .withColumn(
            "event_timestamp",
            F.timestamp_millis("server_timestamp_epoch_ms"),
        )
        .withColumn(
            "source_event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("session_id_hash"),
                    F.col("event_type"),
                    F.col("product_action"),
                    F.col("product_sku_hash"),
                    F.col("server_timestamp_epoch_ms").cast("string"),
                    F.col("hashed_url"),
                ),
                256,
            ),
        )
        .withColumn("event_date", F.to_date("event_timestamp"))
        .select(
            F.lit("coveo_sigir_2021").alias("source"),
            "source_event_id",
            F.col("session_id_hash").alias("session_id"),
            F.col("event_type_mapped").alias("event_type"),
            "event_timestamp",
            F.col("product_sku_hash").alias("product_id"),
            "category_hash",
            "price_bucket",
            "hashed_url",
            F.col("product_action").alias("source_product_action"),
            "event_date",
        )
    )

    (
        enriched_events.repartition(32, "event_date")
        .sortWithinPartitions("session_id", "event_timestamp")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(OUTPUT_PATH)
    )

    print(f"\nPrepared Coveo events written to {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
