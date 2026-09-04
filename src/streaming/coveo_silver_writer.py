from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.silver_writer import BRONZE_SCHEMA

BRONZE_PATH = "data/processed/bronze/coveo_events"
SILVER_PATH = "data/processed/silver/coveo_events"
CHECKPOINT_PATH = "checkpoints/silver-coveo-events"


COVEO_EVENT_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("source_event_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("product_id", StringType(), False),
        StructField("category_hash", StringType(), True),
        StructField("price_bucket", IntegerType(), True),
        StructField("hashed_url", StringType(), True),
        StructField("source_product_action", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("CoveoRetailSilverWriter")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_stream = (
        spark.readStream.schema(BRONZE_SCHEMA).format("parquet").load(BRONZE_PATH)
    )

    parsed_events = (
        bronze_stream.select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            from_json("raw_json", COVEO_EVENT_SCHEMA).alias("event"),
        )
        .filter(col("event").isNotNull())
        .select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "event.*",
        )
    )

    valid_events = parsed_events.filter(
        (col("source") == "coveo_sigir_2021")
        & col("source_event_id").isNotNull()
        & col("session_id").isNotNull()
        & (col("kafka_key") == col("session_id"))
        & col("event_type").isin(
            "product_view",
            "add_to_cart",
            "remove_from_cart",
            "purchase",
        )
        & col("event_timestamp").isNotNull()
        & col("product_id").isNotNull()
        & (col("price_bucket").isNull() | col("price_bucket").between(1, 10))
    )

    silver_events = (
        valid_events.withWatermark("event_timestamp", "30 minutes")
        .dropDuplicatesWithinWatermark(["source_event_id"])
        .withColumn("event_date", to_date("event_timestamp"))
    )

    query = (
        silver_events.writeStream.format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Writing real validated events to {SILVER_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
