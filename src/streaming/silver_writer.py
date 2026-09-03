from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, length, to_date
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.kafka_stream import EVENT_SCHEMA

BRONZE_PATH = "data/processed/bronze/retail_events"
SILVER_PATH = "data/processed/silver/retail_events"
CHECKPOINT_PATH = "checkpoints/silver-retail-events"


BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType(), True),
        StructField("raw_json", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingested_at", TimestampType(), False),
        StructField("ingestion_date", DateType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("LuxuryRetailSilverWriter")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
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
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            from_json("raw_json", EVENT_SCHEMA).alias("event"),
        )
        .filter(col("event").isNotNull())
        .select(
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "event.*",
        )
    )

    valid_events = parsed_events.filter(
        col("event_id").isNotNull()
        & col("event_type").isin(
            "product_view",
            "add_to_cart",
            "remove_from_cart",
            "purchase",
        )
        & col("event_timestamp").isNotNull()
        & col("session_id").isNotNull()
        & (col("kafka_key") == col("session_id"))
        & col("product_id").isNotNull()
        & (col("unit_price") > 0)
        & (col("quantity") > 0)
        & (length("country") == 2)
    )

    silver_events = (
        valid_events.withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id"])
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

    print(f"Writing validated events to {SILVER_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
