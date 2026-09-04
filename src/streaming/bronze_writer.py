import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "luxury-retail-events"
DEFAULT_OUTPUT_PATH = "data/processed/bronze/retail_events"
DEFAULT_CHECKPOINT_PATH = "checkpoints/bronze-retail-events"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist raw Kafka events to a Bronze Parquet layer."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
    )
    parser.add_argument(
        "--checkpoint-path",
        default=DEFAULT_CHECKPOINT_PATH,
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("LuxuryRetailBronzeWriter")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        )
        .getOrCreate()
    )


def main() -> None:
    arguments = parse_arguments()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            arguments.bootstrap_servers,
        )
        .option("subscribe", arguments.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_events = kafka_stream.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    ).withColumn(
        "ingestion_date",
        to_date("ingested_at"),
    )

    query = (
        bronze_events.writeStream.format("parquet")
        .outputMode("append")
        .option("path", arguments.output_path)
        .option(
            "checkpointLocation",
            arguments.checkpoint_path,
        )
        .partitionBy("ingestion_date")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Streaming {arguments.topic} to {arguments.output_path}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
