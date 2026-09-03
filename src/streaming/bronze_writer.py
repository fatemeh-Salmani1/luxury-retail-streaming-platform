from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "luxury-retail-events"
BRONZE_PATH = "data/processed/bronze/retail_events"
CHECKPOINT_PATH = "checkpoints/bronze-retail-events"


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
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
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
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingestion_date")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Writing raw Kafka events to {BRONZE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
