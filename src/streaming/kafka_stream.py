from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "luxury-retail-events"
CHECKPOINT_LOCATION = "checkpoints/kafka-console-preview"


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("unit_price", DoubleType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("device", StringType(), nullable=False),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("LuxuryRetailKafkaStream")
        .config("spark.sql.shuffle.partitions", "3")
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

    parsed_events = (
        kafka_stream.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_value"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "kafka_key",
            "partition",
            "offset",
            "kafka_timestamp",
            from_json("json_value", EVENT_SCHEMA).alias("event"),
        )
        .filter(col("event").isNotNull())
        .select(
            "partition",
            "offset",
            "kafka_key",
            "event.event_timestamp",
            "event.event_type",
            "event.session_id",
            "event.product_id",
            "event.category",
            "event.unit_price",
            "event.country",
        )
    )

    query = (
        parsed_events.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
