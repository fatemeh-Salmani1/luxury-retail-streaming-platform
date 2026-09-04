import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PREPARED_PATH = "data/processed/coveo/prepared_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "coveo-retail-events"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("ReplayCoveoEventsToKafka")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        )
        .getOrCreate()
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay complete Coveo sessions into Kafka."
    )
    parser.add_argument(
        "--sample-modulus",
        type=int,
        default=200,
        help="Select approximately one out of this many sessions.",
    )
    parser.add_argument(
        "--sample-remainder",
        type=int,
        default=0,
        help="Hash remainder identifying the selected sessions.",
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    if arguments.sample_modulus <= 0:
        raise ValueError("--sample-modulus must be greater than zero")

    if not 0 <= arguments.sample_remainder < arguments.sample_modulus:
        raise ValueError(
            "--sample-remainder must be between zero and sample-modulus minus one"
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(PREPARED_PATH)

    selected_events = events.filter(
        F.pmod(
            F.xxhash64("session_id"),
            F.lit(arguments.sample_modulus),
        )
        == arguments.sample_remainder
    ).cache()

    event_count = selected_events.count()
    session_count = selected_events.select("session_id").distinct().count()

    if event_count == 0:
        raise ValueError("The sampling configuration selected no events")

    print(f"\nSelected {event_count} events from {session_count} complete sessions:")

    selected_events.groupBy("event_type").count().orderBy(F.desc("count")).show()

    payload_columns = [
        "source",
        "source_event_id",
        "session_id",
        "event_type",
        "event_timestamp",
        "product_id",
        "category_hash",
        "price_bucket",
        "hashed_url",
        "source_product_action",
    ]

    kafka_records = (
        selected_events.repartition(6, "session_id")
        .sortWithinPartitions("session_id", "event_timestamp")
        .select(
            F.col("session_id").cast("string").alias("key"),
            F.to_json(F.struct(*[F.col(column) for column in payload_columns])).alias(
                "value"
            ),
        )
    )

    (
        kafka_records.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC)
        .save()
    )

    print(
        f"\nPublished {event_count} events "
        f"from {session_count} complete sessions "
        f"to {KAFKA_TOPIC}."
    )

    selected_events.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
