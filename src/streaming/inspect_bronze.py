import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_BRONZE_PATH = "data/processed/bronze/retail_events"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect a Bronze Parquet dataset.")
    parser.add_argument(
        "--path",
        default=DEFAULT_BRONZE_PATH,
        help="Path to the Bronze dataset.",
    )
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("InspectLuxuryRetailBronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(arguments.path)

    print("\nBronze schema:")
    bronze.printSchema()

    print("\nBronze metrics:")
    bronze.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("kafka_key").alias("unique_sessions"),
    ).show()

    print("\nOffsets by Kafka partition:")
    bronze.groupBy("kafka_partition").agg(
        F.count("*").alias("event_count"),
        F.min("kafka_offset").alias("first_offset"),
        F.max("kafka_offset").alias("last_offset"),
    ).orderBy("kafka_partition").show()

    spark.stop()


if __name__ == "__main__":
    main()
