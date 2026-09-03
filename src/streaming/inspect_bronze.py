from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "data/processed/bronze/retail_events"


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("InspectLuxuryRetailBronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(BRONZE_PATH)

    print("\nBronze schema:")
    bronze.printSchema()

    print(f"\nTotal Bronze events: {bronze.count()}")

    print("\nOffsets by Kafka partition:")
    bronze.groupBy("kafka_partition").agg(
        F.count("*").alias("event_count"),
        F.min("kafka_offset").alias("first_offset"),
        F.max("kafka_offset").alias("last_offset"),
    ).orderBy("kafka_partition").show()

    print("\nSample records:")
    bronze.select(
        "kafka_partition",
        "kafka_offset",
        "kafka_key",
        "kafka_timestamp",
        "ingestion_date",
    ).orderBy(
        "kafka_partition",
        "kafka_offset",
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
