from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PREPARED_PATH = "data/processed/coveo/prepared_events"


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("InspectPreparedCoveoEvents")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(PREPARED_PATH)

    print("\nPrepared schema:")
    events.printSchema()

    print("\nDataset quality metrics:")
    events.agg(
        F.count("*").alias("total_events"),
        F.approx_count_distinct("source_event_id").alias("approximate_unique_events"),
        F.approx_count_distinct("session_id").alias("approximate_sessions"),
        F.min("event_timestamp").alias("first_event"),
        F.max("event_timestamp").alias("last_event"),
        F.sum(F.when(F.col("category_hash").isNull(), 1).otherwise(0)).alias(
            "events_without_category"
        ),
        F.sum(F.when(F.col("price_bucket").isNull(), 1).otherwise(0)).alias(
            "events_without_price_bucket"
        ),
    ).show(truncate=False)

    print("\nMapped event types:")
    events.groupBy("event_type").count().orderBy(F.desc("count")).show(truncate=False)

    print("\nSample enriched events:")
    events.select(
        "source_event_id",
        "session_id",
        "event_type",
        "event_timestamp",
        "product_id",
        "category_hash",
        "price_bucket",
    ).show(5, truncate=20)

    spark.stop()


if __name__ == "__main__":
    main()
