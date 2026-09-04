from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "data/processed/silver/coveo_events"


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("InspectCoveoSilver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER_PATH)

    print("\nSilver quality metrics:")
    silver.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("source_event_id").alias("unique_events"),
        F.countDistinct("session_id").alias("unique_sessions"),
        F.sum(F.when(F.col("category_hash").isNull(), 1).otherwise(0)).alias(
            "missing_categories"
        ),
        F.sum(F.when(F.col("price_bucket").isNull(), 1).otherwise(0)).alias(
            "missing_price_buckets"
        ),
    ).show()

    print("\nEvents by type:")
    silver.groupBy("event_type").count().orderBy(F.desc("count")).show()

    print("\nKafka partitions:")
    silver.groupBy("kafka_partition").agg(
        F.count("*").alias("event_count"),
        F.min("kafka_offset").alias("first_offset"),
        F.max("kafka_offset").alias("last_offset"),
    ).orderBy("kafka_partition").show()

    spark.stop()


if __name__ == "__main__":
    main()
