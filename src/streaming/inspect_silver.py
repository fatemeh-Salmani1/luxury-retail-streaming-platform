from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "data/processed/silver/retail_events"


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("InspectLuxuryRetailSilver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER_PATH)

    metrics = silver.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_id").alias("unique_events"),
        F.countDistinct("session_id").alias("unique_sessions"),
    )

    print("\nSilver metrics:")
    metrics.show()

    print("\nEvents by type:")
    silver.groupBy("event_type").count().orderBy("event_type").show()

    print("\nEvents by country:")
    silver.groupBy("country").count().orderBy(
        F.desc("count"),
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()
