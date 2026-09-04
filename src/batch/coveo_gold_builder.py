from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "data/processed/silver/coveo_events"
GOLD_PATH = "data/processed/gold/coveo"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("BuildCoveoGoldTables")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def event_count(event_type: str):
    return F.sum(
        F.when(
            F.col("event_type") == event_type,
            1,
        ).otherwise(0)
    )


def build_funnel(events):
    funnel = events.agg(
        F.count("*").alias("total_events"),
        F.countDistinct("session_id").alias("total_sessions"),
        F.countDistinct("product_id").alias("total_products"),
        F.countDistinct(
            F.when(
                F.col("event_type") == "add_to_cart",
                F.col("session_id"),
            )
        ).alias("cart_sessions"),
        F.countDistinct(
            F.when(
                F.col("event_type") == "purchase",
                F.col("session_id"),
            )
        ).alias("purchase_sessions"),
        event_count("product_view").alias("product_views"),
        event_count("add_to_cart").alias("cart_additions"),
        event_count("remove_from_cart").alias("cart_removals"),
        event_count("purchase").alias("purchases"),
    )

    return (
        funnel.withColumn(
            "view_to_cart_rate",
            F.round(
                F.try_divide(
                    F.col("cart_sessions"),
                    F.col("total_sessions"),
                )
                * 100,
                2,
            ),
        )
        .withColumn(
            "purchase_conversion_rate",
            F.round(
                F.try_divide(
                    F.col("purchase_sessions"),
                    F.col("total_sessions"),
                )
                * 100,
                2,
            ),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.round(
                F.try_divide(
                    F.col("purchase_sessions"),
                    F.col("cart_sessions"),
                )
                * 100,
                2,
            ),
        )
    )


def build_category_metrics(events):
    return (
        events.withColumn(
            "category",
            F.coalesce(
                F.col("category_hash"),
                F.lit("unknown"),
            ),
        )
        .groupBy("category")
        .agg(
            F.countDistinct("session_id").alias("sessions"),
            F.countDistinct("product_id").alias("products"),
            event_count("product_view").alias("product_views"),
            event_count("add_to_cart").alias("cart_additions"),
            event_count("purchase").alias("purchases"),
        )
        .withColumn(
            "purchase_per_view_rate",
            F.round(
                F.try_divide(
                    F.col("purchases"),
                    F.col("product_views"),
                )
                * 100,
                2,
            ),
        )
        .orderBy(F.desc("purchases"))
    )


def build_price_bucket_metrics(events):
    return (
        events.withColumn(
            "price_segment",
            F.coalesce(
                F.col("price_bucket").cast("string"),
                F.lit("unknown"),
            ),
        )
        .groupBy("price_segment")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("session_id").alias("sessions"),
            event_count("product_view").alias("product_views"),
            event_count("add_to_cart").alias("cart_additions"),
            event_count("purchase").alias("purchases"),
        )
        .withColumn(
            "sort_order",
            F.when(
                F.col("price_segment") == "unknown",
                999,
            ).otherwise(F.col("price_segment").cast("integer")),
        )
        .orderBy("sort_order")
        .drop("sort_order")
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(SILVER_PATH)

    funnel = build_funnel(silver)
    categories = build_category_metrics(silver)
    price_buckets = build_price_bucket_metrics(silver)

    funnel.write.mode("overwrite").parquet(f"{GOLD_PATH}/funnel")
    categories.write.mode("overwrite").parquet(f"{GOLD_PATH}/categories")
    price_buckets.write.mode("overwrite").parquet(f"{GOLD_PATH}/price_buckets")

    print("\nReal customer funnel:")
    funnel.show(truncate=False)

    print("\nTop ten categories by purchases:")
    categories.show(10, truncate=20)

    print("\nPerformance by anonymized price bucket:")
    price_buckets.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
