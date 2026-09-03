from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "data/processed/silver/retail_events"
GOLD_PATH = "data/processed/gold"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("LuxuryRetailGoldBuilder")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_funnel_metrics(events):
    return events.agg(
        F.countDistinct("session_id").alias("total_sessions"),
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
        F.round(
            F.countDistinct(
                F.when(
                    F.col("event_type") == "add_to_cart",
                    F.col("session_id"),
                )
            )
            / F.countDistinct("session_id")
            * 100,
            2,
        ).alias("view_to_cart_rate"),
        F.round(
            F.countDistinct(
                F.when(
                    F.col("event_type") == "purchase",
                    F.col("session_id"),
                )
            )
            / F.countDistinct("session_id")
            * 100,
            2,
        ).alias("purchase_conversion_rate"),
        F.round(
            F.sum(
                F.when(
                    F.col("event_type") == "purchase",
                    F.col("unit_price") * F.col("quantity"),
                ).otherwise(0)
            ),
            2,
        ).alias("total_revenue"),
    )


def build_product_metrics(events):
    return (
        events.groupBy(
            "product_id",
            "product_name",
            "category",
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias(
                "product_views"
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "cart_additions"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
            F.round(
                F.sum(
                    F.when(
                        F.col("event_type") == "purchase",
                        F.col("unit_price") * F.col("quantity"),
                    ).otherwise(0)
                ),
                2,
            ).alias("revenue"),
        )
        .orderBy(F.desc("revenue"))
    )


def build_country_metrics(events):
    return (
        events.groupBy("country")
        .agg(
            F.countDistinct("session_id").alias("sessions"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
            F.round(
                F.sum(
                    F.when(
                        F.col("event_type") == "purchase",
                        F.col("unit_price") * F.col("quantity"),
                    ).otherwise(0)
                ),
                2,
            ).alias("revenue"),
        )
        .orderBy(F.desc("revenue"))
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_events = spark.read.parquet(SILVER_PATH)

    funnel_metrics = build_funnel_metrics(silver_events)
    product_metrics = build_product_metrics(silver_events)
    country_metrics = build_country_metrics(silver_events)

    funnel_metrics.write.mode("overwrite").parquet(f"{GOLD_PATH}/funnel_metrics")
    product_metrics.write.mode("overwrite").parquet(f"{GOLD_PATH}/product_metrics")
    country_metrics.write.mode("overwrite").parquet(f"{GOLD_PATH}/country_metrics")

    print("\nCustomer funnel:")
    funnel_metrics.show(truncate=False)

    print("\nProduct performance:")
    product_metrics.show(truncate=False)

    print("\nCountry performance:")
    country_metrics.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
