import pytest

from src.batch.coveo_gold_builder import (
    build_category_metrics,
    build_funnel,
)


def test_funnel_metrics(spark) -> None:
    events = spark.createDataFrame(
        [
            ("s1", "p1", "cat1", "product_view"),
            ("s1", "p1", "cat1", "add_to_cart"),
            ("s1", "p1", "cat1", "purchase"),
            ("s2", "p2", "cat2", "product_view"),
            ("s3", "p3", "cat3", "product_view"),
            ("s3", "p3", "cat3", "add_to_cart"),
        ],
        [
            "session_id",
            "product_id",
            "category_hash",
            "event_type",
        ],
    )

    result = build_funnel(events).first()

    assert result.total_events == 6
    assert result.total_sessions == 3
    assert result.total_products == 3
    assert result.cart_sessions == 2
    assert result.purchase_sessions == 1
    assert result.product_views == 3
    assert result.cart_additions == 2
    assert result.cart_removals == 0
    assert result.purchases == 1
    assert result.view_to_cart_rate == pytest.approx(66.67)
    assert result.purchase_conversion_rate == pytest.approx(33.33)
    assert result.cart_to_purchase_rate == pytest.approx(50.0)


def test_zero_view_category_returns_null_rate(spark) -> None:
    events = spark.createDataFrame(
        [
            ("s1", "p1", "cat1", "product_view"),
            ("s2", "p2", None, "purchase"),
        ],
        [
            "session_id",
            "product_id",
            "category_hash",
            "event_type",
        ],
    )

    results = {row.category: row for row in build_category_metrics(events).collect()}

    unknown = results["unknown"]

    assert unknown.product_views == 0
    assert unknown.purchases == 1
    assert unknown.purchase_per_view_rate is None
