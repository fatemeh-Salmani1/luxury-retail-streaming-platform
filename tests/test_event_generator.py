import random

from src.producer.event_generator import generate_customer_journey


def test_journey_starts_with_product_view() -> None:
    events = generate_customer_journey(random.Random(42))

    assert events[0].event_type == "product_view"


def test_events_share_session_information() -> None:
    events = generate_customer_journey(random.Random(42))

    assert len({event.customer_id for event in events}) == 1
    assert len({event.session_id for event in events}) == 1
    assert len({event.country for event in events}) == 1
    assert len({event.device for event in events}) == 1


def test_events_are_chronological() -> None:
    events = generate_customer_journey(random.Random(42))
    timestamps = [event.event_timestamp for event in events]

    assert timestamps == sorted(timestamps)


def test_purchase_only_occurs_after_add_to_cart() -> None:
    for seed in range(100):
        events = generate_customer_journey(random.Random(seed))
        event_types = [event.event_type for event in events]

        if "purchase" in event_types:
            assert "add_to_cart" in event_types
            assert event_types.index("add_to_cart") < event_types.index("purchase")
