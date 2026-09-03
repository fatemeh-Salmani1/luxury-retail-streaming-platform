from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.producer.event_model import RetailEvent


def test_valid_retail_event() -> None:
    event = RetailEvent(
        event_type="add_to_cart",
        event_timestamp=datetime.now(UTC),
        customer_id="customer_1042",
        session_id="session_392",
        product_id="product_205",
        product_name="Camellia Eau de Parfum",
        category="fragrance",
        unit_price=145.00,
        quantity=1,
        country="FR",
        device="mobile",
    )

    assert event.event_type == "add_to_cart"
    assert event.unit_price == 145.00
    assert event.country == "FR"
    assert event.event_id is not None


def test_negative_price_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RetailEvent(
            event_type="product_view",
            event_timestamp=datetime.now(UTC),
            customer_id="customer_1042",
            session_id="session_392",
            product_id="product_205",
            product_name="Camellia Eau de Parfum",
            category="fragrance",
            unit_price=-145.00,
            quantity=1,
            country="FR",
            device="desktop",
        )


def test_unknown_event_type_is_rejected() -> None:
    with pytest.raises(ValidationError):
        RetailEvent(
            event_type="liked_product",
            event_timestamp=datetime.now(UTC),
            customer_id="customer_1042",
            session_id="session_392",
            product_id="product_205",
            product_name="Camellia Eau de Parfum",
            category="fragrance",
            unit_price=145.00,
            quantity=1,
            country="FR",
            device="tablet",
        )
