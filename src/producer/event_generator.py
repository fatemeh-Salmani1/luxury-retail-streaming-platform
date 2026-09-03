import random
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from src.producer.event_model import RetailEvent

PRODUCTS: list[dict[str, Any]] = [
    {
        "product_id": "FRG-001",
        "product_name": "Camellia Eau de Parfum",
        "category": "fragrance",
        "unit_price": 145.00,
    },
    {
        "product_id": "FRG-002",
        "product_name": "Midnight Iris Parfum",
        "category": "fragrance",
        "unit_price": 185.00,
    },
    {
        "product_id": "SKN-001",
        "product_name": "Radiance Face Serum",
        "category": "skincare",
        "unit_price": 120.00,
    },
    {
        "product_id": "SKN-002",
        "product_name": "Hydrating Camellia Cream",
        "category": "skincare",
        "unit_price": 95.00,
    },
    {
        "product_id": "BAG-001",
        "product_name": "Classic Quilted Handbag",
        "category": "handbag",
        "unit_price": 4250.00,
    },
    {
        "product_id": "JWL-001",
        "product_name": "Celestial Gold Bracelet",
        "category": "jewellery",
        "unit_price": 2800.00,
    },
    {
        "product_id": "ACC-001",
        "product_name": "Silk Garden Scarf",
        "category": "accessory",
        "unit_price": 420.00,
    },
]

COUNTRIES = ["FR", "DE", "IT", "ES", "GB", "US", "CH"]
DEVICES = ["mobile", "desktop", "tablet"]


def generate_customer_journey(
    rng: random.Random | None = None,
) -> list[RetailEvent]:
    rng = rng or random.Random()

    customer_id = f"customer_{uuid4().hex[:8]}"
    session_id = f"session_{uuid4().hex[:8]}"
    country = rng.choice(COUNTRIES)
    device = rng.choice(DEVICES)

    timestamp = datetime.now(UTC)
    events: list[RetailEvent] = []

    viewed_products = rng.sample(PRODUCTS, k=rng.randint(1, 4))

    for product in viewed_products:
        events.append(
            RetailEvent(
                event_type="product_view",
                event_timestamp=timestamp,
                customer_id=customer_id,
                session_id=session_id,
                country=country,
                device=device,
                **product,
            )
        )
        timestamp += timedelta(seconds=rng.randint(2, 20))

    selected_product = viewed_products[-1]

    if rng.random() < 0.65:
        events.append(
            RetailEvent(
                event_type="add_to_cart",
                event_timestamp=timestamp,
                customer_id=customer_id,
                session_id=session_id,
                country=country,
                device=device,
                **selected_product,
            )
        )
        timestamp += timedelta(seconds=rng.randint(5, 40))

        if rng.random() < 0.45:
            events.append(
                RetailEvent(
                    event_type="purchase",
                    event_timestamp=timestamp,
                    customer_id=customer_id,
                    session_id=session_id,
                    country=country,
                    device=device,
                    **selected_product,
                )
            )

    return events
