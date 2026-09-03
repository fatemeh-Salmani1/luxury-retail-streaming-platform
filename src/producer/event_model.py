from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(StrEnum):
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    PURCHASE = "purchase"


class ProductCategory(StrEnum):
    FRAGRANCE = "fragrance"
    SKINCARE = "skincare"
    HANDBAG = "handbag"
    JEWELLERY = "jewellery"
    ACCESSORY = "accessory"


class DeviceType(StrEnum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class RetailEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    event_timestamp: datetime

    customer_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)

    product_id: str = Field(min_length=1)
    product_name: str = Field(min_length=1)
    category: ProductCategory

    unit_price: float = Field(gt=0)
    quantity: int = Field(default=1, gt=0)

    country: str = Field(min_length=2, max_length=2)
    device: DeviceType
