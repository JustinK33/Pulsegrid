from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class EventType(StrEnum):
    VIEW = "view"
    ADD_TO_CART = "addtocart"
    TRANSACTION = "transaction"


class Event(BaseModel):
    timestamp: int
    visitorid: int
    event: EventType
    itemid: int
    transactionid: int | None = None
