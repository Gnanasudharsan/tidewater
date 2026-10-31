"""Pure, DB-free logic for the simulator: event mix, status machine, pacing."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum


class EventKind(str, Enum):
    NEW_ORDER = "new_order"
    ADVANCE_STATUS = "advance_status"
    NEW_CUSTOMER = "new_customer"
    ADDRESS_CHANGE = "address_change"
    PRICE_CHANGE = "price_change"
    REFUND = "refund"
    CANCEL_DELETE = "cancel_delete"
    DELETE_CUSTOMER = "delete_customer"


# Relative weights of each event. Inserts dominate, like a real shop, but every
# CDC operation (c/u/d) shows up continuously.
DEFAULT_WEIGHTS: dict[EventKind, float] = {
    EventKind.NEW_ORDER: 40,
    EventKind.ADVANCE_STATUS: 25,
    EventKind.NEW_CUSTOMER: 10,
    EventKind.ADDRESS_CHANGE: 8,
    EventKind.PRICE_CHANGE: 6,
    EventKind.REFUND: 5,
    EventKind.CANCEL_DELETE: 4,
    EventKind.DELETE_CUSTOMER: 2,
}

# Forward-only order lifecycle. Refunds and cancellations are separate events.
NEXT_STATUS: dict[str, str] = {
    "placed": "paid",
    "paid": "shipped",
    "shipped": "delivered",
}

REFUNDABLE_STATUSES = ("shipped", "delivered")


def pick_event(rng: random.Random, weights: dict[EventKind, float] | None = None) -> EventKind:
    weights = weights or DEFAULT_WEIGHTS
    kinds = list(weights)
    return rng.choices(kinds, weights=[weights[k] for k in kinds], k=1)[0]


def next_status(status: str) -> str | None:
    return NEXT_STATUS.get(status)


@dataclass
class Pacer:
    """Schedules events at a fixed rate without drifting.

    Each tick is due at start + n / rate, so a slow event is caught up on later
    instead of permanently lowering throughput.
    """

    events_per_sec: float
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    _start: float | None = field(default=None, init=False)
    _n: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.events_per_sec <= 0:
            raise ValueError("events_per_sec must be > 0")

    def wait(self) -> None:
        now = self.clock()
        if self._start is None:
            self._start = now
        due = self._start + self._n / self.events_per_sec
        if due > now:
            self.sleep(due - now)
        self._n += 1
