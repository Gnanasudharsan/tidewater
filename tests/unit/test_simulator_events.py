import random
from collections import Counter

import pytest

from tidewater_sim.events import DEFAULT_WEIGHTS, EventKind, Pacer, next_status, pick_event


def test_pick_event_covers_every_kind_and_respects_weights():
    rng = random.Random(42)
    counts = Counter(pick_event(rng) for _ in range(20_000))
    assert set(counts) == set(EventKind)
    # Highest-weight event should be the most frequent.
    assert counts.most_common(1)[0][0] == max(DEFAULT_WEIGHTS, key=DEFAULT_WEIGHTS.get)


def test_status_machine_is_forward_only_and_terminates():
    assert next_status("placed") == "paid"
    assert next_status("paid") == "shipped"
    assert next_status("shipped") == "delivered"
    for terminal in ("delivered", "cancelled", "refunded"):
        assert next_status(terminal) is None


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.slept = []

    def clock(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def test_pacer_hits_target_rate_without_drift():
    fc = FakeClock()
    pacer = Pacer(10, clock=fc.clock, sleep=fc.sleep)
    for _ in range(11):
        pacer.wait()
    # 11 ticks at 10/s: the 11th is due exactly 1.0s after the first.
    assert fc.t == pytest.approx(1.0)


def test_pacer_catches_up_after_slow_event():
    fc = FakeClock()
    pacer = Pacer(10, clock=fc.clock, sleep=fc.sleep)
    pacer.wait()  # t=0
    fc.t += 0.35  # a slow event
    pacer.wait()  # due at 0.1, already late: no sleep
    pacer.wait()  # due at 0.2, still late: no sleep
    pacer.wait()  # due at 0.3, still late: no sleep
    pacer.wait()  # due at 0.4: sleeps 0.05
    assert fc.slept == [pytest.approx(0.05)]


def test_pacer_rejects_non_positive_rate():
    with pytest.raises(ValueError):
        Pacer(0)
