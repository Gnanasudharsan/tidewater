"""Pure offset-accounting checks for bronze. No Spark import, so it is unit-testable anywhere.

Debezium's Kafka producer is non-transactional and topics are not compacted, so
offsets in each partition are contiguous. Bronze must therefore hold every
offset from Kafka's earliest to (when caught up) its latest, each exactly once.
"""

from __future__ import annotations

from dataclasses import dataclass

TopicPartition = tuple[str, int]


@dataclass(frozen=True)
class BronzeStats:
    rows: int
    distinct_offsets: int
    min_offset: int
    max_offset: int


@dataclass(frozen=True)
class KafkaRange:
    """Offsets present in Kafka: earliest retained and last written (inclusive)."""

    earliest: int
    latest: int


@dataclass(frozen=True)
class Finding:
    partition: TopicPartition
    kind: str  # duplicate | gap | missing_head | lag | missing_partition
    detail: str
    fatal: bool


def check_partitions(
    bronze: dict[TopicPartition, BronzeStats],
    kafka: dict[TopicPartition, KafkaRange],
    require_caught_up: bool,
) -> list[Finding]:
    findings: list[Finding] = []
    for tp in sorted(set(bronze) | set(kafka)):
        b, k = bronze.get(tp), kafka.get(tp)
        if b is None:
            findings.append(
                Finding(tp, "missing_partition", "in Kafka but not in bronze", require_caught_up)
            )
            continue
        if b.rows != b.distinct_offsets:
            findings.append(
                Finding(tp, "duplicate", f"{b.rows - b.distinct_offsets} duplicate offsets", True)
            )
        span = b.max_offset - b.min_offset + 1
        if span != b.distinct_offsets:
            findings.append(
                Finding(tp, "gap", f"{span - b.distinct_offsets} offsets missing in range", True)
            )
        if k is None:
            continue
        if b.min_offset != k.earliest:
            findings.append(
                Finding(
                    tp,
                    "missing_head",
                    f"bronze starts at {b.min_offset}, Kafka earliest is {k.earliest}",
                    True,
                )
            )
        if b.max_offset < k.latest:
            behind = k.latest - b.max_offset
            findings.append(
                Finding(tp, "lag", f"{behind} offsets not ingested yet", require_caught_up)
            )
    return findings
