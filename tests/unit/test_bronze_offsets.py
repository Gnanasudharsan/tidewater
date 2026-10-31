from tidewater_spark.offsets import BronzeStats, KafkaRange, check_partitions

TP = ("tidewater.public.orders", 0)


def kinds(findings, fatal_only=True):
    return {f.kind for f in findings if f.fatal or not fatal_only}


def test_complete_partition_passes():
    bronze = {TP: BronzeStats(rows=100, distinct_offsets=100, min_offset=0, max_offset=99)}
    kafka = {TP: KafkaRange(earliest=0, latest=99)}
    assert check_partitions(bronze, kafka, require_caught_up=True) == []


def test_duplicates_are_fatal():
    bronze = {TP: BronzeStats(rows=105, distinct_offsets=100, min_offset=0, max_offset=99)}
    kafka = {TP: KafkaRange(0, 99)}
    assert kinds(check_partitions(bronze, kafka, False)) == {"duplicate"}


def test_gap_inside_range_is_fatal():
    bronze = {TP: BronzeStats(rows=95, distinct_offsets=95, min_offset=0, max_offset=99)}
    kafka = {TP: KafkaRange(0, 99)}
    assert kinds(check_partitions(bronze, kafka, False)) == {"gap"}


def test_missing_head_is_fatal():
    bronze = {TP: BronzeStats(rows=90, distinct_offsets=90, min_offset=10, max_offset=99)}
    kafka = {TP: KafkaRange(0, 99)}
    assert kinds(check_partitions(bronze, kafka, False)) == {"missing_head"}


def test_lag_is_only_fatal_when_caught_up_required():
    bronze = {TP: BronzeStats(rows=50, distinct_offsets=50, min_offset=0, max_offset=49)}
    kafka = {TP: KafkaRange(0, 99)}
    assert kinds(check_partitions(bronze, kafka, require_caught_up=False)) == set()
    assert kinds(check_partitions(bronze, kafka, require_caught_up=False), fatal_only=False) == {
        "lag"
    }
    assert kinds(check_partitions(bronze, kafka, require_caught_up=True)) == {"lag"}


def test_partition_absent_from_bronze():
    kafka = {TP: KafkaRange(0, 9)}
    assert kinds(check_partitions({}, kafka, require_caught_up=True)) == {"missing_partition"}
    assert kinds(check_partitions({}, kafka, require_caught_up=False)) == set()
