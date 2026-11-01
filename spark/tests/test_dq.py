import pytest

from tidewater_spark.dq import split_by_rules, with_dq_failures
from tidewater_spark.tables import Rule

RULES = (
    Rule("status_valid", "accepted_values", column="status", values=("placed", "paid")),
    Rule("total_non_negative", "range", column="total", min=0),
    Rule("email_format", "regex", column="email", pattern=r"^[^@]+@[^@]+$"),
    Rule("email_not_null", "not_null", column="email"),
    Rule("sign", "expression", expr="(status = 'refunded') = (total < 0)"),
)


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [
            (1, "placed", 10.0, "a@b.co", False),
            (2, "lost", -5.0, "nope", False),
            (3, "paid", 1.0, None, False),
            (4, "lost", -1.0, None, True),  # delete: never quarantined
        ],
        "id long, status string, total double, email string, is_deleted boolean",
    )


def test_every_failed_rule_is_named(df):
    got = {r.id: set(r._dq_failures) for r in with_dq_failures(df, RULES).collect()}
    assert got[1] == set()
    assert got[2] == {"status_valid", "total_non_negative", "email_format", "sign"}
    assert got[3] == {"email_not_null"}  # null email passes regex, fails not_null only


def test_split_routes_bad_rows_but_never_deletes(df):
    good, bad = split_by_rules(df, RULES)
    assert sorted(r.id for r in good.collect()) == [1, 4]
    assert sorted(r.id for r in bad.collect()) == [2, 3]
    assert "_dq_failures" not in good.columns


def test_no_rules_means_everything_passes(df):
    good, bad = split_by_rules(df, ())
    assert good.count() == 4 and bad.count() == 0
