"""Data-quality rules engine: turn YAML rules into Spark expressions and split good from bad."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from tidewater_spark.tables import Rule

DQ_REASONS = "_dq_failures"


def rule_passes(rule: Rule) -> Column:
    """Boolean column that is true when the rule passes. Null inputs pass every rule
    except not_null, so each rule checks one thing."""
    col = F.col(rule.column) if rule.column else None
    if rule.type == "not_null":
        return col.isNotNull()
    if rule.type == "accepted_values":
        return col.isNull() | col.isin(list(rule.values))
    if rule.type == "range":
        check = F.lit(True)
        if rule.min is not None:
            check = check & (col >= F.lit(rule.min))
        if rule.max is not None:
            check = check & (col <= F.lit(rule.max))
        return col.isNull() | check
    if rule.type == "regex":
        return col.isNull() | col.rlike(rule.pattern)
    if rule.type == "expression":
        return F.coalesce(F.expr(rule.expr), F.lit(True))
    raise ValueError(f"unknown rule type {rule.type}")


def with_dq_failures(df: DataFrame, rules: tuple[Rule, ...]) -> DataFrame:
    """Add an array column with the names of every rule the row fails (empty if none)."""
    if not rules:
        return df.withColumn(DQ_REASONS, F.array().cast("array<string>"))
    failures = [F.when(~F.coalesce(rule_passes(r), F.lit(False)), F.lit(r.name)) for r in rules]
    return df.withColumn(DQ_REASONS, F.array_compact(F.array(*failures)))


def split_by_rules(df: DataFrame, rules: tuple[Rule, ...]) -> tuple[DataFrame, DataFrame]:
    """(passing rows, failing rows with _dq_failures). Delete events skip DQ: the row is gone,
    and blocking a delete would leave silver showing a row the source no longer has."""
    checked = with_dq_failures(df, rules)
    is_bad = (F.size(DQ_REASONS) > 0) & ~F.col("is_deleted")
    return checked.where(~is_bad).drop(DQ_REASONS), checked.where(is_bad)
