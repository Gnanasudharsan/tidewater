"""Silver table contracts loaded from conf/silver_tables.yml. Pure Python (no Spark)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "conf" / "silver_tables.yml"

RULE_TYPES = {
    "not_null": {"column"},
    "accepted_values": {"column", "values"},
    "range": {"column"},
    "regex": {"column", "pattern"},
    "expression": {"expr"},
}


@dataclass(frozen=True)
class Rule:
    name: str
    type: str
    column: str | None = None
    values: tuple = ()
    min: float | None = None
    max: float | None = None
    pattern: str | None = None
    expr: str | None = None


@dataclass(frozen=True)
class TableContract:
    name: str
    primary_key: tuple[str, ...]
    rules: tuple[Rule, ...] = field(default_factory=tuple)


def parse_rule(table: str, raw: dict) -> Rule:
    name, rtype = raw.get("name"), raw.get("type")
    if not name or rtype not in RULE_TYPES:
        raise ValueError(f"{table}: rule needs a name and a type in {sorted(RULE_TYPES)}: {raw}")
    missing = RULE_TYPES[rtype] - raw.keys()
    if missing:
        raise ValueError(f"{table}.{name}: {rtype} rule is missing {sorted(missing)}")
    if rtype == "range" and raw.get("min") is None and raw.get("max") is None:
        raise ValueError(f"{table}.{name}: range rule needs min and/or max")
    return Rule(
        name=name,
        type=rtype,
        column=raw.get("column"),
        values=tuple(raw.get("values", ())),
        min=raw.get("min"),
        max=raw.get("max"),
        pattern=raw.get("pattern"),
        expr=raw.get("expr"),
    )


def load_contracts(path: Path | str = DEFAULT_CONFIG) -> dict[str, TableContract]:
    doc = yaml.safe_load(Path(path).read_text())
    contracts = {}
    for name, spec in doc["tables"].items():
        pk = spec.get("primary_key")
        if not pk:
            raise ValueError(f"{name}: primary_key is required")
        rules = tuple(parse_rule(name, r) for r in spec.get("rules", []))
        if len({r.name for r in rules}) != len(rules):
            raise ValueError(f"{name}: rule names must be unique")
        contracts[name] = TableContract(name, tuple(pk), rules)
    return contracts
