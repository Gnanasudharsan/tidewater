import pytest

from tidewater_spark.tables import load_contracts


def test_shipped_config_covers_every_source_table():
    contracts = load_contracts()
    assert set(contracts) == {"customers", "products", "orders", "order_items", "payments"}
    assert contracts["orders"].primary_key == ("order_id",)
    assert "status_valid" in {r.name for r in contracts["orders"].rules}


@pytest.mark.parametrize(
    "yaml_text, message",
    [
        ("tables: {t: {rules: []}}", "primary_key"),
        ("tables: {t: {primary_key: [id], rules: [{name: r, type: bogus}]}}", "type"),
        ("tables: {t: {primary_key: [id], rules: [{name: r, type: regex, column: c}]}}", "pattern"),
        ("tables: {t: {primary_key: [id], rules: [{name: r, type: range, column: c}]}}", "min"),
        (
            "tables: {t: {primary_key: [id], rules: ["
            "{name: r, type: not_null, column: a}, {name: r, type: not_null, column: b}]}}",
            "unique",
        ),
    ],
)
def test_invalid_configs_are_rejected(tmp_path, yaml_text, message):
    path = tmp_path / "t.yml"
    path.write_text(yaml_text)
    with pytest.raises(ValueError, match=message):
        load_contracts(path)
