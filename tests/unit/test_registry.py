import json

from tidewater_spark.registry import SchemaRegistry, inline_references, row_temporal_fields

SOURCE = {
    "type": "record",
    "name": "Source",
    "namespace": "io.debezium",
    "fields": [{"name": "lsn", "type": ["null", "long"]}],
}


def envelope():
    value = {
        "type": "record",
        "name": "Value",
        "fields": [
            {"name": "id", "type": "long"},
            {
                "name": "created_at",
                "type": {"type": "string", "connect.name": "io.debezium.time.ZonedTimestamp"},
            },
            {
                "name": "born",
                "type": ["null", {"type": "int", "connect.name": "io.debezium.time.Date"}],
            },
        ],
    }
    return {
        "type": "record",
        "name": "Envelope",
        "fields": [
            {"name": "before", "type": ["null", value]},
            {"name": "after", "type": ["null", "Value"]},
            {"name": "source", "type": "io.debezium.Source"},
            {"name": "source_again", "type": "io.debezium.Source"},
        ],
    }


def test_first_reference_is_inlined_later_ones_stay_names():
    out = inline_references(envelope(), {"io.debezium.Source": SOURCE})
    fields = {f["name"]: f["type"] for f in out["fields"]}
    assert fields["source"] == SOURCE
    assert fields["source_again"] == "io.debezium.Source"
    assert fields["after"] == ["null", "Value"]


def test_registry_resolves_references_and_caches():
    calls = []
    docs = {
        "http://r/schemas/ids/5": {
            "schema": json.dumps(envelope()),
            "references": [{"name": "io.debezium.Source", "subject": "s", "version": 2}],
        },
        "http://r/subjects/s/versions/2": {"schema": json.dumps(SOURCE)},
    }

    def fetch(url):
        calls.append(url)
        return docs[url]

    reg = SchemaRegistry("http://r/", fetch=fetch)
    first = json.loads(reg.schema_json(5))
    reg.schema_json(5)
    assert first["fields"][2]["type"]["name"] == "Source"
    assert len(calls) == 2  # second lookup served from cache


def test_temporal_fields_found_in_plain_and_nullable_columns():
    assert row_temporal_fields(json.dumps(envelope())) == {
        "created_at": "zoned_timestamp",
        "born": "date",
    }
