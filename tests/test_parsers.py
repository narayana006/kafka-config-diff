import json
from pathlib import Path

import pytest

from kafka_config_diff.parsers import ParseError, load_path, parse_kafka_configs, parse_snapshot

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

# Lines copied from kafka-configs.sh --describe of Apache Kafka 4.4.0.
BROKER_ALL = """All configs for broker 1 are:
  advertised.listeners=PLAINTEXT://localhost:19192 sensitive=false synonyms={STATIC_BROKER_CONFIG:advertised.listeners=PLAINTEXT://localhost:19192}
  alter.config.policy.class.name=null sensitive=false synonyms={}
  listeners=PLAINTEXT://:19192,CONTROLLER://:9093 sensitive=false synonyms={STATIC_BROKER_CONFIG:listeners=PLAINTEXT://:19192,CONTROLLER://:9093, DEFAULT_CONFIG:listeners=PLAINTEXT://:9092}
  log.cleaner.threads=2 sensitive=false synonyms={DYNAMIC_BROKER_CONFIG:log.cleaner.threads=2, DEFAULT_CONFIG:log.cleaner.threads=1}
  log.retention.ms=259200000 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=259200000, DEFAULT_CONFIG:log.retention.hours=168}
  ssl.keystore.password=null sensitive=true synonyms={}
"""

DYNAMIC = """Dynamic configs for broker 1 are:
  log.cleaner.threads=2 sensitive=false synonyms={DYNAMIC_BROKER_CONFIG:log.cleaner.threads=2, DEFAULT_CONFIG:log.cleaner.threads=1}
Default configs for brokers in the cluster are:
  log.retention.ms=259200000 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=259200000}
Dynamic configs for topic audit are:
Dynamic configs for topic orders are:
  retention.ms=86400000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=86400000, DEFAULT_CONFIG:log.retention.hours=168}
"""


def test_all_configs_for_a_broker():
    snapshot = parse_kafka_configs(BROKER_ALL)
    assert snapshot.brokers == {"1": {
        "advertised.listeners": "PLAINTEXT://localhost:19192",
        "alter.config.policy.class.name": None,  # no value
        "listeners": "PLAINTEXT://:19192,CONTROLLER://:9093",
        "log.cleaner.threads": "2",
        "log.retention.ms": "259200000",
        "ssl.keystore.password": None,  # sensitive
    }}
    assert snapshot.topics == {}


def test_dynamic_sections_defaults_and_empty_topics():
    snapshot = parse_kafka_configs(DYNAMIC)
    assert snapshot.brokers == {"1": {"log.cleaner.threads": "2"}}
    assert snapshot.broker_defaults == {"log.retention.ms": "259200000"}
    assert snapshot.topics == {"audit": {}, "orders": {"retention.ms": "86400000"}}


def test_a_real_value_spelled_null_is_kept():
    text = "All configs for topic t are:\n  some.name=null sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:some.name=null}\n"
    assert parse_kafka_configs(text).topics["t"]["some.name"] == "null"


def test_other_entity_types_are_skipped():
    text = ("Quota configs for user-principal 'alice' are producer_byte_rate=1024.0\n"
            "Dynamic configs for group g1 are:\n"
            "  consumer.session.timeout.ms=50000 sensitive=false synonyms={DYNAMIC_GROUP_CONFIG:consumer.session.timeout.ms=50000}\n"
            + DYNAMIC)
    snapshot = parse_kafka_configs(text)
    assert set(snapshot.topics) == {"audit", "orders"}
    assert "consumer.session.timeout.ms" not in snapshot.brokers["1"]


def test_json_snapshot_and_round_trip():
    snapshot = parse_snapshot((EXAMPLES / "prod.json").read_text())
    assert snapshot.label == "prod"
    assert list(snapshot.brokers) == ["11", "12", "13"]
    assert snapshot.brokers["11"]["ssl.keystore.password"] is None
    assert parse_snapshot(json.dumps(snapshot.to_dict())) == snapshot


def test_json_values_are_normalised_to_strings():
    snapshot = parse_snapshot('{"topics": {"t": {"retention.ms": 1000, "x": null}}}')
    assert snapshot.topics["t"] == {"retention.ms": "1000", "x": None}


def test_directory_of_dumps_is_merged():
    merged = load_path(EXAMPLES / "staging-dump")
    assert sorted(merged.brokers) == ["1", "2", "3"]
    assert merged.topics == parse_snapshot((EXAMPLES / "staging.json").read_text()).topics


def test_bad_input(tmp_path):
    for text in ("nothing useful here\n", '{"brokerz": {}}', '{"brokers": []}', '{"topics": {"t": "x"}}', "{broken"):
        with pytest.raises(ParseError):
            parse_snapshot(text)
    with pytest.raises(ParseError):
        load_path(tmp_path)  # empty directory
