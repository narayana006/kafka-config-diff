from kafka_config_diff import IgnoreList, Snapshot, diff_snapshots, diff_within
from kafka_config_diff.diff import ABSENT


def cluster(broker_ids, topics=None, **configs):
    base = {"num.io.threads": "8", "log.retention.hours": "168"}
    base.update({k.replace("_", "."): v for k, v in configs.items()})
    return Snapshot(brokers={str(i): {**base, "node.id": str(i), "listeners": f"SSL://host-{i}:9093"} for i in broker_ids},
                    topics=topics or {})


def keyed(result):
    return {(d.scope, d.entity, d.key): d for d in result.differences}


def test_identical_clusters_with_different_broker_ids_do_not_differ():
    result = diff_snapshots(cluster([1, 2, 3]), cluster([101, 102]))
    assert result.differences == []
    assert result.ignored == 2  # node.id and listeners


def test_broker_config_changed_between_clusters():
    result = diff_snapshots(cluster([1, 2]), cluster([7, 8], log_retention_hours="72"))
    (d,) = result.differences
    assert (d.scope, d.kind, d.key) == ("broker", "changed", "log.retention.hours")
    assert d.left == [{"value": "168", "brokers": ["1", "2"]}]
    assert d.right == [{"value": "72", "brokers": ["7", "8"]}]


def test_one_odd_broker_on_one_side_is_a_difference():
    right = cluster([1, 2, 3])
    right.brokers["3"]["num.io.threads"] = "16"
    (d,) = diff_snapshots(cluster([1, 2, 3]), right).differences
    assert d.key == "num.io.threads"
    assert d.right == [{"value": "8", "brokers": ["1", "2"]}, {"value": "16", "brokers": ["3"]}]


def test_setting_present_on_one_side_only():
    right = cluster([1])
    right.brokers["1"]["auto.create.topics.enable"] = "false"
    (d,) = diff_snapshots(cluster([1]), right).differences
    assert d.kind == "only-right"
    assert d.left == [{"value": ABSENT, "brokers": ["1"]}]
    (d,) = diff_snapshots(right, cluster([1])).differences
    assert d.kind == "only-left"


def test_default_ignores_can_be_removed_and_extended():
    left, right = cluster([1]), cluster([2], num_io_threads="16")
    assert {d.key for d in diff_snapshots(left, right, IgnoreList(keys=())).differences} == {
        "node.id", "listeners", "num.io.threads"}
    result = diff_snapshots(left, right, IgnoreList(keys=("node.id", "listeners", "num.*")))
    assert result.differences == [] and result.ignored == 3


def test_listener_prefixed_keystore_locations_are_ignored_by_default():
    ignore = IgnoreList()
    assert ignore.key("ssl.keystore.location")
    assert ignore.key("listener.name.internal.ssl.truststore.location")
    assert not ignore.key("ssl.keystore.type")


def test_topic_differences():
    left = cluster([1], topics={"orders": {"retention.ms": "1000", "cleanup.policy": "delete"},
                                "tmp": {}, "__consumer_offsets": {"segment.bytes": "1"}})
    right = cluster([1], topics={"orders": {"retention.ms": "2000", "cleanup.policy": "delete", "max.message.bytes": "5"},
                                 "payments": {}, "__consumer_offsets": {"segment.bytes": "2"}})
    result = diff_snapshots(left, right, IgnoreList(topics=("__*",)))
    found = keyed(result)
    assert found[("topic", "orders", "retention.ms")].kind == "changed"
    assert (found[("topic", "orders", "retention.ms")].left, found[("topic", "orders", "retention.ms")].right) == ("1000", "2000")
    assert found[("topic", "orders", "max.message.bytes")].kind == "only-right"
    assert found[("topic", "tmp", None)].kind == "only-left"
    assert found[("topic", "payments", None)].kind == "only-right"
    assert not any(d.entity == "__consumer_offsets" for d in result.differences)
    common = diff_snapshots(left, right, IgnoreList(topics=("__*",)), common_topics_only=True)
    assert {d.entity for d in common.differences} == {"orders"}


def test_scope_limits_what_is_compared():
    left = cluster([1], topics={"t": {"retention.ms": "1"}})
    right = cluster([1], topics={"t": {"retention.ms": "2"}}, num_io_threads="16")
    assert [d.scope for d in diff_snapshots(left, right, scope="brokers").differences] == ["broker"]
    assert [d.scope for d in diff_snapshots(left, right, scope="topics").differences] == ["topic"]


def test_hidden_values_on_both_sides_are_equal():
    left, right = cluster([1]), cluster([2])
    left.brokers["1"]["ssl.key.password"] = None
    right.brokers["2"]["ssl.key.password"] = None
    assert diff_snapshots(left, right).differences == []


def test_side_without_brokers_or_topics_is_noted_not_reported_as_drift():
    baseline = Snapshot(topics={"t": {"retention.ms": "1"}})
    result = diff_snapshots(cluster([1], topics={"t": {"retention.ms": "1"}}), baseline)
    assert result.differences == []
    assert result.notes == ["Broker configs not compared: the right side has none."]
    result = diff_snapshots(Snapshot(brokers={"1": {"a": "1"}}), cluster([1], topics={"t": {}}), IgnoreList(keys=("*",)))
    assert "Topic configs not compared: the left side has none." in result.notes


def test_cluster_wide_dynamic_defaults_are_compared():
    left = Snapshot(broker_defaults={"log.retention.ms": "1"})
    right = Snapshot(broker_defaults={"log.retention.ms": "2"})
    (d,) = diff_snapshots(left, right).differences
    assert (d.scope, d.key, d.left, d.right) == ("broker-default", "log.retention.ms", "1", "2")


def test_within_finds_inconsistent_brokers():
    snapshot = cluster([1, 2, 3])
    snapshot.brokers["2"]["num.io.threads"] = "16"
    del snapshot.brokers["3"]["log.retention.hours"]
    result = diff_within(snapshot)
    found = {d.key: d for d in result.differences}
    assert set(found) == {"num.io.threads", "log.retention.hours"}
    assert found["num.io.threads"].kind == "inconsistent"
    assert found["num.io.threads"].left == [{"value": "8", "brokers": ["1", "3"]}, {"value": "16", "brokers": ["2"]}]
    assert found["log.retention.hours"].left[1] == {"value": ABSENT, "brokers": ["3"]}
    assert result.ignored == 2


def test_within_consistent_cluster_and_single_broker():
    assert diff_within(cluster([1, 2])).differences == []
    single = diff_within(cluster([1]))
    assert single.differences == [] and "1 broker" in single.notes[0]


def test_broker_ids_sort_numerically():
    snapshot = cluster([10, 9, 2])
    snapshot.brokers["10"]["num.io.threads"] = "4"
    (d,) = diff_within(snapshot).differences
    assert d.left[0]["brokers"] == ["2", "9"]
