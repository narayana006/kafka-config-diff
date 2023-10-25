"""Pure comparison logic over Snapshot objects."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from fnmatch import fnmatchcase

from .model import Configs, Snapshot, sort_broker_ids

# Broker settings that identify a broker or point at its own host, disks or peers.
# They are expected to differ between brokers and between clusters.
DEFAULT_IGNORES = (
    "broker.id",
    "node.id",
    "broker.rack",
    "listeners",
    "advertised.listeners",
    "log.dir",
    "log.dirs",
    "metadata.log.dir",
    "controller.quorum.voters",
    "controller.quorum.bootstrap.servers",
    "zookeeper.connect",
    "*ssl.keystore.location",
    "*ssl.truststore.location",
)

ABSENT = "(not set)"


@dataclass
class IgnoreList:
    keys: tuple[str, ...] = DEFAULT_IGNORES  # glob patterns matched against config names
    topics: tuple[str, ...] = ()  # glob patterns matched against topic names

    def key(self, name: str) -> bool:
        return any(fnmatchcase(name, pattern) for pattern in self.keys)

    def topic(self, name: str) -> bool:
        return any(fnmatchcase(name, pattern) for pattern in self.topics)


@dataclass
class Difference:
    scope: str  # "broker", "broker-default" or "topic"
    kind: str  # "changed", "only-left", "only-right" or "inconsistent"
    entity: str | None = None  # topic name; None for broker scopes
    key: str | None = None  # config name; None when a whole topic exists on one side only
    left: object = None  # topic scopes: the value. broker scope: [{"value": ..., "brokers": [...]}]
    right: object = None


@dataclass
class DiffResult:
    differences: list[Difference] = field(default_factory=list)
    ignored: int = 0  # differences hidden by the ignore list
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"differences": [asdict(d) for d in self.differences], "ignored": self.ignored, "notes": self.notes,
                "drift": bool(self.differences)}


def _groups(brokers: dict[str, Configs], key: str) -> list[dict]:
    """Group brokers by the value they have for one key: [{"value": v, "brokers": [...]}]."""
    by_value: dict[str | None, list[str]] = {}
    missing = object()
    for broker in sort_broker_ids(brokers):
        value = brokers[broker].get(key, missing)
        by_value.setdefault(ABSENT if value is missing else value, []).append(broker)
    return [{"value": value, "brokers": ids} for value, ids in by_value.items()]


def _values(groups: list[dict]) -> set:
    return {g["value"] for g in groups}


def _diff_flat(scope: str, entity: str | None, left: Configs, right: Configs,
               ignore: IgnoreList, result: DiffResult) -> None:
    for key in sorted(set(left) | set(right)):
        if key in left and key in right and left[key] == right[key]:
            continue
        if ignore.key(key):
            result.ignored += 1
            continue
        kind = "changed" if key in left and key in right else "only-left" if key in left else "only-right"
        result.differences.append(Difference(scope, kind, entity, key, left.get(key, ABSENT), right.get(key, ABSENT)))


def diff_snapshots(left: Snapshot, right: Snapshot, ignore: IgnoreList | None = None,
                   scope: str = "all", common_topics_only: bool = False) -> DiffResult:
    """Compare two clusters, or a cluster with a baseline.

    Broker ids rarely line up between clusters, so broker configs are compared at
    cluster level: for each setting, the set of values in use on the left against the
    set of values in use on the right.
    """
    ignore = ignore or IgnoreList()
    result = DiffResult()

    if scope in ("all", "brokers"):
        if left.brokers and right.brokers:
            keys = {k for configs in (*left.brokers.values(), *right.brokers.values()) for k in configs}
            for key in sorted(keys):
                lg, rg = _groups(left.brokers, key), _groups(right.brokers, key)
                if _values(lg) == _values(rg):
                    continue
                if ignore.key(key):
                    result.ignored += 1
                    continue
                kind = ("only-right" if _values(lg) == {ABSENT} else
                        "only-left" if _values(rg) == {ABSENT} else "changed")
                result.differences.append(Difference("broker", kind, None, key, lg, rg))
        elif left.brokers or right.brokers:
            side = "right" if left.brokers else "left"
            result.notes.append(f"Broker configs not compared: the {side} side has none.")
        if left.broker_defaults or right.broker_defaults:
            _diff_flat("broker-default", None, left.broker_defaults, right.broker_defaults, ignore, result)

    if scope in ("all", "topics"):
        if left.topics and right.topics:
            for topic in sorted(set(left.topics) | set(right.topics)):
                if ignore.topic(topic):
                    continue
                if topic in left.topics and topic in right.topics:
                    _diff_flat("topic", topic, left.topics[topic], right.topics[topic], ignore, result)
                elif not common_topics_only:
                    result.differences.append(
                        Difference("topic", "only-left" if topic in left.topics else "only-right", topic))
        elif left.topics or right.topics:
            side = "right" if left.topics else "left"
            result.notes.append(f"Topic configs not compared: the {side} side has none.")
    return result


def diff_within(snapshot: Snapshot, ignore: IgnoreList | None = None) -> DiffResult:
    """Find broker settings that are not the same on every broker of one cluster."""
    ignore = ignore or IgnoreList()
    result = DiffResult()
    if len(snapshot.brokers) < 2:
        result.notes.append(f"Nothing to compare: the snapshot has {len(snapshot.brokers)} broker(s).")
        return result
    keys = {k for configs in snapshot.brokers.values() for k in configs}
    for key in sorted(keys):
        groups = _groups(snapshot.brokers, key)
        if len(groups) < 2:
            continue
        if ignore.key(key):
            result.ignored += 1
            continue
        result.differences.append(Difference("broker", "inconsistent", None, key, groups))
    return result
