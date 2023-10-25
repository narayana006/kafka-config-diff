"""A snapshot of the configuration of one Kafka cluster."""

from __future__ import annotations

from dataclasses import dataclass, field

Configs = dict[str, "str | None"]  # None: the value is unset or hidden because it is sensitive


@dataclass
class Snapshot:
    brokers: dict[str, Configs] = field(default_factory=dict)  # broker id -> configs
    topics: dict[str, Configs] = field(default_factory=dict)  # topic name -> configs
    broker_defaults: Configs = field(default_factory=dict)  # cluster-wide dynamic broker defaults
    label: str = ""

    def merge(self, other: "Snapshot") -> None:
        """Add the entities of another snapshot; later values win for the same key."""
        for broker, configs in other.brokers.items():
            self.brokers.setdefault(broker, {}).update(configs)
        for topic, configs in other.topics.items():
            self.topics.setdefault(topic, {}).update(configs)
        self.broker_defaults.update(other.broker_defaults)

    def is_empty(self) -> bool:
        return not (self.brokers or self.topics or self.broker_defaults)

    def to_dict(self) -> dict:
        def ordered(configs: Configs) -> Configs:
            return dict(sorted(configs.items()))

        data: dict = {}
        if self.label:
            data["label"] = self.label
        data["brokers"] = {b: ordered(c) for b, c in sorted(self.brokers.items(), key=lambda item: broker_sort_key(item[0]))}
        if self.broker_defaults:
            data["broker_defaults"] = ordered(self.broker_defaults)
        data["topics"] = {t: ordered(c) for t, c in sorted(self.topics.items())}
        return data


def broker_sort_key(broker_id: str) -> tuple[int, int | str]:
    """Numeric ids in numeric order, anything else after them."""
    return (0, int(broker_id)) if broker_id.isdigit() else (1, broker_id)


def sort_broker_ids(ids) -> list[str]:
    return sorted(ids, key=broker_sort_key)
