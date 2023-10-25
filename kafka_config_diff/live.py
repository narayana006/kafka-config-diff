"""Optional adapter: snapshot broker and topic configs of a live cluster.

Uses confluent-kafka and only read-only calls: Metadata and DescribeConfigs.
"""

from __future__ import annotations

from .model import Snapshot


def fetch_snapshot(bootstrap: str, client_config: dict | None = None, timeout: float = 15.0) -> Snapshot:
    try:
        from confluent_kafka.admin import AdminClient, ConfigResource
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError(
            "reading from a live cluster needs the confluent-kafka package: pip install 'kafka-config-diff[live]'"
        ) from exc

    admin = AdminClient({"bootstrap.servers": bootstrap, **(client_config or {})})
    metadata = admin.list_topics(timeout=timeout)
    resources = [ConfigResource(ConfigResource.Type.BROKER, str(broker)) for broker in sorted(metadata.brokers)]
    resources += [ConfigResource(ConfigResource.Type.TOPIC, name) for name in sorted(metadata.topics)]

    snapshot = Snapshot(label=bootstrap)
    for resource, future in admin.describe_configs(resources, request_timeout=timeout).items():
        # entry.value is None for sensitive settings and for settings without a value
        configs = {key: entry.value for key, entry in future.result().items()}
        if resource.restype == ConfigResource.Type.BROKER:
            snapshot.brokers[resource.name] = configs
        else:
            snapshot.topics[resource.name] = configs
    return snapshot
