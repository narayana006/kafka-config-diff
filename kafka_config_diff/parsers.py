"""Read snapshots from JSON or from `kafka-configs --describe` text output."""

from __future__ import annotations

import json
import re
from pathlib import Path

from .model import Configs, Snapshot


class ParseError(ValueError):
    """Raised when a snapshot cannot be read."""


_HEADER = re.compile(r"^(All|Dynamic|Default) configs for (\S+) (.*?)\s*are:\s*$")
_ENTRY = re.compile(r"^\s+(\S+?)=(.*) sensitive=(true|false) synonyms=\{(.*)\}\s*$")


def parse_kafka_configs(text: str) -> Snapshot:
    """Parse the output of `kafka-configs --describe` for brokers and topics.

    Any number of sections may be concatenated (several brokers, all topics, with or
    without `--all`). Sections for other entity types (users, clients, groups) are skipped.
    """
    snapshot = Snapshot()
    target: Configs | None = None
    for raw in text.splitlines():
        header = _HEADER.match(raw)
        if header:
            kind, entity_type, name = header.groups()
            if kind == "Default" and entity_type == "brokers":
                target = snapshot.broker_defaults
            elif entity_type == "broker" and name:
                target = snapshot.brokers.setdefault(name, {})
            elif entity_type == "topic" and name:
                target = snapshot.topics.setdefault(name, {})
            else:
                target = None
            continue
        entry = _ENTRY.match(raw)
        if entry and target is not None:
            key, value, sensitive, synonyms = entry.groups()
            # Kafka prints null for sensitive values and for settings that have no value.
            hidden = value == "null" and (sensitive == "true" or not synonyms.strip())
            target[key] = None if hidden else value
        elif raw.strip() and not raw[:1].isspace():
            target = None  # unrelated line, e.g. a quota section or a warning
    if snapshot.is_empty():
        raise ParseError("no broker or topic config sections found "
                         "(expected a JSON snapshot or kafka-configs --describe output)")
    return snapshot


def _configs(value, where: str) -> Configs:
    if not isinstance(value, dict):
        raise ParseError(f"{where} must be an object of config name to value")
    return {str(k): None if v is None else str(v) for k, v in value.items()}


def snapshot_from_dict(data) -> Snapshot:
    if not isinstance(data, dict):
        raise ParseError("a JSON snapshot must be an object with 'brokers' and/or 'topics'")
    unknown = set(data) - {"label", "brokers", "topics", "broker_defaults"}
    if unknown:
        raise ParseError(f"unknown snapshot key(s): {', '.join(sorted(unknown))}")
    brokers, topics = data.get("brokers", {}), data.get("topics", {})
    if not isinstance(brokers, dict) or not isinstance(topics, dict):
        raise ParseError("'brokers' and 'topics' must be objects keyed by broker id and topic name")
    return Snapshot(
        brokers={str(b): _configs(c, f"brokers[{b}]") for b, c in brokers.items()},
        topics={str(t): _configs(c, f"topics[{t}]") for t, c in topics.items()},
        broker_defaults=_configs(data.get("broker_defaults", {}), "broker_defaults"),
        label=str(data.get("label", "")),
    )


def parse_snapshot(text: str) -> Snapshot:
    """Detect the format (JSON snapshot or kafka-configs text) and parse it."""
    if text.lstrip().startswith("{"):
        try:
            return snapshot_from_dict(json.loads(text))
        except json.JSONDecodeError as exc:
            raise ParseError(f"invalid JSON: {exc}") from exc
    return parse_kafka_configs(text)


def load_path(path: str | Path) -> Snapshot:
    """Load a snapshot from a file, or merge every file in a directory."""
    path = Path(path)
    files = sorted(p for p in path.iterdir() if p.is_file() and not p.name.startswith(".")) if path.is_dir() else [path]
    if not files:
        raise ParseError(f"{path}: directory contains no files")
    snapshot = Snapshot()
    for file in files:
        try:
            part = parse_snapshot(file.read_text(encoding="utf-8"))
        except (ParseError, UnicodeDecodeError) as exc:
            raise ParseError(f"{file}: {exc}") from exc
        snapshot.merge(part)
        snapshot.label = snapshot.label or part.label
    return snapshot
