"""Compare Kafka broker and topic configuration between clusters or against a baseline."""

from .diff import Difference, DiffResult, IgnoreList, diff_snapshots, diff_within
from .model import Snapshot

__version__ = "0.1.0"
__all__ = ["DiffResult", "Difference", "IgnoreList", "Snapshot", "diff_snapshots", "diff_within"]
