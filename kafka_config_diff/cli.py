"""Command line entry point."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .diff import DEFAULT_IGNORES, Difference, DiffResult, IgnoreList, diff_snapshots, diff_within
from .model import Snapshot
from .parsers import ParseError, load_path, parse_snapshot


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kafka-config-diff",
        description="Compare Kafka broker and topic configuration between two clusters, against a baseline "
                    "file, or between the brokers of one cluster.",
        epilog="A SOURCE is a JSON snapshot, a file with `kafka-configs --describe` output, a directory of such "
               "files, - for stdin, or the HOST:PORT of a live cluster. Exit status: 0 no differences, "
               "1 differences found, 2 error.",
    )
    mode = p.add_argument_group("what to compare")
    mode.add_argument("--left", metavar="SOURCE", help="first cluster or baseline")
    mode.add_argument("--right", metavar="SOURCE", help="second cluster or baseline")
    mode.add_argument("--within", metavar="SOURCE", help="compare the brokers of one cluster with each other")
    mode.add_argument("--snapshot", metavar="SOURCE", help="write a JSON snapshot of SOURCE and exit")
    mode.add_argument("-o", "--output", metavar="FILE", help="where --snapshot writes (default: stdout)")
    mode.add_argument("-X", dest="client_config", action="append", default=[], metavar="KEY=VALUE",
                      help="client property for live sources, e.g. -X security.protocol=SASL_SSL")
    flt = p.add_argument_group("filters")
    flt.add_argument("--scope", choices=["all", "brokers", "topics"], default="all",
                     help="compare only broker or only topic configs (default all)")
    flt.add_argument("--ignore", action="append", default=[], metavar="GLOB",
                     help="config name (glob) expected to differ; repeatable")
    flt.add_argument("--ignore-file", metavar="FILE", help="file with one config name glob per line, # for comments")
    flt.add_argument("--no-default-ignores", action="store_true",
                     help="do not ignore the built-in list: " + ", ".join(DEFAULT_IGNORES))
    flt.add_argument("--ignore-topic", action="append", default=[], metavar="GLOB",
                     help="topic name (glob) to leave out; repeatable")
    flt.add_argument("--common-topics-only", action="store_true",
                     help="do not report topics that exist on one side only")
    p.add_argument("--json", action="store_true", help="print the result as JSON")
    return p


def load_source(source: str, client_config: dict, name_files: bool = True) -> Snapshot:
    """Resolve a SOURCE argument: stdin, an existing path, otherwise a bootstrap address.

    Unlabelled file sources are labelled with their path for display unless `name_files` is off.
    """
    if source == "-" or os.path.exists(source):
        snapshot = parse_snapshot(sys.stdin.read()) if source == "-" else load_path(source)
        if name_files and not snapshot.label:
            snapshot.label = "stdin" if source == "-" else source
        return snapshot
    host, _, port = source.rpartition(":")
    if not host or not port.isdigit():
        raise ParseError(f"{source}: no such file or directory, and not a HOST:PORT address")
    from . import live

    return live.fetch_snapshot(source, client_config)


def _read_ignore_file(path: str) -> list[str]:
    with open(path, encoding="utf-8") as handle:
        lines = (line.split("#", 1)[0].strip() for line in handle)
        return [line for line in lines if line]


def _show(value) -> str:
    return "(unset or hidden)" if value is None else str(value)


def _show_groups(groups: list[dict], total: int) -> str:
    parts = []
    for group in groups:
        ids = group["brokers"]
        where = (f"all {total} brokers" if len(ids) == total and total > 1 else
                 f"broker{'s' if len(ids) > 1 else ''} {', '.join(ids)}")
        parts.append(f"{_show(group['value'])} ({where})")
    return "; ".join(parts)


def _count(snapshot: Snapshot) -> str:
    brokers, topics = len(snapshot.brokers), len(snapshot.topics)
    return f"{brokers} broker{'' if brokers == 1 else 's'}, {topics} topic{'' if topics == 1 else 's'}"


def render_text(result: DiffResult, left: Snapshot, right: Snapshot | None) -> str:
    out: list[str] = []
    if right is None:
        count = len(left.brokers)
        out.append(f"Comparing the {count} broker{'' if count == 1 else 's'} of {left.label} with each other")
    else:
        out.append(f"left:  {left.label} ({_count(left)})")
        out.append(f"right: {right.label} ({_count(right)})")
    out += result.notes

    def section(title: str, items: list[Difference]) -> None:
        if items:
            out.extend(["", f"{title} ({len(items)})"])

    brokers = [d for d in result.differences if d.scope == "broker"]
    section("Broker settings that differ between brokers" if right is None else "Broker configs", brokers)
    for d in brokers:
        out.append(f"  {d.key}")
        if right is None:
            for group in d.left:
                out.append(f"    {_show(group['value'])}  on broker{'s' if len(group['brokers']) > 1 else ''} "
                           f"{', '.join(group['brokers'])}")
        else:
            out.append(f"    left:  {_show_groups(d.left, len(left.brokers))}")
            out.append(f"    right: {_show_groups(d.right, len(right.brokers))}")

    defaults = [d for d in result.differences if d.scope == "broker-default"]
    section("Cluster-wide dynamic broker defaults", defaults)
    for d in defaults:
        out.append(f"  {d.key}")
        out.append(f"    left:  {_show(d.left)}")
        out.append(f"    right: {_show(d.right)}")

    topics = [d for d in result.differences if d.scope == "topic"]
    section("Topic configs", [d for d in topics if d.key])
    current = None
    for d in (d for d in topics if d.key):
        if d.entity != current:
            current = d.entity
            out.append(f"  {current}")
        out.append(f"    {d.key}")
        out.append(f"      left:  {_show(d.left)}")
        out.append(f"      right: {_show(d.right)}")
    for kind, side in (("only-left", "left"), ("only-right", "right")):
        names = [d.entity for d in topics if not d.key and d.kind == kind]
        if names:
            out.extend(["", f"Topics only on the {side} ({len(names)})", "  " + ", ".join(names)])

    out.append("")
    count = len(result.differences)
    summary = f"{count} difference{'' if count == 1 else 's'}." if count else "No differences."
    if result.ignored:
        summary += f" {result.ignored} more hidden by the ignore list."
    out.append(summary)
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    modes = [bool(args.left or args.right), bool(args.within), bool(args.snapshot)]
    if sum(modes) != 1 or (modes[0] and not (args.left and args.right)):
        parser.error("use --left SOURCE --right SOURCE, or --within SOURCE, or --snapshot SOURCE")

    try:
        client_config = dict(item.split("=", 1) for item in args.client_config)
        keys = [] if args.no_default_ignores else list(DEFAULT_IGNORES)
        keys += args.ignore + (_read_ignore_file(args.ignore_file) if args.ignore_file else [])
        ignore = IgnoreList(keys=tuple(keys), topics=tuple(args.ignore_topic))

        if args.snapshot:
            text = json.dumps(load_source(args.snapshot, client_config, name_files=False).to_dict(), indent=2) + "\n"
            if args.output:
                with open(args.output, "w", encoding="utf-8") as handle:
                    handle.write(text)
            else:
                sys.stdout.write(text)
            return 0
        if args.within:
            left, right = load_source(args.within, client_config), None
            result = diff_within(left, ignore)
        else:
            left, right = load_source(args.left, client_config), load_source(args.right, client_config)
            result = diff_snapshots(left, right, ignore, args.scope, args.common_topics_only)
    except (ParseError, OSError, ValueError) as exc:
        print(f"kafka-config-diff: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # live adapter errors: unreachable broker, missing package
        print(f"kafka-config-diff: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        sys.stdout.write(render_text(result, left, right))
    return 1 if result.differences else 0


if __name__ == "__main__":
    sys.exit(main())
