import io
import json
from pathlib import Path

import pytest

from kafka_config_diff.cli import main

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
STAGING, PROD = str(EXAMPLES / "staging.json"), str(EXAMPLES / "prod.json")


def test_diff_two_snapshots_exits_1_and_lists_differences(capsys):
    assert main(["--left", STAGING, "--right", PROD]) == 1
    out = capsys.readouterr().out
    assert "left:  staging (3 brokers, 4 topics)" in out
    assert "right: 16 (brokers 11, 12); 8 (broker 13)" in out
    assert "Topics only on the right (1)\n  payments.dlq" in out
    assert "9 differences. 5 more hidden by the ignore list." in out
    assert "advertised.listeners" not in out


def test_no_drift_exits_0_text_dump_against_baseline(capsys):
    assert main(["--left", str(EXAMPLES / "staging-dump"), "--right", STAGING]) == 0
    assert "No differences." in capsys.readouterr().out


def test_ignore_options(capsys):
    args = ["--left", STAGING, "--right", PROD, "--json", "--ignore-file", str(EXAMPLES / "ignore.txt"),
            "--ignore", "min.insync.*", "--ignore", "retention.ms", "--ignore-topic", "load-*",
            "--ignore-topic", "payments.dlq"]
    assert main(args) == 1
    data = json.loads(capsys.readouterr().out)
    assert [d["key"] for d in data["differences"]] == ["num.io.threads"]
    assert data["drift"] is True
    assert main(args + ["--ignore", "num.io.threads"]) == 0
    assert json.loads(capsys.readouterr().out)["drift"] is False


def test_scope_and_common_topics_only(capsys):
    main(["--left", STAGING, "--right", PROD, "--scope", "topics", "--common-topics-only", "--json"])
    data = json.loads(capsys.readouterr().out)
    assert {d["entity"] for d in data["differences"]} == {"__consumer_offsets", "orders"}


def test_within(capsys):
    assert main(["--within", PROD]) == 1
    out = capsys.readouterr().out
    assert "Comparing the 3 brokers of prod with each other" in out
    assert "16  on brokers 11, 12" in out
    assert main(["--within", STAGING]) == 0


def test_snapshot_from_text_dump_then_diff(tmp_path, capsys):
    baseline = tmp_path / "baseline.json"
    assert main(["--snapshot", str(EXAMPLES / "staging-dump"), "-o", str(baseline)]) == 0
    assert set(json.loads(baseline.read_text())) == {"brokers", "topics"}
    assert main(["--left", str(baseline), "--right", STAGING]) == 0
    assert main(["--snapshot", STAGING]) == 0
    assert json.loads(capsys.readouterr().out.split("No differences.\n")[-1])["label"] == "staging"


def test_stdin_source(monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO((EXAMPLES / "staging-dump" / "topics.txt").read_text()))
    assert main(["--left", "-", "--right", STAGING, "--scope", "topics"]) == 0
    assert "left:  stdin (0 brokers, 4 topics)" in capsys.readouterr().out


def test_errors_exit_2(capsys):
    assert main(["--left", "no-such-file.json", "--right", PROD]) == 2
    assert "no such file" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        main(["--left", STAGING])
    with pytest.raises(SystemExit):
        main(["--within", STAGING, "--snapshot", STAGING])


def test_host_port_source_uses_the_live_adapter(monkeypatch, capsys):
    from kafka_config_diff import Snapshot, live

    calls = []

    def fake(bootstrap, config):
        calls.append((bootstrap, config))
        return Snapshot(brokers={"1": {"num.io.threads": "8" if "staging" in bootstrap else "16"}}, label=bootstrap)

    monkeypatch.setattr(live, "fetch_snapshot", fake)
    assert main(["--left", "staging:9092", "--right", "prod:9092", "-X", "security.protocol=SSL"]) == 1
    assert calls == [("staging:9092", {"security.protocol": "SSL"}), ("prod:9092", {"security.protocol": "SSL"})]
    assert "left:  8 (broker 1)" in capsys.readouterr().out
