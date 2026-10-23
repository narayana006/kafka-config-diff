# kafka-config-diff

Compare Kafka broker and topic configuration between two clusters, against a baseline file kept in git, or between the brokers of a single cluster, and exit non-zero when they have drifted.

**Status: early but usable (v0.1.0).** All three comparisons, the ignore list, JSON output and the exit code work today. Sources can be JSON snapshots, saved `kafka-configs --describe` output, or live clusters. See [Limitations](#limitations) for what is not covered.

Staging and production drift apart one `kafka-configs --alter` at a time, and the difference tends to be found during an incident. This tool prints only the settings that differ, leaving out the ones that are supposed to (`node.id`, `listeners`, `log.dirs` and so on).

## Install

Python 3.10 or newer. The core has no dependencies.

```bash
git clone https://github.com/narayana006/kafka-config-diff.git
cd kafka-config-diff
pip install .            # compare snapshots and saved kafka-configs output
pip install ".[live]"    # also installs confluent-kafka to read live clusters
```

## Quick start (no cluster needed)

`examples/` contains snapshots of a made-up staging and production cluster, three brokers each.

```bash
kafka-config-diff --left examples/staging.json --right examples/prod.json
```

```text
left:  staging (3 brokers, 4 topics)
right: prod (3 brokers, 4 topics)

Broker configs (4)
  auto.create.topics.enable
    left:  true (all 3 brokers)
    right: false (all 3 brokers)
  log.retention.hours
    left:  72 (all 3 brokers)
    right: 168 (all 3 brokers)
  min.insync.replicas
    left:  1 (all 3 brokers)
    right: 2 (all 3 brokers)
  num.io.threads
    left:  8 (all 3 brokers)
    right: 16 (brokers 11, 12); 8 (broker 13)

Topic configs (3)
  __consumer_offsets
    min.insync.replicas
      left:  1
      right: 2
  orders
    min.insync.replicas
      left:  1
      right: 2
    retention.ms
      left:  86400000
      right: 604800000

Topics only on the left (1)
  load-test

Topics only on the right (1)
  payments.dlq

9 differences. 5 more hidden by the ignore list.
```

The exit status is 1 because differences were found. The five hidden differences are per-broker settings on the built-in ignore list.

The production snapshot also has one broker that missed a change. Compare the brokers of one cluster with each other:

```bash
kafka-config-diff --within examples/prod.json
```

```text
Comparing the 3 brokers of prod with each other

Broker settings that differ between brokers (1)
  num.io.threads
    16  on brokers 11, 12
    8  on broker 13

1 difference. 4 more hidden by the ignore list.
```

## Usage

```text
kafka-config-diff --left SOURCE --right SOURCE     compare two clusters, or a cluster and a baseline
kafka-config-diff --within SOURCE                  compare the brokers of one cluster with each other
kafka-config-diff --snapshot SOURCE [-o FILE]      write a JSON snapshot (a baseline) and exit
```

A `SOURCE` is one of:

- a JSON snapshot file (format below),
- a text file holding `kafka-configs --describe` output,
- a directory of such files, which are merged,
- `-` for standard input,
- `HOST:PORT` of a live cluster (anything that is not an existing path).

### Two live clusters

```bash
kafka-config-diff --left staging:9092 --right prod:9092
```

Reads every broker's and every topic's configuration with read-only Admin API calls (Metadata and DescribeConfigs). `-X key=value` passes client properties such as `-X security.protocol=SASL_SSL`; the same properties are used for both sides. If the clusters need different credentials, snapshot each one separately and compare the files.

### A cluster against a baseline in git

```bash
kafka-config-diff --snapshot prod:9092 -o baseline/prod.json     # once, then commit it
kafka-config-diff --left baseline/prod.json --right prod:9092    # in CI or on a schedule
```

Exit status `0` means no drift, `1` means drift, `2` means the comparison could not be made.

### Without network access to the cluster: `kafka-configs` output

Dump with the tool that ships with Kafka, on a host that can reach the brokers, and compare the files anywhere:

```bash
kafka-configs.sh --bootstrap-server broker:9092 --describe --all --entity-type brokers --entity-name 1 > dump/broker-1.txt
kafka-configs.sh --bootstrap-server broker:9092 --describe --all --entity-type topics > dump/topics.txt
kafka-config-diff --left dump/ --right baseline/prod.json
```

`examples/staging-dump/` shows the layout; `kafka-config-diff --left examples/staging-dump --right examples/staging.json` reports no differences. Use `--all` on both sides, or on neither: without it `kafka-configs` prints only dynamic overrides, and comparing overrides on one side with full effective configs on the other reports every default as a difference.

### Ignoring expected differences

Ignored by default (config names, `*` is a wildcard):

```text
broker.id  node.id  broker.rack  listeners  advertised.listeners  log.dir  log.dirs  metadata.log.dir
controller.quorum.voters  controller.quorum.bootstrap.servers  zookeeper.connect
*ssl.keystore.location  *ssl.truststore.location
```

| Option | Effect |
| --- | --- |
| `--ignore GLOB` | Also ignore config names matching the pattern. Repeatable. |
| `--ignore-file FILE` | One pattern per line, `#` starts a comment. |
| `--no-default-ignores` | Drop the built-in list. |
| `--ignore-topic GLOB` | Leave out topics by name, e.g. `--ignore-topic '__*'`. Repeatable. |
| `--common-topics-only` | Do not report topics that exist on one side only. |
| `--scope brokers\|topics` | Compare only broker or only topic configs. |

`examples/ignore.txt`:

```text
# Settings we accept as different between staging and prod
log.retention.hours     # staging keeps less data on purpose
auto.create.topics.enable
```

```bash
kafka-config-diff --left examples/staging.json --right examples/prod.json --ignore-file examples/ignore.txt
```

The last line of the report always says how many differences the ignore list hid, so an over-broad pattern does not go unnoticed.

### How brokers are compared

Broker ids rarely match between clusters, so brokers are not paired up. For each setting the tool compares the set of values in use on the left with the set in use on the right, and shows which brokers hold which value (`16 (brokers 11, 12); 8 (broker 13)`). `--within` reports every setting that is not identical on all brokers of one cluster. Topics are matched by name.

Sensitive settings (passwords, JAAS configs) are never returned by Kafka; they appear as `null` on both sides and never show up as a difference.

### JSON

`--json` prints:

```json
{
  "differences": [
    {"scope": "topic", "kind": "changed", "entity": "orders", "key": "retention.ms",
     "left": "86400000", "right": "604800000"}
  ],
  "ignored": 5,
  "notes": [],
  "drift": true
}
```

`scope` is `broker`, `broker-default` (cluster-wide dynamic defaults, from `kafka-configs` text input) or `topic`. `kind` is `changed`, `only-left`, `only-right` or `inconsistent` (`--within`). For `broker` scope, `left` and `right` are lists of `{"value": ..., "brokers": [...]}`.

Snapshot format, as written by `--snapshot`:

```json
{
  "label": "prod",
  "brokers": {"11": {"num.io.threads": "16", "ssl.keystore.password": null}},
  "topics": {"orders": {"retention.ms": "604800000"}}
}
```

All keys are optional. A baseline that holds only `topics` is compared on topics only, and the report says that brokers were skipped.

## Features

- [x] Diff broker configs across brokers in one cluster (`--within`)
- [x] Diff two clusters
- [x] Diff against a baseline file kept in git (`--snapshot` writes it)
- [x] Ignores settings that are expected to differ, with your own additions
- [x] Exit code for CI and scheduled drift checks
- [x] Input from `kafka-configs --describe` text, JSON snapshots, or live clusters
- [ ] Ignore rules scoped to a single topic or broker
- [ ] Reading `server.properties` files

## Limitations

- **Values are compared as strings.** `168` hours in `log.retention.hours` and the equivalent `log.retention.ms` are different settings to this tool, and list values in a different order count as different.
- **No per-broker pairing across clusters.** See "How brokers are compared". If both clusters are internally inconsistent in the same way, no difference is reported between them; `--within` catches that.
- **Effective values, not their origin.** A snapshot does not record whether a value is a default, a static `server.properties` entry or a dynamic override. Two clusters on different Kafka versions will show differences that are only changed defaults or settings that exist in one version.
- **Only brokers and topics.** Client quotas, user (SCRAM) settings, group configs, ACLs and KRaft controller-only nodes are not compared.
- **Ignore patterns apply to config names in every scope**; a pattern cannot be limited to one topic.
- **Output of `kafka-configs --zookeeper`** (removed from Kafka long ago) is not parsed.
- **Live mode** was tested with confluent-kafka 2.16 against two single-node Apache Kafka 4.4.0 brokers (KRaft, PLAINTEXT), including a cluster-to-cluster diff and a baseline round trip. A snapshot parsed from real `kafka-configs.sh --describe --all` output was verified to be identical to the snapshot read through the Admin API. Multi-broker clusters and secured listeners have not been tested live; multi-broker behaviour is covered by tests on fixture data.

## Contributing

Issues and pull requests are welcome, especially `kafka-configs` output from Kafka versions that fail to parse (redact hostnames first). Run `pip install -e ".[test]" && pytest` before sending a change.

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

## License

[MIT](LICENSE)
