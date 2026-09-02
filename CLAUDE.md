# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A plugin package for eccenca Corporate Memory (CMEM) that adds two workflow operators to the
Data Integration workspace: a Kafka Producer (send messages) and a Kafka Consumer (receive
messages). It is distributed on PyPI and installed into CMEM via
`cmemc admin workspace python install cmem-plugin-kafka`.

The repo is generated from the [cmem-plugin-template](https://github.com/eccenca/cmem-plugin-template)
copier template — `Taskfile.yaml`, `.gitlab-ci.yml`, `.pre-commit-config.yaml` and most of
`pyproject.toml` are template-managed. Put project-specific tasks in `TaskfileCustom.yaml`
(flattened into the `task` namespace) instead of editing `Taskfile.yaml`.

## Commands

Development is driven by [task](https://taskfile.dev/); `task` alone lists everything.

```shell
task check              # linters + full test suite (what CI runs)
task check:linters      # ruff + mypy + deptry + trivy
task check:ruff         # lint + format check
task check:mypy         # type check (mypy -p tests -p cmem_plugin_kafka)
task format:fix         # ruff format + autofix
task check:pytest       # pytest with coverage, memray, junit/html reports into dist/
task build              # sdist + wheel + dist/requirements.txt
task install            # build, then install the package into the configured CMEM
task kafka:start        # start local Kafka via docker/docker-compose.yml (custom task)
task kafka:stop
```

Single test / subset (Taskfile has no target for this — call pytest directly):

```shell
poetry run pytest tests/test_producer.py::test_execution_plain_kafka
poetry run pytest -k tombstone
```

## Test environment

Almost every test is an integration test. Tests are decorated with `needs_cmem`
(`tests/utils.py`), which skips on missing `CMEM_BASE_URI`. A green run with everything
skipped is not a passing run — check the skip count.

CMEM must be reachable:

```shell
eval $(cmemc -c my-cmem config eval)   # CMEM credentials into the environment
```

Kafka needs no manual setup: the session-scoped `kafka_broker` fixture in `tests/conftest.py`
starts a `testcontainers`-managed single-node KRaft broker automatically for any test module
that needs one (via `pytestmark = pytest.mark.usefixtures("kafka_broker")`), and tears it down
at the end of the run — this just needs a running local Docker daemon, nothing else. To poke at
a broker by hand instead (e.g. with a CLI tool), `task kafka:start`/`task kafka:stop` still
bring up `docker/docker-compose.yml` on `localhost:9093`, independent of the test suite.

`pytest-dotenv` loads `.env` (git-ignored) for CMEM connection variables.

Tests create and delete real CMEM projects (`kafka_test_project`, `kafka_consumer_project`, …)
and real Kafka topics (the `topic` fixture in `tests/conftest.py` makes a randomly suffixed
`cmem_*` topic per test). A crashed run can leave both behind.

On ARM Macs, `confluent-kafka` needs librdkafka from Homebrew plus `CPATH=/opt/homebrew/include`
and `LIBRARY_PATH=/opt/homebrew/lib` before `poetry install` (see README.md).

## Architecture

Three layers, deliberately separated so message-format logic is testable apart from both Kafka
and CMEM:

1. **`workflow/producer.py` / `workflow/consumer.py`** — the `@Plugin`-decorated
   `WorkflowPlugin` subclasses. These own the user-facing parameter declarations (also the
   plugin's rendered documentation), build the confluent config dict, and wire everything up in
   `execute()`. Ports are computed in `_set_ports()` from whether `message_dataset` is set: a
   producer with a dataset has no input port, a consumer with a dataset has no output port;
   otherwise the consumer exposes a `FixedSchemaPort` with the entity schema from
   `KafkaEntitiesDataHandler.get_schema()`.
2. **`kafka_handlers.py`** — format handlers. `KafkaDataHandler` defines `_split_data`
   (bytes/entities → `KafkaMessage`s, producer direction) and `_aggregate_data`
   (`KafkaMessage`s → bytes/entities, consumer direction). Subclasses:
   `KafkaJSONDataHandler`, `KafkaXMLDataHandler` (both `KafkaDatasetHandler`, i.e. usable as a
   context manager that commits and closes the consumer on clean exit) and
   `KafkaEntitiesDataHandler` (DI entities in/out, no dataset).
3. **`utils.py`** — `KafkaProducer` / `KafkaConsumer` wrappers over confluent-kafka,
   CMEM dataset access, and `DatasetParameterType` (autocompletion for the dataset dropdown,
   filtered to `xml,json`).

`constants.py` holds only timeouts, choice dictionaries and the long markdown description
strings shown in the plugin UI.

### Streaming is the point

Producing and consuming must work on datasets far larger than memory, so nothing on the data
path is materialized:

- Producer: `get_resource_from_dataset()` returns an unread streaming `httpx` response;
  XML is fed to `ElementTree.iterparse` through `as_file_object()` (a `ChunkReader` adapting
  the chunk iterator to a file object), JSON through `json_stream`.
- Consumer: `_aggregate_data()` is a generator yielding byte chunks, and
  `post_resource()` in `consumer.py` PUTs it as a streamed request body. That request is
  hand-rolled rather than using `client.datasets.post_file_resource`, which would read the
  whole resource into memory first — keep it that way.

Preserve generator/iterator semantics when touching these paths, and be careful with anything
that would `.read()`, `list()`, or `json.loads()` a whole payload. `tests/test_perf_messages.py`
guards this with memray.

### CMEM access

All communication with Corporate Memory goes through `cmem_client.client.Client`, obtained via
`Client.from_context(context=...)` — this replaced the older `cmempy` API, so ignore
`cmempy`-style patterns in older docs. Note `tests/utils.py::get_client()` creates a fresh
client per operation on purpose: a pooled connection idling while messages are produced gets
closed by the server before reuse.

### Message conventions

- XML: exactly one child element per `<Message>`; a `key` attribute becomes the Kafka key and
  `tombstone="true"` produces a null-valued record. More than one child is an error the handler
  logs and skips.
- JSON: array of `{"message": {"key", "headers", "content", "tombstone"}}` objects.
- Entities: consumed messages map to a flat schema of `key`, `content`, `offset`,
  `ts-production`, `ts-consumption`; entity URIs are `urn:hash::sha256:<sha256 of key>`.

## Conventions

- Ruff with `select = ["ALL"]` and a 100-char line length; see `pyproject.toml` for the ignore
  list. Docstrings are required on everything, so match the existing style rather than dropping
  them.
- mypy runs over both `tests` and the package; `warn_return_any` is on.
- Versions come from git tags via poetry-dynamic-versioning — the `0.0.0` in `pyproject.toml`
  is a placeholder, never bump it by hand. Record changes in `CHANGELOG.md`
  (Keep a Changelog + SemVer).
- `deptry` runs in CI, so a new import needs a matching dependency entry.