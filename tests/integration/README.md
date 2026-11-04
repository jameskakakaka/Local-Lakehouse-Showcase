# Fix 4B: opt-in Trino/Iceberg integration tests

These tests are prepared for later execution. They have not been run against
Trino as part of implementation. They do not start services or use Dagster.

## Safe offline verification

From the repository root:

```bash
env -u RUN_TRINO_ICEBERG_TESTS .venv/bin/python -W error -m unittest discover -s tests -v
```

Expected: 70 offline tests pass, four integration tests skip, zero connections.
Offline database behavior is simulated; pandas/NumPy serialization uses real scalars.

## Live execution (writes data; explicit opt-in required)

Only run after reviewing the code and authorizing isolated database writes:

```bash
RUN_TRINO_ICEBERG_TESTS=1 .venv/bin/python -W error -m unittest discover -s tests/integration -p 'test_trino_iceberg.py' -v
```

Defaults mirror the local Trino resource: localhost:8085, user admin, catalog
iceberg. Optional connection overrides are FIX4B_TRINO_HOST, FIX4B_TRINO_PORT,
and FIX4B_TRINO_USER. No credentials or service configuration are modified.

Each of four tests generates a fresh UUID and creates:

- Schema `iceberg.fix4b_<uuid>`.
- Table `iceberg.fix4b_<uuid>.yellow_trips_<uuid>`.
- Schema location `s3://warehouse/_integration_tests/fix4b/<uuid>/`.
- Table location beneath that prefix at `table/`.

The harness rejects raw/silver/gold targets and mismatched storage prefixes.
It shares production column definitions, validation, serialization, single-delivery
policy, and MERGE construction. Production's default target is unchanged.
Schema creation deliberately fails on a name collision. Before DDL, the harness
prints identifiers for retained objects and reads the actual server version.

Expected live results: four passes. Each final table contains five rows with
five unique provenance keys, exact values (including microsecond timestamps,
nulls, quotes, and duplicate-valued occurrences), and one source hash. Replays
leave values/counts unchanged. Changed hashes fail before MERGE. Partial-failure
tests verify two or four committed rows before retry converges to five.

The post-commit failure is injected in Python after real Trino fetchall(); it
models a lost acknowledgement but is not a genuine network/server crash.
Tests assume serialized writers, autocommit, reachable Trino/REST/MinIO services,
a pre-existing warehouse bucket, permission to create isolated objects, and
Iceberg format-v2 MERGE support. The installed Python client version is not the
server version. No running server version or existing production table schema
was inspected over the network.

There is no automatic DROP, data deletion, schema cleanup, or volume reset.
Objects remain after success or failure and consume a small amount of storage.
Review printed identifiers before any separately authorized manual cleanup.
