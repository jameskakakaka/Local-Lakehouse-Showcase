# Development and demonstration

Use Linux/WSL, Python 3.12 and uv 0.13.6. The project declares Python >=3.10;
other versions/platforms are not verified. Source acquisition requires atomic
same-directory hard links. Run commands from the repository root.

## Setup

```bash
uv sync --locked --python 3.12
```

Installation needs network access or a populated cache. The verification workload
is offline. A prior clean Python 3.12 locked installation and the initial root-commit
hosted CI run passed. Final local results and exact-commit hosted CI are tracked
separately in [release evidence](RELEASE_READINESS.md).

```bash
env -u RUN_TRINO_ICEBERG_TESTS uv run --no-sync python -W error -m unittest discover -s tests -v
# The comprehensive verifier deliberately accepts pristine source exports only.
# Reuse this checkout's locked interpreter, but exclude Git metadata and caches.
VERIFY_EXPORT=$(mktemp -d)
git archive HEAD | tar -x -C "$VERIFY_EXPORT"
env -u RUN_TRINO_ICEBERG_TESTS uv run --no-sync python "$VERIFY_EXPORT/scripts/verify_candidate.py"
```

The comprehensive verifier runs on the committed HEAD export above, not on
uncommitted edits. Release preparation separately verifies the final intended
working-tree bytes against the manifest before committing. The verifier blocks
outbound Python connections, runs unittest,
compiles/parses dbt with introspection disabled, validates Dagster lineage,
validates Compose using dummy inputs, and checks the export manifest. It never
loads .env. Temporary logs and outputs are outside the candidate; it briefly
creates an ignored dbt manifest for actual Definitions loading and removes only
that manifest afterward. If a default dbt target already exists, run this export
verification on a fresh candidate instead. This prevents touching local evidence.
Expected unittest result: 70 passes, four explicitly skipped live cases. PyYAML
comes from Dagster's locked dependencies; no separate test framework is needed.

## Synthetic demo

```bash
env -u RUN_TRINO_ICEBERG_TESTS uv run --no-sync python -W error -m unittest discover -s tests -p test_ingestion_idempotency.py -v
```

Explain the generated records in tests/test_ingestion_idempotency.py: two equal
payloads occupy distinct source positions; identical-file replay preserves them;
an injected failure before or after a batch commit converges on retry. This is
a deterministic cursor simulation, not a live Iceberg transaction demonstration.
Other tests generate their own Parquet bytes; no dataset or secrets are required.

## dbt checks

```bash
DBT_SEND_ANONYMOUS_USAGE_STATS=false uv run --no-sync dbt parse --project-dir dbt_project --profiles-dir lakehouse_pipeline/trino
DBT_SEND_ANONYMOUS_USAGE_STATS=false uv run --no-sync dbt compile --project-dir dbt_project --profiles-dir lakehouse_pipeline/trino --no-introspect --no-populate-cache
DBT_SEND_ANONYMOUS_USAGE_STATS=false uv run --no-sync dbt ls --project-dir dbt_project --profiles-dir lakehouse_pipeline/trino --resource-type test --output name
```

The local_acceptance target discovers 11 reusable tests. Add
`--vars '{dq_local_acceptance: true}'` for 12, including single-file coverage.
The dev target preserves prefixed schemas and does not enable that coverage test.
Parsing and compilation do not execute Trino SQL or validate existing live tables.
Generated artifacts are ignored; do not overwrite retained runtime evidence.

## Optional live demonstration

Read [SECURITY.md](SECURITY.md) first and use a disposable, isolated environment. Supply private
credentials explicitly; Compose's .env does not export them to host Dagster.
Run dbt parse before loading Dagster definitions outside dagster dev. A full
pipeline run downloads public taxi data, creates schemas/tables and writes the
first 10,000 rows plus transformations. It is not part of offline verification.
The four live acceptance cases also write isolated objects and retain them;
see [integration instructions](../tests/integration/README.md) before separately
authorizing execution.
Never reuse an existing persistent environment for a demonstration without a
maintenance plan, backups and explicit permission.

## Deployment scope

The candidate keeps a minimal six-input local template. Use a privately
provisioned non-root, warehouse-scoped S3 identity; it does not create IAM users.
The separately verified operational deployment has per-consumer identities,
pinned images and a private launcher. Those private settings are not exported.
Do not run this template against existing fixed container names or volumes
without a separately approved maintenance plan. Existing PostgreSQL passwords
must be changed at the database level, not by editing environment inputs alone.
