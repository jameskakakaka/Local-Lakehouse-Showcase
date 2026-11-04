# Data quality and reconciliation

All tests are read-only failure-row queries. No packages are required.
Existing model SQL and business rules are unchanged. No pre-existing dbt tests
were configured at inspection time.

## Test inventory

| File | Test names | Assertion |
| --- | --- | --- |
| `../models/sources.yml` | `source_not_null_raw_taxi_yellow_trips_source_file_sha256`, `source_not_null_raw_taxi_yellow_trips_source_row_number` | Provenance is populated. |
| `../models/sources.yml` | `raw_sha256_format`, `raw_nonnegative_position`, `raw_unique_provenance` | 64 hexadecimal characters, zero-based/nonnegative positions, unique hash/position pair. |
| `../models/data_quality.yml` | `silver_filters` | Both implemented positive-value filters hold, including rejection of NULL. |
| `../models/data_quality.yml` | `gold_unique_group_keys`, `gold_valid_group_keys` | Unique date/weekday pair; weekday equals Trino day_of_week(date), NULL-safe. |
| `silver_row_count.sql` | `silver_row_count` | Silver count equals eligible Raw count. |
| `silver_occurrence_reconciliation.sql` | `silver_occurrence_reconciliation` | All 12 transformed columns and occurrence multiplicities match eligible Raw, with NULL-safe grouping. Signed count reports missing minus extra occurrences. |
| `gold_reconciliation.sql` | `gold_reconciliation` | Full outer, NULL-safe group coverage and independent recomputation of all eight metrics using current formulas. Reports keys, metric, expected value, actual value. |
| `raw_acceptance_coverage.sql` | `raw_acceptance_coverage` | Opt-in single-file prototype: exactly 10,000 distinct positions spanning 0–9999. Requires local_acceptance target and dq_local_acceptance=true. |

Generic implementations are in `generic/dq_assertions.sql`; not_null is built in.

## Semantics and limits

Silver omits provenance: comparison is a multiset, not a source identity join.
Equal-valued occurrences are preserved, but swapping their hidden provenance
cannot be detected. Values are compared exactly, including floating values,
because staging passes them through without arithmetic. Concurrent refreshes
can cause reconciliation failures; test a stable set of all three tables.

Gold allows the NULL date/weekday group produced by staging. Negative duration,
nullable passengers/tips/revenue, and unusual pickup dates are not rejected:
model SQL does not prohibit them. SUM/AVG ignore NULLs and all-NULL inputs stay
NULL. Counts and passenger sums compare exactly. Rounded floating metrics use
absolute tolerance 0.000001; this tolerates sub-cent accumulation noise but
cannot prevent a false positive if parallel aggregation crosses a half-cent
rounding boundary. A full cent difference fails. Gold row order is irrelevant.

The optional acceptance check intentionally fails for multiple files or larger
loads. It validates coverage, not the actual source file contents or hash
correctness. General tests allow empty, consistently empty layers.

## Live tests (repository root; existing tables required)

```bash
.venv/bin/dbt test --project-dir dbt_project --profiles-dir lakehouse_pipeline/trino --target local_acceptance --no-store-failures
```

Include prototype-only coverage when appropriate:

```bash
.venv/bin/dbt test --project-dir dbt_project --profiles-dir lakehouse_pipeline/trino --target local_acceptance --no-store-failures --vars '{dq_local_acceptance: true}'
```

Do not enable store_failures: persisting failure relations would write to the database.
Offline compile checks dbt/Jinja compilation, not Trino execution or live schemas.
