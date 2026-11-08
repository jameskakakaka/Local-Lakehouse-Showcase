# Portfolio description

## Short technical description

Local Lakehouse Pipeline is a Python data infrastructure prototype combining
Dagster, Iceberg, Trino, dbt, MinIO and a PostgreSQL-backed REST catalog. It
validates a source Parquet delivery, uses file/row provenance for serialized
insert-only replay, and reconciles trip-level filters with daily aggregates.
The controlled acceptance dataset contains 10,000 source occurrences, 9,737
eligible Silver rows and two Gold groups.

## Three CV bullets

- Built a five-asset Dagster lakehouse across Iceberg, MinIO, a REST catalog,
  PostgreSQL and Trino/dbt; validated a 10,000-occurrence prototype producing
  9,737 filtered rows and two daily aggregate groups.
- Implemented SHA-256/source-position keys and insert-only MERGE ingestion;
  tested ordered replay, duplicate-valued occurrences and recovery from failures
  before submission and after simulated batch acknowledgement loss.
- Developed 70 offline Python tests and 11 reusable dbt checks plus one opt-in
  acceptance assertion, with locked Python 3.12 dependencies and offline GitHub
  Actions CI; the controlled live dbt acceptance suite passed all 12 checks.

## Positioning by role

| Role | Discuss |
| --- | --- |
| Data Engineer Internship | SQL transformations, Parquet validation, source contracts and measurable reconciliation. |
| Data Infrastructure Engineer | Object/catalog/compute boundaries, atomic no-clobber acquisition, retry semantics and reproducible CI. |
| Data/ML Infrastructure Engineering | Provenance, lineage, reusable data-quality assertions and reproducible inputs; no model training or inference is implemented. |

## Engineering decisions

| Decision | Reason and limit |
| --- | --- |
| Atomic same-directory hard-link publication | A completed validated download cannot overwrite an existing cache; platform support is required. |
| File hash plus absolute source position | Equal payloads at different positions remain distinct; one immutable delivery is supported. |
| Insert-only batch MERGE | Serialized replay converges after partial failure; there is no global uniqueness constraint or all-batch transaction. |
| Multiset Silver and recomputed Gold checks | Reconcile values and multiplicity rather than hiding duplicates with DISTINCT. |
| Separate offline and live checks | Synthetic tests need no credentials; write integration tests require explicit opt-in and retain their isolated objects. |

## Interview boundaries

Offline results are simulations and source validation. The 2026-11-06 operational
acceptance/recovery evidence is from a separately configured local deployment;
it is not a new full materialization of this publication candidate. Four live
write tests remain skipped. No full-month ingestion, benchmark, general
concurrent-writer guarantee or production-hardening claim is made. Frontend
authentication, elevated database privileges and local unencrypted backups remain
disclosed limitations. See [release evidence](RELEASE_READINESS.md).
