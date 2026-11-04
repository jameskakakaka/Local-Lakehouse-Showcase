# Project description for a CV

Local Lakehouse Pipeline — Python, Dagster, Iceberg, Trino, dbt, MinIO, Docker

- Built a local lakehouse pipeline separating object storage, Iceberg metadata,
  SQL compute and orchestration, with dbt trip-level and daily aggregate models.
- Implemented SHA-256 and source-row provenance with insert-only MERGE ingestion;
  tested deterministic replay, duplicate occurrences and partial-failure recovery.
- Developed 70 passing offline tests and 11 reusable dbt data-quality checks,
  plus an opt-in single-file acceptance check; kept live write tests gated.
- Externalized credentials, added security configuration regression checks and
  documented a controlled private-bucket migration with rollback conditions.

Interview boundaries: offline tests use simulations and generated fixtures;
live integration, concurrent writers, production deployment and performance
benchmarks are not established by these results. Do not describe this as a
production-hardened or fault-tolerant service.
