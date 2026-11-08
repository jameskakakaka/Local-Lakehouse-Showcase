# Release evidence and boundaries

The publication candidate has independent source-only ancestry. Original Git
objects, operational credentials, data files, databases and private recovery
artifacts are excluded. The local publication tree is 50 files: 48 SHA-256/byte
manifest entries and two explicitly generated metadata files.

| Evidence layer | Result and scope |
| --- | --- |
| Final local verification | Recorded in VERIFICATION_RESULTS.json: Python tests, offline dbt parse/compile/discovery, schema routing, Dagster lineage, dummy Compose configuration and manifest/link checks. |
| Reproducibility | uv.lock and pinned CI actions; a clean Python 3.12 locked installation previously passed. The final verification records its locked interpreter and lock consistency. |
| Dedicated scanning | Gitleaks 8.30.1, complete redaction, no suppression baseline: candidate directory/history and existing hosted Actions logs reviewed. Final scan reports remain outside the repository. |
| Initial hosted CI | Run 41825427315 succeeded on root commit 649ad267291fca4b6fcefa39918ebaf3b3fd6b3a. This is not CI evidence for later local commits. |
| Final hosted CI | Requires an explicitly approved private push, then a successful run on the exact intended SHA. No older run substitutes for that gate. |
| Controlled live data, 2026-11-06 | Existing Raw/Silver/Gold counts 10000/9737/2; provenance/reconciliation and 12 dbt acceptance tests passed. All 11 tables and 204 object hashes verified. |
| Operational security, 2026-11-06 | A_COMPLETE and B_COMPLETE: old root/S3/PostgreSQL pairs rejected; replacement consumers worked; anonymous warehouse access denied. No full Dagster materialization ran in that verification. |
| Optional live write tests | Four tests remain gated and unexecuted; do not confuse them with read-only dbt acceptance. |
| Rights | MIT copyright retained. Owner confirmed publication rights and commit-author metadata disclosure on 2026-11-08. No third-party dataset, media or vendored tool source is included. |

The operational results belong to a separately configured deployment with
independent scoped identities. This candidate's minimal deployment template is
not that private runtime configuration. No live credential, policy, database or
volume changes are part of final publication verification.

## Remaining release gates

1. Complete final local verification and redacted directory/history scans.
2. Obtain explicit owner authorization before pushing the exact local commits to
   the private Showcase remote; require hosted CI success for the resulting SHA.
3. Recheck source/history, existing Actions logs/artifacts, rights and visibility,
   then obtain separate approval before making the reviewed repository public.

Neither a green local suite nor the initial CI run is publication permission.
The original operational repository must remain private and cannot be used as a
publication source. The private release report retains exact SHAs and gates.

## Disclosed limitations

Only the first 10,000 ordered occurrences from one immutable January 2024 file
are ingested. Batch commits are separate; writers must be serialized. Full-month,
concurrent-writer and production-scale guarantees are not established. REST and
Trino frontend authentication is absent, the local JDBC role remains privileged,
and backups are local and unencrypted. These are bounded local-development
limitations, not resolved production-hardening claims.

The [NYC TLC source page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
provides the upstream delivery and notices. The code license does not license
the upstream data. Future data/media distribution requires its own review.
