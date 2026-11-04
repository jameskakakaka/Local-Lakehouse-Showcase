# Showcase candidate release assessment

Status: CANDIDATE_READY_FOR_MANUAL_REVIEW (subject to the verification results
recorded in VERIFICATION_RESULTS.json). This source-only export has no Git
history, runtime databases, credentials or datasets. It does not establish that
historical credentials are revoked or existing infrastructure is secure.

| Gate | Assessment |
| --- | --- |
| Source completeness | Explicit allowlist and SHA-256 manifest; test and dbt source retained. |
| Privacy | No source datasets, database records, private audits or local credential files exported. |
| Offline correctness | Verifier runs synthetic tests, dbt compilation and actual Dagster Definitions loading. |
| Reproducibility | Lockfile and CI retained; existing dependencies reused for review. Fresh installation and hosted CI pending. |
| Secret risk | Metadata-only heuristic scan; no dedicated scanner installed. Prior exposure response pending. |
| License | MIT source license retained. Tests generate synthetic fixtures. No external datasets or media redistributed. |
| Dataset rights | Runtime fetches public NYC Taxi source; upstream terms/attribution and any future dataset redistribution require operator review. No network legal review performed. |
| Runtime security | Unverified; internal HTTP, shared network, floating images and G.2B maintenance gates remain. |
| Publication | Manual approval and prior credential exposure response required. Nothing published. |

Before publication: privately complete credential exposure response, perform a
final independent secret scan with an approved tool, review the file manifest,
confirm authorship/license permissions, and test a fresh private CI candidate.
Any future screenshots or data bundles need separate privacy/rights review.
Live demonstration requires the separately authorized procedure in SECURITY.md.
Do not bring original Git history or runtime state into this directory.
