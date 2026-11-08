# Security design and operator gates

This is an isolated local prototype. Loopback host bindings reduce host exposure;
they do not isolate services on the shared container network. Trino and REST use
HTTP without client authentication. No production security claim is made.

## Secret inputs

.env.example contains empty required variables. If .env does not already exist,
create it privately with mode 0600 and provision values using a secret manager or
private editor. Never overwrite an existing file. Never commit it, enable shell
tracing or print resolved Compose/container/Dagster environments.

| Required inputs | Consumers |
| --- | --- |
| MINIO_ROOT_USER / MINIO_ROOT_PASSWORD | MinIO server and init client |
| POSTGRES_USER / POSTGRES_PASSWORD | PostgreSQL and REST JDBC client |
| LAKEHOUSE_S3_ACCESS_KEY_ID / LAKEHOUSE_S3_SECRET_ACCESS_KEY | REST, Trino, optional host MinIOResource |

Compose fails when inputs are missing or empty and passes them explicitly into
consumers. Shell references use $$; Trino catalog properties use native ENV
references, supported by Trino 480. Bind-mounted file contents are not expanded
by Compose. Host Dagster needs its own explicit environment injection: do not
blindly source dotenv files as shell programs. MinIOResource resolves required
credentials on use and has no fallback; the current Definitions uses Trino for
ingestion, not this optional resource. CI and offline checks require no secrets.
Use distinct least-privilege service identities when provisioning a deployment;
changing from existing identities is a separate coordinated migration.

## Existing deployment: controlled G.2B activation

1. Obtain explicit maintenance approval, stop producers, record image and volume
   identities and obtain consistent configuration, object-store, catalog and
   orchestration backups. Capture the existing bucket policy privately.
2. Preserve current credentials during externalization. PostgreSQL environment
   variables do not update users in an existing volume. Inject values privately
   and use only Compose config --quiet; keep failures private, never print the
   resolved configuration. Confirm deployed Trino version and ENV compatibility.
3. Establish authenticated object reads, REST catalog reads and Trino Raw/Silver/
   Gold SELECT baselines before activation. No such checks are implied by this
   candidate's offline results.
4. Authorize configuration activation separately. Preserve volume identities,
   objects, images and catalog state; check health and signed reads afterward.
   Restart/recreate services only within that approval. Do not run asset
   materialization or dbt models as a read-only validation step.
5. After signed access works, separately authorize removing anonymous warehouse
   policy using a privately configured administrative client. Removing the startup
   public-policy command does not revoke an existing policy or delete objects.
6. From host and an approved same-network client with no credentials, verify
   anonymous listing and GET of a known existing object return authorization
   rejection (403/AccessDenied). DNS errors, missing objects and connection errors
   are not evidence of denied authorization. Discard bodies; keep object paths
   private. Recheck signed reads and read-only dbt tests with --no-store-failures.
7. Abort on authentication failures, missing catalog objects, changed volume
   identity or regressions. Use current verified credentials with the same images and volumes; do not
   automatically reinstate an exposed credential or recreate data. Restoring a public policy
   reopens access and requires explicit approval. Resume producers after checks.

## Prior exposure response

A source-only export does not revoke previously exposed credentials or secure a
running deployment. The operator must assess prior distribution and credential
reuse privately, coordinate replacement across all consumers, verify replacement,
then revoke old identities/passwords and verify rejection. PostgreSQL password
changes require a database operation; changing Compose inputs alone is insufficient.
No credential values, private history references or original runtime records belong here.
Original historical evidence must remain private. Do not import the original Git
history into this candidate. For the separately operated local deployment, A and B were completed on
2026-11-06: exposed credentials were rejected, replacement consumers verified,
and anonymous warehouse listing/reads denied. This historical disposition is
not a live check by this source-only verifier; private evidence is not exported.

## Supply chain and scanner limits

Python versions are locked and action references are pinned. Some container image
references still float; review and pin verified versions/digests before deployment.
The included metadata-only scanner checks all allowlisted text for common secret
formats, credential assignment literals, private paths, prohibited artifacts and
symlinks. It emits file/rule metadata, never matching values. Dedicated fully redacted Gitleaks directory/history scans and an existing
Actions-log review were subsequently completed. Final release scans are recorded
in [release evidence](RELEASE_READINESS.md); heuristic checks alone cannot certify
absence of secrets.
No dependency vulnerability service was queried. Source verification does not
probe live operational services; repository visibility and hosted CI are reviewed separately.

## Template and operational limitations

The template shares one externally supplied non-root S3 pair across REST, Trino
and the optional host resource. Separate scoped identities and pinned digests
were verified operationally, but are not provisioned by this source template.
The template's container-local alias setup passes administrative values as
arguments inside the init container; use a reviewed environment-based private
launcher when command-argument isolation is required. No live initializer or
credential change is performed by publication checks.

REST/Trino frontend authentication is not configured. The existing JDBC role
retains elevated privileges. Backups are local and unencrypted. Ports bind
loopback; these choices are acceptable only for the documented isolated local
prototype and are not production assurances. Do not expose the stack publicly.
Public source publication does not publish the operator's private runtime files.
