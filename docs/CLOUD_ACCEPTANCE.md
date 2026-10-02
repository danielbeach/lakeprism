# Opt-in Databricks cloud acceptance contract

Cloud validation is intentionally manual (`workflow_dispatch`) and disabled by
default. It never runs on pull requests, forks, or ordinary release jobs.

Run `.github/workflows/cloud-acceptance.yml` only in a protected repository
environment containing these **ephemeral CI secrets**:

| Input | Contract |
| --- | --- |
| `DATABRICKS_HOST` | HTTPS workspace origin, without query, fragment, or credentials. |
| `DATABRICKS_CLIENT_ID` | Databricks service-principal OAuth client ID, injected only as an environment secret. |
| `DATABRICKS_CLIENT_SECRET` | Service-principal OAuth client secret, injected only as an environment secret. The script exchanges it for an in-memory `all-apis` token. |
| `DATABRICKS_TABLE` | Optional Unity three-part table name used for an authenticated metadata read. |
| `DATABRICKS_VOLUME_PATH` | Optional `/Volumes/catalog/schema/volume/path` checked only as a governed path shape. |
| `LAKEPRISM_LIVE_CLOUD` | Must equal `1`; this prevents accidental shell execution. |

The acceptance script verifies host shape, exchanges the service principal's
client credentials only in process for the Unity `all-apis` scope, and performs
one authenticated Unity table metadata `GET` when `DATABRICKS_TABLE` is
supplied. It neither downloads table data nor requests temporary object-store
credentials. Client secrets and access tokens are never echoed, written to
files, embedded in URLs, persisted in catalogs/manifests, or passed to the
Python binding.

This is a protected **workspace-authentication contract**, not a data-plane
test. S3/Unity-managed Delta scans are exercised only by an operator-run
acceptance environment with a disposable table and narrowly scoped bucket
policy; no CI test receives live S3 credentials. The Rust wiremock suite
validates the credential-envelope translation without real values.

Remote managed Delta writes are experimental. They require an explicit
`ExperimentalRemoteWriteGuard::SingleWriter` (an external coordinator ensures
one active writer) or `ConditionalPutVerified` (the exact delta-rs/object-store
backend has been live-validated for conditional Delta-log creation). Neither
guard proves safety at runtime. LakePrism makes no Unity remote `MERGE`,
checkpoint, concurrent-writer, or incremental-write guarantee.
