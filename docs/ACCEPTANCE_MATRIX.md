# Local acceptance matrix

This matrix is the supported-host validation contract. Every fixture is
created locally by its test and is credential-free; native codecs and the
PyO3 extension are validated only in the target-matched CI job.

| Capability | Fixture and acceptance path | Expected result on this host |
| --- | --- | --- |
| Durable catalog and CLI | `lakeprism-cli/tests/acceptance.rs`; a fresh catalog under `target/lakeprism-cli-acceptance` | `init`, `register`, reopened SQL, DDL discovery, CSV output, and `EXPLAIN MEDIA` agree with the Rust catalog/session path. |
| Governed Rust SQL and lifecycle | `lakeprism-datafusion` lifecycle, permit, deadline, cancellation, audit, and mixed-workload tests | Query/cpu permits remain bounded; cancellation and expired deadlines become terminal statuses; planning failures become `Failed` rather than remaining `Pending`. |
| Local documents, tables, images, and OCR | generated minimal DOCX/PDF ZIP fixtures in `lakeprism-datafusion` and `lakeprism-documents` tests | Sections, normalized tables, and image payloads are bounded. Default OCR returns a provider-unavailable error; the registered deterministic fixture proves the contract without fabricated OCR. |
| Local video and audio | missing local media URIs in `lakeprism-datafusion` tests | This arm64 host returns explicit `NotImplemented` errors because target-matched FFmpeg is unavailable; it never emits fake frame/audio rows. CI runs actual decode fixtures on matched x86_64 Linux. |
| Transcript, semantic, hybrid, and cross-modal indexes | deterministic lineage-bearing fixtures in `lakeprism-index` and `lakeprism-datafusion` tests | Exact lineage is required; transcript search remains index-only; semantic/hybrid ranking labels and bounded cold work are deterministic. |
| Optimizer explain and cold/warm | `lakeprism-index` unit tests, `benches/cold_warm.rs`, and `scripts/check-cold-warm-benchmark.sh` | Exact persisted lineage selects warm work with zero new provider calls; changed lineage selects bounded cold work. The benchmark prints machine-readable local timings. |
| Flight SQL equivalence | `lakeprism-flight` tonic client, prepared statement, metadata, cancellation, and disconnect tests | Flight’s statement/ticket/DoGet path returns the same Arrow batches from the shared `MediaSession`; handles are opaque and ticket use is bounded. |
| Local distributed execution | `lakeprism-execution` coordinator tests | Stable source-identity partitioning and partition-order results are independent of input/completion order. Cancellation is cooperative; lease tokens reject stale workers; only explicitly idempotent tasks retry. The implementation is in-memory worker abstraction, not process supervision or remote transport. |
| Python source surface | `lakeprism-python` unit tests and `cargo check --workspace --all-features` | Python SQL-plan construction, lifecycle methods, Arrow C stream/IPC source, and optional Flight code compile. Runtime Maturin/PyO3 tests require matched Python libraries. |
| Local and S3-compatible paths | `lakeprism-storage --features s3` Wiremock tests; Delta/Iceberg/Unity optional-feature tests | Local range reads and loopback S3 mock HEAD/range/staging paths are bounded and credential-safe. Optional catalog adapters compile and their local/mock registrations pass; no live remote backend is claimed. |

## Required commands

```bash
cargo test --workspace --exclude lakeprism-python --all-targets
cargo test -p lakeprism-storage --features s3
cargo test -p lakeprism-delta --features delta-rs
cargo test -p lakeprism-iceberg --features iceberg-rust
cargo test -p lakeprism-unity --features unity
cargo check --workspace --all-features
cargo clippy --workspace --exclude lakeprism-python --all-targets -- -D warnings
bash scripts/check-cold-warm-benchmark.sh
```

Use the `native-runtime` CI job for actual FFmpeg and Maturin/PyO3 execution.
It is intentionally not substituted with cross-architecture local results.
