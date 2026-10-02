# Python feature parity

The `lakeprism` extension is a direct PyO3 façade over the same local
`MediaSession`, catalog, DataFusion, Arrow, index, and Flight implementations
used by Rust and the CLI. It does not reimplement query behavior in Python.

| Rust capability | Python surface | Feature / scope |
| --- | --- | --- |
| Local media, catalog DDL, SQL, Arrow, explain, lifecycle | `MediaRef`, `LocalCatalog`, `MediaSession`, `LazyPlan` | Default |
| Local documents, video/audio plans, transcript/semantic/hybrid indexes | `document_*`, `video_frames`, `audio_segments`, search methods | Native FFmpeg only for actual decode |
| Derived snapshots | `refresh_embedding_index`, `register_derived_snapshot` | Default |
| Configured local batch embeddings | `EmbeddingSubprocessConfig`, `MediaSession.with_embedding_subprocess` | `embedding-subprocess`; no model or executable is bundled |
| Configured local audio/video transcription | `WhisperSubprocessConfig`, `MediaSession.with_transcription_subprocess`, `MediaSession.transcribe` | `whisper-subprocess`; local `file://` only; no model or executable is bundled |
| Delta read/write | `register_delta`, `write_delta_ipc` | `delta-rs`; local `file://` only |
| Unity Catalog REST | `UnityCatalog`, `UnityQueryContext` | `unity`; OAuth callback is request-local |
| Flight SQL | `FlightServer`, `FlightClient` | `flight`; client auth callback is request-local |
| Iceberg | no direct constructor | See limits below |

## Credential boundary

`OAuthTokenSupplier(query_id, principal, catalog_identity)` and
`FlightAuthSupplier()` are callback protocols. LakePrism retains the callable,
not the callback result. Returned OAuth/Flight bearer tokens are converted
directly into a single request header and are neither modeled, serialized,
logged, nor attached to `MediaRef`, a catalog, an index, or a session.

Unity temporary credentials are vended and consumed inside
`UnityCatalog.resolve_and_register_managed_delta`; Python receives no
temporary credential payload.
Remote Delta/S3 operations remain Rust application-boundary APIs because their
write safety requires scoped credentials plus an explicit remote-write guard.

## Explicit upstream and ABI limits

* The Arrow-59-compatible Iceberg provider accepts a constructed Rust
  `iceberg::Table`; upstream does not provide a safe stable Python constructor
  for the provider/catalog/storage-client combination. Use Rust's Iceberg REST
  adapter, then pass the resulting credential-free session to Python.
* Custom `MediaResolver`, OCR, image-understanding, S3 staging, and distributed
  `Worker` implementations are Rust traits. Python exposes only the bounded
  Whisper-compatible subprocess configuration; binding arbitrary provider
  callbacks would make cancellation, bounded I/O, and credential-lifetime
  guarantees unenforceable.
* Flight SQL prepared statements support the native server's documented
  DataFusion subset; parameter binding and server-side callback authentication
  remain upstream/application-boundary concerns. Python can use request-local
  Flight bearer supply for client requests.
* This workstation has mismatched arm64 Rust and x86_64 FFmpeg/Python native
  libraries. Source/all-feature checks are valid locally; matched-host CI runs
  Maturin, PyO3, and FFmpeg runtime acceptance.

See `docs/ACCEPTANCE_MATRIX.md` for commands and the CI runtime contract.

## Wheel build policy

Use `bash scripts/build-python-wheels.sh --platform native --out dist/python`
from the repository root. The script invokes Maturin through `uv`, builds the
PyO3 `abi3-py39` extension, and rejects cross-target builds. This is required
because the extension and optional FFmpeg bindings must link and run on their
actual target host. The root README documents the supported native manylinux,
macOS x86_64/aarch64, and Windows x86_64 matrix. Standard portable wheels omit
`native-media`; build that feature separately only with
target-matched FFmpeg development libraries.

## Equivalent workflows

* **Rust:** create a `MediaSession`, register a local relation or adapter
  provider, then execute SQL or `execute_stream`.
* **CLI:** use `lakeprism init`, `register`, and `sql`; the acceptance test
  verifies its durable catalog and SQL results against the same session path.
* **Python:** see `examples/notebooks/local_and_databricks.py`; construct
  `MediaSession`, call `plan`/`sql`, and consume through PyArrow or pandas.
* **Flight:** start `FlightServer(session)`, execute through
  `FlightClient(endpoint)`, and consume the same Arrow batches. The Rust tonic
  integration test covers statement/ticket/DoGet, prepared statements,
  metadata, cancellation, and disconnect cleanup.

## Optional local embedding notebook configuration

Build a target-matched wheel with the Rust `embedding-subprocess` feature, then
configure only an application-installed absolute executable/model and a private
existing staging directory. This uses direct argv, never a shell, and does not
download or package a model:

```python
import lakeprism

embedding = lakeprism.EmbeddingSubprocessConfig(
    executable="/opt/local/bin/embedding-adapter",
    arguments=["--input", "{input}", "--output", "{output}", "--model", "{model}"],
    model_artifact="/opt/models/e5-large-v2.gguf",
    staging_directory="/var/lib/my-app/lakeprism-staging",
    max_batch_items=64,
    max_input_bytes=8 * 1024 * 1024,
    max_output_bytes=8 * 1024 * 1024,
    timeout_seconds=60,
    operator_version="embedding-subprocess-v1",
    model="e5-large-v2",
    model_version="local-2026-10",
    parameters={"normalize": "true"},
)
session = lakeprism.MediaSession.with_embedding_subprocess(embedding)
```

The executable consumes LakePrism's bounded batch JSON protocol and must
return real vectors. It is governor-bounded with cooperative
cancellation/timeout cleanup, but cannot preempt an individual child-system
call. The binding reports only redacted validation/execution categories.
