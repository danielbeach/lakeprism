# LakePrism

LakePrism is a local-first Rust query engine with a Python API for querying
media, documents, Parquet, and derived features through SQL and Apache Arrow.
It is designed for notebooks and small governed services where execution,
credentials, and data locality must remain explicit.

**Status: alpha.** The Python extension is usable for local development and
the release workflow is in place, but version `0.1.0` has not been published
to PyPI. See [Release readiness](#release-readiness) before depending on it.

## Install

Until the first PyPI release exists, build and install a wheel from a checkout:

```bash
git clone https://github.com/danielbeach/lakeprism.git
cd lakeprism
bash scripts/build-python-wheels.sh --platform native --out dist/python
python -m pip install dist/python/lakeprism-*.whl
```

The wheel uses the CPython `abi3-py39` ABI and supports Python 3.9 or newer.
For notebook helpers, install `lakeprism[notebook]` after installing the wheel.
Those helpers load PyArrow, pandas, IPython, and tqdm only when called.

The standard wheel includes the local SQL, Arrow, catalog, Delta, Unity, and
Flight bindings. It deliberately does **not** bundle FFmpeg, a transcription
model, an embedding model, cloud credentials, or a cloud credential broker.

## Start with Python

Create a local session, register media metadata, build a lazy plan, and consume
it with PyArrow:

```python
import lakeprism

session = lakeprism.MediaSession()
session.register_media_refs(
    "clips",
    [lakeprism.MediaRef("file:///data/clip.mp4", "video")],
)

plan = session.plan(
    "SELECT media.uri, media.media_type FROM clips"
)
reader = plan.to_pyarrow()
table = reader.read_all()
print(table.to_pylist())
```

`MediaSession.plan()` does not execute SQL. A plan runs only when you call
`collect()`, `to_pyarrow()`, `to_arrow_c_stream()`, or `to_arrow_ipc()`.
Prefer `to_pyarrow()` for large results: it transfers a native Arrow C stream
instead of first converting every value into Python objects.

For small inspections, `sql()` and `collect()` return a list of dictionaries:

```python
session.sql("SELECT count(*) AS clips FROM clips")
```

LakePrism renders SQL result values as strings in this convenience form. Use
PyArrow when preserving Arrow types matters.

### Notebook display and query lifecycle

```python
lakeprism.display(plan, max_rows=20)

rows = lakeprism.collect_with_progress(
    session,
    query_id=None,
    query="SELECT count(*) AS clips FROM clips",
)
```

`collect_with_progress()` reports LakePrism's real query state and row count.
It does not invent a completion percentage. Queries can be registered with
deadlines and cooperatively cancelled:

```python
query_id = session.create_query(deadline_millis=30_000)
session.cancel_query(query_id)
status = session.query_status(query_id)
```

Cancellation is cooperative. Planning-time document work, codec calls, and
remote scans cannot be interrupted in the middle of a blocking operation.

## Python functionality

The Python module is a thin PyO3 facade over LakePrism's Rust execution
engine. All SQL, planning, resource limits, catalog state, and lineage checks
run in Rust; Python provides notebook-friendly construction and consumption.

### Sessions, data registration, and SQL

| API | What it does | Result / constraint |
| --- | --- | --- |
| `MediaSession()` | Creates an isolated local DataFusion/LakePrism session. | No network connection or credentials are retained. |
| `MediaRef(uri, media_type, storage_mode="external")` | Creates a validated, credential-free media reference. | Register a list with `register_media_refs(table, refs)`. |
| `register_parquet(table, path)` | Lazily registers a local path or `file://` Parquet relation. | Any other URI scheme is rejected. |
| `register_derived_snapshot(table, path)` | Registers an immutable local Parquet feature/index snapshot. | It does not automatically trust it as a semantic index. |
| `register_delta(table, file_uri, version=None)` | Lazily registers a local Delta table, optionally at a version. | Requires `delta-rs`; only credential-free `file://` URIs. |
| `register_transcript_segments(segments)` | Adds transcript records to the session's search relation. | Each `TranscriptSegment` retains source/operator lineage. |
| `register_embedding_records(records)` | Adds compatible `EmbeddingRecord` values for semantic/hybrid search. | Vector dimensions and lineage are validated. |
| `sql(query)` / `execute(query)` | Runs SQL and returns Python dictionaries. | Values are display strings or `None`; column aliases must be unique. |
| `plan(query)` | Creates a deferred SQL plan. | Call `collect`, `to_pyarrow`, `to_arrow_c_stream`, or `to_arrow_ipc` to run it. |
| `explain(query)` | Creates a lazy `EXPLAIN` plan. | Consume it like every other `LazyPlan`. |
| `catalog_tables()` | Returns registered catalog/schema/table/provider metadata. | Does not expose provider credentials. |

### Arrow, notebook, and query-control APIs

| API | What it does |
| --- | --- |
| `LazyPlan.collect()` | Executes a plan and returns display dictionaries. |
| `LazyPlan.to_pyarrow()` / `MediaSession.sql_arrow(query)` | Returns a PyArrow `RecordBatchReader` using Arrow's C stream interface. |
| `LazyPlan.to_arrow_c_stream()` / `MediaSession.sql_arrow_c_stream(query)` | Returns an Arrow C data-interface capsule for another compatible consumer. |
| `LazyPlan.to_arrow_ipc()` / `MediaSession.sql_arrow_ipc(query)` | Materializes the result as Arrow IPC bytes for compatibility. |
| `to_pandas(value)` / `display(value, max_rows=100)` | Optional notebook helpers that materialize a plan through PyArrow and pandas. |
| `create_query(deadline_millis=None)`, `execute_query(id, sql)`, `query_status(id)`, `cancel_query(id)` | Exposes governed query IDs, deadline, status/row metrics, and cooperative cancellation. |
| `collect_with_progress(session, id, sql)` | Runs a registered query in a worker and reports actual status/row updates; it never fabricates a percent complete. |

### Documents, media, and retrieval

| API | Functionality | Availability |
| --- | --- | --- |
| `document_sections(uri)` | Lazy PDF page / DOCX paragraph rows. | Local path or `file://`. |
| `document_tables(uri)` | Lazy DOCX table-cell rows. | Local path or `file://`. |
| `document_images(uri, include_bytes=False)` | Lazy DOCX embedded-image metadata, with bytes only when requested. | Local path or `file://`. |
| `document_search(uri, query, limit)` | Bounded local document text search. | Query must be nonempty; limit is at most 1,024. |
| `video_frames(uri, start, end, every, limit, include_rgb24=False)` | Lazy sampled video-frame rows. | `native-media` wheel with target-matched FFmpeg; max 32 frames. |
| `audio_segments(uri, start, end, segment, limit, include_payload=False)` | Lazy normalized mono 16 kHz `f32le` audio chunks. | `native-media` wheel with target-matched FFmpeg; max 1,024 chunks. |
| `plan_video_frames(timestamps, ...)` | Plans bounded frame timestamps without decoding media. | Available in every wheel. |
| `semantic_search(query, limit, candidate_limit=0)` | Lazy embedding search. | `candidate_limit=0` is exact; a positive value is explicitly approximate. |
| `hybrid_search(query, limit, candidate_limit=0, semantic_weight=0.5)` | Lazy lexical plus embedding ranking. | Requires compatible registered indexes; weight is 0–1. |
| `refresh_embedding_index(snapshot, manifest, rows)` | Persists derived embedding rows and returns emitted/changed counts. | Local paths only; rows use `DerivedIndexRow` lineage. |

### Durable catalog and optional integrations

| API | Functionality | Boundary |
| --- | --- | --- |
| `LocalCatalog(path=None)` | Durable local catalog; `execute_ddl`, `table_names`, `refresh`, and `register_in_session`. | Local-only. |
| `write_delta_ipc(uri, ipc, mode, schema_mode, ...)` | Writes a local Delta table from Arrow IPC and returns the committed version. | `delta-rs`; local `file://` only; modes are create/append/overwrite. |
| `UnityQueryContext`, `UnityCatalog(base_url, token_supplier)` | Resolves and registers Unity metadata with a fresh per-request token callback. | `unity`; validate live access in protected CI. |
| `register_unity_local_parquet` / `attach_unity_schema` | Attaches already resolved local Parquet Unity metadata. | `unity`; remote temporary credentials never cross the Python boundary. |
| `FlightServer(session)`, `FlightClient(endpoint, auth_supplier=None)` | Starts a local Flight SQL server or executes a Flight SQL query. | `flight`; client endpoint validation is loopback/HTTPS only. |
| `EmbeddingSubprocessConfig`, `MediaSession.with_embedding_subprocess(config)` | Uses a locally installed batch embedding adapter through direct argv. | `embedding-subprocess`; no model/executable is bundled. |

`EmbeddingRecord` and `EmbeddingSubprocessConfig` are public package exports.
The latter is present only in a wheel built with `embedding-subprocess`.

For example, document results remain lazy until consumed:

```python
sections = session.document_sections("file:///data/report.pdf")
for row in sections.collect():
    print(row["text"])
```

Native decoding is intentionally separate from the portable Python wheel:

```bash
# Build only on the host that supplies compatible FFmpeg development libraries.
bash scripts/build-python-wheels.sh \
  --platform native --native-media --out dist/python
```

## Delta, Unity, Flight, and credentials

LakePrism treats credentials as request-scoped capabilities, not data. Tokens,
temporary object-store credentials, OAuth client secrets, and credential
providers must not enter `MediaRef`, catalog records, Arrow batches, indexes,
SQL text, or logs.

- **Delta:** local reads and explicit local writes are available from Python.
  Remote writes require an application-owned Rust boundary, scoped credentials,
  and an explicit remote-write concurrency guard.
- **Unity Catalog:** `UnityCatalog` accepts an application callback that returns
  a fresh token for one request. LakePrism keeps the callback, not the token.
  Validate this path in a protected Databricks environment before production.
- **Flight SQL:** `FlightServer` and `FlightClient` are feature-gated. The
  Python client only accepts loopback endpoints; remote server deployment and
  authentication policy belong at the Rust application boundary.
- **Iceberg, S3 staging, custom media resolvers, OCR, transcription, image
  understanding, and distributed workers:** these are intentionally not
  arbitrary Python callbacks. Their resource, cancellation, and credential
  contracts are implemented by Rust application integrations.

`discover_databricks_workspace()` reads only an HTTPS workspace host from
environment/configuration. It never returns a configured token.

## Python API at a glance

| Object | Purpose |
| --- | --- |
| `MediaRef` | Validated, credential-free media identity. |
| `MediaSession` | Local SQL execution, relation registration, Arrow export, query lifecycle, and media/document plans. |
| `LazyPlan` | Deferred SQL result with row, Arrow C stream, PyArrow, and IPC consumers. |
| `LocalCatalog` | Durable local DDL and session registration. |
| `DerivedIndexRow`, `TranscriptSegment` | Credential-free derived feature records with lineage. |
| `OAuthTokenSupplier`, `FlightAuthSupplier` | Callback protocols for per-request authentication. |
| `UnityCatalog`, `UnityQueryContext` | Optional Unity resolution bindings. |
| `FlightServer`, `FlightClient` | Optional local Flight SQL server/client. |

The repository's [Python parity document](docs/PYTHON_PARITY.md) lists all
feature gates and deliberately unsupported boundaries. The runnable notebook
example is [`examples/notebooks/local_and_databricks.py`](examples/notebooks/local_and_databricks.py).

## Rust and CLI

The Python extension is a PyO3 facade over the same Rust session used by the
CLI. It does not have a second query implementation. Build and run the CLI:

```bash
cargo run -p lakeprism-cli -- init
cargo run -p lakeprism-cli -- sql "SELECT 1 AS answer"
```

Use Rust directly when integrating production credential providers, remote
object storage, custom media/model providers, or worker execution.

## Release readiness

The package is **not ready to publish to PyPI today**. The local package
artifacts are healthy: the wheel and source distribution pass `twine check`,
include Apache-2.0 license text, install into clean environments, and import
successfully on the development host. The package metadata, ABI policy, and
trusted-publisher workflow are present.

Before publishing `0.1.0`, complete these release gates:

1. Run the PyPI workflow successfully on its supported matrix: manylinux
   x86_64, macOS x86_64, macOS arm64, and Windows x86_64. Each job builds,
   installs, and imports its wheel in a clean environment.
2. Configure the repository as a PyPI trusted publisher and protect the
   `pypi` GitHub environment. Do not add a long-lived PyPI token.
3. Decide whether Alpine Linux support is a release requirement. It is not
   currently distributed; add and validate a dedicated musllinux build before
   claiming it.
4. Run the target-matched FFmpeg/PyO3 acceptance workflow and the protected
   Databricks/Unity acceptance workflow with real non-production credentials.
5. Promote the package from alpha only after the supported-wheel matrix and
   release artifacts have been tested from a release tag.

The workflow publishes only portable base wheels. FFmpeg-enabled wheels need a
separate dependency-bundling and target-runtime policy before public release.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Python binding tests run on a target-matched native host:

```bash
python -m pip install maturin pytest
maturin develop --manifest-path crates/lakeprism-python/Cargo.toml \
  --features native-media,flight,delta-rs,unity
LAKEPRISM_NATIVE_MEDIA=1 pytest crates/lakeprism-python/python/lakeprism/test_bindings.py
```

See [docs/ACCEPTANCE_MATRIX.md](docs/ACCEPTANCE_MATRIX.md) for the current
test contract and [docs/CLOUD_ACCEPTANCE.md](docs/CLOUD_ACCEPTANCE.md) for the
opt-in cloud acceptance boundary.

## License

Apache-2.0. See [LICENSE](LICENSE).
