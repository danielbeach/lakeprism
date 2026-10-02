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

## Local data workflows

| Need | Python API | Notes |
| --- | --- | --- |
| Local Parquet | `session.register_parquet(name, path)` | Query it with normal SQL. |
| Local Delta table | `session.register_delta(name, file_uri)` | Requires the `delta-rs` feature included in the standard wheel. |
| Write local Delta | `lakeprism.write_delta_ipc(...)` | Accepts Arrow IPC with explicit create/append/overwrite modes. |
| Durable catalog | `LocalCatalog(path)` | Register it in a `MediaSession` after executing catalog DDL. |
| PDF/DOCX | `document_sections`, `document_tables`, `document_images`, `document_search` | Each method returns a lazy plan with bounded results. |
| Video frames | `video_frames(...)` | Requires a wheel built with target-matched `native-media` and FFmpeg libraries. |
| Audio chunks | `audio_segments(...)` | Same native-media requirement; payloads are opt-in. |
| Search | `semantic_search`, `hybrid_search` | Requires compatible registered embedding/index records. |

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
