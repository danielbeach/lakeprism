# LakePrism

LakePrism is a local-first Rust query engine with a Python API for querying
media, documents, Parquet, and derived features through SQL and Apache Arrow.
It is designed for notebooks and small governed services where execution,
credentials, and data locality must remain explicit.

**Status: alpha.** Version [`0.1.0a1`](https://pypi.org/project/lakeprism/0.1.0a1/)
is published on PyPI with seven platform wheels and a source distribution.
See [Release readiness](#release-readiness) for validated capabilities and limits.

**Next candidate: `0.1.0a2`.** The checkout adds bundled native FFmpeg decoding
and a [real local speech-to-SQL workflow](examples/local_transcription/README.md).
This candidate is not yet published; the installation below selects the existing release.

## Install

Install the explicit alpha version:

```bash
python -m pip install lakeprism==0.1.0a1
```

Or build and install a wheel from a checkout:

```bash
git clone https://github.com/danielbeach/lakeprism.git
cd lakeprism
bash scripts/build-python-wheels.sh --platform native --out dist/python
python -m pip install dist/python/lakeprism-*.whl
```

The wheel uses the CPython `abi3-py39` ABI and supports Python 3.9 or newer.
For notebook helpers, install `lakeprism[notebook]==0.1.0a1`.
Those helpers load PyArrow, pandas, IPython, and tqdm only when called.

The published `0.1.0a1` wheel includes local SQL, Arrow, catalog, Delta, Unity,
and Flight bindings, but no native FFmpeg runtime. The `0.1.0a2` candidate
bundles decode-only FFmpeg shared libraries, LGPL notices, build details, and
complete corresponding source. Neither version bundles speech/embedding models,
cloud credentials, or a cloud credential broker.

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
| `MediaSession.with_transcription_subprocess(config)` | Creates a session with a bounded local Whisper-compatible adapter. | Requires `whisper-subprocess`; no model or executable is bundled. |
| `session.transcribe(uri, media_type, source_version, ...)` | Transcribes a local audio/video file, returns typed timestamped rows, and registers them in the transcript index. | Local `file://` only; media type is `audio` or `video`; max 1,024 segments. |
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
| `FlightServer(session)`, `FlightClient(endpoint, auth_supplier=None)` | Starts a local Flight SQL server or executes a Flight SQL query. | `flight`; local round trips are tested; remote TLS policy is application-owned. |
| `EmbeddingSubprocessConfig`, `MediaSession.with_embedding_subprocess(config)` | Uses a locally installed batch embedding adapter through direct argv. | `embedding-subprocess`; no model/executable is bundled. |

`EmbeddingRecord` and `EmbeddingSubprocessConfig` are public package exports.
The latter is present only in a wheel built with `embedding-subprocess`.

For example, document results remain lazy until consumed:

```python
sections = session.document_sections("file:///data/report.pdf")
for row in sections.collect():
    print(row["text"])
```

Published `0.1.0a1` wheels lack native decoding. Candidate wheels include it.
To build a candidate locally, use a matching Rust/C toolchain (NASM on x86)
and libclang; the builder compiles and bundles its own pinned FFmpeg:

```bash
# Build only on the matching native host; no system FFmpeg is bundled.
bash scripts/build-python-wheels.sh \
  --platform native --native-media --out dist/python
```

### Transcribe local audio or video

The standard wheel includes the **provider contract**, not a model. Supply an
application-installed adapter, an existing local model artifact, and an
application-owned private staging directory. The adapter receives direct argv
with staged `{input}`, `{output}`, and `{model}` paths; LakePrism never invokes
a shell or stores the model path in transcript rows.

```python
import lakeprism

whisper = lakeprism.WhisperSubprocessConfig(
    executable="/opt/local/bin/whisper-adapter",
    arguments=[
        "--input", "{input}",
        "--output", "{output}",
        "--model", "{model}",
    ],
    model_artifact="/opt/models/whisper-large-v3.bin",
    staging_directory="/var/lib/my-app/lakeprism-staging",
    timeout_seconds=600,
    operator_version="whisper-adapter-v1",
    model="whisper-large-v3",
    model_version="2026-10",
)
session = lakeprism.MediaSession.with_transcription_subprocess(whisper)

segments = session.transcribe(
    "file:///data/interview.mp4",
    media_type="video",
    source_version="sha256:...",
)
for segment in segments:
    print(segment.start_millis, segment.end_millis, segment.text)
```

The adapter writes `{"segments": [...]}` JSON with `start_millis`, `end_millis`,
`text`, and optional `confidence_millis`. LakePrism rejects invalid output,
oversized inputs/results, lineage mismatches, non-local sources, unsafe
configuration, timeouts, cancellation, and resource exhaustion without
including paths, command arguments, tokens, or transcript text in errors.

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
- **Flight SQL:** `FlightServer` and `FlightClient` are feature-gated. Local
  Python round trips are tested. The client accepts loopback HTTP or HTTPS
  endpoint shapes, but does not expose remote TLS deployment configuration.
  Remote server deployment and authentication policy belong at the Rust
  application boundary.
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

The first release is the explicit prerelease **`0.1.0a1`**. The PyPI workflow
builds seven native-target wheel variants: manylinux x86_64/arm64, musllinux
x86_64/arm64 (Alpine), macOS Intel/Apple Silicon, and Windows x86_64.
The ABI policy targets ordinary GIL-enabled CPython 3.9+; free-threaded Python,
PyPy, Windows arm64, and 32-bit systems are not part of this matrix.

All release gates passed for `0.1.0a1` in
[the publication run](https://github.com/danielbeach/lakeprism/actions/runs/41513596139),
including seven-platform TestPyPI installation tests and production artifact
hash and installation checks.

The `0.1.0a2` candidate extends that matrix with installed native decoding,
build-library isolation, FFmpeg source/license/configuration verification,
and an actual offline CPU speech-model workflow. It requires new runner proof
before a new publication; `0.1.0a1` files remain immutable.

Publishing requires all of these gates in the same workflow run:

1. Reusable core/native CI, including actual FFmpeg/PyO3 runtime acceptance.
2. Clean installed-wheel tests on CPython 3.9, 3.12, and 3.14 for every
   platform, plus an installed source-distribution test. Tests run outside the
   source package so a development extension cannot mask a broken wheel.
3. Exactly seven ABI3 wheels and one source archive with matching versions,
   license files, valid metadata, corresponding FFmpeg source and all five
   dynamically linked FFmpeg libraries.
4. OIDC publication to TestPyPI, hash verification of all uploaded files,
   and installed TestPyPI wheel tests on all seven platforms.
5. Publication of those identical files to production PyPI, followed by
   production hash and installation checks.

The candidate also runs the real model example before publication.
Alpine tests cover native video/audio decoding without installing PyArrow, whose
musllinux distribution is outside this release's control. PyArrow-dependent
notebook extras on Alpine are not a validated capability.

The GitHub environments `testpypi` and `pypi` allow only `main` and `v*` tags.
Each package index needs its own trusted publisher for owner `danielbeach`,
repository `lakeprism`, workflow `pypi.yml`, and the matching environment name.
No long-lived PyPI API token is needed.

Use the Actions **PyPI** workflow with `publish=false` for build-only
validation. Set `publish=true` on `main` to perform the full staged release,
or push a tag matching the package version, such as `v0.1.0a1`. Publishing is
an external, immutable release action; do not reuse a version for new bytes.

Live Databricks/Unity data-plane acceptance remains an opt-in requirement
before claiming validated production cloud support. It is not a prerequisite
for this explicitly local-first alpha.

Candidate FFmpeg builds disable GPL/nonfree/version3 components, networking,
autodetected dependencies, encoders, and unused formats. They support MP4/MOV,
Matroska/WebM, and WAV with H.264, HEVC, VP9, MPEG-4, MJPEG, AAC, MP3, Opus,
Vorbis, FLAC and PCM decoders; AV1 is not included in this minimal bundle.
Other formats/codecs require a custom native build/profile. Codec patent
obligations remain separate from the supplied LGPL redistribution materials.
The architecture spans several future releases; see
[plan versus alpha](docs/ACCEPTANCE_MATRIX.md#architecture-plan-versus-the-010-alpha)
for implemented boundaries and unmet roadmap exit criteria.

## Development

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Python binding tests run on a target-matched native host:

```bash
bash scripts/build-wheel-ffmpeg.sh
export FFMPEG_DIR="$PWD/target/wheel-ffmpeg"
export PKG_CONFIG_PATH="$FFMPEG_DIR/lib/pkgconfig"
export LD_LIBRARY_PATH="$FFMPEG_DIR/lib"
python -m venv .native-venv
.native-venv/bin/python -m pip install maturin==1.10.2 pytest pyarrow
VIRTUAL_ENV="$PWD/.native-venv" .native-venv/bin/maturin develop \
  --locked --manifest-path crates/lakeprism-python/Cargo.toml --all-features
LAKEPRISM_NATIVE_MEDIA=1 .native-venv/bin/python -m pytest \
  crates/lakeprism-python/python/lakeprism/test_bindings.py
```

See [docs/ACCEPTANCE_MATRIX.md](docs/ACCEPTANCE_MATRIX.md) for the current
test contract and [docs/CLOUD_ACCEPTANCE.md](docs/CLOUD_ACCEPTANCE.md) for the
opt-in cloud acceptance boundary.

## License

Apache-2.0. See [LICENSE](LICENSE).
