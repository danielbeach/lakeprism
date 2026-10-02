# LakePrism

LakePrism is a Rust-native foundation for lazy multimodal lakehouse queries. Source media is represented by portable `MediaRef` values, resolved at execution time, and kept separate from Arrow result payloads.

## Available capabilities and boundaries

LakePrism is a local-first query engine, not a managed cloud service. Its
implemented surface is deliberately split between credential-free portable
metadata and application-owned, request-scoped credential providers:

- **Local and S3-compatible data:** governed local range reads, local Parquet,
  lazy local Delta/Iceberg providers, and opt-in S3-compatible ranged reads,
  bounded document loading, remote-media staging, and Delta scans/writes.
  S3 credentials are supplied only at the Rust application boundary.
- **Databricks and Unity Catalog:** query-scoped OAuth/M2M credential vending,
  table/schema discovery and attach, governed `/Volumes/...` references, FILE
  metadata conversion, local Parquet/Delta registration, and explicit
  S3-backed managed-Delta registration. Unsupported storage schemes, table
  formats, and views are reported as unsupported rather than silently read.
- **Media, documents, and indexes:** bounded PDF/DOCX extraction; opt-in,
  host-native FFmpeg video/audio decode; explicit OCR/transcription/embedding
  provider contracts; and lineage-verified transcript, semantic, hybrid, and
  cross-modal indexes. Default providers never fabricate inference.
- **Interfaces:** Rust/DataFusion, a durable local CLI and REPL, Arrow Flight
  SQL, and a thin PyO3 extension sharing the same session implementation.
  Python supports local SQL/Arrow transfer and feature-gated local Delta,
  Unity REST registration, and loopback Flight; it does not expose S3
  configuration, remote Delta writes, Iceberg construction, or custom Rust
  provider traits.
- **Execution:** a bounded local query governor and an in-memory distributed
  coordinator/worker protocol with leases, deterministic ordering, retries
  only for idempotent work, and cooperative cancellation.

Important limits: SQL media/document UDTFs accept credential-free local
`file://` literals; remote documents and staged media are bounded
materializations, not remote lazy scans. Cancellation is cooperative and
cannot interrupt a request or synchronous FFmpeg decode already in progress.
The distributed core is not a remote scheduler, durable queue, or
exactly-once system. Flight prepared-statement parameters and transactions are
explicitly unsupported. Details and feature flags follow.

## Delta writes and S3-compatible reads

Enable the S3 resolver with `--features lakeprism-storage/s3`. Construct
`S3Credentials` and `S3CompatibleConfig` in application request/session scope,
then use `S3CompatibleResolver` with a `ResourceManager`. Credentials have no
`Debug` or serialization implementation; never place them in a `MediaRef`,
catalog, SQL string, Arrow result, or telemetry payload. The resolver accepts
only credential-free `s3://configured-bucket/key` references, enforces the
existing I/O-slot and per-range byte budgets, and uses object-store `HEAD`
plus HTTP `Range` reads. Cancellation/deadlines are checked before acquiring
I/O and after each completed remote operation; they cannot abort an already
issued object-store HTTP request. Endpoint URLs must use TLS;
plain HTTP is accepted only for loopback local S3 mocks.

Remote document sections are available through the explicit async API:

```rust,no_run
session.register_remote_document_sections(
    "remote_report",
    &media_ref,
    &access_context,
    &s3_resolver,
).await?;
// Ordinary SQL then reads the bounded registered relation:
let batches = session.collect("SELECT ordinal, text FROM remote_report").await?;
```

Only PDF and DOCX sections are currently supported remotely. LakePrism heads
the object first and rejects it over `DocumentLimits::max_input_bytes` (64 MiB
by default) before a single bounded full-object range read. The relation is
then an in-memory SQL table; this is not a lazy document scan. Remote document
table/image/search/OCR UDTFs remain unsupported.

### Bounded remote FFmpeg staging

Enable `lakeprism-datafusion/remote-media-s3` (which also enables native
FFmpeg and the S3 resolver) to bind a remote object to the existing video or
audio SQL relation APIs. Construct `S3MediaStagingResolver` from the same
query-scoped `S3CompatibleResolver`, `ResourceManager`, and a
`MediaStagingConfig` with an application-owned private staging directory:

```rust,no_run
# use lakeprism_datafusion::{LocalMediaDecodeOptions, MediaSession};
# use lakeprism_storage::{MediaStagingConfig, S3MediaStagingResolver};
# use lakeprism_core::{AccessContext, MediaRef};
# async fn example(
# session: MediaSession, media: MediaRef, access: AccessContext,
# stager: S3MediaStagingResolver,
# ) -> datafusion::error::Result<()> {
session.register_s3_video_frames(
    "remote_clip_frames", &media, &access, &stager,
    LocalMediaDecodeOptions {
        start_millis: 0, end_millis: 5_000, interval_millis: 1_000,
        limit: 6, include_payload: false,
    },
).await?;
let batches = session.collect("SELECT timestamp_millis, width FROM remote_clip_frames").await?;
# Ok(()) }
```

`register_s3_video_frames` and `register_s3_audio_chunks` issue a HEAD first,
require a known object size at or below `MediaStagingConfig::max_source_bytes`,
then fetch exact `chunk_bytes` ranges through the shared I/O/byte governor.
Cancellation and deadlines are checked between ranges (not during an
already-issued HTTP request). Each object is written to a random owner-only
file, and the relation retains the cleanup handle until the table is dropped;
the path is never made a `MediaRef`, logged, or persisted. This is bounded
full-object staging, not custom FFmpeg I/O and not a lazy remote scan.
The literal FFmpeg UDTFs still accept only `file://` paths: SQL cannot supply
or retain S3 credentials. Endpoint TLS/loopback safeguards and credential
non-persistence are the same as ordinary S3 reads.

For Delta, enable `--features lakeprism-delta/delta-rs`. The read APIs
`register_s3_delta_table` and `register_s3_delta_table_version` pass the same
query-scoped S3 config directly to delta-rs' native lazy `TableProvider`,
preserving pruning and pushdown. `write_delta_table` provides verified local
create/append/overwrite writes of Arrow `RecordBatch` values; strict schema
matching is the default, while `Merge` and `Overwrite` schema modes are
explicit. Partition columns and credential-free commit audit metadata are
validated before delta-rs commits. `write_delta_table_with_retry` retries only
classified optimistic-commit conflicts, so callers must use it only for
replay-safe input.

`write_s3_delta_table` accepts `S3CompatibleConfig` only at call time and
does not persist or log its credentials. It requires an explicit
`ExperimentalRemoteWriteGuard`: use `SingleWriter` only with externally
enforced single-writer ownership, or `ConditionalPutVerified` only after
live-validation of conditional Delta-log object creation for the exact
delta-rs/object-store backend. LakePrism does not infer either condition. If
the installed delta-rs 1.1 S3 backend is unavailable, the call fails
explicitly; LakePrism never substitutes an unauthenticated/custom writer.

### AWS operational guardrails

- Require HTTPS and validate the S3-compatible endpoint; use loopback HTTP
  only for MinIO/mock integration tests. Keep bucket policies and TLS
  enforcement enabled.
- Read-only queries need only `s3:ListBucket` (prefix-scoped where possible),
  `s3:GetObject`, and, when applicable, `kms:Decrypt` for the exact bucket,
  prefix, and SSE-KMS key. The explicit S3 Delta writer additionally needs
  narrowly scoped data/log write permissions and a backend/policy preserving
  conditional log-object creation; do not grant it broad bucket mutation
  rights. SSE-S3/SSE-KMS is transparent to reads but KMS permissions and
  request charges still apply.
- Enable CloudTrail **S3 data events** for audit trails and watch CloudWatch
  S3 request/error/latency metrics. LakePrism's local query audit deliberately
  excludes SQL, URIs, principals, request headers, and credentials.
- Range reads reduce transferred bytes but each `HEAD`/`GET` is billable and
  can increase request counts. Size I/O and byte budgets accordingly; monitor
  4xx/5xx rates. Object-store retries are provider-controlled, so applications
  should use bounded exponential backoff for S3 `503 Slow Down` at the
  request/session boundary rather than retrying indefinitely.

The local-first workspace implements these boundaries:

- `lakeprism-core`: media references, access contexts, source identities, lineage, and pushdown constraints.
- `lakeprism-arrow`: portable Arrow schemas and bounded `RecordBatch` construction.
- `lakeprism-storage`: range-capable local media resolution with byte and I/O budgets.
- `lakeprism-media`: lazy frame request planning plus opt-in FFmpeg-backed local media probing.
- `lakeprism-documents`: bounded local PDF/DOCX text, table-cell, and embedded-image extraction.
- `lakeprism-catalog`: a durable local catalog for credential-free portable media-reference tables and lineage.
- `lakeprism-datafusion`: local DataFusion registration for portable media-reference tables.
- `lakeprism-delta`: opt-in delta-rs lazy scans plus transactional Arrow-batch create/append/overwrite writes.
- `lakeprism-iceberg`: opt-in Iceberg static-table registration, REST-catalog namespace resolution, and optional snapshot pinning.
- `lakeprism-unity`: opt-in Unity Catalog REST resolution, explicit managed-Iceberg REST mapping, and Databricks FILE conversion.
- `lakeprism-index`: governed transcription/embedding provider contracts, deterministic mock-only local providers, and exact-lineage semantic/hybrid ranking.
- `lakeprism-whisper`: opt-in, subprocess-backed local Whisper-compatible transcription adapter; it is excluded from base wheels and never bundles or downloads a model.
- `lakeprism-embedding`: opt-in, subprocess-backed batch embedding adapter; it is excluded from base wheels and never bundles, downloads, or synthesizes model vectors.
- `lakeprism-flight`: Arrow Flight SQL statement and prepared-statement execution over a shared `MediaSession`.
- `lakeprism-execution`: deterministic local coordinator/worker protocol with leases, heartbeats, safe retries, ordered Arrow results, and cooperative cancellation.
- `lakeprism-python`: a thin PyO3 extension exposing `MediaRef`, `MediaSession`, lazy local SQL plans, and frame planning.

The core rejects credential-bearing media URLs so authorization remains in the resolver and access context.

### Optional local Whisper-compatible transcription

`lakeprism-whisper` is the first real local model adapter, but remains an
explicit Rust application dependency (`lakeprism-whisper = { path = "..."}`
in a consuming application). It is not a default workspace member, wheel
feature, bundled executable, or bundled model. Configure an **absolute**
application-installed executable, an existing absolute model artifact, and an
existing private staging directory. Its argument vector is passed to
`std::process::Command` directly—never a shell—and must contain each
individual placeholder exactly once:

```rust,no_run
# use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
# use lakeprism_storage::{ExecutionGovernor, ExecutionGovernorConfig};
# use lakeprism_whisper::{WhisperSubprocessConfig, WhisperSubprocessProvider};
let provider = WhisperSubprocessProvider::new(
    WhisperSubprocessConfig {
        executable: PathBuf::from("/opt/local/bin/whisper-adapter"),
        arguments: vec![
            "--input".into(), "{input}".into(),
            "--output".into(), "{output}".into(),
            "--model".into(), "{model}".into(),
        ],
        model_artifact: PathBuf::from("/opt/models/ggml-base.bin"),
        staging_directory: PathBuf::from("/var/lib/my-app/lakeprism-staging"),
        max_input_bytes: 512 * 1024 * 1024,
        max_output_bytes: 8 * 1024 * 1024,
        timeout: Duration::from_secs(120),
        operator_version: "whisper-subprocess-v1".into(),
        model: "whisper-base".into(),
        model_version: "local-2026-10".into(),
        parameters: BTreeMap::from([("language".into(), "en".into())]),
    },
    Arc::new(ExecutionGovernor::new(ExecutionGovernorConfig::default())?),
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The adapter accepts only credential-free local `file://` audio. It stages at
most `max_input_bytes` into an owner-only per-run directory, checks
`QueryControl` between copy chunks and child polls, reserves one CPU and I/O
permit for the full operation, enforces a timeout, and kills/reaps its child
on cancellation or timeout. It emits exact request lineage
(`source`, operator, model, version, parameters) and rejects mismatches.
The adapter protocol writes bounded JSON to `{output}`:
`{"segments":[{"start_millis":0,"end_millis":500,"text":"...","confidence_millis":987}]}`.
Errors expose only structured categories (for example `TimedOut` or
`ChildFailed`), never command arguments, model/source paths, stdout, or
stderr. Child startup and synchronous local I/O cannot be forcibly interrupted
mid-system-call; cancellation is cooperative at the documented boundaries.

### Optional local batch embedding provider

`lakeprism-embedding` follows the same application-owned boundary as Whisper.
It is an explicit Rust dependency, not a default member, wheel feature,
executable, or model distribution. Configure absolute paths to an
application-installed executable and existing model artifact, together with an
existing private staging directory. The command is direct `Command` argv, never
a shell; `{input}`, `{output}`, and `{model}` must each be standalone argv
values exactly once.

```rust,no_run
# use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
# use lakeprism_embedding::{EmbeddingSubprocessConfig, EmbeddingSubprocessProvider};
# use lakeprism_storage::{ExecutionGovernor, ExecutionGovernorConfig};
let provider = EmbeddingSubprocessProvider::new(
    EmbeddingSubprocessConfig {
        executable: PathBuf::from("/opt/local/bin/embedding-adapter"),
        arguments: vec![
            "--input".into(), "{input}".into(), "--output".into(), "{output}".into(),
            "--model".into(), "{model}".into(),
        ],
        model_artifact: PathBuf::from("/opt/models/e5-large-v2.gguf"),
        staging_directory: PathBuf::from("/var/lib/my-app/lakeprism-staging"),
        max_batch_items: 64,
        max_input_bytes: 8 * 1024 * 1024,
        max_output_bytes: 8 * 1024 * 1024,
        timeout: Duration::from_secs(60),
        operator_version: "embedding-subprocess-v1".into(),
        model: "e5-large-v2".into(),
        model_version: "local-2026-10".into(),
        parameters: BTreeMap::from([("normalize".into(), "true".into())]),
    },
    Arc::new(ExecutionGovernor::new(ExecutionGovernorConfig::default())?),
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The adapter stages a bounded JSON input file:
`{"requests":[{"id":"r0","text":"..."}]}`, and expects bounded JSON output:
`{"embeddings":[{"id":"r0","values":[0.1,...]}]}`. Every returned ID must
match exactly once, every vector must be finite and 1–4096 dimensions, and a
batch must use one dimension. The provider supplies exact configured
operator/model/parameter lineage for each request; callers must present that
same expected lineage. It reserves CPU and I/O permits, checks `QueryControl`
before staging and while polling, kills/reaps children on cancellation or
timeout, owner-restricts and removes each run directory, and emits only
redacted error categories. It does not expose text, paths, arguments, child
output, stdout, or stderr in errors. It cannot interrupt a child during an
individual spawn, filesystem, or process-poll system call.

`embed_batch_governed` is the batch API; `EmbeddingProvider::embed` delegates
to a one-item batch, so existing semantic/hybrid/cross-modal session flows and
derived `EmbeddingRecord` persistence remain unchanged. Use
`embed_records_governed` to produce lineage-bearing records for
`refresh_embedding_index_parquet`; it performs no persistence itself.

The test-only `fixture-provider` feature emits fixed protocol values solely to
verify parsing, cleanup, timeout, and cancellation. It is not model inference,
is not enabled by applications, and must not be used for semantic results.
Run its overhead scaffold with:

```bash
cargo bench -p lakeprism-embedding --features fixture-provider --bench batch_throughput
```

`ExecutionGovernor` provides configurable process-local limits for concurrent queries, CPU work, and I/O work. `MediaSession` creates a UUID-backed local query record for every streamed execution, with `Pending`, `Running`, terminal status, batch/row counters, timestamps, and elapsed time. `MediaSession::query(id)` returns its status and metrics; `cancel_query(id)` is cooperative and reports `Cancelled` explicitly. `execute_stream_with_deadline` accepts a Tokio deadline and reports `DeadlineExceeded`.

Query and CPU permits live in the governed result stream, so reaching a cancellation/deadline or dropping that stream releases both permits. `QueryControl` reaches `ResourceManager` local reads: it is checked before acquiring an I/O permit, while waiting for one, and after local metadata/read operations. DataFusion plans and local FFmpeg decoding remain cooperative rather than preemptive: cancellation is observed between produced batches and FFmpeg cannot interrupt a synchronous codec call already in progress. Remote Delta/Iceberg providers and planning-time document/media UDTFs do not yet receive query control, so this is not a remote-storage abort guarantee.

## Initial distributed execution (phase 9)

`lakeprism-execution` is a **local, in-memory coordinator plus process-like
worker abstraction**, not a cloud scheduler or remote transport. A
`TaskRequest` contains credential-free `MediaRef`s, explicit `SourceIdentity`
values, identity-only `TaskAccessContext`, retry safety, and an opaque
operation label. Credentials and credential-provider implementations are
deliberately excluded from its serde protocol and must be vended by the
application at each real worker boundary.

`LocalCoordinator` assigns sources to stable FNV-1a partitions, sorts sources
within each partition, dispatches in partition order, and returns completed
`TaskResult` Arrow `RecordBatch` values in partition order regardless of worker
completion order. Results retain source identities and feature lineage; the
coordinator rejects mismatched identities or lineage before accepting them.
Worker contexts receive the coordinator's `ExecutionGovernor` and cooperative
`QueryControl`, rather than caller-supplied resource handles.

Leases have opaque tokens, heartbeats extend them, and stale results cannot
complete a reissued lease. Expired/retryable work is requeued only when marked
`Idempotent`; a non-idempotent task with an ambiguous worker failure is failed,
never duplicated. Cancellation prevents queued dispatch and signals leased
workers cooperatively. Arrow batches stay in memory in this initial boundary:
there is no child-process launch, durable task/result store, exactly-once
external side-effect protocol, remote RPC, cloud transport, or forced
interruption of an already-running worker operation.

## SQL extensions

Every `MediaSession` registers this small local extension surface:

| SQL function | Arguments | Result | Contract |
| --- | --- | --- | --- |
| `lakeprism_media_uri(media)` | one non-null LakePrism `media` struct | `UTF8` | Returns the portable media URI. |
| `lakeprism_media_type(media)` | one non-null LakePrism `media` struct | `UTF8` | Returns the declared media type. |
| `lakeprism_document_sections(uri)` | one literal `file://` URI ending in `.pdf` or `.docx` | table `(source_uri UTF8, source_version UTF8, ordinal UINT32, heading UTF8 NULL, text UTF8)` | Reads a bounded local document during SQL planning and returns PDF pages or DOCX paragraphs. |
| `lakeprism_document_tables(uri)` | one literal `file://` URI ending in `.pdf` or `.docx` | table `(source_uri UTF8, source_version UTF8, table_ordinal UINT32, row_ordinal UINT32, column_ordinal UINT32, text UTF8)` | Returns bounded DOCX table cells or conservative PDF text-layout tables. PDF uses only lines with two or more whitespace-separated cells; it does not infer ruled, merged, or image-only tables. |
| `lakeprism_document_images(uri, include_bytes)` | literal `.pdf`/`.docx` `file://` URI and boolean | table `(source_uri UTF8, source_version UTF8, ordinal UINT32, name UTF8, media_type UTF8 NULL, bytes BINARY NULL)` | Returns actual embedded DOCX package-image bytes or PDF image-XObject encoded streams only when requested. Only DCT/JPX PDF streams are reported as standalone JPEG/JP2 media; other encoded PDF streams have null media type. |
| `lakeprism_document_search(uri, query, limit)` | literal `.pdf`/`.docx` `file://` URI, non-empty query, `0 <= limit <= 1024` | table `(source_uri UTF8, source_version UTF8, kind UTF8, ordinal UINT32, text UTF8)` | Case-insensitive bounded local search across PDF/DOCX text, DOCX table cells, and DOCX image file names. It does not OCR image pixels. |
| `lakeprism_document_ocr(uri, limit)` | literal `.pdf`/`.docx` `file://` URI and `0 <= limit <= 1024` | table `(source_uri UTF8, source_version UTF8, image_ordinal UINT32, ordinal UINT32, text UTF8, confidence_millis UINT16 NULL)` | Invokes an application-installed bounded OCR provider over embedded images. The default unavailable provider returns an execution error and never fabricates OCR text. |
| `lakeprism_video_frames(uri, start_millis, end_millis, every_millis, limit, include_rgb24)` | six literals; `0 < limit <= 32` | table `(source_uri UTF8, timestamp_millis UINT64 NULL, width UINT32, height UINT32, rgb24 BINARY NULL)` | Feature-gated local FFmpeg decode at scan time. Scans no more than 1 GiB input and 64 MiB returned RGB24 data. |
| `lakeprism_audio_segments(uri, start_millis, end_millis, segment_millis, limit, include_payload)` | six literals; `0 < limit <= 1024` | table `(source_uri UTF8, start_millis UINT64, end_millis UINT64, sample_rate_hz UINT32, channel_count UINT16, f32le BINARY NULL)` | Feature-gated FFmpeg decode, downmix, and resample at scan time. `f32le` is mono 16 kHz IEEE-754 little-endian samples when requested. |
| `lakeprism_transcript_search(query, limit)` | two literals; `0 <= limit <= 1024` | table `(media_id UTF8, start_millis UINT64, end_millis UINT64, text UTF8, confidence_millis UINT16 NULL, source_uri UTF8, source_version UTF8)` | Searches only lineage-bearing transcript rows registered with the session; it never triggers cold transcription. |
| `lakeprism_semantic_search(query, limit, candidate_limit)` | literals; `0 <= limit <= 10000`, `0 <= candidate_limit <= 10000` | table `(id UTF8, text UTF8, score FLOAT32, ranking_semantics UTF8)` | Ranks one exact-lineage registered embedding family. `candidate_limit = 0` scores all compatible rows and returns `ranked`; positive limits are insertion-order candidate truncation and return `approximate`. |
| `lakeprism_hybrid_search(query, limit, candidate_limit, semantic_weight)` | semantic arguments plus literal `0.0 <= semantic_weight <= 1.0` | same semantic table | Deterministically combines cosine semantic score and token-presence keyword score. Its ranked/approximate semantics are identical to semantic search. |

For example:

```sql
SELECT lakeprism_media_uri(media), lakeprism_media_type(media)
FROM media_objects;

SELECT ordinal, text
FROM lakeprism_document_sections('file:///data/report.docx');

SELECT table_ordinal, row_ordinal, column_ordinal, text
FROM lakeprism_document_tables('file:///data/report.docx');

SELECT name, media_type, bytes
FROM lakeprism_document_images('file:///data/report.docx', true);

SELECT kind, ordinal, text
FROM lakeprism_document_search('file:///data/report.docx', 'revenue', 20);

SELECT timestamp_millis, width, height
FROM lakeprism_video_frames('file:///data/clip.mp4', 0, 5000, 1000, 6, false);

SELECT text, start_millis, end_millis
FROM lakeprism_transcript_search('meeting action', 20);

SELECT id, score, ranking_semantics
FROM lakeprism_semantic_search('lake data', 20, 0);
```

The media functions are immutable DataFusion scalar UDFs over the Arrow 59 portable
`media` struct. The document UDTFs create in-memory `TableProvider`s from bounded local
extraction under a `MediaSession` CPU permit; when no CPU permit is available they fail rather
than performing ungoverned work. They deliberately accept only literal local paths and are
**not** lazy remote-document scans. Their emitted `source_version` is the local file size plus
modification timestamp, providing local-source lineage suitable for invalidating derived results.
PDF/DOCX parsing limits input to 64 MiB by default. PDF pages, sections, and recognized
tables are capped at 16,384; PDF image XObjects are capped at 1,024 images, 32 MiB per image,
and 64 MiB in aggregate. DOCX parsing additionally limits `word/document.xml` to 64 MiB and caps sections/tables,
caps table cells at 65,536, caps images at 1,024, caps each image at 32 MiB, and caps total
returned image bytes at 64 MiB. Image bytes are real package payloads or encoded PDF image
streams, not renderings; metadata search matches image names only. OCR receives no more than 32
MiB for one image and returns no more than 1,024 rows globally. Its provider must return exact
source/operator/model/parameter lineage; the default unavailable provider performs no inference.
Document parsing happens at SQL planning time, so it can acquire the CPU governor
but cannot observe a query cancellation token once parsing has begun. Use ordinary DataFusion SQL
for strings, maps, timestamps, joins, filters, aggregates, and all standard SQL operations.
The frame and audio UDTFs only parse/validate their bounded local request during planning;
FFmpeg opening, decoding, resampling, payload allocation, and I/O-permit acquisition occur
when DataFusion scans the resulting provider. The provider honors projection pushdown (so
unprojected `rgb24`/`f32le` payloads are not materialized), DataFusion limit pushdown, and
safe/inexact `start_millis`/`end_millis` filter pushdown (DataFusion retains the filter for
correctness).
They accept only credential-free `file://` URIs, no remote source, and no expressions or
columns as arguments. For bounded query-scoped S3 staging, use
`register_s3_video_frames` or `register_s3_audio_chunks` and query the
registered relation; literal UDTFs never accept remote URIs. DataFusion 55 table functions cannot turn an input `media` column into
multiple rows safely; the supported column-safe equivalent is
`MediaSession::register_video_frames(table, &media_ref, options)` or
`register_audio_chunks(table, &media_ref, options)`, which binds a validated `MediaRef` as an
ordinary optimizer-visible SQL relation. Enable native decoding with
`--features lakeprism-datafusion/native-media`; without target-matched FFmpeg development
libraries they explicitly return `NotImplemented` rather than pretending to decode.
`rgb24` and `f32le` are null unless requested. Audio is normalized to mono, 16 kHz packed
little-endian `f32`; it is real decoded PCM, not compressed bytes or metadata-only segmentation.
The decoder stops at the requested bounded segment limit but is synchronous between produced
batches, so cancellation cannot preempt an active FFmpeg codec call. Transcript search is an
index lookup, not a transcription API; populate it with
`MediaSession::register_transcript_segments` and retain/query the emitted source lineage.
There are no SQL wrappers for Unity credentials or catalog mutation.

Register ordinary Parquet data through `MediaSession::register_parquet`. Enable the Delta adapter with `--features lakeprism-delta/delta-rs`; it preserves delta-rs file pruning, predicate pushdown, projection pushdown, and limit handling by registering its lazy `TableProvider` directly.

Enable Iceberg with `--features lakeprism-iceberg/iceberg-rust`. Its adapter accepts an `iceberg::table::Table` plus an optional snapshot ID, registers the compatible lazy DataFusion provider, and keeps snapshot selection explicit for derived-feature lineage.

`--features lakeprism-iceberg/iceberg-rest` enables the compatible Arrow-59 Iceberg REST
client. `RestCatalogConfig` accepts only a credential-free endpoint, optional warehouse, and
the REST `prefix` property. A `RestCatalogClientProvider` creates a fresh HTTP client from an
`AccessContext` for each `ScopedRestCatalog::open`; LakePrism does not retain or serialize
credentials. `ScopedRestCatalog::register_catalog` exposes top-level Iceberg namespaces as
DataFusion schemas. The upstream Arrow-59 bridge flattens namespace components, so nested
namespaces are intentionally rejected there; use `register_table` with an explicit namespace
and local SQL table name instead. Protocol tests cover `/v1/config` and `/v1/namespaces`; no
live catalog or fake remote scan is claimed. Remote object-store authorization and scan
cancellation remain upstream Iceberg/FileIO concerns.

Enable Unity Catalog with `--features lakeprism-unity/unity`. Every Unity request accepts a
`UnityQueryContext` (query ID plus `AccessContext`) and obtains a fresh credential from its
`CredentialProvider`; vended tokens are neither retained nor serializable. `DatabricksFileProvider`
is the explicit managed/external FILE boundary: it may return credential-free FILE metadata for a
query, which LakePrism converts to a portable `MediaRef`, but LakePrism does not perform remote
FILE reads or invent cloud credentials. `UnityCatalogClient::resolve_and_register_table` registers
only `file://` physical `PARQUET` `MANAGED`/`EXTERNAL` tables with DataFusion. S3/ABFS/GCS,
Delta/Iceberg, views, and unknown formats return `UnityTableRegistration::Unsupported`; no remote
query is faked. `UnityCatalogRegistry` atomically persists only endpoint/table/location/format
metadata and rejects credential-bearing values. Python and Flight deliberately do not expose Unity
credentials; embed a Rust `CredentialProvider`/`DatabricksFileProvider` at the application boundary.

### Unity Catalog notebook compatibility (P0/P1)

`UnityVolumePath::parse("/Volumes/catalog/schema/volume/path")` maps a governed
Volume path to a managed, credential-free
`unity-volume://catalog/schema/volume/path` `MediaRef`. It records only the
governed Volume identity in `catalog_ref`; it does not turn a Volume into an
S3/ABFS URL or infer storage credentials.

`UnityCatalogClient::new` validates endpoints: production endpoints must use
HTTPS and may not include user info, query parameters, or fragments. Plain HTTP
is limited to loopback mocks. The concrete
`vend_temporary_table_credentials` and `vend_temporary_path_credentials` calls
POST to Unity's temporary-credential endpoints with fresh query-scoped bearer
authorization. Their response payloads are private, non-serializable,
redacted under `Debug`, and available only through an immediate callback; they
are never persisted in LakePrism registries, `MediaRef`s, Arrow data, or logs.

For a one-call Rust schema attach, call
`UnityCatalogClient::attach_schema(catalog, schema, &context, &session)`.
It lists the schema once and attaches supported local providers under qualified
`catalog.schema.table` names, returning explicit per-table unsupported reasons
instead of fabricating remote readers. The Python notebook surface exposes
`MediaSession.attach_unity_schema(catalog, schema, tables, context)` for
already-resolved metadata. Python deliberately accepts no cloud credential or
credential-provider callback; an embedding Rust application must do REST
resolution with the Rust client.

With `--features lakeprism-unity/delta-rs`, local `file://` Unity `DELTA`
tables attach through delta-rs' native lazy `TableProvider`. The pinned
delta-rs 1.1 provider validates protocol reader features and supports deletion
vectors; LakePrism reports this as `deletion_vectors_verified`. Do **not**
register a Delta directory as ordinary Parquet.

For an S3-backed physical Unity Delta table,
`resolve_and_register_managed_delta_table` (or `attach_schema`) obtains a
fresh temporary-table credential envelope, translates it immediately into the
existing non-serializable `S3CompatibleConfig`, and registers delta-rs'
native lazy provider. The source location must be a credential-free
`s3://bucket/path`; ABFS/GCS and arbitrary remote URLs are still rejected.
The credentials never enter a registration, `MediaRef`, Arrow batch, log, or
Python API. The M2M entry point is `DatabricksM2mConfig` plus
`DatabricksM2mCredentialProvider`; it requests a fresh OAuth
client-credentials `all-apis` bearer token for each Unity request.

`write_managed_delta_table_experimental` is deliberately gated by
`ExperimentalRemoteWriteGuard`. Callers must select `SingleWriter` only when
an external coordinator guarantees one active writer, or
`ConditionalPutVerified` only after live validation of conditional Delta-log
object creation for the exact delta-rs/object-store backend. The guard is an
explicit acknowledgement, not a runtime proof. No remote `MERGE`, checkpoint,
concurrent-writer, or incremental-write guarantee is made. Unity Iceberg
remains available only through the existing explicit REST mapping; no Iceberg
deletion-vector-like feature is claimed because the Arrow-59 upstream bridge
does not expose a comparable verifier.

With `--features lakeprism-unity/iceberg-rest`,
`resolve_and_register_iceberg_rest_table` maps only physical `MANAGED` or `EXTERNAL` tables
declared as `ICEBERG` with explicit `iceberg.rest.uri` metadata (and optional
`iceberg.rest.catalog`, `iceberg.rest.warehouse`, `iceberg.rest.prefix`, and
`iceberg.table.identifier`). It uses a freshly vended Unity credential only to build the REST
HTTP client; neither the registered table nor the mapping stores it. LakePrism deliberately does
not infer a REST endpoint from a Unity storage location. The adapter loads genuine Iceberg
metadata and delegates scans to the upstream provider; a live Unity/Iceberg server and remote
object-store scan are outside this test suite.

`TranscriptIndex` selects indexed search only for exact source, operator, model, and parameter lineage matches. Its transcript rows are serializable for persistence in Delta or Iceberg tables; cold extraction validates lineage and stops as soon as the requested global limit is reached.

## Media-aware explanation and bounded index substitution

Phase 8 adds the explicit `MediaSession::explain_media_transcript_search` and
`MediaSession::search_media_transcript` integration points. They accept a
`MediaTranscriptSearchRequest` and `ProgressiveScheduleLimits`, rather than
claiming an unsupported `EXPLAIN MEDIA` grammar or an arbitrary DataFusion 55
optimizer rewrite. `MediaSearchExplanation` reports the selected
`PersistedIndex` or `ColdExtraction` strategy, exact-lineage capability,
automatic-substitution status, calibrated cost units, and (for cold work) the
deterministic schedule.

Call `register_persisted_transcript_segments` after an application has loaded
credential-free `TranscriptSegment` rows from its immutable Parquet,
Delta, or Iceberg snapshot. The query path automatically substitutes those
rows only when the full source/operator/model/parameter lineage is equal; a
source-version or model change produces a cold plan instead. Ordinary
`lakeprism_transcript_search` SQL deliberately remains index-only: it does
not initiate model work. This keeps SQL evaluation, governance, and model
authorization explicit.

`MediaCostModel` uses application-calibrated **cost units**, not fabricated
latency. `ProgressiveScheduleLimits` bounds all cold work globally by work
item count, aggregate cost, aggregate transcript segments, and deterministic
wave width. `ProgressiveSchedule` sorts stable IDs before grouping waves, so
admission, skipped IDs, and ordering are reproducible. Items that exceed an
aggregate bound are deterministically skipped rather than silently exceeding
it. A `MediaTranscriptSearch` uses one
bounded `TranscriptionRequest` per admitted media item, retaining existing
provider validation. `MediaSession::search_media_transcript` obtains
non-waiting shared query and CPU permits, so cold work cannot bypass the
session's global capacities; exhausted capacity is reported explicitly.
It cannot preempt a provider already executing synchronously, and this
explicit synchronous API has no query ID/cancellation handle. Use the existing
streaming `MediaSession` query APIs when cancellation/status lifecycle is
required; applications executing concurrent schedule waves must share the
same `ExecutionGovernor`.

Run the reproducible cold/warm scaffold with:

```bash
cargo bench -p lakeprism-index --bench cold_warm
bash scripts/check-cold-warm-benchmark.sh
```

It proves the warm exact-lineage path makes zero additional provider calls and
prints machine-readable local timings. `benchmarks/cold-warm-baseline.json`
deliberately contains no measured values: this host has no approved baseline.
The regression script validates a reviewed baseline only after an operator
records measurements on a controlled, architecture-matched host. Its timings
are illustrative only, not a cross-machine performance claim.

## Governed audio transcription and embeddings

`lakeprism-index` supplies `TranscriptionProvider` and `EmbeddingProvider` contracts.
`UnavailableTranscriptionProvider` and `UnavailableEmbeddingProvider` are the default safe
behavior: they return explicit `ProviderUnavailable` errors and never download a model, use cloud
credentials, or fabricate inference. `DeterministicMockTranscriptionProvider` returns registered
fixture rows, while `DeterministicMockEmbeddingProvider` uses normalized token hashing; both are
named mocks, are deterministic, and are unsuitable as real ML inference.

Providers receive bounded transcription requests (`max_segments <= 1024`) and embedding/ranking
limits (embedding dimensions <= 4096; approximate candidates <= 10000). `MediaSession` accepts
only one exact `FeatureLineage` family through `register_embedding_records`, rejecting mixed
source/operator/model/parameter lineage. This makes index substitution exact rather than
best-effort. A plain `MediaSession` has no embedding runtime; construct it with an
application-owned provider or, only for tests/local demos, `DeterministicMockEmbeddingProvider`.

`ranking_semantics = ranked` means every compatible session row was scored and sorted by the
documented formula. `approximate` means only a bounded insertion-ordered candidate subset was
scored, so better rows may be omitted. Flight SQL exposes the same table functions and advertises
their contracts through LakePrism SQL-info. Python exposes `EmbeddingRecord`,
`MediaSession.register_embedding_records`, and lazy `semantic_search`/`hybrid_search` builders;
`MediaSession(embedding_mock_dimensions=N)` is explicitly mock-only.

`lakeprism-index` also defines application-owned `OcrProvider`, `ImageMetadataProvider`, and
`ImageUnderstandingProvider` contracts. They accept a bounded `OcrRequest` (at most 32 MiB,
at most 1,024 OCR results), and every returned OCR/metadata/understanding result must carry
exact `FeatureLineage`. `UnavailableOcrProvider`, `UnavailableImageMetadataProvider`, and
`UnavailableImageUnderstandingProvider` are safe production defaults; they return
`ProviderUnavailable` rather than claiming OCR, metadata parsing, captions, or tags. The named
deterministic OCR mock returns only registered fixtures and is test-only.

`CrossModalIndex` accepts transcript, document, image, audio, and video feature records with a
common bounded embedding dimension. It performs cosine ranking across those modalities only after
an exact source/operator/model/parameter lineage match; modality does not relax lineage. It
contains at most 10,000 records, validates dimensions, and labels bounded candidate searches
`approximate` exactly like `EmbeddingIndex`. Applications must explicitly register produced
feature vectors; LakePrism does not infer captions, OCR text, transcripts, or embeddings.
`MediaSession::register_cross_modal_records` and
`MediaSession::search_cross_modal` expose that bounded index under the session's query/CPU
governor; default sessions retain the unavailable embedding provider and do not generate query
vectors.

## Governed derived-feature snapshots

`lakeprism-derived` persists `DerivedFeature` records to immutable Parquet snapshots. Each
record contains the full portable Arrow `MediaRef` struct, source URI/version, feature kind,
canonical JSON payload, and complete operator/model/parameter lineage. Construction rejects a
lineage URI that differs from its `MediaRef` and rejects credential-like payload fields.
`refresh_parquet(snapshot, manifest, features)` writes only sources absent from the manifest or
whose source version changed, then advances the manifest after the Parquet writer closes. It
therefore never silently re-emits an unchanged source.

Register a particular immutable snapshot through
`lakeprism_derived::register_parquet_snapshot`. The test suite proves DataFusion's physical
Parquet scan retains a projected nested `media.uri`, a source-version predicate/pruning
predicate, and a pushed `fetch=1` limit. MediaRef accessor SQL works over the stored struct.
Document/video/audio table functions still require a literal local URI or a separately registered
MediaRef-bound relation; DataFusion 55 cannot safely expand a stored `media` column through those
table functions. `lakeprism_transcript_search` remains an explicitly registered,
lineage-verified in-session index rather than automatically trusting payload JSON from storage.

Enable `lakeprism-derived/delta-rs` for `register_delta_snapshot`, which pins a Delta transaction
version and delegates lazy scans, pruning, projection, and limit handling to delta-rs. Direct
Delta writes live in `lakeprism-delta` and use delta-rs 1.1 transactions rather than emulating
commit, merge, or checkpoint behavior. Iceberg snapshot reads remain available through
`lakeprism-iceberg`'s existing explicit snapshot-ID adapter.

`DerivedIndexRow` and `refresh_embedding_index_parquet` persist a separate, credential-free
embedding-index Parquet table (`id`, text, canonical `values_json`, source/version, and complete
operator/model/parameter lineage). Refresh uses the same manifest-driven incremental rule as
derived features: only new or source-version-changed rows are written. It stores no model
credentials and performs no inference. Load the immutable table with
`register_parquet_snapshot`, validate dimensions and full lineage, then explicitly register rows
into a session index before semantic SQL; automatic payload-to-index promotion is intentionally
not performed.

`FlightSqlServer` serves Flight SQL statement and prepared-statement queries using opaque UUID handles, caps server-side prepared state at 1,024 handles, streams DataFusion result batches after the Arrow schema message, and returns `NotFound` for closed or unknown handles. Statement tickets are one-use; prepared handles remain valid until explicitly closed. Creating a prepared statement validates the SQL and returns its Arrow IPC dataset schema; it returns no parameter schema. It exposes live DataFusion catalog, schema, table, and table-type metadata, plus capability-accurate SQL-info: transactions are unsupported, while cancellation is cooperatively supported for an active `DoGet` query. A Flight stream disconnect drops the governed execution stream, releases permits, and marks unfinished local work cancelled; cancellation cannot preempt planning, remote scans, or an in-progress synchronous FFmpeg codec call. Prepared parameter binding and transaction actions explicitly return `Unimplemented`: DataFusion 55 offers no safe Flight SQL parameter-binding API for this session model, and `MediaSession` has no isolation primitive. An optional bearer token requires an exact Bearer authorization header on every implemented Flight SQL request.

Flight SQL executes the same **local** extension functions in the table above, including
the feature-gated media UDTFs and session-local transcript index. Python's
`MediaSession.sql` executes this same SQL surface. It does not add remote-media scans,
Unity credential access, or lazy media semantics. In addition
to the standard Flight SQL information keys, `FlightSqlServer` publishes these vendor keys as
string lists through `CommandGetSqlInfo`:

| Key | Meaning |
| --- | --- |
| `10000` (`LAKEPRISM_SQL_INFO_LOCAL_FUNCTIONS`) | The local function contracts listed above, including bounded document support, native-media requirements, and transcript-index-only search. |
| `10001` (`LAKEPRISM_SQL_INFO_EXECUTION_GOVERNOR`) | The process-local `MediaSession` governor limits (`max_concurrent_queries`, `max_cpu_permits`, `max_io_permits`) and lifecycle capabilities. |

These limits govern local execution; they are not client quotas or remote-storage guarantees.

## Python API completion

`lakeprism-python` is a thin PyO3 surface over the Rust implementations; it
does not reimplement execution or accept cloud credentials. In addition to
`MediaRef`, `MediaSession`, `LazyPlan`, transcript/embedding records, Arrow
exports, and local document/media plan builders, the extension exposes:

- `LocalCatalog(path=None)`: durable local catalog open/create, media-table
  registration, `execute_ddl("CREATE MEDIA TABLE …" | "DROP TABLE …" |
  "SHOW TABLES")`, discovery, refresh, and registration into a
  `MediaSession`. Persistent metadata remains credential-free.
- `MediaSession.register_parquet(table_name, path)` and
  `register_derived_snapshot(table_name, snapshot_path)`: local lazy Parquet
  registration. `DerivedIndexRow` plus
  `refresh_embedding_index(snapshot_path, manifest_path, rows)` persist the
  deterministic, lineage-bearing derived-index snapshot contract. Loading a
  snapshot never automatically promotes vectors to a semantic index.
- `MediaSession(embedding_mock_dimensions=N)`: explicitly selects the named
  deterministic mock provider; no Python constructor selects or downloads a
  production transcription/embedding runtime.
- With `--features lakeprism-python/delta-rs`,
  `MediaSession.register_delta(table_name, local_uri, version=None)` registers
  delta-rs' native lazy provider, optionally pinned to a transaction version.
  The Python wrapper requires a credential-free local `file://` URI.
- With `--features lakeprism-python/unity`, `UnityQueryContext(query_id,
  principal, catalog_identity=None)` and `UnityCatalog(base_url,
  token_supplier)` support a fresh-token Unity REST request and credential-safe
  registration. `MediaSession.register_unity_local_parquet(...)` provides
  governed registration for already-resolved local physical Parquet metadata.
  `MediaSession.attach_unity_schema(catalog, schema, tables, context)` is the
  one-call notebook API for a batch of such resolved tables and returns attached
  names plus explicit unsupported reasons. `UnityCatalog.resolve_and_register`
  vends an OAuth token only for its REST request; LakePrism retains the callback,
  not a returned token. With both `unity` and `delta-rs`, its managed-Delta
  method consumes temporary S3 credentials internally. Python never receives,
  serializes, or persists OAuth or temporary object-store credentials.
- With `--features lakeprism-python/flight`, `FlightServer(session)` starts a
  loopback-only Flight SQL server and `FlightClient(endpoint).execute(sql)`
  performs local Flight SQL statement/ticket/DoGet execution. `FlightServer`
  exposes `start(port=0)`, `endpoint`, and `stop()`. The Python client rejects
  non-loopback endpoints. Its optional `auth_supplier` is called per request
  and applies only to that loopback client; authenticated remote Flight
  deployment remains an application-owned Rust boundary.

The published Python project is configured for Maturin (`pip install
lakeprism`); `pip install "lakeprism[notebook]"` adds optional PyArrow, pandas,
IPython, and tqdm. `LazyPlan.to_pyarrow()` transfers a native Arrow C stream,
while `lakeprism.to_pandas(plan)` and `lakeprism.display(plan, max_rows=100)`
provide notebook materialization and a bounded preview. For lifecycle-aware
interactive work, `lakeprism.collect_with_progress(session, None, sql)` polls
the actual Rust query status and row count; it does not invent a completion
percentage. See `examples/notebooks/local_and_databricks.py`.
`lakeprism.discover_databricks_workspace()` discovers and validates only an
HTTPS workspace host from `DATABRICKS_HOST` or the selected Databricks CLI
profile. It never exposes a profile token or OAuth/service-principal secret;
credential vending remains a Rust application boundary.

`docs/CLOUD_ACCEPTANCE.md` and the manually dispatched
`cloud-acceptance.yml` workflow define the only live cloud contract. They use
short-lived environment-injected OAuth/service-principal tokens for an
authenticated Unity metadata read, never persist a secret, and never claim a
remote Delta write.

The Python wrapper intentionally has **no** Iceberg constructor or remote REST
catalog registration. The existing Iceberg adapter requires an already-open
Rust `iceberg::table::Table` (or an application-owned request-scoped REST
client); creating either from Python would force a remote/catalog credential
policy that this local-first binding must not fabricate. Use the Rust
`register_iceberg_table`/`ScopedRestCatalog` APIs with their explicit feature
checks instead. Similarly, Python does not expose remote Delta, direct Unity
FILE reads, or cloud object-store configuration.

```bash
cargo test
```

[`docs/ACCEPTANCE_MATRIX.md`](docs/ACCEPTANCE_MATRIX.md) maps each claimed
local capability to its generated credential-free fixture, test command, and
host-native limit.

Build native media support with system FFmpeg development libraries that match the Rust target architecture:

```bash
cargo build -p lakeprism-media --features native-media
```

`validate_native_runtime()` initializes the FFmpeg libraries linked into the Rust process and
returns their loaded format/codec/utility versions. It is the authoritative matched-target
runtime check; LakePrism does not trust a separate shell `ffmpeg` executable. The default bounded
native profile accepts MP4-family, Matroska/WebM, and WAV containers; H.264/HEVC/VP9/AV1/MPEG-4/
MJPEG video; and AAC/MP3/Opus/Vorbis/FLAC/PCM-S16LE/PCM-F32LE audio. Unsupported containers or
codecs return explicit errors before decode. `probe_media_with_profile` permits an
application-owned explicit profile when that small coverage is insufficient.

Build the Python extension with a PyO3-compatible tool such as Maturin:

```bash
maturin develop --manifest-path crates/lakeprism-python/Cargo.toml
pytest crates/lakeprism-python/python/lakeprism/test_bindings.py
```

`LocalCatalog::open(path)` persists local media-table definitions and optional
`FeatureLineage` in a versioned `catalog.json`. Each mutation takes an exclusive
cross-process advisory lock, reloads the latest committed state, writes and syncs a
unique temporary file, atomically renames it, and syncs the catalog directory. This
prevents lost updates between cooperating local processes and ensures restart sees a
complete old or new catalog. Catalog identifiers are restricted to ASCII
`[A-Za-z_][A-Za-z0-9_]*`, and catalog paths are fixed beneath the supplied root.
`CREATE MEDIA TABLE name`, `DROP TABLE name`, and `SHOW TABLES` are the supported
local DDL via `LocalCatalog::execute_ddl`; use `table_names` and `table_metadata`
for discovery. The persistent catalog rejects signed media URLs and credential-like
metadata or lineage fields; it never persists vended credentials.

## Local CLI and REPL

`lakeprism-cli` provides a local-only binary backed by the same durable
credential-free `LocalCatalog` and governed `MediaSession`:

```bash
# The default catalog is ./.lakeprism; choose a durable location explicitly in automation.
cargo run -p lakeprism-cli -- --catalog ./catalog init
cargo run -p lakeprism-cli -- --catalog ./catalog register media file:///data/clip.mp4 video
cargo run -p lakeprism-cli -- --catalog ./catalog ddl 'CREATE MEDIA TABLE documents'
cargo run -p lakeprism-cli -- --catalog ./catalog sql --format json \
  'SELECT lakeprism_media_uri(media) AS uri FROM media'
cargo run -p lakeprism-cli -- --catalog ./catalog sql --format csv \
  'SELECT count(*) AS media_count FROM media'
cargo run -p lakeprism-cli -- --catalog ./catalog explain-media \
  'SELECT lakeprism_media_type(media) FROM media'
cargo run -p lakeprism-cli -- --catalog ./catalog shell
```

The `register` command accepts only portable `MediaRef` values and persists no
credentials. Its optional storage mode is `external`, `managed`, or `inline`.
`ddl` deliberately supports only `CREATE MEDIA TABLE name`, `DROP TABLE name`,
and `SHOW TABLES`; ordinary SQL belongs to `sql`. Results are JSON by default,
RFC-4180-style quoted CSV with `--format csv`, or Arrow IPC stream bytes with
`--format arrow` (redirect binary output to a file). JSON and CSV use Arrow's
stable display representation for non-null values, so nested Arrow values are
readable but are not typed JSON objects.

`EXPLAIN MEDIA` in the shell and `explain-media` in the CLI return the honest
DataFusion `EXPLAIN` plan for local SQL. Exact-lineage persisted-index versus
cold-extraction selection remains the explicit Rust
`MediaSession::explain_media_transcript_search` API; this CLI does not claim a
DataFusion optimizer rewrite that does not exist. Every `sql` execution emits a
process-local query ID on stderr. `query-status` and `query-cancel` expose the
same cooperative, process-local lifecycle controls. In the shell, `\run SQL`
starts a local background query and prints its ID; use `\status ID` or
`\audit ID` or `\cancel ID` from that same shell while it runs. `query-audit`
and `\audit` emit one structured JSON lifecycle event containing only the
query ID, status, timestamps, elapsed time, batch count, and row count. They
never include SQL text, URIs, principals, request metadata, credentials, or
error text. One-shot `query-status`, `query-audit`, and
`query-cancel` commands cannot discover jobs from another process, by design:
LakePrism does not create a daemon or persist query state. The shell's
`\history` prints the persisted input history.

## CI and release acceptance

`.github/workflows/ci.yml` runs formatting, Clippy, default Rust tests, CLI
acceptance, Flight/Rust gRPC integration, and a target-matched x86_64 Linux
native job. The native job installs FFmpeg development libraries and the
FFmpeg executable, then runs native media, DataFusion media UDTF, Flight SQL,
and PyO3/Maturin Python-to-Flight acceptance tests in one architecture-matched
environment. This local macOS host may not link its installed x86_64 FFmpeg or
Python libraries from an arm64 Rust target; no native result is claimed here.

### Python wheels

Build wheels with `scripts/build-python-wheels.sh`. It uses `uv tool run` to
install and invoke Maturin 1.10.2 without publishing or requiring
credentials:

```bash
# Default: a host-native abi3 wheel with Flight, Delta, and Unity bindings.
bash scripts/build-python-wheels.sh --platform native --out dist/python

# Only on a target-matched host with FFmpeg development libraries.
bash scripts/build-python-wheels.sh --platform native --native-media --out dist/python
```

The script accepts an explicit `--platform` and `--target`, but rejects any
target that differs from `rustc -vV`'s host. This is intentional: neither
PyO3 extension linking nor optional FFmpeg can reliably be cross-compiled and
runtime-tested from one machine. `--manylinux POLICY` is required for the
`manylinux-x86_64` platform and is passed through to Maturin/auditwheel; use a
matching manylinux image. `musllinux-x86_64` likewise requires a native musl
host and `--manylinux off`.

The extension is built with PyO3 `abi3-py39`. One wheel is tagged
`cp39-abi3` and is intended for CPython 3.9+; the build interpreter should be
CPython 3.9 or newer. This is an ABI policy, not a claim that every optional
native dependency works on every Python or operating-system release.

The PyPI workflow runs the supported base-wheel matrix using these **native**
invocations (never a cross-compiling runner):

| Wheel target | Native runner/image | Invocation |
| --- | --- | --- |
| manylinux x86_64 | x86_64 `manylinux_2_28` image with Rust/Python 3.9 | `bash scripts/build-python-wheels.sh --platform manylinux-x86_64 --manylinux 2_28 --python python3.9` |
| macOS x86_64 | `macos-13` x86_64 runner with Python 3.9 | `bash scripts/build-python-wheels.sh --platform macos-x86_64 --python python3.9` |
| macOS aarch64 | Apple Silicon runner with Python 3.9 | `bash scripts/build-python-wheels.sh --platform macos-aarch64 --python python3.9` |
| Windows x86_64 | Windows x86_64 runner with Git Bash, Rust, and Python 3.9 | `bash scripts/build-python-wheels.sh --platform windows-x86_64 --python python.exe` |

The standard PyPI matrix excludes `--native-media`: FFmpeg is a system/native
dependency and a portable wheel must not falsely imply bundled codecs. Each
wheel and the sdist are installed into a clean virtual environment and imported
before trusted publication. Build and test a separate `--native-media`
artifact only in a target-matched environment with compatible FFmpeg
development libraries and an explicit distribution policy.

`.github/workflows/release.yml` runs only on `v*` tags (or manually), repeats
locked validation and native acceptance, builds the release CLI binary, emits
a source archive and `SHA256SUMS`, uploads them as workflow artifacts, and
attaches them to tag releases. It makes no reproducibility claim: toolchains
and native dependencies still affect binary output. PyPI wheels and sdists are
built, smoke-tested, and published exclusively by `.github/workflows/pypi.yml`.
Reproduce the CLI build locally on a matching Linux host with
`cargo build --locked --release -p lakeprism-cli`.

For Flight SQL, the CLI is intentionally an honest local server only:

```bash
cargo run -p lakeprism-cli -- --catalog ./catalog flight --addr 127.0.0.1:5005
```

It refuses non-loopback addresses, does not accept bearer tokens on the command
line, and does not provide cloud, remote Flight, or daemon management. Use the
Rust `lakeprism-flight` integration for an application-owned authenticated
deployment. The interactive shell stores entered commands in
`CATALOG_DIR/history`, supports `\tables`, `\ddl`, `\register`, `\status`,
`\cancel`, `EXPLAIN MEDIA ...`, and `\quit`; it has no shell escapes or remote
connection commands.

`MediaSession.register_media_refs(name, media_refs)` registers Python `MediaRef`
values in a local DataFusion session. `MediaSession.register_transcript_segments(segments)`
adds `TranscriptSegment` values to that session's bounded SQL search index.
`MediaSession.sql(query)` (or `execute`) returns
`list[dict[str, str | None]]`; non-null Arrow values use their stable Arrow display
representation, so this binding requires neither PyArrow nor Python-side row decoding.
Result column names must be unique; use SQL aliases for duplicate expressions.
Python also exposes `create_query(deadline_millis=None)`, `execute_query(query_id, sql)`,
`query_status(query_id)`, and `cancel_query(query_id)`. Execute from one Python
thread and cancel/status-check from another; these APIs are intentionally local and
cooperative, with the same DataFusion/native-decoder limitations stated above.
For lazy execution, `MediaSession.plan(sql)` returns a `LazyPlan`; it does not parse,
decode, or execute SQL until `collect`, `to_pyarrow`, `to_arrow_c_stream`, or
`to_arrow_ipc` is called. `MediaSession.document_sections`, `document_tables`,
`document_images`, `document_search`, `video_frames`, and `audio_segments` are thin
plan builders over the same local SQL functions above: they do not implement a second
Python execution path. Video and audio validation is immediate, but native FFmpeg
opening/decoding remains scan-time work.

`LazyPlan.to_arrow_c_stream()` and `MediaSession.sql_arrow_c_stream(sql)` return a
standard `arrow_array_stream` Arrow C Data Interface capsule. `LazyPlan.to_pyarrow()`
and `MediaSession.sql_arrow(sql)` import that stream directly into a
`pyarrow.RecordBatchReader`, preserving Arrow types and streaming batches without IPC
serialization or Python row conversion. PyArrow is optional and imported only by the
PyArrow methods. `sql_arrow_ipc` and `LazyPlan.to_arrow_ipc` remain explicit
compatibility exports; because they return one `bytes` value, they intentionally
materialize the complete result.

Prepared Flight SQL statements are typed only where the underlying engine can bind
them. DataFusion 55 does not expose safe Flight SQL placeholder binding for this
session model, so Flight SQL advertises an empty parameter schema and
`DoPutPreparedStatementQuery` explicitly returns gRPC `Unimplemented`; it never
interpolates parameter values into SQL text.
