# Local acceptance matrix

This matrix is the supported-host validation contract. Every fixture is
created locally by its test and is credential-free; native codecs and the
PyO3 extension are validated only in the target-matched CI job.

| Capability | Fixture and acceptance path | Expected result on this host |
| --- | --- | --- |
| Durable catalog and CLI | `lakeprism-cli/tests/acceptance.rs`; a fresh catalog under `target/lakeprism-cli-acceptance` | `init`, `register`, reopened SQL, DDL discovery, CSV output, and `EXPLAIN MEDIA` agree with the Rust catalog/session path. |
| Governed Rust SQL and lifecycle | `lakeprism-datafusion` lifecycle, permit, deadline, cancellation, audit, and mixed-workload tests | Query/cpu permits remain bounded; cancellation and expired deadlines become terminal statuses; planning failures become `Failed` rather than remaining `Pending`. |
| Local documents, tables, images, and OCR | generated minimal DOCX/PDF ZIP fixtures in `lakeprism-datafusion` and `lakeprism-documents` tests | Sections, normalized tables, and image payloads are bounded. Default OCR returns a provider-unavailable error; the registered deterministic fixture proves the contract without fabricated OCR. |
| Local video and audio | generated MP4/stereo WAV/WebM fixtures in `lakeprism-media` and Python tests; missing local URIs without `native-media` | Matched hosts decode real frames and normalized mono 16 kHz audio. Audio acceptance checks resampler tails, sample counts, stream offsets/gaps, shorter audio streams, quantized timestamps, and pre-allocation PCM bounds. Builds without native codecs return explicit `NotImplemented` errors. |
| Durable recording workflow | `lakeprism-derived::recordings` and installed Python tests; real-model release example | Fingerprints include content and model/operator/parameter lineage. Crash/disk-full publication boundaries preserve complete commits. Explicit recovery/removal/vacuum retain the previous generation and protect lazy/physical/Arrow readers. Interrupted adapters are reaped. Late audio windows seek with decoder preroll. |
| Transcript, semantic, hybrid, and cross-modal indexes | deterministic lineage-bearing fixtures in `lakeprism-index` and `lakeprism-datafusion` tests | Exact lineage is required; transcript search remains index-only; semantic/hybrid ranking labels and bounded cold work are deterministic. |
| Optimizer explain and cold/warm | `lakeprism-index` unit tests, `benches/cold_warm.rs`, and `scripts/check-cold-warm-benchmark.sh` | Exact persisted lineage selects warm work with zero new provider calls; changed lineage selects bounded cold work. The benchmark prints machine-readable local timings. |
| Flight SQL equivalence | `lakeprism-flight` tonic client, prepared statement, metadata, cancellation, and disconnect tests | Flight’s statement/ticket/DoGet path returns the same Arrow batches from the shared `MediaSession`; handles are opaque and ticket use is bounded. |
| Local distributed execution | `lakeprism-execution` coordinator tests | Stable source-identity partitioning and partition-order results are independent of input/completion order. Cancellation is cooperative; lease tokens reject stale workers; only explicitly idempotent tasks retry. The implementation is in-memory worker abstraction, not process supervision or remote transport. |
| Python source and runtime surface | `lakeprism-python` unit tests, all-feature check, and `python/lakeprism/test_bindings.py` | SQL plans, Arrow C stream/IPC, catalog, search, local Flight, transcription configuration, and native media execute against the shared Rust engine. PyArrow must be installed in the runtime job so Arrow checks do not silently skip. |
| Local and S3-compatible paths | `lakeprism-storage --features s3` Wiremock tests; Delta/Iceberg/Unity optional-feature tests | Local range reads and loopback S3 mock HEAD/range/staging paths are bounded and credential-safe. Optional catalog adapters compile and their local/mock registrations pass; no live remote backend is claimed. |

## Required commands

```bash
cargo test --workspace --exclude lakeprism-python --all-targets
cargo test -p lakeprism-storage --features s3
cargo test -p lakeprism-delta --features delta-rs
cargo test -p lakeprism-iceberg --features iceberg-rest
cargo test -p lakeprism-unity --features unity,delta-rs,iceberg-rest
cargo test -p lakeprism-embedding -p lakeprism-whisper --all-targets --features fixture-provider
cargo check --workspace --all-features
cargo clippy --workspace --exclude lakeprism-python --all-targets -- -D warnings
bash scripts/check-cold-warm-benchmark.sh
```

The `native-runtime` CI job runs actual FFmpeg and Maturin/PyO3 execution in
an isolated Python environment. CI and release Rust jobs also run optional
S3/Delta/Iceberg REST/Unity tests and bounded subprocess-provider fixtures
rather than checking only default features.
The published `0.1.0a1` matrix tested portable bindings without native decode.
Release `1.0.0rc1` passed real native video/audio tests on every installed
wheel, with its FFmpeg build prefix hidden. Python fixtures do not invoke a
system FFmpeg executable. Alpine exercises native decoding without PyArrow.
An additional CPU workflow runs a pinned real speech model on an openly
licensed recording, performs SQL search, and checks persisted Parquet results.

Local native validation also works when the Rust compiler, FFmpeg libraries,
and Python interpreter share the same architecture. On macOS with both Intel
and Apple Silicon Homebrew installations, select the matching `pkg-config`
directory and executable path explicitly. For an arm64 installation:

```bash
export PKG_CONFIG_PATH=/opt/homebrew/lib/pkgconfig
export PATH=/opt/homebrew/bin:$PATH
bash scripts/build-wheel-ffmpeg.sh
export FFMPEG_DIR="$PWD/target/wheel-ffmpeg"
export PKG_CONFIG_PATH="$FFMPEG_DIR/lib/pkgconfig"
python3 -m venv .native-venv
.native-venv/bin/python -m pip install maturin==1.10.2 pytest pyarrow
VIRTUAL_ENV="$PWD/.native-venv" .native-venv/bin/maturin develop \
  --locked --manifest-path crates/lakeprism-python/Cargo.toml --all-features
LAKEPRISM_NATIVE_MEDIA=1 .native-venv/bin/python -m pytest \
  crates/lakeprism-python/python/lakeprism/test_bindings.py
```

## Architecture plan versus the 0.1.0 alpha

The architecture document is a multi-release roadmap, not the acceptance
contract for one alpha. The current release proves local relational workflows
and integration boundaries; it does not satisfy every phase's original exit
criterion. In particular, the recommended correlated Delta-to-video `LATERAL`
query is not implemented: media table functions require literal arguments.

| Architecture phase | Present in the alpha | Remaining exit criteria |
| --- | --- | --- |
| 0: Spike | Shared Rust/DataFusion/Arrow/PyO3 engine, lazy Python plans, governed query lifecycle. | Streaming media operators that can be cancelled during decode, rather than bounded materialization during planning/scan. |
| 1: Video/audio | MediaRef, local and mock S3 storage, native frames/audio, metadata, time/limit/output-column pushdown. | Seek-aware decode and proof of avoided source bytes/seconds. Current video sampling reopens and decodes from the beginning; omitting payload columns does not avoid RGB conversion or PCM decode. |
| 2: Delta/Parquet | Local Delta read/write and version selection; Parquet feature/index snapshots with lineage. | Correlated media execution over pruned Delta rows; atomic dataset publication and equivalent governed feature persistence across table formats. |
| 3: Unity/Databricks | Scoped REST/OAuth and credential vending, FILE/Volume adapters, mock metadata/managed Delta registration. | Live non-production governed scans and authorization validation. Protected cloud CI checks authentication/metadata only, not the data plane. |
| 4: Iceberg | Rust snapshot/table providers and scoped REST catalog registration; compatible dependencies compile. | Live Delta/Iceberg multimodal equivalence, managed Databricks Iceberg acceptance, and a safe Python construction boundary. |
| 5: Search/indexes | Configured local transcription/embedding subprocesses, keyword/semantic/hybrid/cross-modal contracts, lineage-based warm substitution and source-version refresh. | Model-quality evaluations, full operator/model/policy-aware refresh publication, and durable governed indexes in both Delta and Iceberg. Models are application supplied. |
| 6: Documents | Bounded PDF/DOCX extraction, DOCX tables/images, heuristic PDF tables/encoded images, OCR provider contract. | Layout-quality evaluation, production OCR provider, and generalized extraction/search that streams instead of bounded planning-time work. |
| 7: Flight SQL | Native statement/ticket/DoGet, metadata, prepared-statement subset, cancellation/disconnect cleanup and local Python round trips. | Production remote TLS/authentication policy, parameter binding, and supported-client compatibility acceptance. |
| 8: Optimization | Deterministic bounded transcription waves, cold/warm explain/index tests and avoided-provider-call benchmark. | Cost-based physical media planning, broad progressive decode/inference, production detection/action models, and per-operator avoided-work telemetry. |
| 9: Distribution | In-memory coordinator, stable partitions, leases, idempotent retries and ordered results. | Worker processes, remote transport, supervision, durable recovery and remote semantic equivalence. |

The feature tiers also remain separate from alpha completion. Transcription
and embeddings have configured-provider paths; OCR and image understanding
have contracts, not bundled production models. Diarization, scene extraction,
clip/region export, perceptual deduplication, and action/event detection are
future product work, not capabilities implied by a passing local suite.

## Local product 1.0 acceptance

The proposed 1.0 support contract is local recordings to a persistent,
searchable dataset through the installed Python package. Cloud integrations,
distributed workers, production remote Flight, OCR, diarization and scene
detection are not prerequisites for this product contract.

Release `0.1.0a2` supplies file/folder ingestion, an optional packaged English
CPU speech adapter, explicit model preparation, bounded 30-second decode
windows, immutable per-recording Parquet, atomic manifest publication,
unchanged reuse, content/model/operator/parameter invalidation, and reopening
for SQL without a model. Recovery is per recording, not per inference window.
Search is case-insensitive substring matching, not semantic retrieval.

**Published RC1 media hardening:** `1.0.0rc1` preserves audio-stream offsets
and gaps, accepts audio ending before its video container, limits requested PCM
to 64 MiB per decode call, and reports fixed redacted speech failure categories.
Installed-package tests cover these contracts; native WebM tests compare sample
counts against FFmpeg. These changes are not in published `0.1.0a2`.

**Published RC1 lifecycle hardening:** an owned batch writer validates one manifest
per folder refresh. Explicit recovery uses only a validated prior commit;
removal/vacuum preserve reader leases and one recovery generation. Fault tests
inject storage-full errors and abrupt child death at nine publication
boundaries. Installed tests preserve the exact transcript Arrow schema and
reopen original Parquet bytes written by production `0.1.0a2`.
The [public candidate contract](LOCAL_RECORDINGS_CONTRACT.md) specifies stable
APIs, results/errors, format handling and local-filesystem limitations.

Before calling this 1.0, require:

1. A successful staged seven-platform publication of the exact product bytes,
   including installed native and public workflow acceptance. Optional model
   dependencies have a narrower support matrix than core ABI3 wheels.
2. Licensed real-recording evaluation covering long natural speech, silence,
   noise, window boundaries and representative container/codecs. Publish
   transcription quality, peak memory, cold throughput and unchanged reuse
   measurements. Repeating the short sample proves execution, not quality.
3. Explicit compatibility commitments for public Python APIs, error/result
   shapes, the transcript Arrow schema, and manifest version handling, with
   supported-version reopening/upgrade tests.
4. Crash and disk-failure acceptance around publication, supported-host
   interruption acceptance, and a documented snapshot-retention strategy.
   Explicit vacuum now reclaims eligible old/orphaned files while preserving
   readers and one prior generation. Local filesystem atomic rename and
   advisory locking remain required; shared network storage and Windows
   power-loss durability are not promised.
5. Feedback from users processing their own recording collections, followed
   by a beta/API freeze and a release candidate with no known blocking defects.

The automated publication, quality, compatibility and lifecycle gates passed
for published `1.0.0rc1` in
[run 42722620622](https://github.com/danielbeach/lakeprism/actions/runs/42722620622),
from source `191a937110a30b2d70a2622ec43ab1624bfec580`.
Actual user-collection feedback remains outstanding; stable 1.0 is not released.

The model runtime uses one inference thread; the SQL runtime can use more.
The adapter bounds each file to 1 GiB, two hours and 8,192 segments, with a
two-hour timeout. It requires a known duration and does not overlap inference
windows; boundary words and context can be lost. Audio windows seek with one
second of preroll, but container/keyframe layout can require extra decoding.
No general avoided-byte performance guarantee is implied.

### Reproducible candidate observations

`examples/local_transcription/evaluate.py` pins a SHA-256-verified 9.2 MB
LibriSpeech subset (CC BY 4.0), reconstructing **73 distinct excerpts in three
chapters**. Longest recording: **209.2 seconds**. It evaluates clean speech,
seeded 15 dB white-noise variants, a 45-second digital-silence control,
timestamp bounds, reopening and unchanged reuse. Thresholds were declared
before execution: corpus word error rate at most 25% clean and 40% noisy,
zero silence segments, and a natural-speech timeline longer than 120 seconds.
The first run failed silence acceptance; a digital-zero guard fixed that
defect without loosening the gate.

Linux x86_64 / Python 3.12.3 RC1 artifact results from the publication run:

| Measurement | Observed |
| --- | --- |
| Clean / noisy word error rate | 10.01% / 16.85% |
| Silence transcript segments | 0 |
| Evaluated audio / cold wall time | 1,043.57 s / 45.829 s |
| Cold real-time factor | 0.0439 |
| Unchanged refresh | 0.075 s |
| Maximum parent / child RSS | 374.5 / 451.0 MiB |

RSS values are independent process maxima, not simultaneous aggregate memory.
The sample is limited read-English narration, not conversational/accent
coverage or an accuracy guarantee. These are observations on one hosted
release-test machine, not cross-platform performance promises.

`benchmark_refresh.py` separately measures **synthetic adapter and metadata
overhead**, never ASR throughput. For 256 files, cold fixture publication took
6.6565 s; unchanged folder refresh took 0.0377 s versus 6.4927 s for 256 separate
one-file calls. Both warm paths verified zero extra provider calls.

Hosted candidate gates repeat natural-speech acceptance and refresh measurement
on Linux x86_64. The real-model sample, long-window workflow and Parquet
reopening run on five native non-Alpine platforms with Python 3.12.
Those hosted gates passed for `1.0.0rc1`; every new release must repeat them.

### Release gates outside local validation

Release `1.0.0rc1` passed these hosted gates and is published on PyPI.
[Publication evidence](https://github.com/danielbeach/lakeprism/actions/runs/42722620622)
covers all seven wheel platforms, the source distribution, TestPyPI rehearsal,
and production artifact hash and installation checks.

Local checks do not prove a successful GitHub runner matrix or a public
release. The PyPI workflow requires reusable core/native CI and native-target
manylinux/musllinux x86_64/arm64, macOS Intel/Apple Silicon, and Windows x86_64
wheel tests. Each wheel is installed on CPython 3.9, 3.12, and 3.14. Alpine
skips the optional PyArrow checks; notebook extras on Alpine are not validated.
Tests run outside the Python source package in isolated mode, preventing a
local development extension from masking installation errors.

Before production publication, the workflow uploads to TestPyPI, verifies the
complete eight-file release by SHA-256, and tests installed TestPyPI wheels on
every platform. Production receives exactly those same files and is checked
again after upload. The `testpypi` and `pypi` GitHub environments restrict
deployment to `main` and matching `v*` tags; each index has a separate OIDC
trusted publisher. Release `0.1.0a2` adds bundled native FFmpeg and the persistent
recording workflow to this matrix; `1.0.0rc1` adds the lifecycle, quality and
five-platform speech gates above. Every new version must repeat these gates.
Wheel checks require all five shared libraries, the exact corresponding source archive, LGPL
notices, and a configuration disabling GPL/nonfree/version3 components.

Protected non-production cloud/data-plane acceptance is still required before
advertising validated production cloud support, but does not gate the
explicitly local-first release candidate.
