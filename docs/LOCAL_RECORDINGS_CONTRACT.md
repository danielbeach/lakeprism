# Local recordings: 1.0 candidate contract

The supported product is **local English recordings to a persistent searchable
dataset**, through the installed Python package. This contract is frozen for
the 1.0 release candidate; stable 1.0 still requires the
[release gates](ACCEPTANCE_MATRIX.md#local-product-10-acceptance), including
feedback on users' own recordings. The architecture roadmap is not a promise
that every integration is production-ready.

## Support boundary

Core ABI3 wheels target CPython 3.9 and newer on Linux glibc/musl x86_64 and
ARM64, macOS Intel/Apple Silicon, and Windows x86_64. Release installation gates
exercise Python 3.9, 3.12 and 3.14 on all seven platforms.

The optional `speech` extra has a narrower release gate: **CPython 3.12** on
Linux glibc x86_64/ARM64, macOS Intel/Apple Silicon and Windows x86_64.
Alpine/musl speech dependencies, other Python versions, GPU inference,
diarization and non-English accuracy are not supported claims. The supported
targets passed the
[RC1 hosted publication run](https://github.com/danielbeach/lakeprism/actions/runs/42722620622).
Stable 1.0 still requires actual user-collection feedback.

Windows speech also requires the
[Microsoft x64 Visual C++ runtime](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist),
a [CTranslate2 prerequisite](https://opennmt.net/CTranslate2/installation.html).
Hosted runners already provide it; a clean desktop may need installation.
CPU inference does not require CUDA, a GPU, system FFmpeg or a Rust compiler.

Native decoding supports WAV, MP4/MOV/M4A and Matroska/WebM with the codecs
listed in [the README](../README.md#documents-media-and-retrieval). Unsupported
containers/codecs fail explicitly. The speech adapter selects the first audio
stream, requires a known positive duration, and bounds a recording to 1 GiB,
two hours, 8,192 segments and a two-hour subprocess timeout.

Model preparation is an explicit download of pinned tiny.en weights.
Inference uses local files, CPU/int8, one inference thread, independent
30-second windows and no voice activity detector. Digital-zero audio windows
are not sent to the model. This does not guarantee rejection of all nonspeech
or hallucinations; window boundaries may lose words or context. SQL can use
multiple threads. There is no strict single-core or accuracy guarantee.

Use a **dedicated application-owned dataset directory**, separate from original
recordings and unrelated files. Local filesystems must support advisory file
locks and atomic replacement. Shared/network filesystems, hostile concurrent
filesystem edits and tamper-proof storage are outside the contract.
Original inputs must remain stable during ingestion.

## Frozen Python surface

| API | Contract |
| --- | --- |
| `prepare_speech_model(path)` | Explicitly prepares and verifies the pinned model; no ingestion-time download. |
| `SpeechModel(path)` | Configures the prepared English CPU model and complete model/operator/runtime lineage. |
| `RecordingDataset(path)` | Creates a new local dataset or validates/reopens an existing one. Does not silently reset damaged datasets. |
| `dataset.path` | Canonical dataset `Path`. |
| `dataset.ingest(file_or_folder, model=...)` | Sorted, deduplicated recursive selection; at most 10,000 files per call. Each successful recording commits independently. Deleted input files are not implicitly removed. |
| `dataset.session().plan(sql)` | Lazy SQL over the pinned `transcripts` relation. |
| `dataset.search(text, limit=100)` | Lazy case-insensitive substring search, source/time/text columns; limit 1–10,000. Not semantic retrieval. |
| `dataset.recordings()` | List of `source_uri`, `source_version`, `segments` dictionaries, ordered by canonical URI. |
| `dataset.remove(path_or_exact_file_uri)` | Remove a source's current entry; return whether present. Does not delete original files. A missing original path is accepted. |
| `dataset.vacuum()` | Explicitly reclaim eligible unreferenced snapshots/manifest temporaries; return `removed_files`, `reclaimed_bytes`. |
| `RecordingDataset.recover(path)` | Explicitly restore a validated previous commit only when the active manifest is missing/invalid; return `restored`, `recordings`. Reopen afterward. |

`collect()` is a convenience display conversion: values are strings or `None`.
Use `to_pyarrow()` to retain Arrow types. Close Arrow readers and drop sessions
and lazy plans when finished. Reader leases pin old snapshots until release.
No background garbage collector or within-recording resume is implied.

Ingestion success entries contain `path`, `status` (`written` or `reused`) and
integer `segments`. A failed entry contains `path`, `status="failed"`, `error`,
`error_code`, and boolean `published`. All selected files are attempted after
ordinary per-file failures; then `IngestionError.results` includes all
outcomes. Invalid selection/model configuration can raise `ValueError`,
`FileNotFoundError` or the originating configuration error before any worker
starts. `KeyboardInterrupt` cancels/reaps the current adapter, stops remaining
files and propagates; already committed recordings remain available.

Dataset operations raise `RecordingError`, a `RuntimeError` subclass, with
stable `.code` and `.published` attributes. Messages are diagnostic, not a
parsing interface. Dataset codes include `invalid_manifest`,
`unsupported_manifest_version`, `manifest_missing`, `dataset_limit`,
`durability_uncertain`, `unsafe_path`, `writer_busy`, `readers_busy`,
`invalid_source`, `source_changed`, `invalid_transcript`, `io`,
`snapshot_failed`, `provider_failed`, `query_failed`, and `dataset_failed`.
Provider codes additionally include the
[fixed adapter categories](../README.md#transcribe-local-audio-or-video)
and governed execution failures such as timeout/cancellation.

**`durability_uncertain` / `published=True` means the new manifest is already
visible but its final directory sync failed.** Reopen and inspect before
retrying; never assume the old state remains. Other publication failures leave
the previous active manifest intact. On Windows, file sync and process-crash
acceptance do not imply a directory-fsync/power-loss guarantee.

## Disk lifecycle and recovery

Changed ingestion writes and syncs an immutable UUID Parquet file, publishes
the prior validated manifest as `manifest.previous.json`, then atomically
replaces `manifest.json`. A writer lock serializes changes. Fingerprints use
source content and model/operator/inference/runtime lineage, not just names
or modification times. An unchanged folder batch validates one manifest
and skips inference for reused entries.

Removal changes only the manifest. Vacuum requires an exclusive reader lease:
`readers_busy` means sessions, lazy/physical plans or Arrow readers still pin
snapshots. Vacuum preserves snapshots referenced by either the current or
previous manifest, so the last removed recording may still occupy disk as a
recovery generation. A subsequent successful commit advances that generation.
Vacuum deletes only reserved root-level UUID `.parquet` files and
`.manifest-UUID.tmp` files; it rejects symlinks/nonregular reserved paths.
Do not store your own files under those reserved names.

Legacy alpha inputs co-located inside a dataset must be moved out before
removal/vacuum; the API refuses to risk deleting them. New ingestion rejects
co-located inputs. Query staging is private per attempt and normal failures
and interruptions clean it. Abrupt OS/process termination can leave staging
directories; vacuum does not remove staging. Inspect/reclaim those only while
no ingestion is active.

Recovery never promotes orphan snapshots, never rolls back a valid active
manifest and never downgrades an unsupported format version. If neither
manifest is valid, preserve the directory and restore an external backup.
The prior generation is not a substitute for backups.

## Format and upgrade policy

Manifest format **1** remains readable, including actual Parquet bytes produced
by production `0.1.0a2`. Opening does not rewrite it. New commits add a previous
manifest without changing the format. Unknown versions fail explicitly rather
than being guessed or rewritten. Copy the entire dataset while no writer is
active to back it up or move it; source URIs retain their original provenance
and do not automatically relocate original recordings.

Validation checks bounded JSON, snapshot names, lineage, regular files,
Parquet footers, exact Arrow schema and declared row counts. It is not a full
data-page integrity scrub or a cryptographically authenticated backup.
The manifest is bounded to 64 MiB and 100,000 recordings; the lower applicable
bound wins.

The persisted `transcripts` Arrow schema is:

| Column | Type | Nullable |
| --- | --- | --- |
| `source_uri`, `source_version` | `string` | no |
| `start_millis`, `end_millis` | `uint64` | no |
| `text` | `string` | no |
| `confidence_millis` | `uint16` | yes |
| `operator_version` | `string` | no |
| `model`, `model_version` | `string` | yes |
| `parameters_json` | `string` | no |

Column order is the order shown, left to right within grouped rows. Times are
milliseconds on the source timeline, including delayed stream starts/gaps.
The built-in model does not provide calibrated confidence; its confidence is
null. Null model metadata is allowed for other configured providers.

Within 1.x, incompatible changes to this named surface, result/error meanings
or schema require an explicit compatibility transition, not silent mutation.
Additional diagnostic codes/optional metadata may be added. Future disk
formats require a documented migration/backup path and explicit version checks.
Cloud catalogs, remote Flight, document/OCR, embeddings and distributed worker
abstractions remain experimental, outside this local product's stability
commitment.
