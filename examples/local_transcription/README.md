# Local recording to searchable transcripts

This example runs a real speech model, not a fixture provider:

1. LakePrism's bundled native FFmpeg decodes a local recording to mono 16 kHz audio.
2. The installed speech adapter runs Whisper tiny.en on CPU, with one inference thread and int8 computation.
3. `RecordingDataset` publishes timestamped transcript segments and their source/model/operator lineage as immutable Parquet.
4. SQL searches persistent transcripts, and unchanged ingestion reuses the result without inference.
5. A reopened dataset and a separate exported Parquet file return identical search results.

## Install

Use Python 3.12 and the published **0.1.0a2** release from PyPI.
The older 0.1.0a1 wheels do not contain native decoding.

To run this example from a checkout:

```bash
uv venv --python 3.12 .demo-venv
uv pip install --python .demo-venv/bin/python "lakeprism[speech]==0.1.0a2"

# The only network/model preparation step; about 76 MB of weights.
.demo-venv/bin/python examples/local_transcription/demo.py --prepare

# Actual native decoding, inference, SQL search, persistence, and reload.
.demo-venv/bin/python examples/local_transcription/demo.py --check-sample
```

On Windows, use `.demo-venv\Scripts\python.exe` instead of
`.demo-venv/bin/python`.
Neither platform needs a system `ffmpeg` executable or a Rust compiler when
using the published wheel.
Windows speech requires Microsoft's x64 Visual C++ runtime for CTranslate2;
install the [supported redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)
if it is missing. No GPU/CUDA installation is needed for this CPU workflow.

`--prepare` downloads a pinned, checksummed recording and a pinned,
checksummed model. It copies attribution/license notices beside the assets.
Subsequent inference runs use local model/tokenizer files with Hub offline
mode enabled. No paid API, credentials, GPU, or automatic model download is used.

## Use the installed package without this repository

Install the published wheel with its `speech` extra:

```bash
python -m pip install "lakeprism[speech]==0.1.0a2"
```

```python
from lakeprism import RecordingDataset, SpeechModel, prepare_speech_model

prepare_speech_model("./models/tiny.en")  # explicit download, once
dataset = RecordingDataset("./recording-index")
model = SpeechModel("./models/tiny.en")
results = dataset.ingest("./recordings", model=model)  # file or recursive folder
hits = dataset.search("country").to_pyarrow().read_all()

# Later, including in a different process; no model is needed for search.
reopened = RecordingDataset("./recording-index")
print(reopened.search("country").collect())
```

`collect()` renders values as strings; use PyArrow to preserve timestamp types.
The SQL table is named `transcripts`; use `dataset.session().plan(...)` for
arbitrary SQL. Sessions pin a consistent set of immutable snapshots.

Each recording commits independently. Content/model/operator/parameter changes
refresh that recording, while unchanged inputs skip inference. Folder ingestion
attempts all selected files, then raises `IngestionError` with a `results` list
if any failed. A failed refresh retains its last successful transcript. An
interruption reaps the adapter; restarting reuses completed recordings but
restarts the interrupted recording. Deleted inputs are not implicitly removed.
Concurrent writers to the same dataset fail explicitly rather than racing.

## Results

The sample is approximately eleven seconds of speech from John F. Kennedy's
inaugural address. The acceptance check requires real speech containing
"country", a nonempty indexed search result, and identical search results
after Parquet reload. It does not require a brittle exact model transcript.

Generated example files live under `target/local-transcription/`:

- `dataset/`: manifest plus immutable per-recording Parquet snapshots.
- `transcripts.parquet`: a portable export of all segments, timestamps, confidence when available,
  source SHA-256, model fingerprint, operator version, and inference parameters.
- `results.json`: per-file outcomes, matching segments and measured ingestion
  time. This is one run's timing, not a cross-platform performance guarantee.
- `model/`, `sample.wav`, and license notices: prepared offline inputs.
- `dataset/staging/`: application-owned staging root; individual query directories
  are private and cleaned by the existing governed subprocess provider.

To try your own short recording:

```bash
.demo-venv/bin/python examples/local_transcription/demo.py \
  --input /absolute/path/interviews --search "your phrase"
```

The installed workflow accepts WAV and supported MP4/MOV/M4A/Matroska/WebM
recordings with an audio stream and known duration. Each file is bounded to
**1 GiB, two hours, 8,192 transcript segments, and a two-hour subprocess timeout**.
Native decoding seeks through 30-second windows rather than materializing all
PCM. Inference windows do not overlap, so boundary words/context can be lost.
Unsupported inputs, missing models, failed inference and inconsistent persisted
results fail explicitly. No shell is invoked.

The release example also checks repeated licensed speech longer than 60 seconds:

```bash
.demo-venv/bin/python examples/local_transcription/demo.py --check-long-recording
```

This proves cross-window execution and timestamps, not long-recording speech
quality. The candidate adds a separate pinned natural-speech quality gate:

```bash
.demo-venv/bin/python examples/local_transcription/evaluate.py --prepare
.demo-venv/bin/python examples/local_transcription/evaluate.py
.demo-venv/bin/python examples/local_transcription/benchmark_refresh.py
```

Use an installed candidate wheel, not published `0.1.0a2`, for these gates.
Preparation downloads a 9.2 MB, checksummed LibriSpeech subset and writes CC BY
4.0 attribution beside it. Evaluation reconstructs 73 distinct read-speech
excerpts into three long chapters, then checks clean/noisy word error rates,
digital silence, timestamp bounds, reopening and unchanged reuse. Reports
include runtime versions, throughput and process RSS. This limited narration
corpus does not establish conversational or accent robustness.

The refresh benchmark uses a deterministic subprocess fixture, **not a speech
model**. It measures publication/metadata overhead and asserts no repeated
inference on unchanged inputs. See [measured candidate results](../../docs/ACCEPTANCE_MATRIX.md#reproducible-candidate-observations).

A local Apple Silicon acceptance run also processed one hour of repeated
licensed sample audio (115,200,044-byte WAV), producing 659 segments in
400.126 seconds. Unchanged ingestion took 0.266 seconds; maximum child-process
RSS was 554.4 MiB. Speech was recovered in the final minute and after reopening.
This is one development-host observation with one inference thread, not a
controlled benchmark, a natural-recording quality result, or a cross-platform
performance promise.

Old and unpublished orphan Parquet files are intentionally retained to protect
lazy readers. Automatic garbage collection and shared/network filesystem use
are not supported. Use an application-owned local dataset directory.
The unreleased candidate adds explicit reader-safe `remove`, `vacuum` and
`recover` operations; see the [candidate contract](../../docs/LOCAL_RECORDINGS_CONTRACT.md).

Only model inference is configured for one CPU thread. LakePrism's SQL runtime
can use multiple threads; this is not a claim of strict single-core execution.
The first proven model workflow uses ordinary CPython 3.12. Alpine core media
decoding is tested separately; PyArrow/CTranslate2-based model dependencies
on Alpine are not a validated capability.

## Attribution

Sample source: [ggml-org/whisper.cpp at the pinned revision](https://github.com/ggml-org/whisper.cpp/tree/482279b8a18855ba741d7b88c30fcb3874691f59/samples).
Model source: [Systran/faster-whisper-tiny.en at the pinned revision](https://huggingface.co/Systran/faster-whisper-tiny.en/tree/0d3d19a32d3338f10357c0889762bd8d64bbdeba).
Original weights: [OpenAI Whisper](https://github.com/openai/whisper).
Both assets are distributed under MIT terms; see `THIRD-PARTY-LICENSES.txt`.
