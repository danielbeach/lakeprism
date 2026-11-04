# Local recording to searchable transcripts

This example runs a real speech model, not a fixture provider:

1. LakePrism's bundled native FFmpeg decodes a local recording to mono 16 kHz audio.
2. A bounded subprocess runs Whisper tiny.en on CPU, with one inference thread and int8 computation.
3. LakePrism indexes typed, timestamped transcript segments and searches them through SQL.
4. The example writes all transcripts and their source/model/operator lineage to Parquet.
5. A fresh session reloads that Parquet file and repeats the search without inference.

## Install

Use Python 3.12 and a **0.1.0a2 candidate wheel** from the repository's PyPI
build workflow. The published 0.1.0a1 wheels do not contain native decoding.
The candidate is not published automatically by a build-only run.

From a checkout, after downloading the wheel for your platform:

```bash
uv venv --python 3.12 .demo-venv
uv pip install --python .demo-venv/bin/python /path/to/lakeprism-0.1.0a2-*.whl \
  -r examples/local_transcription/requirements.txt

# The only network/model preparation step; about 76 MB of weights.
.demo-venv/bin/python examples/local_transcription/demo.py --prepare

# Actual native decoding, inference, SQL search, persistence, and reload.
.demo-venv/bin/python examples/local_transcription/demo.py --check-sample
```

On Windows, use `.demo-venv\Scripts\python.exe` instead of
`.demo-venv/bin/python`, and supply the exact wheel path rather than a shell glob.
Neither platform needs a system `ffmpeg` executable or a Rust compiler when
using the candidate wheel.

`--prepare` downloads a pinned, checksummed recording and a pinned,
checksummed model. It copies attribution/license notices beside the assets.
Subsequent inference runs use local model/tokenizer files with Hub offline
mode enabled. No paid API, credentials, GPU, or automatic model download is used.

## Results

The sample is approximately eleven seconds of speech from John F. Kennedy's
inaugural address. The acceptance check requires real speech containing
"country", a nonempty indexed search result, and identical search results
after Parquet reload. It does not require a brittle exact model transcript.

Generated files live under `target/local-transcription/`:

- `transcripts.parquet`: all segments, timestamps, confidence when available,
  source SHA-256, model fingerprint, operator version, and inference parameters.
- `results.json`: matching segments and measured cold native-decode/inference
  time. This is one run's timing, not a cross-platform performance guarantee.
- `model/`, `sample.wav`, and license notices: prepared offline inputs.
- `staging/`: application-owned staging root; individual query directories
  are private and cleaned by the existing governed subprocess provider.

To try your own short recording:

```bash
.demo-venv/bin/python examples/local_transcription/demo.py \
  --input /absolute/path/interview.mp4 --search "your phrase"
```

The narrow demo accepts WAV or supported MP4/Matroska video, files up to 8 MiB,
and recordings **shorter than 60 seconds**. Oversized/overlong recordings,
missing models, unsupported codecs, failed inference, and inconsistent
persisted results fail explicitly. The subprocess has a 120-second timeout
and a 128-segment output bound. It never invokes a shell.

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
