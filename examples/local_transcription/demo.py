"""Real local recording -> native decode -> CPU Whisper -> SQL -> Parquet."""

import argparse
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import shutil
import sys
import time
import urllib.request

import lakeprism
import pyarrow as pa
import pyarrow.parquet as pq


SAMPLE_URL = (
    "https://raw.githubusercontent.com/ggml-org/whisper.cpp/"
    "482279b8a18855ba741d7b88c30fcb3874691f59/samples/jfk.wav"
)
SAMPLE_SHA256 = "59dfb9a4acb36fe2a2affc14bacbee2920ff435cb13cc314a08c13f66ba7860e"
MODEL_ID = "Systran/faster-whisper-tiny.en"
MODEL_REVISION = "0d3d19a32d3338f10357c0889762bd8d64bbdeba"
MODEL_SHA256 = "1a5afae06a4db91c975c9a9d78be5cc110ee4ea022ad57d55492e4550e936b2a"
MODEL_FILES = ("model.bin", "config.json", "tokenizer.json", "vocabulary.txt")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare(work):
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    from huggingface_hub import hf_hub_download

    sample = work / "sample.wav"
    if not sample.exists():
        with urllib.request.urlopen(SAMPLE_URL, timeout=30) as response:
            data = response.read(1024 * 1024 + 1)
        if hashlib.sha256(data).hexdigest() != SAMPLE_SHA256:
            raise ValueError("sample SHA-256 mismatch; refusing to use downloaded audio")
        sample.write_bytes(data)
    if sha256(sample) != SAMPLE_SHA256:
        raise ValueError("cached sample differs from the pinned licensed recording")
    shutil.copyfile(
        Path(__file__).with_name("THIRD-PARTY-LICENSES.txt"), work / "THIRD-PARTY-LICENSES.txt"
    )
    for filename in (*MODEL_FILES, "README.md"):
        hf_hub_download(
            MODEL_ID,
            filename,
            revision=MODEL_REVISION,
            local_dir=work / "model",
            token=False,
        )
    shutil.copyfile(
        Path(__file__).with_name("THIRD-PARTY-LICENSES.txt"),
        work / "model" / "THIRD-PARTY-LICENSES.txt",
    )
    if sha256(work / "model" / "model.bin") != MODEL_SHA256:
        raise ValueError("model SHA-256 mismatch; refusing to use downloaded weights")
    print(f"Prepared pinned recording and model in {work}. Inference needs no network.")


def run(work, source, search, check_sample=False):
    model_directory = work / "model"
    if any(not (model_directory / name).is_file() for name in MODEL_FILES):
        raise ValueError("model files missing; run demo.py --prepare first")
    if not source.is_file():
        raise ValueError("recording missing; run --prepare or supply --input FILE")
    if source.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("this short-recording demo accepts files up to 8 MiB")
    if not search.strip():
        raise ValueError("search text must not be empty")
    if check_sample and sha256(source) != SAMPLE_SHA256:
        raise ValueError("--check-sample requires the pinned sample recording")
    if sha256(model_directory / "model.bin") != MODEL_SHA256:
        raise ValueError("local weights differ from the pinned model")
    model_files = {name: sha256(model_directory / name) for name in MODEL_FILES}
    model_version = "sha256:" + hashlib.sha256(
        json.dumps(model_files, sort_keys=True).encode()
    ).hexdigest()
    parameters = {
        "model_revision": MODEL_REVISION,
        "decoder": "lakeprism-native-mono-16khz",
        "runtime": "faster-whisper-" + version("faster-whisper"),
        "compute_runtime": "ctranslate2-" + version("ctranslate2"),
        "decoder_runtime": "lakeprism-" + version("lakeprism"),
        "text_runtime": "tokenizers-" + version("tokenizers"),
        "compute_type": "int8",
        "cpu_threads": "1",
        "language": "en",
        "beam_size": "1",
        "vad_filter": "false",
        "condition_on_previous_text": "false",
    }
    staging = work / "staging"
    staging.mkdir(mode=0o700, exist_ok=True)
    config = lakeprism.WhisperSubprocessConfig(
        # Resolving a virtualenv executable's symlink loses its site-packages.
        str(Path(sys.executable).absolute()),
        [str(Path(__file__).with_name("whisper_adapter.py").resolve()),
         "{input}", "{output}", "{model}"],
        str(model_directory / "model.bin"),
        str(staging),
        max_input_bytes=8 * 1024 * 1024,
        max_output_bytes=1024 * 1024,
        timeout_seconds=120,
        operator_version="native-whisper-demo-v1",
        model=MODEL_ID,
        model_version=model_version,
        parameters=parameters,
    )
    session = lakeprism.MediaSession.with_transcription_subprocess(config)
    started = time.perf_counter()
    media_type = "audio" if source.suffix.lower() == ".wav" else "video"
    segments = session.transcribe(
        source.as_uri(), media_type, "sha256:" + sha256(source), max_segments=128
    )
    elapsed = time.perf_counter() - started
    search_literal = search.replace("'", "''")
    hits = session.sql(
        "SELECT start_millis, end_millis, text "
        f"FROM lakeprism_transcript_search('{search_literal}', 128) ORDER BY start_millis"
    )
    records = [{
        "media_id": row.media_id,
        "start_millis": row.start_millis,
        "end_millis": row.end_millis,
        "text": row.text,
        "confidence_millis": row.confidence_millis,
        "source_version": row.source_version,
        "operator_version": row.operator_version,
        "model": row.model,
        "model_version": row.model_version,
        "parameters_json": json.dumps(parameters, sort_keys=True),
    } for row in segments]
    if not records:
        raise ValueError("model returned no transcript records")
    if check_sample and (not hits or "country" not in " ".join(row["text"] for row in records).lower()):
        raise ValueError("real model did not recover the expected sample speech/search result")
    snapshot = work / "transcripts.parquet"
    pq.write_table(pa.Table.from_pylist(records), snapshot)
    reloaded = lakeprism.MediaSession()
    reloaded.register_parquet("transcripts", str(snapshot))
    restored = reloaded.sql(
        "SELECT start_millis, end_millis, text FROM transcripts "
        f"WHERE strpos(lower(text), lower('{search_literal}')) > 0 ORDER BY start_millis"
    )
    if restored != hits:
        raise ValueError("persisted transcript search differs from the in-memory index")
    results = {
        "segments": len(records),
        "cold_decode_and_inference_seconds": round(elapsed, 3),
        "source_version": records[0]["source_version"],
        "model_version": model_version,
        "search": search,
        "matches": hits,
        "parquet": str(snapshot),
    }
    (work / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("target/local-transcription"))
    parser.add_argument("--prepare", action="store_true", help="explicitly download sample/model")
    parser.add_argument("--input", type=Path, help="your local WAV or supported video, under 60 seconds")
    parser.add_argument("--search", default="country")
    parser.add_argument("--check-sample", action="store_true", help="assert real sample speech is recovered")
    args = parser.parse_args()
    work = args.work_dir.resolve()
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    if args.prepare:
        prepare(work)
    else:
        os.environ["HF_HUB_OFFLINE"] = "1"
        source = args.input.resolve() if args.input else work / "sample.wav"
        run(work, source, args.search, args.check_sample)


if __name__ == "__main__":
    main()
