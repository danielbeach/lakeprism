"""Evaluate real read English speech, controlled noise, silence and long windows.

The corpus is 73 distinct LibriSpeech excerpts, reconstructed in chapter order.
It is not a conversational-speech, accent-coverage or general accuracy guarantee.
"""

import argparse
import hashlib
from importlib.metadata import version
import io
import json
import os
from pathlib import Path
import platform
import re
import time
import urllib.request
import uuid
import wave

import lakeprism


CORPUS_REVISION = "5be91486e11a2d616f4ec5db8d3fd248585ac07a"
CORPUS_URL = (
    "https://huggingface.co/datasets/hf-internal-testing/librispeech_asr_dummy/"
    f"resolve/{CORPUS_REVISION}/clean/validation-00000-of-00001.parquet"
)
CORPUS_SHA256 = "4e69a06fa5edc90921e5e7e39a7084881f8b3ed9c805c574f4f39c6fde27c603"
CORPUS_MAX_BYTES = 10 * 1024 * 1024
CLEAN_WER_LIMIT = 0.25
NOISY_WER_LIMIT = 0.40


def prepare(work, model):
    work.mkdir(parents=True, exist_ok=True)
    corpus = work / "librispeech.parquet"
    if not corpus.exists():
        with urllib.request.urlopen(CORPUS_URL, timeout=120) as response:
            data = response.read(CORPUS_MAX_BYTES + 1)
        if hashlib.sha256(data).hexdigest() != CORPUS_SHA256:
            raise ValueError("corpus differs from the pinned CC BY 4.0 LibriSpeech subset")
        corpus.write_bytes(data)
    verify_corpus(corpus)
    (work / "CORPUS-ATTRIBUTION.txt").write_text(
        "LibriSpeech ASR corpus, CC BY 4.0: https://www.openslr.org/12\n"
        "Vassil Panayotov, Guoguo Chen, Daniel Povey, Sanjeev Khudanpur.\n"
        "LibriSpeech: an ASR corpus based on public domain audio books, ICASSP 2015.\n"
        "Subset mirror: hf-internal-testing/librispeech_asr_dummy\n"
        f"Revision: {CORPUS_REVISION}\nSHA-256: {CORPUS_SHA256}\n"
        "License: https://creativecommons.org/licenses/by/4.0/\n"
        "Evaluation modifications: FLAC-to-WAV conversion; ordered excerpt joins;\n"
        "15 dB seeded white noise; a separate digital-silence control.\n",
        encoding="utf-8",
    )
    lakeprism.prepare_speech_model(model)


def verify_corpus(path):
    if path.stat().st_size > CORPUS_MAX_BYTES or hashlib.sha256(path.read_bytes()).hexdigest() != CORPUS_SHA256:
        raise ValueError("cached corpus differs from the pinned evaluation bytes")


def words(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def word_error_rate(reference, hypothesis):
    expected, actual = words(reference), words(hypothesis)
    if not expected:
        raise ValueError("quality evaluation requires nonempty reference speech")
    previous = list(range(len(actual) + 1))
    for index, word in enumerate(expected, 1):
        current = [index]
        for column, candidate in enumerate(actual, 1):
            current.append(min(
                current[-1] + 1, previous[column] + 1,
                previous[column - 1] + (word != candidate),
            ))
        previous = current
    return previous[-1] / len(expected)


def write_wave(path, samples):
    with wave.open(str(path), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(16_000)
        output.writeframes(samples.astype("<i2", copy=False).tobytes())


def inputs(work, corpus):
    import av
    import numpy as np
    import pyarrow.parquet as pq

    root = work / f"evaluation-{uuid.uuid4()}"
    recordings = root / "inputs"
    recordings.mkdir(parents=True)
    chapters = {}
    rows = pq.read_table(corpus).to_pylist()
    if len(rows) != 73:
        raise ValueError("unexpected evaluation corpus shape")
    for row in sorted(rows, key=lambda row: row["id"]):
        blocks = []
        with av.open(io.BytesIO(row["audio"]["bytes"])) as recording:
            resampler = av.AudioResampler(format="s16", layout="mono", rate=16_000)
            for frame in recording.decode(audio=0):
                blocks.extend(converted.to_ndarray().reshape(-1) for converted in resampler.resample(frame))
            blocks.extend(converted.to_ndarray().reshape(-1) for converted in resampler.resample(None))
        key = (row["speaker_id"], row["chapter_id"])
        audio, text = chapters.setdefault(key, ([], []))
        audio.extend([np.concatenate(blocks), np.zeros(4000, dtype=np.int16)])
        text.append(row["text"])
    references, durations = {}, {}
    random = np.random.default_rng(20261106)
    for index, (audio, text) in enumerate(chapters.values()):
        clean = np.concatenate(audio)
        reference = " ".join(text)
        rms = np.sqrt(np.mean(clean.astype(np.float64) ** 2))
        noisy = np.clip(
            clean + random.normal(0, rms / (10 ** (15 / 20)), clean.size),
            -32768, 32767,
        ).astype(np.int16)
        for kind, samples in (("clean", clean), ("noisy", noisy)):
            path = recordings / f"{kind}-{index}.wav"
            write_wave(path, samples)
            references[path.resolve().as_uri()] = (kind, reference)
            durations[path.resolve().as_uri()] = samples.size / 16_000
    silence = recordings / "silence.wav"
    write_wave(silence, np.zeros(45 * 16_000, dtype=np.int16))
    durations[silence.resolve().as_uri()] = 45
    return root, recordings, references, durations, len(rows), len(chapters)


def evaluate(work, model):
    verify_corpus(work / "librispeech.parquet")
    root, recordings, references, durations, excerpts, chapters = inputs(work, work / "librispeech.parquet")
    os.environ["HF_HUB_OFFLINE"] = "1"
    dataset = lakeprism.RecordingDataset(root / "dataset")
    speech = lakeprism.SpeechModel(model)
    started = time.perf_counter()
    outcomes = dataset.ingest(recordings, model=speech)
    cold_seconds = time.perf_counter() - started
    rows = dataset.session().plan(
        "SELECT source_uri, start_millis, end_millis, text "
        "FROM transcripts ORDER BY source_uri, start_millis"
    ).to_pyarrow().read_all().to_pylist()
    hypotheses = {}
    for row in rows:
        if not 0 <= row["start_millis"] < row["end_millis"] <= round(durations[row["source_uri"]] * 1000):
            raise ValueError("inference returned timestamps outside the recording timeline")
        hypotheses.setdefault(row["source_uri"], []).append(row["text"])
    scores = {}
    for kind in ("clean", "noisy"):
        expected, actual = [], []
        for uri, (category, text) in references.items():
            if category == kind:
                expected.append(text)
                actual.append(" ".join(hypotheses.get(uri, [])))
        scores[kind] = word_error_rate(" ".join(expected), " ".join(actual))
    silence_uri = (recordings / "silence.wav").resolve().as_uri()
    silence_segments = len(hypotheses.get(silence_uri, []))
    started = time.perf_counter()
    warm = dataset.ingest(recordings, model=speech)
    warm_seconds = time.perf_counter() - started
    if not all(row["status"] == "reused" for row in warm):
        raise ValueError("unchanged evaluation recordings repeated inference")
    reopened = lakeprism.RecordingDataset(dataset.path)
    if reopened.recordings() != dataset.recordings():
        raise ValueError("reopening changed committed source metadata")
    memory = {"measurement": "unavailable on this platform"}
    if platform.system() in ("Linux", "Darwin"):
        import resource
        scale = 1 if platform.system() == "Darwin" else 1024
        memory = {
            "max_parent_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale,
            "max_child_rss_bytes": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * scale,
            "measurement": "independent process maxima, not simultaneous aggregate RSS",
        }
    result = {
        "corpus_revision": CORPUS_REVISION,
        "corpus_sha256": CORPUS_SHA256,
        "platform": platform.platform(), "python": platform.python_version(),
        "runtimes": {
            name: version(name)
            for name in ("lakeprism", "faster-whisper", "ctranslate2", "numpy", "av")
        },
        "model": lakeprism.speech.MODEL_ID,
        "model_revision": lakeprism.speech.MODEL_REVISION,
        "excerpts": excerpts, "chapters": chapters,
        "scope": "reconstructed read-English chapters; not conversational or accent coverage",
        "normalization": "lowercase ASCII alphanumeric word tokens; no number expansion",
        "noise_snr_db": 15,
        "clean_wer": round(scores["clean"], 5), "clean_wer_limit": CLEAN_WER_LIMIT,
        "noisy_wer": round(scores["noisy"], 5), "noisy_wer_limit": NOISY_WER_LIMIT,
        "silence_segments": silence_segments,
        "max_recording_seconds": round(max(durations.values()), 3),
        "evaluated_audio_seconds": round(sum(durations.values()), 3),
        "cold_seconds": round(cold_seconds, 3), "warm_seconds": round(warm_seconds, 3),
        "cold_realtime_factor": round(cold_seconds / sum(durations.values()), 4),
        "recordings": len(outcomes), "segments": len(rows),
        **memory,
    }
    (work / "quality-results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if scores["clean"] > CLEAN_WER_LIMIT or scores["noisy"] > NOISY_WER_LIMIT or silence_segments:
        raise ValueError("speech quality failed the declared acceptance thresholds; see quality-results.json")
    if max(durations.values()) < 120:
        raise ValueError("quality corpus did not exercise a long natural-speech timeline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("target/speech-evaluation"))
    parser.add_argument("--model-dir", type=Path, default=Path("target/local-transcription/model"))
    parser.add_argument("--prepare", action="store_true")
    args = parser.parse_args()
    work, model = args.work_dir.resolve(), args.model_dir.resolve()
    if args.prepare:
        prepare(work, model)
    else:
        evaluate(work, model)


if __name__ == "__main__":
    main()
