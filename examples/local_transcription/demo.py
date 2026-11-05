"""Exercise the installed recording workflow with real, openly licensed speech."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import urllib.request
import wave

import lakeprism
import pyarrow.parquet as pq


SAMPLE_URL = (
    "https://raw.githubusercontent.com/ggml-org/whisper.cpp/"
    "482279b8a18855ba741d7b88c30fcb3874691f59/samples/jfk.wav"
)
SAMPLE_SHA256 = "59dfb9a4acb36fe2a2affc14bacbee2920ff435cb13cc314a08c13f66ba7860e"


def prepare(work):
    sample = work / "sample.wav"
    if not sample.exists():
        with urllib.request.urlopen(SAMPLE_URL, timeout=30) as response:
            data = response.read(1024 * 1024 + 1)
        if hashlib.sha256(data).hexdigest() != SAMPLE_SHA256:
            raise ValueError("downloaded recording differs from the pinned sample")
        sample.write_bytes(data)
    if hashlib.sha256(sample.read_bytes()).hexdigest() != SAMPLE_SHA256:
        raise ValueError("cached recording differs from the pinned sample")
    shutil.copyfile(
        Path(__file__).with_name("THIRD-PARTY-LICENSES.txt"),
        work / "THIRD-PARTY-LICENSES.txt",
    )
    lakeprism.prepare_speech_model(work / "model")
    print(f"Prepared recording and model in {work}. Inference needs no network.")


def run(work, source, search, check_sample=False):
    if check_sample and hashlib.sha256(source.read_bytes()).hexdigest() != SAMPLE_SHA256:
        raise ValueError("--check-sample requires the pinned recording")
    model = lakeprism.SpeechModel(work / "model")
    dataset = lakeprism.RecordingDataset(work / "dataset")
    started = time.perf_counter()
    ingestion = dataset.ingest(source, model=model)
    elapsed = time.perf_counter() - started
    hits = dataset.search(search).to_pyarrow().read_all().to_pylist()
    if check_sample and not any(row["source_uri"] == source.as_uri() for row in hits):
        raise ValueError("real speech model did not recover the expected search phrase")
    warm = dataset.ingest(source, model=model)
    if any(row["status"] != "reused" for row in warm):
        raise ValueError("unchanged recordings unexpectedly repeated inference")
    reopened = lakeprism.RecordingDataset(dataset.path)
    if reopened.search(search).to_pyarrow().read_all().to_pylist() != hits:
        raise ValueError("reopened dataset differs from the original search")
    snapshot = work / "transcripts.parquet"
    table = dataset.session().plan(
        "SELECT * FROM transcripts ORDER BY source_uri, start_millis"
    ).to_pyarrow().read_all()
    pq.write_table(table, snapshot)
    exported = lakeprism.MediaSession()
    exported.register_parquet("transcripts", str(snapshot))
    if exported.plan(dataset.search(search).sql).to_pyarrow().read_all().to_pylist() != hits:
        raise ValueError("exported Parquet search differs from the dataset")
    results = {
        "ingestion": ingestion,
        "ingest_seconds": round(elapsed, 3),
        "unchanged_reused": True,
        "segments": table.num_rows,
        "matches": hits,
        "parquet": str(snapshot),
    }
    (work / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))
    return results


def check_long_recording(work):
    """Cross two chunk boundaries using repeated licensed speech, not fake ASR."""
    sample = work / "sample.wav"
    if hashlib.sha256(sample.read_bytes()).hexdigest() != SAMPLE_SHA256:
        raise ValueError("long-recording proof requires the pinned sample")
    with wave.open(str(sample), "rb") as source:
        parameters = source.getparams()
        frames = source.readframes(source.getnframes())
    long_source = work / "long-recording.wav"
    repetitions = 65 * parameters.framerate // parameters.nframes + 1
    with wave.open(str(long_source), "wb") as output:
        output.setparams(parameters)
        for _ in range(repetitions):
            output.writeframes(frames)
    results = run(work, long_source, "country")
    if not any(row["start_millis"] >= 60_000 for row in results["matches"]):
        raise ValueError("real model failed to recover speech after the second chunk boundary")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("target/local-transcription"))
    parser.add_argument("--prepare", action="store_true", help="explicitly download recording/model")
    parser.add_argument("--input", type=Path, help="your supported recording or recording folder")
    parser.add_argument("--search", default="country")
    parser.add_argument("--check-sample", action="store_true")
    parser.add_argument("--check-long-recording", action="store_true")
    args = parser.parse_args()
    work = args.work_dir.resolve()
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    if args.prepare:
        prepare(work)
    else:
        os.environ["HF_HUB_OFFLINE"] = "1"
        if args.check_long_recording:
            check_long_recording(work)
        else:
            run(work, args.input.resolve() if args.input else work / "sample.wav",
                args.search, args.check_sample)


if __name__ == "__main__":
    main()
