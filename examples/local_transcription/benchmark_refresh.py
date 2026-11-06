"""Measure dataset publication/reuse overhead, not speech-model throughput."""

import argparse
import json
from pathlib import Path
import platform
import sys
import time
import uuid

import lakeprism


def benchmark(work, count):
    root = work / f"refresh-{uuid.uuid4()}"
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    for index in range(count):
        (inputs / f"{index:05}.wav").write_bytes(b"fixture recording")
    artifact = root / "model.bin"
    artifact.write_bytes(b"protocol fixture, not speech weights")
    calls = root / "calls"
    adapter = root / "fixture.py"
    adapter.write_text(
        "import json, pathlib, sys\n"
        f"with pathlib.Path({str(calls)!r}).open('a') as log: log.write('call\\n')\n"
        "pathlib.Path(sys.argv[2]).write_text(json.dumps({'segments': ["
        "{'start_millis': 0, 'end_millis': 500, 'text': 'fixture'}]}))\n",
        encoding="utf-8",
    )

    class FixtureModel:
        def configuration(self, staging):
            return lakeprism.WhisperSubprocessConfig(
                sys.executable, [str(adapter), "{input}", "{output}", "{model}"],
                str(artifact), str(staging), operator_version="refresh-benchmark-v1",
                model="protocol-fixture", model_version="1",
            )

    dataset = lakeprism.RecordingDataset(root / "dataset")
    model = FixtureModel()
    started = time.perf_counter()
    cold = dataset.ingest(inputs, model=model)
    cold_seconds = time.perf_counter() - started
    assert len(cold) == count and all(row["status"] == "written" for row in cold)
    assert len(calls.read_text().splitlines()) == count
    started = time.perf_counter()
    warm = dataset.ingest(inputs, model=model)
    batch_seconds = time.perf_counter() - started
    assert len(warm) == count and all(row["status"] == "reused" for row in warm)
    started = time.perf_counter()
    for source in sorted(inputs.iterdir()):
        assert dataset.ingest(source, model=model)[0]["status"] == "reused"
    single_seconds = time.perf_counter() - started
    assert len(calls.read_text().splitlines()) == count, "unchanged inputs repeated inference"
    result = {
        "kind": "synthetic protocol/metadata benchmark; not ASR throughput",
        "platform": platform.platform(),
        "python": platform.python_version(),
        "recordings": count,
        "cold_fixture_seconds": round(cold_seconds, 4),
        "warm_batch_seconds": round(batch_seconds, 4),
        "warm_individual_calls_seconds": round(single_seconds, 4),
        "unchanged_provider_calls": 0,
    }
    work.mkdir(parents=True, exist_ok=True)
    (work / "refresh-results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=Path("target/recording-benchmarks"))
    parser.add_argument("--recordings", type=int, default=256)
    args = parser.parse_args()
    if not 1 <= args.recordings <= 10_000:
        parser.error("--recordings must be in 1..10000")
    benchmark(args.work_dir.resolve(), args.recordings)


if __name__ == "__main__":
    main()
