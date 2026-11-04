"""Consume the existing bounded Whisper protocol using native audio and CPU ASR."""

import json
import os
from pathlib import Path
import sys

os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"

import lakeprism
import numpy as np
from faster_whisper import WhisperModel


def transcribe(input_path, model_artifact):
    rows = lakeprism.MediaSession().audio_segments(
        input_path.resolve().as_uri(), 0, 60_000, 1_000, 60, include_payload=True
    ).to_pyarrow().read_all().to_pylist()
    if not rows or rows[-1]["end_millis"] >= 60_000:
        raise ValueError("this demo requires nonempty recordings shorter than 60 seconds")
    if any(row["sample_rate_hz"] != 16_000 or row["channel_count"] != 1 for row in rows):
        raise ValueError("native decoder did not return mono 16 kHz audio")
    audio = np.concatenate([np.frombuffer(row["f32le"], dtype="<f4") for row in rows])
    model = WhisperModel(
        str(model_artifact.resolve().parent),
        device="cpu",
        compute_type="int8",
        cpu_threads=1,
        num_workers=1,
        local_files_only=True,
    )
    segments, _ = model.transcribe(
        audio, language="en", beam_size=1, vad_filter=False, condition_on_previous_text=False
    )
    result = []
    for segment in segments:
        if len(result) >= 128:
            raise ValueError("transcript exceeded the demo's 128-segment limit")
        result.append({
            "start_millis": round(segment.start * 1_000),
            "end_millis": round(segment.end * 1_000),
            "text": segment.text.strip(),
            "confidence_millis": None,
        })
    if not result:
        raise ValueError("speech model returned no transcript")
    return {"segments": result}


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: whisper_adapter.py INPUT OUTPUT MODEL_ARTIFACT")
    result = transcribe(Path(sys.argv[1]), Path(sys.argv[3]))
    Path(sys.argv[2]).write_text(json.dumps(result), encoding="utf-8")


if __name__ == "__main__":
    main()
