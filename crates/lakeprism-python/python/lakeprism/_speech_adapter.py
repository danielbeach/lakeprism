"""Installed direct-argv adapter for LakePrism's bounded native provider."""

import json
import os
from pathlib import Path
import sys


CHUNK_MILLIS = 30_000
MAX_DURATION_MILLIS = 2 * 60 * 60 * 1_000
MAX_SEGMENTS = 8_192


def transcribe(input_path, model_artifact):
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    import numpy as np
    from faster_whisper import WhisperModel

    from ._lakeprism import MediaSession

    session = MediaSession()
    uri = Path(input_path).resolve().as_uri()
    duration, _, audio_streams = session.media_metadata(uri)
    if not audio_streams or duration is None or not 0 < duration <= MAX_DURATION_MILLIS:
        raise ValueError("input requires audio and a known duration in (0, 2 hours]")
    audio_start, audio_end = session.audio_metadata(uri)
    audio_end = duration if audio_end is None else min(audio_end, duration)
    if not 0 <= audio_start < audio_end:
        raise ValueError("input requires a nonempty audio timeline")
    model = WhisperModel(
        str(Path(model_artifact).resolve().parent),
        device="cpu",
        compute_type="int8",
        cpu_threads=1,
        num_workers=1,
        local_files_only=True,
    )
    result = []
    decoded_audio = False
    for start in range(audio_start, audio_end, CHUNK_MILLIS):
        end = min(start + CHUNK_MILLIS, audio_end)
        rows = session.audio_segments(
            uri, start, end, CHUNK_MILLIS, 1, include_payload=True,
        ).to_pyarrow().read_all().to_pylist()
        if not rows:
            # Unknown stream durations and gaps may reach normal audio EOF
            # before the container ends. Native decode errors still propagate.
            continue
        if len(rows) != 1:
            raise ValueError("native decoder returned more than one bounded audio chunk")
        decoded_audio = True
        row = rows[0]
        if row["sample_rate_hz"] != 16_000 or row["channel_count"] != 1:
            raise ValueError("native decoder did not produce normalized mono audio")
        audio = np.frombuffer(row["f32le"], dtype="<f4")
        segments, _ = model.transcribe(
            audio, language="en", beam_size=1, vad_filter=False,
            condition_on_previous_text=False,
        )
        for segment in segments:
            text = segment.text.strip()
            first = row["start_millis"] + round(segment.start * 1_000)
            last = min(row["start_millis"] + round(segment.end * 1_000), row["end_millis"])
            if not text or last <= first:
                raise ValueError("model returned empty text or invalid timestamps")
            if len(result) >= MAX_SEGMENTS:
                raise ValueError("recording exceeded the 8192-segment bound")
            result.append({
                "start_millis": first,
                "end_millis": last,
                "text": text,
                "confidence_millis": None,
            })
    if not decoded_audio:
        raise ValueError("input contains no decodable audio")
    return {"segments": result}


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: python -m lakeprism._speech_adapter INPUT OUTPUT MODEL")
    result = transcribe(sys.argv[1], sys.argv[3])
    Path(sys.argv[2]).write_text(json.dumps(result), encoding="utf-8")


if __name__ == "__main__":
    main()
