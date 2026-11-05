"""Explicit preparation and offline configuration for the small English model."""

import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import sys


MODEL_ID = "Systran/faster-whisper-tiny.en"
MODEL_REVISION = "0d3d19a32d3338f10357c0889762bd8d64bbdeba"
MODEL_SHA256 = "1a5afae06a4db91c975c9a9d78be5cc110ee4ea022ad57d55492e4550e936b2a"
MODEL_FILES = ("model.bin", "config.json", "tokenizer.json", "vocabulary.txt")


def _sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare_speech_model(path):
    """Explicitly download pinned MIT-licensed tiny.en weights (~76 MB).

    Requires the ``speech`` extra. No other workflow operation downloads models.
    Model attribution and its upstream model card are saved beside the weights.
    """
    from huggingface_hub import hf_hub_download

    path = Path(path).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    for filename in (*MODEL_FILES, "README.md"):
        hf_hub_download(
            MODEL_ID, filename, revision=MODEL_REVISION, local_dir=path, token=False,
        )
    if _sha256(path / "model.bin") != MODEL_SHA256:
        raise ValueError("downloaded model differs from the pinned weights")
    (path / "LAKEPRISM-ATTRIBUTION.txt").write_text(
        "Model: Systran/faster-whisper-tiny.en\n"
        f"Revision: {MODEL_REVISION}\n"
        "Converted from OpenAI Whisper (https://github.com/openai/whisper).\n"
        "MIT License; copyright (c) 2022 OpenAI.\n\n"
        "Permission is hereby granted, free of charge, to any person obtaining a copy "
        "of this software and associated documentation files (the \"Software\"), to deal "
        "in the Software without restriction, including without limitation the rights "
        "to use, copy, modify, merge, publish, distribute, sublicense, and/or sell "
        "copies of the Software, and to permit persons to whom the Software is "
        "furnished to do so, subject to the following conditions:\n\n"
        "The above copyright notice and this permission notice shall be included in "
        "all copies or substantial portions of the Software.\n\n"
        "THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR "
        "IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, "
        "FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE "
        "AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER "
        "LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, "
        "OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN "
        "THE SOFTWARE.\n",
        encoding="utf-8",
    )
    return path


class SpeechModel:
    """Pinned English CPU/int8 speech model, explicitly prepared on local disk.

    Audio is decoded by LakePrism in 30-second windows. Inference uses one CPU
    thread. Inputs are bounded to 1 GiB and two hours, with at most 8192 segments.
    SQL execution is not restricted to one thread.
    """

    def __init__(self, path):
        self.path = Path(path).expanduser().resolve(strict=True)

    def configuration(self, staging):
        from ._lakeprism import WhisperSubprocessConfig

        hashes = {name: _sha256(self.path / name) for name in MODEL_FILES}
        if hashes["model.bin"] != MODEL_SHA256:
            raise ValueError("local model differs from the pinned tiny.en weights")
        fingerprint = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
        return WhisperSubprocessConfig(
            # Preserve the virtualenv path rather than resolving its symlink.
            str(Path(sys.executable).absolute()),
            ["-m", "lakeprism._speech_adapter", "{input}", "{output}", "{model}"],
            str(self.path / "model.bin"),
            str(Path(staging).resolve()),
            max_input_bytes=1024 * 1024 * 1024,
            max_output_bytes=8 * 1024 * 1024,
            timeout_seconds=7200,
            operator_version="lakeprism-speech-v2",
            model=MODEL_ID,
            model_version="sha256:" + fingerprint,
            parameters={
                "model_revision": MODEL_REVISION,
                "runtime": version("faster-whisper"),
                "compute_runtime": version("ctranslate2"),
                "decoder_runtime": version("lakeprism"),
                "text_runtime": version("tokenizers"),
                "language": "en",
                "compute_type": "int8",
                "cpu_threads": "1",
                "chunk_millis": "30000",
                "beam_size": "1",
                "vad_filter": "false",
                "condition_on_previous_text": "false",
            },
        )
