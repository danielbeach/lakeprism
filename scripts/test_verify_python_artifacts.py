import hashlib
import importlib.util
import io
import json
from pathlib import Path
import zipfile

import pytest


spec = importlib.util.spec_from_file_location(
    "verify_python_artifacts", Path(__file__).with_name("verify-python-artifacts.py")
)
artifacts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifacts)


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("gpl", "LGPL-only"),
        ("source", "checksum"),
        ("library", "swscale"),
    ],
)
def test_rejects_unredistributable_or_incomplete_ffmpeg_wheels(monkeypatch, failure, message):
    source = b"fixture corresponding source"
    digest = hashlib.sha256(source).hexdigest()
    monkeypatch.setattr(artifacts, "FFMPEG_SHA256", digest)
    build = {
        "version": artifacts.FFMPEG_VERSION,
        "sha256": digest,
        "source_changes": "none",
        "configure": [
            "--enable-shared", "--disable-static", "--disable-gpl", "--disable-nonfree",
            "--disable-version3", "--disable-autodetect", "--disable-network",
        ],
    }
    if failure == "gpl":
        build["configure"].append("--enable-gpl")
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        prefix = "lakeprism/_ffmpeg/"
        for name in ("COPYING.LGPLv2.1", "FFMPEG-LICENSE.md", "NOTICE.txt"):
            archive.writestr(prefix + name, "fixture notice")
        archive.writestr(prefix + "BUILD.json", json.dumps(build))
        archive.writestr(
            prefix + f"ffmpeg-{artifacts.FFMPEG_VERSION}.tar.xz",
            b"wrong source bytes" if failure == "source" else source,
        )
        for name in ("avcodec", "avformat", "avutil", "swresample", "swscale"):
            if failure != "library" or name != "swscale":
                archive.writestr(f"lakeprism.libs/{name}.dll", b"fixture shared library")
    with zipfile.ZipFile(stream) as archive:
        with pytest.raises(ValueError, match=message):
            artifacts.verify_ffmpeg_bundle(archive)
