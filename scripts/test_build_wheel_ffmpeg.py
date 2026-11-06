import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest


@pytest.fixture
def download_script(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copyfile(Path(__file__).with_name("build-wheel-ffmpeg.sh"), scripts / "build-wheel-ffmpeg.sh")
    commands = tmp_path / "commands"
    commands.mkdir()
    sleep = commands / "sleep"
    sleep.write_text("#!/usr/bin/env bash\nexit 0\n")
    sleep.chmod(0o700)
    env = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"], PYTHON=sys.executable)
    return scripts / "build-wheel-ffmpeg.sh", commands, env


@pytest.mark.parametrize("curl_exit", [35, 0])
def test_failed_or_corrupt_download_never_becomes_cached_source(tmp_path, download_script, curl_exit):
    script, commands, env = download_script
    curl = commands / "curl"
    curl.write_text(
        f"#!{sys.executable}\n"
        "import pathlib, sys\n"
        "pathlib.Path(sys.argv[sys.argv.index('--output') + 1]).write_bytes(b'partial source')\n"
        f"sys.exit({curl_exit})\n"
    )
    curl.chmod(0o700)
    result = subprocess.run(
        ["bash", str(script)],
        env=env, capture_output=True, text=True, check=False,
    )
    assert result.returncode == (curl_exit or 1)
    if not curl_exit:
        assert "SHA-256 mismatch" in result.stderr
    assert list((tmp_path / "target" / "ffmpeg-download").iterdir()) == []


def test_reset_is_retried_before_verified_source_is_cached(tmp_path, download_script):
    script, commands, env = download_script
    source = b"verified test source"
    script.write_text(re.sub(
        r'^sha256="[^"]+"$',
        f'sha256="{hashlib.sha256(source).hexdigest()}"',
        script.read_text(), flags=re.MULTILINE,
    ))
    counter = tmp_path / "attempts"
    curl = commands / "curl"
    curl.write_text(
        f"#!{sys.executable}\n"
        "import pathlib, sys\n"
        f"counter = pathlib.Path({str(counter)!r})\n"
        "attempt = int(counter.read_text()) + 1 if counter.exists() else 1\n"
        "counter.write_text(str(attempt))\n"
        "target = pathlib.Path(sys.argv[sys.argv.index('--output') + 1])\n"
        f"target.write_bytes(b'partial' if attempt == 1 else {source!r})\n"
        "sys.exit(35 if attempt == 1 else 0)\n"
    )
    curl.chmod(0o700)
    tar = commands / "tar"
    tar.write_text("#!/usr/bin/env bash\nexit 73\n")
    tar.chmod(0o700)
    result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True, check=False)
    assert result.returncode == 73, result.stderr  # Stop after verified publication, before compilation.
    assert counter.read_text() == "2"
    cached = tmp_path / "target" / "ffmpeg-download"
    assert [path.read_bytes() for path in cached.iterdir()] == [source]
