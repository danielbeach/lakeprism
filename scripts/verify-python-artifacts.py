"""Verify the complete release set and optionally its uploaded byte identity."""

import argparse
from email.parser import BytesParser
import hashlib
import json
from pathlib import Path
import tarfile
import time
import urllib.error
import urllib.request
import zipfile


PLATFORMS = {
    "manylinux_2_28_x86_64",
    "manylinux_2_28_aarch64",
    "musllinux_1_2_x86_64",
    "musllinux_1_2_aarch64",
    "macos-x86_64",
    "macos-aarch64",
    "win_amd64",
}


def verify(directory, version):
    files = sorted(directory.iterdir())
    wheels = sorted(directory.glob("*.whl"))
    sources = sorted(directory.glob("*.tar.gz"))
    if len(files) != 8 or len(wheels) != 7 or len(sources) != 1:
        raise ValueError("release must contain exactly seven wheels and one source archive")
    platforms = set()
    for wheel in wheels:
        _, abi_python, abi, platform = wheel.stem.rsplit("-", 3)
        if (abi_python, abi) != ("cp39", "abi3"):
            raise ValueError(f"unexpected wheel ABI: {wheel.name}")
        if platform.startswith("macosx_") and platform.endswith("_x86_64"):
            platform = "macos-x86_64"
        elif platform.startswith("macosx_") and platform.endswith("_arm64"):
            platform = "macos-aarch64"
        else:
            matches = PLATFORMS.intersection(platform.split("."))
            if len(matches) != 1:
                raise ValueError(f"unexpected platform: {wheel.name}")
            platform = matches.pop()
        if platform in platforms:
            raise ValueError(f"duplicate platform: {platform}")
        platforms.add(platform)
        with zipfile.ZipFile(wheel) as archive:
            metadata = [
                name for name in archive.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
            if len(metadata) != 1:
                raise ValueError(f"missing unique metadata: {wheel.name}")
            message = BytesParser().parsebytes(archive.read(metadata[0]))
            if message["Name"] != "lakeprism" or message["Version"] != version:
                raise ValueError(f"wrong package/version: {wheel.name}")
            if not any(
                name.endswith(".dist-info/licenses/LICENSE")
                for name in archive.namelist()
            ):
                raise ValueError(f"missing license: {wheel.name}")
    if platforms != PLATFORMS:
        raise ValueError(f"missing platforms: {PLATFORMS - platforms}")
    with tarfile.open(sources[0]) as archive:
        metadata = [
            member for member in archive.getmembers()
            if member.name.endswith("/PKG-INFO") and member.name.count("/") == 1
        ]
        if len(metadata) != 1:
            raise ValueError("missing unique source package metadata")
        stream = archive.extractfile(metadata[0])
        if stream is None:
            raise ValueError("source metadata is not a regular file")
        message = BytesParser().parsebytes(stream.read())
        if message["Name"] != "lakeprism" or message["Version"] != version:
            raise ValueError("wrong source package/version")
        if not any(name.endswith("/LICENSE") for name in archive.getnames()):
            raise ValueError("missing source license")
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }


def verify_index(index, version, expected):
    url = f"{index.rstrip('/')}/pypi/lakeprism/{version}/json"
    for attempt in range(12):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                release = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code != 404 or attempt == 11:
                raise
        else:
            actual = {
                item["filename"]: item["digests"]["sha256"]
                for item in release["urls"]
            }
            if actual == expected:
                return
            if not actual.keys() <= expected.keys() or any(
                actual[name] != expected[name] for name in actual
            ):
                raise ValueError("uploaded artifacts differ from validated release bytes")
        if attempt != 11:
            time.sleep(10)
    raise ValueError("package index did not expose the complete validated release")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("version")
    parser.add_argument("--index", choices=["https://test.pypi.org", "https://pypi.org"])
    args = parser.parse_args()
    expected = verify(args.directory, args.version)
    if args.index:
        verify_index(args.index, args.version, expected)
    print(f"Verified eight {args.version} artifacts" + (
        f" against {args.index}" if args.index else ""
    ))


if __name__ == "__main__":
    main()
