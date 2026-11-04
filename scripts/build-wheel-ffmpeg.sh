#!/usr/bin/env bash
# Build the same decode-only, dynamically linked FFmpeg on each native host.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
version="9.0.2"
sha256="8c3850283eb25fa026482078a04051e0be17347b09ef81a0849bec15a96e002e"
python="${PYTHON:-python3}"
work="$root/target/ffmpeg-download"
prefix="$root/target/wheel-ffmpeg"
notices="$root/crates/lakeprism-python/python/lakeprism/_ffmpeg"
archive="$work/ffmpeg-$version.tar.xz"
mkdir -p "$work" "$prefix" "$notices"
if [[ ! -f "$archive" ]]; then
  curl --fail --location --retry 3 "https://ffmpeg.org/releases/ffmpeg-$version.tar.xz" \
    --output "$archive"
fi
"$python" - "$archive" "$sha256" <<'PY'
import hashlib
from pathlib import Path
import sys

if hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest() != sys.argv[2]:
    raise SystemExit("FFmpeg source SHA-256 mismatch; refusing to build")
PY

source="$work/ffmpeg-$version"
if [[ ! -d "$source" ]]; then
  tar -xJf "$archive" -C "$work"
fi
arguments=(
  "--prefix=$prefix"
  --enable-shared --disable-static --enable-pic
  --disable-gpl --disable-nonfree --disable-version3
  --disable-autodetect --disable-network --disable-programs --disable-doc --disable-debug
  --disable-avdevice --disable-avfilter
  --disable-everything
  --enable-avcodec --enable-avformat --enable-avutil --enable-swresample --enable-swscale
  --enable-protocol=file
  --enable-demuxer=mov,matroska,wav
  --enable-parser=h264,hevc,vp9,mpeg4video,mjpeg,aac,mpegaudio,opus,vorbis,flac
  --enable-decoder=h264,hevc,vp9,mpeg4,mjpeg,aac,mp3,mp3float,opus,vorbis,flac,pcm_s16le,pcm_f32le
)
case "$(uname -s)" in
  Darwin)
    host="$(rustc -vV | sed -n 's/^host: //p')"
    case "$host" in
      aarch64-apple-darwin)
        export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-11.0}"
        arguments+=(--arch=aarch64 "--cc=clang -arch arm64")
        ;;
      x86_64-apple-darwin)
        export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-10.15}"
        arguments+=(--arch=x86_64 "--cc=clang -arch x86_64")
        ;;
      *)
        printf 'error: unsupported native macOS Rust target %s\n' "$host" >&2
        exit 2
        ;;
    esac
    arguments+=("--extra-cflags=-mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
      "--extra-ldflags=-mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET")
    ;;
  MINGW*|MSYS*|CYGWIN*)
    arguments+=(--toolchain=msvc --ld=lld-link --ar=llvm-lib --nm=llvm-nm)
    ;;
esac
jobs="$("$python" -c 'import os; print(min(os.cpu_count() or 1, 8))')"
(
  cd "$source"
  ./configure "${arguments[@]}"
  make -j "$jobs"
  make install
)
grep -q 'CONFIG_GPL 0' "$source/config.h"
grep -q 'CONFIG_NONFREE 0' "$source/config.h"
grep -q 'CONFIG_VERSION3 0' "$source/config.h"
cp "$source/COPYING.LGPLv2.1" "$notices/COPYING.LGPLv2.1"
cp "$source/LICENSE.md" "$notices/FFMPEG-LICENSE.md"
cp "$archive" "$notices/"
cp "$source/COPYING.LGPLv2.1" "$root/crates/lakeprism-python/FFMPEG-LICENSE.txt"
"$python" - "$notices" "$version" "$sha256" "${arguments[@]}" <<'PY'
import json
from pathlib import Path
import platform
import sys

directory = Path(sys.argv[1])
(directory / "BUILD.json").write_text(json.dumps({
    "version": sys.argv[2],
    "source": f"https://ffmpeg.org/releases/ffmpeg-{sys.argv[2]}.tar.xz",
    "sha256": sys.argv[3],
    "source_changes": "none",
    "configure": sys.argv[4:],
    "build": ["make -j 2", "make install"],
    "host": platform.platform(),
}, indent=2) + "\n", encoding="utf-8")
(directory / "NOTICE.txt").write_text(
    "LakePrism uses shared libraries from FFmpeg under LGPL-2.1-or-later.\n"
    "FFmpeg belongs to its respective copyright holders, not LakePrism.\n"
    "The complete unmodified corresponding source archive, license texts,\n"
    "SHA-256 and configure/build instructions are included in this directory.\n"
    "No GPL, nonfree, version3 or autodetected external components are enabled.\n"
    "Wheel repair changes shared-library names/load paths, not FFmpeg source.\n"
    "You may rebuild/replace these shared libraries and debug modifications;\n"
    "LakePrism imposes no prohibition on reverse engineering for that purpose.\n"
    "Adjust the build prefix for your host; rebuild the wheel using the repository\n"
    "build scripts and its platform repair tool when replacing the libraries.\n"
    "Codec patent obligations may apply separately in some jurisdictions.\n",
    encoding="utf-8",
)
PY
printf 'FFmpeg %s installed in %s; complete corresponding source staged in %s\n' \
  "$version" "$prefix" "$notices"
