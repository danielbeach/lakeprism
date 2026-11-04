#!/usr/bin/env bash
set -euo pipefail

source="${1:?Supply a wheel directory or an exact package requirement}"
index="${2:-}"
versions="${LAKEPRISM_PYTHONS:-3.9 3.12 3.14}"
export LAKEPRISM_NATIVE_MEDIA=1
unset FFMPEG_DIR PKG_CONFIG_PATH LD_LIBRARY_PATH DYLD_LIBRARY_PATH DYLD_FALLBACK_LIBRARY_PATH
build_prefix="$PWD/target/wheel-ffmpeg"
hidden_prefix="$PWD/target/wheel-ffmpeg-build-only"
if [[ -d "$build_prefix" ]]; then
  [[ ! -e "$hidden_prefix" ]] || {
    printf 'error: FFmpeg isolation directory already exists: %s\n' "$hidden_prefix" >&2
    exit 1
  }
  mv "$build_prefix" "$hidden_prefix"
  trap 'mv "$hidden_prefix" "$build_prefix"' EXIT
fi

if [[ -z "$index" ]]; then
  shopt -s nullglob
  wheels=("$source"/*.whl)
  [[ "${#wheels[@]}" -eq 1 ]] || {
    printf 'error: expected one wheel in %s\n' "$source" >&2
    exit 1
  }
fi

# Copy tests outside the source package so an editable extension cannot mask
# a broken installed wheel, even when testing from a development checkout.
mkdir -p target/wheel-smoke-tests
cp crates/lakeprism-python/python/lakeprism/test_bindings.py \
  target/wheel-smoke-tests/test_bindings.py

for version in $versions; do
  interpreter="$version"
  digits="${version//./}"
  python_options=(--managed-python)
  if [[ -x "/opt/python/cp$digits-cp$digits/bin/python" ]]; then
    interpreter="/opt/python/cp$digits-cp$digits/bin/python"
    python_options=()
  fi
  environment="target/wheel-smoke-$version"
  uv venv --clear --python "$interpreter" "${python_options[@]}" "$environment"
  python="$environment/bin/python"
  if [[ -f "$environment/Scripts/python.exe" ]]; then
    python="$environment/Scripts/python.exe"
  fi
  if [[ -n "$index" ]]; then
    uv pip install --python "$python" --no-deps --only-binary=:all: \
      --index-url "$index" "$source"
  else
    uv pip install --python "$python" --no-deps "${wheels[0]}"
  fi
  uv pip install --python "$python" pytest
  if [[ "${LAKEPRISM_MUSL:-0}" != "1" ]]; then
    uv pip install --python "$python" pyarrow
  fi
  "$python" -I - <<'PY'
import importlib.metadata
from pathlib import Path
import sys
import lakeprism

assert Path(lakeprism.__file__).is_relative_to(Path(sys.prefix)), lakeprism.__file__
for name in ("FlightClient", "UnityCatalog", "write_delta_ipc", "WhisperSubprocessConfig"):
    assert hasattr(lakeprism, name), name
print("Installed", importlib.metadata.version("lakeprism"), "from", lakeprism.__file__)
PY
  "$python" -I -m pytest --import-mode=importlib -q \
    target/wheel-smoke-tests/test_bindings.py
done
