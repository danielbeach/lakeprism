#!/usr/bin/env bash
# Build one target-matched, base LakePrism abi3 wheel. See README.md for the CI matrix.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/build-python-wheels.sh [options]

Build one target-matched LakePrism Python wheel with uv-managed Maturin.

Options:
  --platform NAME       manylinux-x86_64, musllinux-x86_64,
                        macos-x86_64, macos-aarch64, windows-x86_64, or native
                        (default: native)
  --target TRIPLE       Rust target triple; it must equal `rustc -vV`'s host.
  --manylinux POLICY    Maturin policy (for example 2_28 or off; default: off).
  --python PATH         Build interpreter (default: python3).
  --features FEATURES   Comma-separated Python crate features
                        (default: flight,delta-rs,unity,whisper-subprocess).
  --native-media        Add the native-media feature. Requires target-matched
                        FFmpeg development libraries; never cross-compile it.
  --out DIRECTORY       Wheel output directory (default: dist/python).
  -h, --help            Show this help.

The extension uses PyO3 abi3-py39, so one successful CPython 3.9-baseline
build produces a cp39-abi3 wheel for supported CPython 3.9+ runtimes. This
script intentionally rejects cross-target requests: PyO3 must be compiled,
linked, and smoke-tested on a matching native host.
USAGE
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 2
}

platform="native"
target=""
manylinux="off"
python_bin="python3"
features="flight,delta-rs,unity,whisper-subprocess"
native_media=0
out="dist/python"

while (($#)); do
  case "$1" in
    --platform)
      (($# >= 2)) || die "--platform requires a value"
      platform="$2"
      shift 2
      ;;
    --target)
      (($# >= 2)) || die "--target requires a value"
      target="$2"
      shift 2
      ;;
    --manylinux)
      (($# >= 2)) || die "--manylinux requires a value"
      manylinux="$2"
      shift 2
      ;;
    --python)
      (($# >= 2)) || die "--python requires a value"
      python_bin="$2"
      shift 2
      ;;
    --features)
      (($# >= 2)) || die "--features requires a value"
      features="$2"
      shift 2
      ;;
    --native-media)
      native_media=1
      shift
      ;;
    --out)
      (($# >= 2)) || die "--out requires a value"
      out="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

command -v uv >/dev/null 2>&1 || die "uv is required; install it before building"
command -v rustc >/dev/null 2>&1 || die "rustc is required"
command -v "$python_bin" >/dev/null 2>&1 || die "Python interpreter not found: $python_bin"

host_target="$(rustc -vV | sed -n 's/^host: //p')"
[[ -n "$host_target" ]] || die "could not determine the Rust host target"

case "$platform" in
  native)
    expected_target="$host_target"
    ;;
  manylinux-x86_64)
    expected_target="x86_64-unknown-linux-gnu"
    [[ "$manylinux" != "off" ]] || die "manylinux-x86_64 requires --manylinux POLICY"
    ;;
  musllinux-x86_64)
    expected_target="x86_64-unknown-linux-musl"
    [[ "$manylinux" == "off" ]] || die "musllinux wheels must use --manylinux off"
    ;;
  macos-x86_64)
    expected_target="x86_64-apple-darwin"
    ;;
  macos-aarch64)
    expected_target="aarch64-apple-darwin"
    ;;
  windows-x86_64)
    expected_target="x86_64-pc-windows-msvc"
    ;;
  *)
    die "unsupported platform: $platform"
    ;;
esac

if [[ -z "$target" ]]; then
  target="$expected_target"
fi
[[ "$target" == "$expected_target" ]] ||
  die "--target $target does not match --platform $platform ($expected_target)"
[[ "$target" == "$host_target" ]] ||
  die "cross-target build rejected: requested $target, but Rust host is $host_target"

if ((native_media)); then
  features="${features:+$features,}native-media"
fi

mkdir -p "$out"

maturin_args=(
  build
  --release
  --locked
  --manifest-path crates/lakeprism-python/Cargo.toml
  --target "$target"
  --interpreter "$python_bin"
  --features "$features"
  --out "$out"
)
if [[ "$manylinux" != "off" ]]; then
  maturin_args+=(--manylinux "$manylinux")
fi

printf 'Building target-matched %s wheel for %s (features: %s)\n' \
  "$platform" "$target" "$features"
uv tool run --from 'maturin==1.10.2' maturin "${maturin_args[@]}"
