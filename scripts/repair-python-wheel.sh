#!/usr/bin/env bash
set -euo pipefail

source="${1:?Supply the unrepaired wheel directory}"
out="${2:?Supply a separate repaired wheel directory}"
prefix="$PWD/target/wheel-ffmpeg"
shopt -s nullglob
wheels=("$source"/*.whl)
[[ "${#wheels[@]}" -eq 1 ]] || {
  printf 'error: expected exactly one wheel in %s\n' "$source" >&2
  exit 1
}
mkdir -p "$out"
case "$(uname -s)" in
  Darwin)
    # Deployment targets/architectures must match every embedded dylib.
    architecture="$(rustc -vV | sed -n 's/^host: //p')"
    case "$architecture" in
      aarch64-apple-darwin) architecture="arm64" ;;
      x86_64-apple-darwin) architecture="x86_64" ;;
      *) printf 'error: unsupported macOS target %s\n' "$architecture" >&2; exit 2 ;;
    esac
    uv tool run --from delocate==0.13.0 delocate-wheel \
      --require-archs "$architecture" -w "$out" "${wheels[0]}"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    uv tool run --from delvewheel==1.13.2 delvewheel repair \
      --add-path "$(cygpath -m "$prefix/bin")" --wheel-dir "$out" "${wheels[0]}"
    ;;
  Linux)
    # Maturin already performs ELF dependency repair inside the libc container.
    cp "${wheels[0]}" "$out/"
    ;;
  *)
    printf 'error: unsupported wheel repair host %s\n' "$(uname -s)" >&2
    exit 2
    ;;
esac
