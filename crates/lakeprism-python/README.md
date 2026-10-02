# LakePrism Python

`lakeprism` is the Maturin/PyO3 notebook binding for the local LakePrism
session. Install a wheel with `pip install lakeprism`, or build from this
repository:

```bash
bash scripts/build-python-wheels.sh --platform native --out dist/python
pip install dist/python/lakeprism-*.whl
```

For notebook display helpers install `lakeprism[notebook]`. They import
PyArrow, pandas, IPython, and tqdm only when called.

The builder uses `uv tool run` to manage Maturin. It intentionally rejects
cross-target builds: PyO3 and optional FFmpeg must link on the target-native
host. LakePrism uses the PyO3 `abi3-py39` policy, producing one `cp39-abi3`
wheel for CPython 3.9+. See the repository README for the supported
manylinux x86_64, macOS (x86_64/aarch64), and Windows x86_64 release matrix.

## Capability parity and limits

`MediaSession` exposes local media registration, SQL, lazy Arrow C-stream/IPC
export, catalog metadata, `explain`, governed query lifecycle, local Parquet,
document/media table functions, transcript/embedding indexes, and derived
Parquet snapshots. `MediaSession.with_transcription_subprocess` plus
`transcribe` provides local audio/video transcription through a validated,
bounded Whisper-compatible adapter. `LocalCatalog` exposes its durable DDL and
registration path. `FlightServer`/`FlightClient` are available with the
`flight` feature.

With `delta-rs`, `register_delta` reads credential-free local `file://` Delta
tables and `write_delta_ipc` writes a PyArrow IPC stream with explicit write
and schema modes. Remote Delta writes are intentionally Rust-only because they
require scoped object-store credentials and an explicit concurrency guard.

With `unity`, `UnityCatalog(base_url, token_supplier)` accepts an
application-owned fresh-token callback and resolves/registers tables for a
`UnityQueryContext`. The callback receives only query identity and returns a
token for that one REST request. LakePrism stores the callback—not its returned
token—and redacts native credential types. Unity temporary table credentials are consumed internally by
`UnityCatalog.resolve_and_register_managed_delta`; they are never exposed to
Python. `OAuthTokenSupplier` and `FlightAuthSupplier` document the callback
shapes. `FlightClient(endpoint, auth_supplier=...)` invokes its callback for
each execution and sends the result only as that request's Authorization
header, but the Python client rejects non-loopback endpoints. Remote Flight
deployment and authentication policy remain Rust application-boundary work.

Iceberg's Arrow-59 upstream provider currently accepts an already constructed
Rust `iceberg::Table`; its REST construction and credentialed storage client
are not safely constructible from this ABI-stable Python extension. Use the
Rust Iceberg/REST adapter at an application boundary, then expose the
credential-free `MediaSession` result to Python. Likewise, custom Rust
`MediaResolver`, OCR/image providers, distributed worker implementations, S3
staging, and Flight server-side authentication remain application-boundary
traits rather than Python object wrappers.

### Optional local Whisper adapter

Python wheels include `WhisperSubprocessConfig` and
`MediaSession.with_transcription_subprocess`, but deliberately do **not**
include a model runtime, model artifact, or adapter executable. The config
requires an absolute executable, explicit argument vector (not a shell
command), validated local model artifact, and private staging directory.
`session.transcribe` accepts only a local `file://` audio/video URI and an
explicit source version; it returns and registers lineage-bearing
`TranscriptSegment` rows. See the root README's “Transcribe local audio or
video” section for the protocol and resource/cancellation limits.

No LakePrism Python object accepts, serializes, logs, or persists OAuth client
secrets, bearer tokens, or temporary cloud object-store credentials. See the
repository's `docs/CLOUD_ACCEPTANCE.md` for the opt-in live contract.
