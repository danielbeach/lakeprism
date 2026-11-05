"""Local workflow orchestration; execution and dataset publication stay in Rust."""

from pathlib import Path

from ._lakeprism import _RecordingDataset


_RECORDINGS = {".wav", ".mp4", ".mov", ".m4a", ".mkv", ".webm"}


class IngestionError(RuntimeError):
    """One or more files failed. ``results`` includes all per-file outcomes."""

    def __init__(self, results):
        self.results = results
        failures = sum(row["status"] == "failed" for row in results)
        super().__init__(f"{failures} recording(s) failed; inspect exception.results")


class RecordingDataset:
    """A local, versioned collection of immutable transcript Parquet snapshots.

    Each successful recording commits independently. Re-ingestion compares
    content, model, operator, and inference parameters before reusing a result.
    Deleted input files are not implicitly deleted from the dataset.
    """

    def __init__(self, path):
        self._native = _RecordingDataset(str(Path(path).expanduser().resolve()))

    @property
    def path(self):
        return Path(self._native.path)

    def ingest(self, source, *, model):
        """Ingest a file or recursively scan a folder for supported recordings.

        Failures do not erase earlier results. All selected files are attempted,
        then ``IngestionError`` exposes failures and successful/reused outcomes.
        Keyboard interruption propagates; rerunning reuses committed recordings.
        """
        source = Path(source).expanduser().resolve(strict=True)
        if source.is_dir():
            paths = []
            for path in source.rglob("*"):
                if path.suffix.lower() in _RECORDINGS and path.is_file():
                    paths.append(path.resolve())
                    if len(paths) > 10_000:
                        raise ValueError("one ingestion may select at most 10000 recordings")
            paths = sorted(set(paths))
            if not paths:
                raise ValueError("folder contains no supported recordings")
        else:
            paths = [source]
        staging = self.path / "staging"
        staging.mkdir(mode=0o700, exist_ok=True)
        config = model.configuration(staging)
        results = []
        for path in paths:
            try:
                reused, count = self._native.ingest_file(str(path), config)
            except (RuntimeError, ValueError) as error:
                results.append({"path": str(path), "status": "failed", "error": str(error)})
            else:
                results.append({
                    "path": str(path),
                    "status": "reused" if reused else "written",
                    "segments": count,
                })
        if any(row["status"] == "failed" for row in results):
            raise IngestionError(results)
        return results

    def session(self):
        """Pin current snapshots as the ``transcripts`` SQL table."""
        return self._native.session()

    def search(self, text, *, limit=100):
        """Return a lazy, case-insensitive substring search with source/times."""
        if not isinstance(text, str) or not text.strip():
            raise ValueError("search text must be nonempty")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10_000:
            raise ValueError("limit must be an integer in 1..10000")
        literal = text.replace("'", "''")
        return self.session().plan(
            "SELECT source_uri, start_millis, end_millis, text FROM transcripts "
            f"WHERE strpos(lower(text), lower('{literal}')) > 0 "
            f"ORDER BY source_uri, start_millis, end_millis, text LIMIT {limit}"
        )
