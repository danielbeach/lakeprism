"""Notebook conveniences layered over the native LakePrism extension.

Optional display dependencies are imported only by the helper that needs them.
The helpers never accept, save, or forward cloud credentials.
"""

from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
import os
from pathlib import Path
from time import sleep
from urllib.parse import urlparse

from .credentials import FlightAuthSupplier, OAuthTokenSupplier
from ._lakeprism import (
    DerivedIndexRow,
    LazyPlan,
    LocalCatalog,
    MediaRef,
    MediaSession,
    TranscriptSegment,
    plan_video_frames,
    refresh_embedding_index,
)

__all__ = [
    "DerivedIndexRow",
    "FlightAuthSupplier",
    "LazyPlan",
    "LocalCatalog",
    "MediaRef",
    "MediaSession",
    "OAuthTokenSupplier",
    "TranscriptSegment",
    "collect_with_progress",
    "discover_databricks_workspace",
    "display",
    "plan_video_frames",
    "refresh_embedding_index",
    "to_pandas",
]


def _arrow_table(value):
    """Return a PyArrow table from a lazy plan, reader, or table."""
    if hasattr(value, "to_pyarrow"):
        value = value.to_pyarrow()
    if hasattr(value, "read_all"):
        return value.read_all()
    if hasattr(value, "to_pandas"):
        return value
    raise TypeError("value must be a LakePrism LazyPlan or a PyArrow reader/table")


def to_pandas(value):
    """Materialize a lazy plan or PyArrow reader/table as a pandas DataFrame."""
    return _arrow_table(value).to_pandas()


def display(value, *, max_rows=100):
    """Display a bounded pandas preview in IPython and return the DataFrame.

    Query execution remains lazy until this helper consumes a ``LazyPlan``.
    ``max_rows`` bounds only notebook presentation; it does not alter query SQL.
    """
    if max_rows < 0:
        raise ValueError("max_rows must be non-negative")
    frame = to_pandas(value)
    preview = frame.head(max_rows)
    try:
        from IPython.display import display as ipython_display
    except ImportError:
        return preview
    ipython_display(preview)
    return preview


def collect_with_progress(session, query_id, query, *, poll_interval=0.1, on_update=None):
    """Execute a registered query and report its real LakePrism lifecycle.

    ``session`` must be a ``MediaSession``. The helper owns only the generated
    query identifier if ``query_id`` is ``None``. It polls Rust's query status
    while execution runs on a worker thread; it does not estimate bytes, invent
    percentages, or claim cancellation can interrupt an in-flight codec call.
    ``on_update`` receives each status dictionary. If omitted, tqdm is used
    when installed and otherwise the query runs without a progress UI.
    """
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive")
    if query_id is None:
        query_id = session.create_query(None)

    progress = None
    if on_update is None:
        try:
            from tqdm.auto import tqdm
        except ImportError:
            pass
        else:
            progress = tqdm(total=None, desc="LakePrism query", unit="rows")
            previous_rows = 0

            def on_update(status):
                nonlocal previous_rows
                rows = status["rows"]
                progress.update(max(0, rows - previous_rows))
                previous_rows = rows
                progress.set_postfix_str(status["status"])

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="lakeprism-query") as executor:
        future = executor.submit(session.execute_query, query_id, query)
        try:
            while not future.done():
                status = session.query_status(query_id)
                if on_update is not None:
                    on_update(status)
                sleep(poll_interval)
            rows = future.result()
            status = session.query_status(query_id)
            if on_update is not None:
                on_update(status)
            return rows
        finally:
            if progress is not None:
                progress.close()


def discover_databricks_workspace(*, profile=None, config_path=None, environ=None):
    """Discover a credential-free Databricks workspace endpoint.

    Environment ``DATABRICKS_HOST`` takes precedence. Otherwise, this reads
    only the selected profile's ``host`` from ``~/.databrickscfg`` (or
    ``config_path``). It never returns, caches, writes, logs, or forwards
    tokens, OAuth client secrets, or service-principal secrets. Use the
    resulting endpoint only to configure an application-owned Rust
    ``CredentialProvider``; the Python binding intentionally cannot mint one.
    """
    environ = os.environ if environ is None else environ
    profile = profile or environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    host = environ.get("DATABRICKS_HOST")
    source = "environment"
    if not host:
        path = Path(config_path or environ.get("DATABRICKS_CONFIG_FILE", "~/.databrickscfg"))
        path = path.expanduser()
        parser = ConfigParser(interpolation=None)
        if not parser.read(path):
            raise ValueError(
                "Databricks host is not configured; set DATABRICKS_HOST or provide a config file"
            )
        section = parser.defaults() if profile == "DEFAULT" else (
            parser[profile] if parser.has_section(profile) else None
        )
        if section is None or not section.get("host"):
            raise ValueError(f"Databricks profile {profile!r} has no host")
        host = section["host"]
        source = "config-file"
    parsed = urlparse(host)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "Databricks host must be an HTTPS origin without credentials, query, or fragment"
        )
    return {"host": host.rstrip("/"), "profile": profile, "source": source}


try:
    from ._lakeprism import UnityCatalog, UnityQueryContext
except ImportError:
    pass
else:
    __all__ += ["UnityCatalog", "UnityQueryContext"]

try:
    from ._lakeprism import FlightClient, FlightServer
except ImportError:
    pass
else:
    __all__ += ["FlightClient", "FlightServer"]

try:
    from ._lakeprism import write_delta_ipc
except ImportError:
    pass
else:
    __all__.append("write_delta_ipc")
