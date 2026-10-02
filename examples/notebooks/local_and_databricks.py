"""Copyable cells for local Python and Databricks single-process notebooks.

Install a wheel built with the feature set you need. This example contains no
credentials: Databricks OAuth/service-principal setup stays in the workspace
or embedding Rust application, not in a notebook artifact.
"""

import lakeprism

session = lakeprism.MediaSession()
session.register_media_refs(
    "clips",
    [lakeprism.MediaRef("file:///data/clip.mp4", "video")],
)

# Native Arrow C-stream transfer, then a bounded pandas preview.
plan = session.plan("SELECT media.uri, media.media_type FROM clips")
lakeprism.display(plan, max_rows=20)

# The helper reports only real lifecycle status/row counters. It does not
# invent a completion percentage for a DataFusion query.
rows = lakeprism.collect_with_progress(session, None, "SELECT count(*) AS clips FROM clips")
print(rows)

# Discovering a workspace reads only its host. It never exposes a token from
# ~/.databrickscfg; OAuth/service-principal vending remains an application-owned
# Rust CredentialProvider boundary.
# workspace = lakeprism.discover_databricks_workspace()
# print(workspace["host"])
#
# Feature-gated Unity REST uses an application callback. The return value is
# used for one request only; it is never a LakePrism session/catalog value:
#
# def fresh_oauth_token(query_id, principal, catalog_identity):
#     return application_token_broker.mint(query_id, principal, catalog_identity)
#
# context = lakeprism.UnityQueryContext("notebook-query", "analyst")
# unity = lakeprism.UnityCatalog(workspace["host"], fresh_oauth_token)
# unity.resolve_and_register("main.media.clips", "clips", context, session)
#
# `write_delta_ipc` accepts a local PyArrow IPC stream with explicit mode:
# # lakeprism.write_delta_ipc(delta_dir.as_uri(), ipc_bytes, mode="create")

# Local Whisper-compatible transcription is configured in the embedding Rust
# application, not this wheel: it needs an application-owned executable, model
# artifact, private staging directory, and execution governor. The Python API
# never accepts a shell command or a model path. That application installs
# WhisperSubprocessProvider before exposing MediaSession to this notebook.

# Optional real batch embedding is a separately built wheel feature, never a
# bundled model. It takes absolute application-owned paths and direct argv:
#
# embedding = lakeprism.EmbeddingSubprocessConfig(
#     executable="/opt/local/bin/embedding-adapter",
#     arguments=["--input", "{input}", "--output", "{output}", "--model", "{model}"],
#     model_artifact="/opt/models/e5-large-v2.gguf",
#     staging_directory="/var/lib/my-app/lakeprism-staging",
#     max_batch_items=64, timeout_seconds=60,
#     operator_version="embedding-subprocess-v1",
#     model="e5-large-v2", model_version="local-2026-10",
#     parameters={"normalize": "true"},
# )
# semantic_session = lakeprism.MediaSession.with_embedding_subprocess(embedding)
# The adapter must produce real vectors using LakePrism's bounded batch JSON
# protocol; it receives no shell, and no model/vector is supplied by LakePrism.

# On Databricks, a governed Volume is represented without resolving its backing
# object-store URL or retaining temporary credentials:
#
# from lakeprism import MediaRef
# volume_file = MediaRef("unity-volume://main/media/raw/clip.mp4", "video", "managed")
#
# With the optional Unity feature, attach *already resolved local* table
# metadata only. Remote Unity/Dela locations remain unsupported instead of
# falling back to unsafe object-store reads.
