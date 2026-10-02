import os
import subprocess

import pytest

import lakeprism


def test_media_ref_exposes_validated_values():
    media = lakeprism.MediaRef("file:///media/video.mp4", "video")

    assert media.uri == "file:///media/video.mp4"
    assert media.media_type == "video"
    assert media.storage_mode == "external"


def test_frame_planning_uses_rust_constraints():
    assert lakeprism.plan_video_frames([0, 100, 200, 300], 100, 300, 2) == [100, 200]


def test_partial_time_range_is_rejected():
    with pytest.raises(ValueError, match="supplied together"):
        lakeprism.plan_video_frames([0], 0, None)


def test_media_session_registers_media_refs_and_executes_sql():
    session = lakeprism.MediaSession()
    session.register_media_refs(
        "media_objects",
        [lakeprism.MediaRef("file:///media/video.mp4", "video")],
    )

    assert session.sql(
        "SELECT media.uri, media.media_type FROM media_objects"
    ) == [{"uri": "file:///media/video.mp4", "media_type": "video"}]


def test_media_session_rejects_invalid_sql():
    with pytest.raises(RuntimeError, match="table"):
        lakeprism.MediaSession().execute("SELECT * FROM missing_table")


def test_media_session_exports_native_arrow_ipc():
    pa = pytest.importorskip("pyarrow")
    session = lakeprism.MediaSession()
    plan = session.plan("SELECT 42 AS answer")
    assert plan.sql == "SELECT 42 AS answer"
    reader = plan.to_pyarrow()
    table = reader.read_all()

    assert table.to_pylist() == [{"answer": 42}]
    c_reader = pa.RecordBatchReader._import_from_c_capsule(
        session.sql_arrow_c_stream("SELECT 7 AS answer")
    )
    assert c_reader.read_all().to_pylist() == [{"answer": 7}]
    assert pa.ipc.open_stream(session.sql_arrow_ipc("SELECT 'ok' AS status")).read_all().to_pylist() == [
        {"status": "ok"}
    ]


def test_local_capability_helpers_are_lazy_plans():
    session = lakeprism.MediaSession()

    assert "lakeprism_document_sections" in session.document_sections(
        "file:///docs/report.docx"
    ).sql
    assert "lakeprism_document_tables" in session.document_tables(
        "file:///docs/report.docx"
    ).sql
    assert "lakeprism_document_images" in session.document_images(
        "file:///docs/report.docx", include_bytes=True
    ).sql
    assert "lakeprism_document_search" in session.document_search(
        "file:///docs/report.docx", "revenue", 10
    ).sql
    assert "lakeprism_video_frames" in session.video_frames(
        "file:///media/clip.mp4", 0, 1000, 250, 4
    ).sql
    assert "lakeprism_audio_segments" in session.audio_segments(
        "file:///media/clip.mp4", 0, 1000, 100, 4
    ).sql


def test_local_capability_helpers_validate_bounded_decode_requests():
    session = lakeprism.MediaSession()

    with pytest.raises(ValueError, match="limit"):
        session.video_frames("file:///media/clip.mp4", 0, 1000, 250, 33)
    with pytest.raises(ValueError, match="non-zero"):
        session.audio_segments("file:///media/clip.mp4", 0, 1000, 0, 4)


def test_mock_embedding_search_is_explicit_and_reports_ranking_semantics():
    assert "EmbeddingRecord" in lakeprism.__all__
    session = lakeprism.MediaSession(embedding_mock_dimensions=8)
    session.register_embedding_records(
        [
            lakeprism.EmbeddingRecord(
                "segment-1",
                "lake data",
                [1.0] + [0.0] * 7,
                "file:///media/a.wav",
                "v1",
                "embed-mock-v1",
                model="deterministic-mock",
                model_version="1",
            )
        ]
    )
    assert session.semantic_search("lake", 1).collect()[0]["ranking_semantics"] == "ranked"
    assert "lakeprism_hybrid_search" in session.hybrid_search(
        "lake", 1, candidate_limit=1
    ).sql


def test_local_catalog_ddl_and_session_registration_are_thin_rust_bindings():
    catalog = lakeprism.LocalCatalog()
    assert catalog.execute_ddl("CREATE MEDIA TABLE videos") == ("created", "videos")
    assert catalog.table_names() == ["videos"]

    session = lakeprism.MediaSession()
    catalog.register_in_session(session)
    assert session.sql("SELECT count(*) AS n FROM videos") == [{"n": "0"}]
    assert session.explain("SELECT * FROM videos").sql == "EXPLAIN SELECT * FROM videos"
    assert any(table["name"] == "videos" for table in session.catalog_tables())


def test_derived_index_rows_keep_lineage_and_do_not_require_a_model_runtime():
    row = lakeprism.DerivedIndexRow(
        "segment-1",
        "lake data",
        [1.0, 0.0],
        "file:///media/a.wav",
        "v1",
        "embed-v1",
        model="deterministic-mock",
        model_version="1",
    )

    assert row is not None
    assert hasattr(lakeprism, "refresh_embedding_index")


def test_notebook_pandas_and_display_helpers_are_lazy_until_consumed(monkeypatch):
    class FakeTable:
        def to_pandas(self):
            return FakeFrame()

    class FakeReader:
        def read_all(self):
            return FakeTable()

    class FakePlan:
        def to_pyarrow(self):
            return FakeReader()

    class FakeFrame:
        def head(self, count):
            self.count = count
            return self

    frame = lakeprism.to_pandas(FakePlan())
    assert isinstance(frame, FakeFrame)
    assert lakeprism.display(FakePlan(), max_rows=2).count == 2
    with pytest.raises(ValueError, match="non-negative"):
        lakeprism.display(FakePlan(), max_rows=-1)


def test_progress_helper_reports_real_query_lifecycle_without_estimating_percentages():
    class FakeSession:
        def __init__(self):
            self.statuses = [
                {"id": "query-1", "status": "running", "rows": 0},
                {"id": "query-1", "status": "succeeded", "rows": 2},
            ]

        def create_query(self, _deadline):
            return "query-1"

        def execute_query(self, query_id, query):
            assert (query_id, query) == ("query-1", "SELECT 1")
            return [{"one": "1"}, {"one": "1"}]

        def query_status(self, query_id):
            assert query_id == "query-1"
            return self.statuses.pop(0) if self.statuses else {
                "id": "query-1",
                "status": "succeeded",
                "rows": 2,
            }

    updates = []
    rows = lakeprism.collect_with_progress(
        FakeSession(), None, "SELECT 1", poll_interval=0.001, on_update=updates.append
    )
    assert rows == [{"one": "1"}, {"one": "1"}]
    assert updates[-1]["status"] == "succeeded"


def test_databricks_config_discovery_returns_only_a_validated_host(tmp_path):
    config = tmp_path / "databrickscfg"
    config.write_text(
        "[DEFAULT]\nhost = https://workspace.cloud.databricks.com\ntoken = must-not-return\n"
    )
    discovered = lakeprism.discover_databricks_workspace(config_path=config)
    assert discovered == {
        "host": "https://workspace.cloud.databricks.com",
        "profile": "DEFAULT",
        "source": "config-file",
    }
    assert "token" not in discovered
    assert lakeprism.discover_databricks_workspace(
        environ={"DATABRICKS_HOST": "https://env.cloud.databricks.com"}
    )["source"] == "environment"
    with pytest.raises(ValueError, match="HTTPS origin"):
        lakeprism.discover_databricks_workspace(
            environ={"DATABRICKS_HOST": "http://workspace.example.test"}
        )


def test_python_flight_round_trip_uses_the_rust_session_when_enabled():
    if not hasattr(lakeprism, "FlightServer"):
        pytest.skip("Flight support was not compiled into this extension")

    session = lakeprism.MediaSession()
    server = lakeprism.FlightServer(session)
    endpoint = server.start()
    try:
        assert endpoint.startswith("http://127.0.0.1:")
        assert lakeprism.FlightClient(
            endpoint, auth_supplier=lambda: "ephemeral-test-token"
        ).execute("SELECT 42 AS answer") == [
            {"answer": "42"}
        ]
    finally:
        server.stop()


def test_python_native_media_path_decodes_a_real_ffmpeg_fixture(tmp_path):
    if os.environ.get("LAKEPRISM_NATIVE_MEDIA") != "1":
        pytest.skip("native FFmpeg acceptance is enabled only on a matched CI host")

    fixture = tmp_path / "native-acceptance.mp4"
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=16x16:d=0.1",
            "-an",
            str(fixture),
        ],
        check=True,
        capture_output=True,
    )
    rows = lakeprism.MediaSession().video_frames(
        fixture.as_uri(), 0, 100, 50, 2, include_rgb24=False
    ).collect()

    assert rows
    assert rows[0]["width"] == "16"
    assert rows[0]["height"] == "16"


def test_optional_callback_and_delta_surfaces_do_not_require_secret_objects():
    assert hasattr(lakeprism, "OAuthTokenSupplier")
    assert hasattr(lakeprism, "FlightAuthSupplier")
    if hasattr(lakeprism, "UnityCatalog"):
        supplier = lambda query_id, principal, catalog_identity: "ephemeral-token"
        catalog = lakeprism.UnityCatalog("http://127.0.0.1:1", supplier)
        assert catalog is not None
    if hasattr(lakeprism, "write_delta_ipc"):
        assert callable(lakeprism.write_delta_ipc)
