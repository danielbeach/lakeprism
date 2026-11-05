import base64
import math
import os
import struct
import sys
import wave
from urllib.parse import unquote

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
    # Synthetic 16x16 black H.264 clip, generated once with FFmpeg's color source.
    # Keep runtime acceptance independent of an installed ffmpeg executable.
    fixture.write_bytes(base64.b64decode(
        "AAAAIGZ0eXBpc29tAAACAGlzb21pc28yYXZjMW1wNDEAAAAIZnJlZQAAADRtZGF0AAAADmWIhAv//vau/MsrdH+BAAAAB0Ga"
        "OxC//uAAAAALQZpPCGTKYQr//sEAAAMQbW9vdgAAAGxtdmhkAAAAAAAAAAAAAAAAAAAD6AAAAHgAAQAAAQAAAAAAAAAAAAAA"
        "AAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAl90cmFr"
        "AAAAXHRraGQAAAADAAAAAAAAAAAAAAABAAAAAAAAAHgAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAA"
        "AAAAAAAAAABAAAAAABAAAAAQAAAAAAAkZWR0cwAAABxlbHN0AAAAAAAAAAEAAAB4AAAAAAABAAAAAAHXbWRpYQAAACBtZGhk"
        "AAAAAAAAAAAAAAAAAAAyAAAABgBVxAAAAAAALWhkbHIAAAAAAAAAAHZpZGUAAAAAAAAAAAAAAABWaWRlb0hhbmRsZXIAAAAB"
        "gm1pbmYAAAAUdm1oZAAAAAEAAAAAAAAAAAAAACRkaW5mAAAAHGRyZWYAAAAAAAAAAQAAAAx1cmwgAAAAAQAAAUJzdGJsAAAA"
        "vnN0c2QAAAAAAAAAAQAAAK5hdmMxAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAABAAEABIAAAASAAAAAAAAAABDExhdmMgbGli"
        "eDI2NAAAAAAAAAAAAAAAAAAAAAAAAAAAGP//AAAANGF2Y0MBZAAK/+EAF2dkAAqssj2AiAAAAwAIAAADAZB4kTJAAQAGaOvD"
        "yyLA/fj4AAAAABBwYXNwAAAAAQAAAAEAAAAUYnRydAAAAAAAAAt1AAAAAAAAABhzdHRzAAAAAAAAAAEAAAADAAACAAAAABRz"
        "dHNzAAAAAAAAAAEAAAABAAAAHHN0c2MAAAAAAAAAAQAAAAEAAAADAAAAAQAAACBzdHN6AAAAAAAAAAAAAAADAAAAEgAAAAsA"
        "AAAPAAAAFHN0Y28AAAAAAAAAAQAAADAAAAA9dWR0YQAAADVtZXRhAAAAAAAAACFoZGxyAAAAAAAAAABtZGlyYXBwbAAAAAAA"
        "AAAAAAAAAAhpbHN0"
    ))
    rows = lakeprism.MediaSession().video_frames(
        fixture.as_uri(), 0, 100, 50, 2, include_rgb24=False
    ).collect()

    assert rows
    assert rows[0]["width"] == "16"
    assert rows[0]["height"] == "16"


def write_stereo_wave(path, sample_rate):
    samples = (
        int(10000 * math.sin(2 * math.pi * 1000 * index / sample_rate))
        for index in range(round(sample_rate * 0.12))
    )
    with wave.open(str(path), "wb") as output:
        output.setnchannels(2)
        output.setsampwidth(2)
        output.setframerate(sample_rate)
        output.writeframes(b"".join(struct.pack("<hh", sample, sample) for sample in samples))


def test_python_native_audio_decodes_without_pyarrow_or_ffmpeg_executable(tmp_path):
    if os.environ.get("LAKEPRISM_NATIVE_MEDIA") != "1":
        pytest.skip("native FFmpeg acceptance is enabled only on a matched CI host")
    fixture = tmp_path / "audio.wav"
    write_stereo_wave(fixture, 44100)
    rows = lakeprism.MediaSession().audio_segments(
        fixture.as_uri(), 0, 120, 40, 3, include_payload=False
    ).collect()
    assert len(rows) == 3
    assert all(row["sample_rate_hz"] == "16000" and row["channel_count"] == "1" for row in rows)
    assert [(row["start_millis"], row["end_millis"]) for row in rows] == [
        ("0", "40"), ("40", "80"), ("80", "120")
    ]


@pytest.mark.parametrize("sample_rate", [8000, 44100, 48000])
def test_python_native_audio_preserves_resampled_tail_and_time_bounds(tmp_path, sample_rate):
    if os.environ.get("LAKEPRISM_NATIVE_MEDIA") != "1":
        pytest.skip("native FFmpeg acceptance is enabled only on a matched CI host")

    pytest.importorskip("pyarrow")
    fixture = tmp_path / "native-audio.wav"
    write_stereo_wave(fixture, sample_rate)
    reader = lakeprism.MediaSession().audio_segments(
        fixture.as_uri(), 0, 120, 40, 3, include_payload=True
    ).to_pyarrow()
    rows = reader.read_all().to_pylist()

    assert len(rows) == 3
    assert [(row["start_millis"], row["end_millis"]) for row in rows] == [
        (0, 40), (40, 80), (80, 120)
    ]
    assert all(row["sample_rate_hz"] == 16000 and row["channel_count"] == 1 for row in rows)
    assert sum(len(row["f32le"]) for row in rows) == 1920 * 4


def test_optional_callback_and_delta_surfaces_do_not_require_secret_objects():
    assert hasattr(lakeprism, "OAuthTokenSupplier")
    assert hasattr(lakeprism, "FlightAuthSupplier")
    if hasattr(lakeprism, "UnityCatalog"):
        supplier = lambda query_id, principal, catalog_identity: "ephemeral-token"
        catalog = lakeprism.UnityCatalog("http://127.0.0.1:1", supplier)
        assert catalog is not None
    if hasattr(lakeprism, "write_delta_ipc"):
        assert callable(lakeprism.write_delta_ipc)


def test_whisper_subprocess_transcribes_local_audio_or_video_with_lineage(tmp_path):
    if not hasattr(lakeprism, "WhisperSubprocessConfig"):
        pytest.skip("Whisper subprocess support was not compiled into this extension")

    source = tmp_path / "sample recording.wav"
    source.write_bytes(b"fixture audio")
    model = tmp_path / "model.bin"
    model.write_bytes(b"fixture model")
    staging = tmp_path / "staging"
    staging.mkdir()
    adapter = tmp_path / "whisper_adapter.py"
    adapter.write_text(
        "import json\n"
        "import pathlib\n"
        "import sys\n"
        "pathlib.Path(sys.argv[2]).write_text(json.dumps({"
        "'segments': [{'start_millis': 0, 'end_millis': 500, "
        "'text': 'fixture transcript', 'confidence_millis': 987}]}))\n"
    )
    config = lakeprism.WhisperSubprocessConfig(
        sys.executable,
        [str(adapter), "{input}", "{output}", "{model}"],
        str(model),
        str(staging),
        operator_version="fixture-whisper-v1",
        model="fixture",
        model_version="1",
    )
    session = lakeprism.MediaSession.with_transcription_subprocess(config)

    segments = session.transcribe(source.as_uri(), "audio", "fixture-source-v1")

    assert len(segments) == 1
    assert segments[0].media_id == source.as_uri()
    assert segments[0].start_millis == 0
    assert segments[0].end_millis == 500
    assert segments[0].text == "fixture transcript"
    assert segments[0].confidence_millis == 987
    assert segments[0].source_version == "fixture-source-v1"
    assert segments[0].operator_version == "fixture-whisper-v1"
    assert segments[0].model == "fixture"
    assert segments[0].model_version == "1"


def test_recording_dataset_folder_reuse_refresh_and_partial_failure(tmp_path):
    if not hasattr(lakeprism, "WhisperSubprocessConfig"):
        pytest.skip("Whisper subprocess support was not compiled")
    recordings = tmp_path / "recordings"
    recordings.mkdir()
    source = recordings / "speech ' unicode \u00e9.wav"
    source.write_bytes(b"fixture")
    artifact = tmp_path / "model.bin"
    artifact.write_bytes(b"fixture model")
    calls = tmp_path / "calls"
    adapter = tmp_path / "fixture_adapter.py"
    adapter.write_text(
        "import json, pathlib, sys\n"
        f"with pathlib.Path({str(calls)!r}).open('a') as log: log.write('call\\n')\n"
        "if pathlib.Path(sys.argv[1]).read_bytes() == b'bad': sys.exit(2)\n"
        "pathlib.Path(sys.argv[2]).write_text(json.dumps({'segments': ["
        "{'start_millis': 0, 'end_millis': 500, 'text': \"country's future\"}]}))\n"
    )

    class FixtureModel:
        revision = "1"

        def configuration(self, staging):
            return lakeprism.WhisperSubprocessConfig(
                sys.executable, [str(adapter), "{input}", "{output}", "{model}"],
                str(artifact), str(staging), operator_version="fixture",
                model="fixture", model_version=self.revision,
            )

    model = FixtureModel()
    dataset = lakeprism.RecordingDataset(tmp_path / "dataset [literal]")
    assert dataset.ingest(recordings, model=model)[0]["status"] == "written"
    assert dataset.ingest(recordings, model=model)[0]["status"] == "reused"
    assert len(calls.read_text().splitlines()) == 1
    reopened = lakeprism.RecordingDataset(dataset.path)
    hits = reopened.search("country's").collect()
    assert unquote(hits[0]["source_uri"]) == unquote(source.as_uri())
    assert [{key: value for key, value in row.items() if key != "source_uri"} for row in hits] == [{
        "start_millis": "0", "end_millis": "500",
        "text": "country's future",
    }]
    source.write_bytes(b"updated")
    assert reopened.ingest(source, model=model)[0]["status"] == "written"
    model.revision = "2"
    assert reopened.ingest(source, model=model)[0]["status"] == "written"
    source.write_bytes(b"bad")
    other = recordings / "other.wav"
    other.write_bytes(b"fixture")
    with pytest.raises(lakeprism.IngestionError) as failed:
        reopened.ingest(recordings, model=model)
    assert {row["status"] for row in failed.value.results} == {"failed", "written"}
    assert len(reopened.search("country").collect()) == 2
    source.write_bytes(b"updated")
    assert reopened.ingest(recordings, model=model)[0]["status"] == "reused"


def test_recording_dataset_empty_search_and_invalid_selection(tmp_path):
    dataset = lakeprism.RecordingDataset(tmp_path / "dataset")
    assert dataset.search("country").collect() == []
    with pytest.raises(ValueError, match="nonempty"):
        dataset.search(" ")
    with pytest.raises(ValueError, match="integer"):
        dataset.search("country", limit=True)
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="no supported"):
        dataset.ingest(empty, model=None)


def test_python_native_audio_seek_preserves_late_window_samples(tmp_path):
    if os.environ.get("LAKEPRISM_NATIVE_MEDIA") != "1":
        pytest.skip("requires native FFmpeg")
    pytest.importorskip("pyarrow")
    fixture = tmp_path / "long.wav"
    with wave.open(str(fixture), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(16000)
        output.writeframes(struct.pack("<h", 4096) * (65 * 16000))
    session = lakeprism.MediaSession()
    assert session.media_metadata(fixture.as_uri()) == (65000, 0, 1)
    assert session.audio_segments(fixture.as_uri(), 66000, 67000, 1000, 1).collect() == []
    rows = session.audio_segments(
        fixture.as_uri(), 60000, 65000, 30000, 1, include_payload=True,
    ).to_pyarrow().read_all().to_pylist()
    assert len(rows) == 1
    assert (rows[0]["start_millis"], rows[0]["end_millis"]) == (60000, 65000)
    samples = struct.unpack("<" + "f" * (5 * 16000), rows[0]["f32le"])
    assert all(abs(sample - 0.125) < 1e-6 for sample in samples)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX child liveness assertion")
def test_recording_ingestion_interrupt_reaps_adapter_and_keeps_dataset_usable(tmp_path):
    if not hasattr(lakeprism, "WhisperSubprocessConfig"):
        pytest.skip("Whisper subprocess support was not compiled")
    import _thread
    import threading
    import time

    source = tmp_path / "source.wav"
    source.write_bytes(b"fixture")
    artifact = tmp_path / "model.bin"
    artifact.write_bytes(b"fixture")
    marker = tmp_path / "child.pid"
    adapter = tmp_path / "waiting_adapter.py"
    adapter.write_text(
        "import os, pathlib, time\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(30)\n"
    )

    class FixtureModel:
        def configuration(self, staging):
            return lakeprism.WhisperSubprocessConfig(
                sys.executable, [str(adapter), "{input}", "{output}", "{model}"],
                str(artifact), str(staging), timeout_seconds=5,
                operator_version="fixture", model="fixture", model_version="1",
            )

    def interrupt_started_adapter():
        for _ in range(400):
            if marker.exists():
                _thread.interrupt_main()
                return
            time.sleep(0.01)

    interrupter = threading.Thread(target=interrupt_started_adapter)
    dataset = lakeprism.RecordingDataset(tmp_path / "dataset")
    interrupter.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            dataset.ingest(source, model=FixtureModel())
    finally:
        interrupter.join()
    with pytest.raises(ProcessLookupError):
        os.kill(int(marker.read_text()), 0)
    assert list((dataset.path / "staging").iterdir()) == []
    assert dataset.search("anything").collect() == []
