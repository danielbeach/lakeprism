import base64
import gc
import hashlib
import json
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
    session = lakeprism.MediaSession()
    assert session.media_metadata(fixture.as_uri()) == (120, 0, 1)
    assert session.audio_metadata(fixture.as_uri()) == (0, 120)
    rows = session.audio_segments(
        fixture.as_uri(), 0, 120, 40, 3, include_payload=False
    ).collect()
    assert len(rows) == 3
    assert all(row["sample_rate_hz"] == "16000" and row["channel_count"] == "1" for row in rows)
    assert [(row["start_millis"], row["end_millis"]) for row in rows] == [
        ("0", "40"), ("40", "80"), ("80", "120")
    ]
    large_interval = session.audio_segments(
        fixture.as_uri(), 0, 120, 1_000_000_000, 1024,
    ).collect()
    assert len(large_interval) == 1
    assert large_interval[0]["end_millis"] == "120"


def test_audio_segments_rejects_excessive_pcm_before_execution():
    with pytest.raises(ValueError, match="64 MiB"):
        lakeprism.MediaSession().audio_segments(
            "file:///missing.wav", 0, 1_000_000_000, 1_000_000_000, 1,
        )


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
    error = next(row for row in failed.value.results if row["status"] == "failed")
    assert error["error_code"] == "child_failed"
    assert error["published"] is False
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


@pytest.fixture
def recorded_dataset(tmp_path):
    if not hasattr(lakeprism, "WhisperSubprocessConfig"):
        pytest.skip("Whisper subprocess support was not compiled")
    source = tmp_path / "source.wav"
    source.write_bytes(b"fixture")
    artifact = tmp_path / "model.bin"
    artifact.write_bytes(b"fixture model")
    adapter = tmp_path / "adapter.py"
    adapter.write_text(
        "import json, pathlib, sys\n"
        "pathlib.Path(sys.argv[2]).write_text(json.dumps({'segments': ["
        "{'start_millis': 0, 'end_millis': 500, 'text': sys.argv[4]}]}))\n"
    )

    class FixtureModel:
        version = "1"

        def configuration(self, staging):
            return lakeprism.WhisperSubprocessConfig(
                sys.executable, [str(adapter), "{input}", "{output}", "{model}", self.version],
                str(artifact), str(staging), operator_version="fixture",
                model="fixture", model_version=self.version,
            )

    dataset = lakeprism.RecordingDataset(tmp_path / "dataset [literal]")
    model = FixtureModel()
    dataset.ingest(source, model=model)
    return dataset, source, model


def test_recording_dataset_remove_vacuum_and_detached_arrow_readers(recorded_dataset):
    pa = pytest.importorskip("pyarrow")
    dataset, source, model = recorded_dataset
    table = dataset.session().plan("SELECT * FROM transcripts").to_pyarrow().read_all()
    assert table.schema == pa.schema([
        pa.field("source_uri", pa.string(), nullable=False),
        pa.field("source_version", pa.string(), nullable=False),
        pa.field("start_millis", pa.uint64(), nullable=False),
        pa.field("end_millis", pa.uint64(), nullable=False),
        pa.field("text", pa.string(), nullable=False),
        pa.field("confidence_millis", pa.uint16()),
        pa.field("operator_version", pa.string(), nullable=False),
        pa.field("model", pa.string()),
        pa.field("model_version", pa.string()),
        pa.field("parameters_json", pa.string(), nullable=False),
    ])
    records = dataset.recordings()
    assert records == [{
        "source_uri": source.resolve().as_uri(),
        "source_version": "sha256:" + hashlib.sha256(b"fixture").hexdigest(),
        "segments": 1,
    }]
    pinned = dataset.search("1")
    model.version = "2"
    dataset.ingest(source, model=model)
    assert dataset.remove(source)
    assert not dataset.remove(source)
    assert source.is_file()
    with pytest.raises(lakeprism.RecordingError) as busy:
        dataset.vacuum()
    assert busy.value.code == "readers_busy"
    assert busy.value.published is False
    reader = pinned.to_pyarrow()
    del pinned
    gc.collect()
    with pytest.raises(lakeprism.RecordingError, match="readers"):
        dataset.vacuum()
    assert reader.read_all().to_pylist()[0]["text"] == "1"
    reader.close()
    del reader
    gc.collect()
    result = dataset.vacuum()
    assert result["removed_files"] == 1
    assert result["reclaimed_bytes"] > 0
    assert dataset.recordings() == []
    assert dataset.search("anything").collect() == []


def test_recording_dataset_recovery_validates_snapshots_and_never_downgrades(recorded_dataset):
    dataset, source, model = recorded_dataset
    model.version = "2"
    dataset.ingest(source, model=model)
    manifest_path = dataset.path / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    snapshot = next(iter(manifest["recordings"].values()))["file"]
    (dataset.path / snapshot).write_bytes(b"interrupted snapshot")
    with pytest.raises(lakeprism.RecordingError) as invalid:
        lakeprism.RecordingDataset(dataset.path)
    assert invalid.value.code == "invalid_manifest"
    assert lakeprism.RecordingDataset.recover(dataset.path) == {"restored": True, "recordings": 1}
    reopened = lakeprism.RecordingDataset(dataset.path)
    assert reopened.search("1").collect()[0]["text"] == "1"
    future = '{"format_version":2,"recordings":{},"future":true}'
    manifest_path.write_text(future)
    with pytest.raises(lakeprism.RecordingError) as invalid:
        lakeprism.RecordingDataset.recover(dataset.path)
    assert invalid.value.code == "unsupported_manifest_version"
    assert manifest_path.read_text() == future


def test_recording_dataset_opens_published_alpha_without_rewriting(tmp_path):
    import base64
    import zlib

    pytest.importorskip("pyarrow")
    # Original manifest and Parquet bytes produced by unmodified PyPI 0.1.0a2.
    fixture = json.loads(zlib.decompress(base64.b64decode(
        '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'
        'Zg=='
    )))
    root = tmp_path / "imported alpha"
    root.mkdir()
    manifest = fixture["manifest"]
    snapshot = next(iter(manifest["recordings"].values()))
    (root / snapshot["file"]).write_bytes(base64.b64decode(fixture["parquet_base64"]))
    manifest_path = root / "manifest.json"
    manifest_bytes = json.dumps(manifest).encode()
    manifest_path.write_bytes(manifest_bytes)
    dataset = lakeprism.RecordingDataset(root)
    assert manifest_path.read_bytes() == manifest_bytes
    assert dataset.search("legacy").collect()[0]["text"] == "legacy alpha transcript"
    uri = next(iter(manifest["recordings"]))
    assert dataset.remove(uri)
    assert dataset.recordings() == []
    assert lakeprism.RecordingDataset.recover(root) == {"restored": False, "recordings": 0}


@pytest.mark.parametrize("payload,exit_code,expected", [
    ({"error": "invalid_duration"}, 1, "invalid_duration"),
    ({"error": "model_failed"}, 1, "model_failed"),
    ({"error": "private source text"}, 1, "child_failed"),
    ({"error": "decode_failed", "details": "private source text"}, 1, "child_failed"),
    ({"segments": []}, 1, "child_failed"),
    ({"error": "decode_failed"}, 0, "invalid_output"),
])
def test_recording_dataset_redacts_typed_and_legacy_child_failures(tmp_path, payload, exit_code, expected):
    if not hasattr(lakeprism, "WhisperSubprocessConfig"):
        pytest.skip("Whisper subprocess support was not compiled")
    source = tmp_path / "source.wav"
    source.write_bytes(b"fixture")
    artifact = tmp_path / "model.bin"
    artifact.write_bytes(b"fixture model")
    adapter = tmp_path / "failure_adapter.py"
    adapter.write_text(
        "import json, pathlib, sys\n"
        "sys.stderr.write('private source text')\n"
        f"pathlib.Path(sys.argv[2]).write_text(json.dumps({payload!r}))\n"
        f"sys.exit({exit_code})\n"
    )

    class FixtureModel:
        def configuration(self, staging):
            return lakeprism.WhisperSubprocessConfig(
                sys.executable, [str(adapter), "{input}", "{output}", "{model}"],
                str(artifact), str(staging), operator_version="fixture",
                model="fixture", model_version="1",
            )

    dataset = lakeprism.RecordingDataset(tmp_path / "dataset")
    with pytest.raises(lakeprism.IngestionError) as failed:
        dataset.ingest(source, model=FixtureModel())
    message = failed.value.results[0]["error"]
    assert expected in message
    assert "private source text" not in message
    assert dataset.search("anything").collect() == []
    assert list((dataset.path / "staging").iterdir()) == []


@pytest.mark.parametrize("stage,expected", [
    ("dependency", "dependency_missing"), ("probe", "invalid_media"),
    ("profile", "unsupported_media"), ("duration", "invalid_duration"),
    ("decode", "decode_failed"), ("model", "model_failed"),
])
def test_speech_adapter_reports_only_fixed_failure_categories(monkeypatch, tmp_path, stage, expected):
    from types import SimpleNamespace
    from lakeprism import _speech_adapter, _lakeprism
    if not hasattr(_lakeprism, "UnsupportedMediaError"):
        pytest.skip("requires native media")

    def metadata(uri):
        if stage == "probe":
            raise RuntimeError("private source text")
        if stage == "profile":
            raise _lakeprism.UnsupportedMediaError("private source text")
        return (None if stage == "duration" else 1000, 0, 1)

    def decode(*args, **kwargs):
        raise RuntimeError("private source text")

    def model(*args, **kwargs):
        if stage == "model":
            raise ValueError("private source text")
        return None

    session = SimpleNamespace(
        media_metadata=metadata, audio_metadata=lambda uri: (0, 1000),
        audio_segments=decode,
    )
    monkeypatch.setattr(_lakeprism, "MediaSession", lambda: session)
    monkeypatch.setitem(sys.modules, "numpy", None if stage == "dependency" else SimpleNamespace())
    monkeypatch.setitem(sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=model))
    output = tmp_path / "result.json"
    monkeypatch.setattr(sys, "argv", ["adapter", str(tmp_path / "input"), str(output), str(tmp_path / "model.bin")])
    with pytest.raises(SystemExit) as failed:
        _speech_adapter.main()
    assert failed.value.code == 1
    assert json.loads(output.read_text()) == {"error": expected}


@pytest.mark.parametrize("audio_end", [8000, None])
@pytest.mark.parametrize("silent", [False, True])
def test_speech_adapter_preserves_offsets_and_skips_digital_silence(monkeypatch, tmp_path, audio_end, silent):
    from types import SimpleNamespace
    from lakeprism import _speech_adapter, _lakeprism

    row = {
        "start_millis": 5000, "end_millis": 8000, "sample_rate_hz": 16000,
        "channel_count": 1, "f32le": b"fixture audio",
    }
    table = SimpleNamespace(to_pylist=lambda: [row])
    reader = SimpleNamespace(read_all=lambda: table)
    plan = SimpleNamespace(to_pyarrow=lambda: reader)
    empty_table = SimpleNamespace(to_pylist=lambda: [])
    empty_reader = SimpleNamespace(read_all=lambda: empty_table)
    empty_plan = SimpleNamespace(to_pyarrow=lambda: empty_reader)
    session = SimpleNamespace(
        media_metadata=lambda uri: (45000, 1, 1),
        audio_metadata=lambda uri: (0, audio_end),
        audio_segments=lambda uri, start, *args, **kwargs: plan if start == 0 else empty_plan,
    )
    segment = SimpleNamespace(start=0.25, end=1.0, text="fixture speech")
    def transcribe(*args, **kwargs):
        assert not silent, "digital silence must not enter the speech model"
        return [segment], None

    model = SimpleNamespace(transcribe=transcribe)
    monkeypatch.setattr(_lakeprism, "MediaSession", lambda: session)
    monkeypatch.setitem(sys.modules, "numpy", SimpleNamespace(
        frombuffer=lambda *args, **kwargs: SimpleNamespace(size=1),
        any=lambda audio: not silent,
    ))
    monkeypatch.setitem(sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=lambda *args, **kwargs: model))
    result = _speech_adapter.transcribe(tmp_path / "input.mp4", tmp_path / "model.bin")
    assert result == {"segments": [] if silent else [{
        "start_millis": 5250, "end_millis": 6000, "text": "fixture speech",
        "confidence_millis": None,
    }]}


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


@pytest.mark.parametrize("stop", ["interrupt", "timeout"])
def test_recording_ingestion_stop_reaps_adapter_and_keeps_dataset_usable(tmp_path, stop):
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
    waiting_worker = (
        "import os, pathlib, time\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(30)\n"
    )
    if stop == "timeout" and sys.platform == "win32":
        # The launcher can exit before its descendant; ownership must outlive it.
        adapter.write_text(
            "import subprocess, sys\n"
            f"subprocess.Popen([sys.executable, '-c', {waiting_worker!r}])\n"
        )
    else:
        adapter.write_text(waiting_worker)

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

    dataset = lakeprism.RecordingDataset(tmp_path / "dataset")
    if stop == "interrupt":
        interrupter = threading.Thread(target=interrupt_started_adapter)
        interrupter.start()
        try:
            with pytest.raises(KeyboardInterrupt):
                dataset.ingest(source, model=FixtureModel())
        finally:
            interrupter.join()
    else:
        with pytest.raises(lakeprism.IngestionError) as failed:
            dataset.ingest(source, model=FixtureModel())
        result = failed.value.results
        assert result[0]["status"] == "failed"
        assert result[0]["error_code"] == "timed_out"
    pid = int(marker.read_text())
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel.WaitForSingleObject.restype = wintypes.DWORD
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE, never terminate.
        if handle:
            try:
                assert kernel.WaitForSingleObject(handle, 0) == 0
            finally:
                kernel.CloseHandle(handle)
        else:
            assert ctypes.get_last_error() == 87  # ERROR_INVALID_PARAMETER: PID is gone.
    else:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    assert list((dataset.path / "staging").iterdir()) == []
    assert dataset.search("anything").collect() == []
