#![cfg(feature = "fixture-provider")]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lakeprism_core::{FeatureLineage, MediaRef, SourceIdentity, StorageMode};
use lakeprism_index::TranscriptionRequest;
use lakeprism_storage::{ExecutionGovernor, ExecutionGovernorConfig, QueryControl};
use lakeprism_whisper::{WhisperSubprocessConfig, WhisperSubprocessProvider};

#[test]
fn fixture_protocol_transcribes_without_a_model_binary() {
    let root = std::env::current_dir().unwrap();
    let staging = root.join("target/lakeprism-whisper-fixture-staging");
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging).unwrap();
    let config = WhisperSubprocessConfig {
        executable: PathBuf::from(env!("CARGO_BIN_EXE_lakeprism-whisper-fixture")),
        arguments: vec![
            "--input".into(),
            "{input}".into(),
            "--output".into(),
            "{output}".into(),
            "--model".into(),
            "{model}".into(),
        ],
        model_artifact: root.join("Cargo.toml"),
        staging_directory: staging.clone(),
        max_input_bytes: 1024 * 1024,
        max_output_bytes: 1024 * 1024,
        timeout: Duration::from_secs(2),
        operator_version: "whisper-subprocess-v1".into(),
        model: "fixture".into(),
        model_version: "protocol-1".into(),
        parameters: BTreeMap::from([("language".into(), "en".into())]),
    };
    let provider = WhisperSubprocessProvider::new(
        config,
        Arc::new(ExecutionGovernor::new(ExecutionGovernorConfig::default()).unwrap()),
    )
    .unwrap();
    let source_uri = format!("file://{}", root.join("Cargo.toml").display());
    let request = TranscriptionRequest {
        media: MediaRef::new(&source_uri, "audio", StorageMode::External).unwrap(),
        lineage: FeatureLineage {
            source: SourceIdentity {
                media_uri: source_uri,
                source_version: "fixture".into(),
            },
            operator_version: "whisper-subprocess-v1".into(),
            model: Some("fixture".into()),
            model_version: Some("protocol-1".into()),
            parameters: BTreeMap::from([("language".into(), "en".into())]),
        },
        start_millis: 0,
        end_millis: 1_000,
        max_segments: 1,
    };
    let rows = provider
        .transcribe_governed(&request, &QueryControl::new(None))
        .unwrap();
    assert_eq!(rows[0].text, "fixture transcript");
    assert_eq!(rows[0].lineage, request.lineage);
    assert!(fs::read_dir(&staging).unwrap().next().is_none());
    let _ = fs::remove_dir_all(staging);
}

#[test]
fn timeout_kills_the_fixture_child_and_cleans_staging() {
    let root = std::env::current_dir().unwrap();
    let staging = root.join("target/lakeprism-whisper-timeout-staging");
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging).unwrap();
    let provider = WhisperSubprocessProvider::new(
        WhisperSubprocessConfig {
            executable: PathBuf::from(env!("CARGO_BIN_EXE_lakeprism-whisper-fixture")),
            arguments: vec![
                "--input".into(),
                "{input}".into(),
                "--output".into(),
                "{output}".into(),
                "--model".into(),
                "{model}".into(),
                "--sleep-millis".into(),
                "500".into(),
            ],
            model_artifact: root.join("Cargo.toml"),
            staging_directory: staging.clone(),
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
            timeout: Duration::from_millis(25),
            operator_version: "whisper-subprocess-v1".into(),
            model: "fixture".into(),
            model_version: "protocol-1".into(),
            parameters: BTreeMap::new(),
        },
        Arc::new(ExecutionGovernor::new(ExecutionGovernorConfig::default()).unwrap()),
    )
    .unwrap();
    let source_uri = format!("file://{}", root.join("Cargo.toml").display());
    let request = TranscriptionRequest {
        media: MediaRef::new(&source_uri, "audio", StorageMode::External).unwrap(),
        lineage: FeatureLineage {
            source: SourceIdentity {
                media_uri: source_uri,
                source_version: "fixture".into(),
            },
            operator_version: "whisper-subprocess-v1".into(),
            model: Some("fixture".into()),
            model_version: Some("protocol-1".into()),
            parameters: BTreeMap::new(),
        },
        start_millis: 0,
        end_millis: 1_000,
        max_segments: 1,
    };
    let error = provider
        .transcribe_governed(&request, &QueryControl::new(None))
        .unwrap_err();
    assert_eq!(
        format!("{error}"),
        "whisper subprocess execution failed: TimedOut"
    );
    assert!(fs::read_dir(&staging).unwrap().next().is_none());
    let _ = fs::remove_dir_all(staging);
}
