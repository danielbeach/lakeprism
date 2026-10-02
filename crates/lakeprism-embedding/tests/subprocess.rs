#![cfg(feature = "fixture-provider")]

use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc, time::Duration};

use lakeprism_core::{FeatureLineage, SourceIdentity};
use lakeprism_embedding::{
    EmbeddingExecutionKind, EmbeddingSubprocessConfig, EmbeddingSubprocessProvider,
};
use lakeprism_index::EmbeddingRequest;
use lakeprism_storage::{ExecutionGovernor, ExecutionGovernorConfig, QueryControl};

fn lineage() -> FeatureLineage {
    FeatureLineage {
        source: SourceIdentity {
            media_uri: "file:///fixture/document.txt".into(),
            source_version: "fixture-v1".into(),
        },
        operator_version: "embedding-subprocess-v1".into(),
        model: Some("fixture-model".into()),
        model_version: Some("protocol-1".into()),
        parameters: BTreeMap::from([("normalize".into(), "true".into())]),
    }
}

fn provider(
    staging: PathBuf,
    timeout: Duration,
    extra_arguments: Vec<String>,
) -> EmbeddingSubprocessProvider {
    let root = std::env::current_dir().unwrap();
    let mut arguments = vec![
        "--input".into(),
        "{input}".into(),
        "--output".into(),
        "{output}".into(),
        "--model".into(),
        "{model}".into(),
    ];
    arguments.extend(extra_arguments);
    EmbeddingSubprocessProvider::new(
        EmbeddingSubprocessConfig {
            executable: PathBuf::from(env!("CARGO_BIN_EXE_lakeprism-embedding-fixture")),
            arguments,
            model_artifact: root.join("Cargo.toml"),
            staging_directory: staging,
            max_batch_items: 4,
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
            timeout,
            operator_version: "embedding-subprocess-v1".into(),
            model: "fixture-model".into(),
            model_version: "protocol-1".into(),
            parameters: BTreeMap::from([("normalize".into(), "true".into())]),
        },
        Arc::new(ExecutionGovernor::new(ExecutionGovernorConfig::default()).unwrap()),
    )
    .unwrap()
}

#[test]
fn fixture_protocol_preserves_batch_order_and_cleans_staging() {
    let root = std::env::current_dir().unwrap();
    let staging = root.join("target/lakeprism-embedding-fixture-staging");
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging).unwrap();
    let provider = provider(staging.clone(), Duration::from_secs(2), vec![]);
    let lineage = lineage();
    let rows = provider
        .embed_batch_governed(
            &[
                EmbeddingRequest {
                    text: "alpha".into(),
                    lineage: lineage.clone(),
                },
                EmbeddingRequest {
                    text: "beta".into(),
                    lineage,
                },
            ],
            &QueryControl::new(None),
        )
        .unwrap();
    assert_eq!(rows, vec![vec![5.0, 1.0], vec![4.0, 1.0]]);
    assert!(fs::read_dir(&staging).unwrap().next().is_none());
    let _ = fs::remove_dir_all(staging);
}

#[test]
fn timeout_kills_fixture_child_and_cleans_staging() {
    let root = std::env::current_dir().unwrap();
    let staging = root.join("target/lakeprism-embedding-timeout-staging");
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging).unwrap();
    let provider = provider(
        staging.clone(),
        Duration::from_millis(25),
        vec!["--sleep-millis".into(), "500".into()],
    );
    let error = provider
        .embed_batch_governed(
            &[EmbeddingRequest {
                text: "alpha".into(),
                lineage: lineage(),
            }],
            &QueryControl::new(None),
        )
        .unwrap_err();
    assert_eq!(error.kind, EmbeddingExecutionKind::TimedOut);
    assert!(fs::read_dir(&staging).unwrap().next().is_none());
    let _ = fs::remove_dir_all(staging);
}

#[test]
fn cancellation_is_reported_without_creating_a_run_directory() {
    let root = std::env::current_dir().unwrap();
    let staging = root.join("target/lakeprism-embedding-cancel-staging");
    let _ = fs::remove_dir_all(&staging);
    fs::create_dir_all(&staging).unwrap();
    let provider = provider(staging.clone(), Duration::from_secs(2), vec![]);
    let control = QueryControl::new(None);
    control.cancel();
    let error = provider
        .embed_batch_governed(
            &[EmbeddingRequest {
                text: "alpha".into(),
                lineage: lineage(),
            }],
            &control,
        )
        .unwrap_err();
    assert_eq!(error.kind, EmbeddingExecutionKind::Cancelled);
    assert!(fs::read_dir(&staging).unwrap().next().is_none());
    let _ = fs::remove_dir_all(staging);
}
