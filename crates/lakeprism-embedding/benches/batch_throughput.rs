//! Simple protocol benchmark scaffold. Supply a real configured provider in an
//! application benchmark; this fixture invocation measures only subprocess
//! protocol overhead and is intentionally not model-performance evidence.
use std::{collections::BTreeMap, sync::Arc, time::Instant};

use lakeprism_core::{FeatureLineage, SourceIdentity};
use lakeprism_embedding::{EmbeddingSubprocessConfig, EmbeddingSubprocessProvider};
use lakeprism_index::EmbeddingRequest;
use lakeprism_storage::{ExecutionGovernor, ExecutionGovernorConfig, QueryControl};

fn main() {
    let root = std::env::current_dir().unwrap();
    let staging = root.join("target/lakeprism-embedding-benchmark-staging");
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging).unwrap();
    let provider = EmbeddingSubprocessProvider::new(
        EmbeddingSubprocessConfig {
            executable: std::path::PathBuf::from(env!("CARGO_BIN_EXE_lakeprism-embedding-fixture")),
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
            max_batch_items: 32,
            max_input_bytes: 1 << 20,
            max_output_bytes: 1 << 20,
            timeout: std::time::Duration::from_secs(2),
            operator_version: "embedding-subprocess-v1".into(),
            model: "fixture-model".into(),
            model_version: "protocol-1".into(),
            parameters: BTreeMap::new(),
        },
        Arc::new(ExecutionGovernor::new(ExecutionGovernorConfig::default()).unwrap()),
    )
    .unwrap();
    let lineage = FeatureLineage {
        source: SourceIdentity {
            media_uri: "file:///benchmark.txt".into(),
            source_version: "v1".into(),
        },
        operator_version: "embedding-subprocess-v1".into(),
        model: Some("fixture-model".into()),
        model_version: Some("protocol-1".into()),
        parameters: BTreeMap::new(),
    };
    let requests = (0..32)
        .map(|index| EmbeddingRequest {
            text: format!("benchmark item {index}"),
            lineage: lineage.clone(),
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let rows = provider
        .embed_batch_governed(&requests, &QueryControl::new(None))
        .unwrap();
    let elapsed = started.elapsed();
    println!(
        "lakeprism_embedding_benchmark batch_items={} elapsed_nanos={} items_per_second={:.2}",
        rows.len(),
        elapsed.as_nanos(),
        rows.len() as f64 / elapsed.as_secs_f64()
    );
    let _ = std::fs::remove_dir_all(staging);
}
