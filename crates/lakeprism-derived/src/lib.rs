//! Governed, portable persistence for derived features.
//!
//! Parquet is the always-available persistence contract. Delta is deliberately
//! read-only here: when enabled it delegates snapshot registration to delta-rs
//! instead of attempting to emulate Delta transaction semantics.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use lakeprism_arrow::{media_ref_data_type, media_ref_record_batch};
use lakeprism_core::{FeatureLineage, MediaRef, SourceIdentity};
use lakeprism_index::EmbeddingRecord;
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DerivedFeature {
    pub media: MediaRef,
    pub lineage: FeatureLineage,
    /// A stable, application-defined feature family (for example `transcript`).
    pub feature_kind: String,
    /// Canonical JSON feature payload. Credentials are never permitted here.
    pub payload_json: String,
}

impl DerivedFeature {
    pub fn new(
        media: MediaRef,
        lineage: FeatureLineage,
        feature_kind: impl Into<String>,
        payload_json: impl Into<String>,
    ) -> Result<Self, DerivedError> {
        let value: serde_json::Value = serde_json::from_str(&payload_json.into())?;
        let feature_kind = feature_kind.into();
        if feature_kind.trim().is_empty() {
            return Err(DerivedError::EmptyFeatureKind);
        }
        if lineage.source.media_uri != media.uri {
            return Err(DerivedError::SourceUriMismatch);
        }
        reject_credential_like_json(&value)?;
        Ok(Self {
            media,
            lineage,
            feature_kind,
            payload_json: serde_json::to_string(&value)?,
        })
    }

    pub fn source_identity(&self) -> &SourceIdentity {
        &self.lineage.source
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RefreshManifest {
    /// The exact source identity last emitted for each credential-free media URI.
    pub sources: BTreeMap<String, String>,
}

impl RefreshManifest {
    pub fn changed_sources<'a>(&self, features: &'a [DerivedFeature]) -> Vec<&'a DerivedFeature> {
        features
            .iter()
            .filter(|feature| {
                self.sources.get(&feature.lineage.source.media_uri)
                    != Some(&feature.lineage.source.source_version)
            })
            .collect()
    }

    fn record(&mut self, features: &[&DerivedFeature]) {
        for feature in features {
            self.sources.insert(
                feature.lineage.source.media_uri.clone(),
                feature.lineage.source.source_version.clone(),
            );
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshResult {
    pub emitted_rows: usize,
    pub changed_sources: usize,
}

/// A portable persisted embedding-index row. `values_json` is canonical JSON
/// rather than an Arrow list so the table can be inspected by generic SQL
/// clients without a custom vector extension. Ranking is performed only after
/// an application validates the complete embedding lineage and dimensions.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DerivedIndexRow {
    pub id: String,
    pub text: String,
    pub values_json: String,
    pub lineage: FeatureLineage,
}

impl TryFrom<EmbeddingRecord> for DerivedIndexRow {
    type Error = DerivedError;

    fn try_from(row: EmbeddingRecord) -> Result<Self, Self::Error> {
        if row.id.trim().is_empty() {
            return Err(DerivedError::EmptyIndexId);
        }
        Ok(Self {
            id: row.id,
            text: row.text,
            values_json: serde_json::to_string(&row.values)?,
            lineage: row.lineage,
        })
    }
}

#[derive(Debug, Error)]
pub enum DerivedError {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("feature kind must not be empty")]
    EmptyFeatureKind,
    #[error("derived index row ID must not be empty")]
    EmptyIndexId,
    #[error("feature lineage source URI must equal the portable MediaRef URI")]
    SourceUriMismatch,
    #[error("derived feature payload must not contain credential-like field {0}")]
    CredentialLikePayload(String),
}

/// Writes one immutable Parquet snapshot and advances the manifest only after a
/// successful close. The caller owns snapshot retention and atomic publication.
pub fn refresh_parquet(
    snapshot_path: impl AsRef<Path>,
    manifest_path: impl AsRef<Path>,
    features: &[DerivedFeature],
) -> Result<RefreshResult, DerivedError> {
    let manifest_path = manifest_path.as_ref();
    let mut manifest = load_manifest(manifest_path)?;
    let changed = manifest.changed_sources(features);
    let changed_sources = changed
        .iter()
        .map(|feature| feature.lineage.source.media_uri.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    write_parquet(snapshot_path, &changed)?;
    manifest.record(&changed);
    save_manifest(manifest_path, &manifest)?;
    Ok(RefreshResult {
        emitted_rows: changed.len(),
        changed_sources,
    })
}

/// Incrementally writes a credential-free embedding-index Parquet snapshot.
/// Source-version changes are tracked with the same manifest rules as other
/// derived features; unchanged sources are omitted from the next immutable
/// snapshot. This function performs no vector inference.
pub fn refresh_embedding_index_parquet(
    snapshot_path: impl AsRef<Path>,
    manifest_path: impl AsRef<Path>,
    rows: &[DerivedIndexRow],
) -> Result<RefreshResult, DerivedError> {
    let manifest_path = manifest_path.as_ref();
    let mut manifest = load_manifest(manifest_path)?;
    let changed = rows
        .iter()
        .filter(|row| {
            manifest.sources.get(&row.lineage.source.media_uri)
                != Some(&row.lineage.source.source_version)
        })
        .collect::<Vec<_>>();
    let changed_sources = changed
        .iter()
        .map(|row| row.lineage.source.media_uri.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    write_embedding_index_parquet(snapshot_path, &changed)?;
    for row in &changed {
        manifest.sources.insert(
            row.lineage.source.media_uri.clone(),
            row.lineage.source.source_version.clone(),
        );
    }
    save_manifest(manifest_path, &manifest)?;
    Ok(RefreshResult {
        emitted_rows: changed.len(),
        changed_sources,
    })
}

/// Registers an immutable Parquet snapshot as a lazy DataFusion table.
pub async fn register_parquet_snapshot(
    session: &lakeprism_datafusion::MediaSession,
    table_name: &str,
    snapshot_path: impl AsRef<Path>,
) -> datafusion::error::Result<()> {
    session
        .register_parquet(table_name, &snapshot_path.as_ref().to_string_lossy())
        .await
}

/// Registers a specific Delta table version using delta-rs' native provider.
/// There is intentionally no Delta write implementation in this crate.
#[cfg(feature = "delta-rs")]
pub async fn register_delta_snapshot(
    session: &lakeprism_datafusion::MediaSession,
    table_name: &str,
    table_uri: impl AsRef<str>,
    version: u64,
) -> Result<(), lakeprism_delta::DeltaAdapterError> {
    lakeprism_delta::register_delta_table_version(session, table_name, table_uri, version).await
}

pub fn derived_feature_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("media", media_ref_data_type(), false),
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("feature_kind", DataType::Utf8, false),
        Field::new("payload_json", DataType::Utf8, false),
        Field::new("operator_version", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, true),
        Field::new("model_version", DataType::Utf8, true),
        Field::new("parameters_json", DataType::Utf8, false),
    ]))
}

fn write_parquet(path: impl AsRef<Path>, features: &[&DerivedFeature]) -> Result<(), DerivedError> {
    let batch = derived_feature_batch(features)?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn embedding_index_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("values_json", DataType::Utf8, false),
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("operator_version", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, true),
        Field::new("model_version", DataType::Utf8, true),
        Field::new("parameters_json", DataType::Utf8, false),
    ]))
}

fn write_embedding_index_parquet(
    path: impl AsRef<Path>,
    rows: &[&DerivedIndexRow],
) -> Result<(), DerivedError> {
    let parameters = rows
        .iter()
        .map(|row| serde_json::to_string(&row.lineage.parameters))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(
        embedding_index_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.text.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.values_json.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.lineage.source.media_uri.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|r| r.lineage.source.source_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.lineage.operator_version.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.lineage.model.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                rows.iter().map(|r| r.lineage.model_version.as_deref()),
            )),
            Arc::new(StringArray::from_iter_values(parameters)),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn derived_feature_batch(features: &[&DerivedFeature]) -> Result<RecordBatch, DerivedError> {
    let media = features
        .iter()
        .map(|feature| feature.media.clone())
        .collect::<Vec<_>>();
    let media_batch = media_ref_record_batch(&media)?;
    let media = media_batch.column(0).clone();
    let parameters = features
        .iter()
        .map(|feature| serde_json::to_string(&feature.lineage.parameters))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(
        derived_feature_schema(),
        vec![
            media,
            Arc::new(StringArray::from_iter_values(
                features.iter().map(|f| f.lineage.source.media_uri.as_str()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                features
                    .iter()
                    .map(|f| f.lineage.source.source_version.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                features.iter().map(|f| f.feature_kind.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                features.iter().map(|f| f.payload_json.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                features.iter().map(|f| f.lineage.operator_version.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                features.iter().map(|f| f.lineage.model.as_deref()),
            )),
            Arc::new(StringArray::from_iter(
                features.iter().map(|f| f.lineage.model_version.as_deref()),
            )),
            Arc::new(StringArray::from_iter_values(parameters)),
        ],
    )
    .map_err(DerivedError::from)
}

fn load_manifest(path: &Path) -> Result<RefreshManifest, DerivedError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(RefreshManifest::default())
        }
        Err(error) => Err(error.into()),
    }
}

fn save_manifest(path: &Path, manifest: &RefreshManifest) -> Result<(), DerivedError> {
    let bytes = serde_json::to_vec_pretty(manifest)?;
    std::fs::write(path, bytes)?;
    Ok(())
}

fn reject_credential_like_json(value: &serde_json::Value) -> Result<(), DerivedError> {
    match value {
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                let normalized = key.to_ascii_lowercase();
                if normalized.contains("token")
                    || normalized.contains("secret")
                    || normalized == "signature"
                    || normalized == "sig"
                {
                    return Err(DerivedError::CredentialLikePayload(key.clone()));
                }
                reject_credential_like_json(value)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                reject_credential_like_json(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use lakeprism_core::StorageMode;

    fn feature(version: &str) -> DerivedFeature {
        let media =
            MediaRef::new("file:///media/clip.mp4", "video", StorageMode::External).unwrap();
        DerivedFeature::new(
            media,
            FeatureLineage {
                source: SourceIdentity {
                    media_uri: "file:///media/clip.mp4".into(),
                    source_version: version.into(),
                },
                operator_version: "transcribe-v1".into(),
                model: Some("local".into()),
                model_version: Some("1".into()),
                parameters: BTreeMap::from([("language".into(), "en".into())]),
            },
            "transcript",
            r#"{"text":"hello local SQL"}"#,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn refresh_is_incremental_and_persists_portable_lineage() {
        let root = std::env::current_dir()
            .unwrap()
            .join(format!("lakeprism-derived-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let snapshot = root.join("features.parquet");
        let manifest = root.join("manifest.json");
        assert_eq!(
            refresh_parquet(&snapshot, &manifest, &[feature("v1")])
                .unwrap()
                .emitted_rows,
            1
        );
        assert_eq!(
            refresh_parquet(&snapshot, &manifest, &[feature("v1")])
                .unwrap()
                .emitted_rows,
            0
        );
        assert_eq!(
            refresh_parquet(&snapshot, &manifest, &[feature("v2")])
                .unwrap()
                .changed_sources,
            1
        );
        let session = lakeprism_datafusion::MediaSession::new();
        register_parquet_snapshot(&session, "derived", &snapshot)
            .await
            .unwrap();
        let batches = session
            .collect("SELECT media.uri, source_version, feature_kind FROM derived")
            .await
            .unwrap();
        let uri = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(uri.value(0), "file:///media/clip.mp4");
        assert_eq!(batches[0].num_columns(), 3);
        let accessor = session
            .collect("SELECT lakeprism_media_type(media) FROM derived")
            .await
            .unwrap();
        let media_type = accessor[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(media_type.value(0), "video");

        let explain = session
            .collect(
                "EXPLAIN VERBOSE SELECT media.uri FROM derived \
                 WHERE source_version = 'v2' LIMIT 1",
            )
            .await
            .unwrap();
        let plans = explain
            .iter()
            .flat_map(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(plans.contains("DataSourceExec"));
        assert!(plans.contains("file_type=parquet"));
        assert!(plans.contains("predicate=source_version"));
        assert!(plans.contains("projection=[get_field(media"));
        assert!(plans.contains("fetch=1"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_credentials_in_derived_payloads() {
        let media = MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap();
        let error = DerivedFeature::new(
            media,
            FeatureLineage {
                source: SourceIdentity {
                    media_uri: "file:///media/a.mp4".into(),
                    source_version: "1".into(),
                },
                operator_version: "x".into(),
                model: None,
                model_version: None,
                parameters: BTreeMap::new(),
            },
            "x",
            r#"{"token":"no"}"#,
        )
        .unwrap_err();
        assert!(matches!(error, DerivedError::CredentialLikePayload(_)));
    }

    #[test]
    fn embedding_index_rows_are_incremental_and_preserve_lineage() {
        let root = std::env::current_dir().unwrap().join(format!(
            "lakeprism-derived-index-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let snapshot = root.join("index.parquet");
        let manifest = root.join("index-manifest.json");
        let row = DerivedIndexRow::try_from(EmbeddingRecord {
            id: "segment-1".into(),
            text: "lake prism".into(),
            values: vec![0.0, 1.0],
            lineage: feature("v1").lineage,
        })
        .unwrap();
        assert_eq!(
            refresh_embedding_index_parquet(&snapshot, &manifest, std::slice::from_ref(&row))
                .unwrap()
                .emitted_rows,
            1
        );
        assert_eq!(
            refresh_embedding_index_parquet(&snapshot, &manifest, std::slice::from_ref(&row))
                .unwrap()
                .emitted_rows,
            0
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
