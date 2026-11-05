//! Local recording snapshots. Inference and publication are serialized by an
//! advisory writer lock; readers pin immutable files from one atomic manifest.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use fs2::FileExt;
use lakeprism_core::{FeatureLineage, MediaRef, StorageMode};
use lakeprism_index::{TranscriptSegment, TranscriptionProvider, TranscriptionRequest};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const MAX_SEGMENTS: usize = 8_192;

#[derive(Debug, Error)]
pub enum RecordingError {
    #[error(transparent)]
    Derived(#[from] crate::DerivedError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Provider(#[from] lakeprism_index::IndexError),
    #[error(transparent)]
    Query(#[from] datafusion::error::DataFusionError),
    #[error("unsupported or invalid recording dataset manifest")]
    InvalidManifest,
    #[error("recording dataset path must not be a symbolic link")]
    UnsafePath,
    #[error("another recording dataset writer is active")]
    WriterBusy,
    #[error("recording source must be a regular local file")]
    InvalidSource,
    #[error("recording changed during transcription; previous snapshot retained")]
    SourceChanged,
    #[error("transcription returned invalid timestamps, text, count, or lineage")]
    InvalidTranscript,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Snapshot {
    file: String,
    lineage: FeatureLineage,
    segments: usize,
}

#[derive(Deserialize, Serialize)]
struct Manifest {
    format_version: u32,
    recordings: BTreeMap<String, Snapshot>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            recordings: BTreeMap::new(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct IngestResult {
    pub reused: bool,
    pub segments: usize,
}

pub struct RecordingDataset {
    root: PathBuf,
}

impl RecordingDataset {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, RecordingError> {
        fs::create_dir_all(root.as_ref())?;
        let dataset = Self {
            root: fs::canonicalize(root)?,
        };
        dataset.load()?;
        Ok(dataset)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Commits one recording, never a partial transcript. Earlier recordings
    /// remain committed when a later recording or a refresh fails.
    pub fn ingest(
        &self,
        source: &Path,
        mut lineage: FeatureLineage,
        provider: &dyn TranscriptionProvider,
    ) -> Result<IngestResult, RecordingError> {
        let lock_path = self.root.join("writer.lock");
        reject_symlink(&lock_path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                RecordingError::WriterBusy
            } else {
                RecordingError::Io(error)
            }
        })?;
        let mut manifest = self.load()?;
        let source = fs::canonicalize(source)?;
        if !source.is_file() {
            return Err(RecordingError::InvalidSource);
        }
        let uri = url::Url::from_file_path(&source)
            .map_err(|_| RecordingError::InvalidSource)?
            .to_string();
        lineage.source.media_uri = uri.clone();
        lineage.source.source_version = fingerprint(&source)?;
        crate::reject_credential_like_json(&serde_json::to_value(&lineage)?)?;
        if let Some(snapshot) = manifest.recordings.get(&uri)
            && snapshot.lineage == lineage
        {
            return Ok(IngestResult {
                reused: true,
                segments: snapshot.segments,
            });
        }
        let media = MediaRef::new(uri.clone(), "audio", StorageMode::External)
            .map_err(|_| RecordingError::InvalidSource)?;
        let rows = provider.transcribe(&TranscriptionRequest {
            media,
            lineage: lineage.clone(),
            start_millis: 0,
            end_millis: u64::MAX,
            max_segments: MAX_SEGMENTS,
        })?;
        if rows.len() > MAX_SEGMENTS
            || rows.iter().any(|row| {
                row.media_id != uri
                    || row.lineage != lineage
                    || row.end_millis <= row.start_millis
                    || row.text.trim().is_empty()
                    || row.confidence_millis.is_some_and(|value| value > 1_000)
            })
            || rows
                .windows(2)
                .any(|pair| pair[1].start_millis < pair[0].start_millis)
        {
            return Err(RecordingError::InvalidTranscript);
        }
        if fingerprint(&source)? != lineage.source.source_version {
            return Err(RecordingError::SourceChanged);
        }
        let file_name = format!("{}.parquet", Uuid::new_v4());
        let snapshot_path = self.root.join(&file_name);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&snapshot_path)?;
        let sync_file = file.try_clone()?;
        let batch = transcript_batch(&rows)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        sync_file.sync_all()?;
        manifest.recordings.insert(
            uri,
            Snapshot {
                file: file_name,
                lineage,
                segments: rows.len(),
            },
        );
        self.publish(&manifest)?;
        Ok(IngestResult {
            reused: false,
            segments: rows.len(),
        })
    }

    /// Pins the manifest's current files. Orphaned and replaced snapshots are
    /// intentionally retained so existing lazy plans cannot lose their inputs.
    pub async fn register(
        &self,
        session: &lakeprism_datafusion::MediaSession,
        table: &str,
    ) -> Result<(), RecordingError> {
        let manifest = self.load()?;
        if manifest.recordings.is_empty() {
            session.register_table_provider(
                table,
                Arc::new(MemTable::try_new(transcript_schema(), vec![vec![]])?),
            )?;
        } else {
            let paths = manifest
                .recordings
                .values()
                .map(|snapshot| {
                    self.root
                        .join(&snapshot.file)
                        .to_string_lossy()
                        .into_owned()
                })
                .collect::<Vec<_>>();
            let context = SessionContext::new();
            let provider = context
                .read_parquet(paths, ParquetReadOptions::default())
                .await?
                .into_view();
            session.register_table_provider(table, provider)?;
        }
        Ok(())
    }

    fn load(&self) -> Result<Manifest, RecordingError> {
        let path = self.root.join("manifest.json");
        reject_symlink(&path)?;
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Manifest::default());
            }
            Err(error) => return Err(error.into()),
        };
        let manifest: Manifest = serde_json::from_reader(file)?;
        if manifest.format_version != FORMAT_VERSION {
            return Err(RecordingError::InvalidManifest);
        }
        for (uri, snapshot) in &manifest.recordings {
            let stem = snapshot
                .file
                .strip_suffix(".parquet")
                .ok_or(RecordingError::InvalidManifest)?;
            if Uuid::parse_str(stem).is_err()
                || snapshot.segments > MAX_SEGMENTS
                || snapshot.lineage.source.media_uri != *uri
            {
                return Err(RecordingError::InvalidManifest);
            }
            crate::reject_credential_like_json(&serde_json::to_value(&snapshot.lineage)?)?;
            let path = self.root.join(&snapshot.file);
            reject_symlink(&path)?;
            if !path.is_file() {
                return Err(RecordingError::InvalidManifest);
            }
        }
        Ok(manifest)
    }

    fn publish(&self, manifest: &Manifest) -> Result<(), RecordingError> {
        let temporary = self.root.join(format!(".manifest-{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(manifest)?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(temporary, self.root.join("manifest.json"))?;
        #[cfg(unix)]
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

fn reject_symlink(path: &Path) -> Result<(), RecordingError> {
    if path.is_symlink() {
        return Err(RecordingError::UnsafePath);
    }
    Ok(())
}

fn fingerprint(path: &Path) -> Result<String, RecordingError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

pub fn transcript_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("start_millis", DataType::UInt64, false),
        Field::new("end_millis", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("confidence_millis", DataType::UInt16, true),
        Field::new("operator_version", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, true),
        Field::new("model_version", DataType::Utf8, true),
        Field::new("parameters_json", DataType::Utf8, false),
    ]))
}

fn transcript_batch(rows: &[TranscriptSegment]) -> Result<RecordBatch, RecordingError> {
    let text_column = |values: Vec<String>| Arc::new(StringArray::from(values)) as ArrayRef;
    Ok(RecordBatch::try_new(
        transcript_schema(),
        vec![
            text_column(rows.iter().map(|r| r.media_id.clone()).collect()),
            text_column(
                rows.iter()
                    .map(|r| r.lineage.source.source_version.clone())
                    .collect(),
            ),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.start_millis),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.end_millis),
            )),
            text_column(rows.iter().map(|r| r.text.clone()).collect()),
            Arc::new(UInt16Array::from(
                rows.iter().map(|r| r.confidence_millis).collect::<Vec<_>>(),
            )),
            text_column(
                rows.iter()
                    .map(|r| r.lineage.operator_version.clone())
                    .collect(),
            ),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.lineage.model.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.lineage.model_version.as_deref())
                    .collect::<Vec<_>>(),
            )),
            text_column(
                rows.iter()
                    .map(|r| serde_json::to_string(&r.lineage.parameters))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakeprism_core::SourceIdentity;
    use lakeprism_index::IndexError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FixtureProvider<'a>(&'a AtomicUsize);

    impl TranscriptionProvider for FixtureProvider<'_> {
        fn transcribe(
            &self,
            request: &TranscriptionRequest,
        ) -> Result<Vec<TranscriptSegment>, IndexError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            if request.lineage.operator_version == "fail" {
                return Err(IndexError::ProviderUnavailable {
                    capability: "fixture",
                });
            }
            if request.lineage.operator_version == "silence" {
                return Ok(Vec::new());
            }
            Ok(vec![TranscriptSegment {
                media_id: request.media.uri.clone(),
                start_millis: 10,
                end_millis: 20,
                text: request.lineage.operator_version.clone(),
                confidence_millis: None,
                lineage: request.lineage.clone(),
                created_at_unix_millis: 0,
            }])
        }
    }

    fn lineage(operator: &str) -> FeatureLineage {
        FeatureLineage {
            source: SourceIdentity {
                media_uri: String::new(),
                source_version: String::new(),
            },
            operator_version: operator.to_owned(),
            model: Some("fixture".to_owned()),
            model_version: Some("1".to_owned()),
            parameters: BTreeMap::new(),
        }
    }

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("lakeprism-recording-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            fs::write(root.join("source.wav"), b"fixture audio").unwrap();
            Self(root)
        }

        fn dataset(&self) -> RecordingDataset {
            RecordingDataset::open(self.0.join("dataset")).unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn reuse_requires_content_model_operator_and_parameters() {
        let fixture = Fixture::new();
        let calls = AtomicUsize::new(0);
        let provider = FixtureProvider(&calls);
        let source = fixture.0.join("source.wav");
        let dataset = fixture.dataset();
        assert!(
            !dataset
                .ingest(&source, lineage("v1"), &provider)
                .unwrap()
                .reused
        );
        assert!(
            fixture
                .dataset()
                .ingest(&source, lineage("v1"), &provider)
                .unwrap()
                .reused
        );
        fs::write(&source, b"changed content").unwrap();
        assert!(
            !dataset
                .ingest(&source, lineage("v1"), &provider)
                .unwrap()
                .reused
        );
        let mut next = lineage("v2");
        dataset.ingest(&source, next.clone(), &provider).unwrap();
        next.model_version = Some("2".to_owned());
        dataset.ingest(&source, next.clone(), &provider).unwrap();
        next.parameters
            .insert("beam_size".to_owned(), "2".to_owned());
        dataset.ingest(&source, next, &provider).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn failed_refresh_preserves_publication_and_old_lazy_readers() {
        let fixture = Fixture::new();
        let calls = AtomicUsize::new(0);
        let provider = FixtureProvider(&calls);
        let source = fixture.0.join("source.wav");
        let dataset = fixture.dataset();
        dataset.ingest(&source, lineage("v1"), &provider).unwrap();
        let old = lakeprism_datafusion::MediaSession::new();
        dataset.register(&old, "transcripts").await.unwrap();
        let manifest_before = fs::read(dataset.root.join("manifest.json")).unwrap();
        assert!(dataset.ingest(&source, lineage("fail"), &provider).is_err());
        assert_eq!(
            fs::read(dataset.root.join("manifest.json")).unwrap(),
            manifest_before
        );
        dataset.ingest(&source, lineage("v2"), &provider).unwrap();
        let old_rows = old
            .sql("SELECT text FROM transcripts WHERE text = 'v1'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(old_rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let current = lakeprism_datafusion::MediaSession::new();
        fixture
            .dataset()
            .register(&current, "transcripts")
            .await
            .unwrap();
        let new_rows = current
            .sql("SELECT text FROM transcripts WHERE text = 'v2'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(new_rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            dataset
                .ingest(&source, lineage("silence"), &provider)
                .unwrap()
                .segments,
            0
        );
        let silent = lakeprism_datafusion::MediaSession::new();
        fixture
            .dataset()
            .register(&silent, "transcripts")
            .await
            .unwrap();
        let rows = silent
            .sql("SELECT * FROM transcripts")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[test]
    fn invalid_manifests_and_concurrent_writers_fail_explicitly() {
        let fixture = Fixture::new();
        let dataset = fixture.dataset();
        let lock = File::create(dataset.root.join("writer.lock")).unwrap();
        lock.lock_exclusive().unwrap();
        let calls = AtomicUsize::new(0);
        assert!(matches!(
            dataset.ingest(
                &fixture.0.join("source.wav"),
                lineage("v1"),
                &FixtureProvider(&calls)
            ),
            Err(RecordingError::WriterBusy)
        ));
        FileExt::unlock(&lock).unwrap();
        fs::write(
            dataset.root.join("manifest.json"),
            r#"{"format_version":999,"recordings":{}}"#,
        )
        .unwrap();
        assert!(matches!(
            fixture.dataset_result(),
            Err(RecordingError::InvalidManifest)
        ));
    }

    impl Fixture {
        fn dataset_result(&self) -> Result<RecordingDataset, RecordingError> {
            RecordingDataset::open(self.0.join("dataset"))
        }
    }
}
