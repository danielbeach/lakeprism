//! Local recording snapshots. Inference and publication are serialized by an
//! advisory writer lock; readers pin immutable files from one atomic manifest.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use fs2::FileExt;
use lakeprism_core::{FeatureLineage, MediaRef, StorageMode};
use lakeprism_index::{TranscriptSegment, TranscriptionProvider, TranscriptionRequest};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const MAX_SEGMENTS: usize = 8_192;
const MAX_RECORDINGS: usize = 100_000;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationStage {
    SnapshotWrite,
    SnapshotSync,
    BackupWrite,
    BackupSync,
    BackupRename,
    ManifestWrite,
    ManifestSync,
    ManifestRename,
    DirectorySync,
}

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
    #[error(
        "recording dataset format version {0} is not supported; recovery will not downgrade it"
    )]
    UnsupportedVersion(u32),
    #[error("recording dataset manifest is missing; explicit recovery is required")]
    MissingManifest,
    #[error("recording dataset exceeds 100000 recordings or the 64 MiB manifest bound")]
    DatasetLimit,
    #[error(
        "snapshot published, but directory durability could not be confirmed; reopen before retrying"
    )]
    DurabilityUncertain(#[source] std::io::Error),
    #[error("recording dataset path must not be a symbolic link")]
    UnsafePath,
    #[error("another recording dataset writer is active")]
    WriterBusy,
    #[error(
        "recording readers are active; drop all dataset sessions, plans, and streams before vacuuming"
    )]
    ReadersBusy,
    #[error("recording source must be a regular local file outside the dataset directory")]
    InvalidSource,
    #[error("recording changed during transcription; previous snapshot retained")]
    SourceChanged,
    #[error("transcription returned invalid timestamps, text, count, or lineage")]
    InvalidTranscript,
}

impl RecordingError {
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidManifest | Self::Json(_) => "invalid_manifest",
            Self::UnsupportedVersion(_) => "unsupported_manifest_version",
            Self::MissingManifest => "manifest_missing",
            Self::DatasetLimit => "dataset_limit",
            Self::DurabilityUncertain(_) => "durability_uncertain",
            Self::UnsafePath => "unsafe_path",
            Self::WriterBusy => "writer_busy",
            Self::ReadersBusy => "readers_busy",
            Self::InvalidSource => "invalid_source",
            Self::SourceChanged => "source_changed",
            Self::InvalidTranscript => "invalid_transcript",
            Self::Io(_) => "io",
            Self::Arrow(_) | Self::Parquet(_) => "snapshot_failed",
            Self::Provider(lakeprism_index::IndexError::ProviderExecution { kind, .. }) => kind,
            Self::Provider(_) => "provider_failed",
            Self::Query(_) => "query_failed",
            Self::Derived(_) => "dataset_failed",
        }
    }

    pub fn published(&self) -> bool {
        matches!(self, Self::DurabilityUncertain(_))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Snapshot {
    file: String,
    lineage: FeatureLineage,
    segments: usize,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format_version: u32,
    recordings: BTreeMap<String, Snapshot>,
}

#[derive(Deserialize)]
struct ManifestVersion {
    format_version: u32,
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

#[derive(Clone)]
pub struct RecordingDataset {
    root: PathBuf,
    #[cfg(test)]
    fault: Option<Arc<dyn Fn(PublicationStage) -> std::io::Result<()> + Send + Sync>>,
}

pub struct RecordingWriter {
    dataset: RecordingDataset,
    _lock: File,
    manifest: Manifest,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecoveryResult {
    pub restored: bool,
    pub recordings: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct VacuumResult {
    pub removed_files: usize,
    pub reclaimed_bytes: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordingInfo {
    pub source_uri: String,
    pub source_version: String,
    pub segments: usize,
}

impl RecordingDataset {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, RecordingError> {
        reject_symlink(root.as_ref())?;
        fs::create_dir_all(root.as_ref())?;
        let dataset = Self {
            root: fs::canonicalize(root)?,
            #[cfg(test)]
            fault: None,
        };
        let _lease = dataset.lock_readers(false)?;
        match dataset.load() {
            Ok(_) => {}
            Err(RecordingError::MissingManifest) => dataset.initialize()?,
            Err(error) => return Err(error),
        }
        Ok(dataset)
    }

    fn initialize(&self) -> Result<(), RecordingError> {
        let _lock = self.lock_writer()?;
        match self.load() {
            Ok(_) => Ok(()),
            Err(RecordingError::MissingManifest) => {
                if self.root.join("manifest.previous.json").exists() || self.has_snapshots()? {
                    return Err(RecordingError::MissingManifest);
                }
                self.publish(&Manifest::default(), None)
            }
            Err(error) => Err(error),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn recordings(&self) -> Result<Vec<RecordingInfo>, RecordingError> {
        let _lease = self.lock_readers(false)?;
        Ok(self
            .load()?
            .recordings
            .into_iter()
            .map(|(uri, snapshot)| RecordingInfo {
                source_uri: uri,
                source_version: snapshot.lineage.source.source_version,
                segments: snapshot.segments,
            })
            .collect())
    }

    /// Remove an exact canonical source URI. Immutable files remain until an
    /// explicit vacuum, and one previous committed manifest remains recoverable.
    pub fn remove(&self, uri: &str) -> Result<bool, RecordingError> {
        let parsed = url::Url::parse(uri).map_err(|_| RecordingError::InvalidSource)?;
        if parsed.scheme() != "file"
            || parsed.host_str().is_some_and(|host| host != "localhost")
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(RecordingError::InvalidSource);
        }
        self.reject_managed_source(uri)?;
        let mut writer = self.writer()?;
        let mut next = writer.manifest.clone();
        if next.recordings.remove(uri).is_none() {
            return Ok(false);
        }
        writer.commit(next)?;
        Ok(true)
    }

    /// Delete only LakePrism-owned snapshots and manifest temporaries that are
    /// referenced by neither the active nor previous manifest. Requires no readers.
    pub fn vacuum(&self) -> Result<VacuumResult, RecordingError> {
        let writer = self.writer()?;
        let _readers = self.lock_readers(true)?;
        for uri in writer.manifest.recordings.keys() {
            self.reject_managed_source(uri)?;
        }
        let mut retained = writer
            .manifest
            .recordings
            .values()
            .map(|snapshot| snapshot.file.clone())
            .collect::<BTreeSet<_>>();
        match self.read_manifest("manifest.previous.json") {
            Ok(previous) => {
                for uri in previous.recordings.keys() {
                    self.reject_managed_source(uri)?;
                }
                retained.extend(
                    previous
                        .recordings
                        .into_values()
                        .map(|snapshot| snapshot.file),
                );
            }
            Err(RecordingError::MissingManifest) => {}
            Err(error) => return Err(error),
        }
        let mut result = VacuumResult::default();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let temporary = name
                .strip_prefix(".manifest-")
                .and_then(|name| name.strip_suffix(".tmp"))
                .is_some_and(|stem| Uuid::parse_str(stem).is_ok());
            if !retained.contains(name) && (snapshot_file_name(name) || temporary) {
                reject_symlink(&entry.path())?;
                let metadata = entry.metadata()?;
                if !metadata.is_file() {
                    return Err(RecordingError::UnsafePath);
                }
                fs::remove_file(entry.path())?;
                result.removed_files += 1;
                result.reclaimed_bytes = result.reclaimed_bytes.saturating_add(metadata.len());
            }
        }
        sync_directory(&self.root)?;
        Ok(result)
    }

    fn lock_readers(&self, exclusive: bool) -> Result<File, RecordingError> {
        let path = self.root.join("readers.lock");
        reject_symlink(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let result = if exclusive {
            FileExt::try_lock_exclusive(&file)
        } else {
            FileExt::try_lock_shared(&file)
        };
        result.map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                RecordingError::ReadersBusy
            } else {
                RecordingError::Io(error)
            }
        })?;
        Ok(file)
    }

    fn reject_managed_source(&self, uri: &str) -> Result<(), RecordingError> {
        let root_uri =
            url::Url::from_directory_path(&self.root).map_err(|_| RecordingError::UnsafePath)?;
        if uri.starts_with(root_uri.as_str()) {
            let path = url::Url::parse(uri)
                .map_err(|_| RecordingError::InvalidManifest)?
                .to_file_path()
                .map_err(|_| RecordingError::InvalidManifest)?;
            if path.try_exists()? {
                return Err(RecordingError::UnsafePath);
            }
        }
        Ok(())
    }

    /// Commits one recording, never a partial transcript. Earlier recordings
    /// remain committed when a later recording or a refresh fails.
    pub fn ingest(
        &self,
        source: &Path,
        lineage: FeatureLineage,
        provider: &dyn TranscriptionProvider,
    ) -> Result<IngestResult, RecordingError> {
        self.writer()?.ingest(source, lineage, provider)
    }

    /// Holds the writer lock and validates the current manifest once for a
    /// batch. Each changed recording still publishes independently.
    pub fn writer(&self) -> Result<RecordingWriter, RecordingError> {
        Ok(RecordingWriter {
            dataset: self.clone(),
            _lock: self.lock_writer()?,
            manifest: self.load()?,
        })
    }

    fn lock_writer(&self) -> Result<File, RecordingError> {
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
        Ok(lock)
    }

    /// Restore the previous complete manifest only when the active manifest
    /// is missing or invalid. Never promote uncommitted orphan snapshots.
    pub fn recover(root: impl AsRef<Path>) -> Result<RecoveryResult, RecordingError> {
        reject_symlink(root.as_ref())?;
        let dataset = Self {
            root: fs::canonicalize(root)?,
            #[cfg(test)]
            fault: None,
        };
        let _lock = dataset.lock_writer()?;
        match dataset.load() {
            Ok(manifest) => Ok(RecoveryResult {
                restored: false,
                recordings: manifest.recordings.len(),
            }),
            Err(
                RecordingError::MissingManifest
                | RecordingError::InvalidManifest
                | RecordingError::Json(_),
            ) => {
                let previous = dataset.read_manifest("manifest.previous.json")?;
                dataset.publish(&previous, None)?;
                Ok(RecoveryResult {
                    restored: true,
                    recordings: previous.recordings.len(),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn has_snapshots(&self) -> Result<bool, RecordingError> {
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if snapshot_file_name(&entry.file_name().to_string_lossy()) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn checkpoint(&self, stage: PublicationStage) -> Result<(), RecordingError> {
        #[cfg(test)]
        if let Some(fault) = &self.fault {
            fault(stage)?;
        }
        #[cfg(not(test))]
        let _ = stage;
        Ok(())
    }
}

impl RecordingWriter {
    pub fn ingest(
        &mut self,
        source: &Path,
        mut lineage: FeatureLineage,
        provider: &dyn TranscriptionProvider,
    ) -> Result<IngestResult, RecordingError> {
        let source = fs::canonicalize(source)?;
        if !source.is_file() || source.starts_with(&self.dataset.root) {
            return Err(RecordingError::InvalidSource);
        }
        let uri = url::Url::from_file_path(&source)
            .map_err(|_| RecordingError::InvalidSource)?
            .to_string();
        lineage.source.media_uri = uri.clone();
        lineage.source.source_version = fingerprint(&source)?;
        crate::reject_credential_like_json(&serde_json::to_value(&lineage)?)?;
        if !self.manifest.recordings.contains_key(&uri)
            && self.manifest.recordings.len() >= MAX_RECORDINGS
        {
            return Err(RecordingError::DatasetLimit);
        }
        if let Some(snapshot) = self.manifest.recordings.get(&uri)
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
        let snapshot_path = self.dataset.root.join(&file_name);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&snapshot_path)?;
        let sync_file = file.try_clone()?;
        let batch = transcript_batch(&rows)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        self.dataset.checkpoint(PublicationStage::SnapshotWrite)?;
        writer.write(&batch)?;
        writer.close()?;
        self.dataset.checkpoint(PublicationStage::SnapshotSync)?;
        sync_file.sync_all()?;
        let mut next = self.manifest.clone();
        next.recordings.insert(
            uri,
            Snapshot {
                file: file_name,
                lineage,
                segments: rows.len(),
            },
        );
        self.commit(next)?;
        Ok(IngestResult {
            reused: false,
            segments: rows.len(),
        })
    }

    fn commit(&mut self, next: Manifest) -> Result<(), RecordingError> {
        let result = self.dataset.publish(&next, Some(&self.manifest));
        if result.is_ok() || matches!(result, Err(RecordingError::DurabilityUncertain(_))) {
            self.manifest = next;
        }
        result
    }
}

impl RecordingDataset {
    /// Pins the manifest's current files. Orphaned and replaced snapshots are
    /// intentionally retained so existing lazy plans cannot lose their inputs.
    pub async fn register(
        &self,
        session: &lakeprism_datafusion::MediaSession,
        table: &str,
    ) -> Result<(), RecordingError> {
        let lease = Arc::new(self.lock_readers(false)?);
        let manifest = self.load()?;
        if manifest.recordings.is_empty() {
            session.register_table_provider(
                table,
                Arc::new(crate::recording_reader::SnapshotTable::new(
                    Arc::new(MemTable::try_new(transcript_schema(), vec![vec![]])?),
                    lease,
                )),
            )?;
        } else {
            let paths = manifest
                .recordings
                .values()
                .map(|snapshot| {
                    url::Url::from_file_path(self.root.join(&snapshot.file))
                        .map(String::from)
                        .map_err(|_| RecordingError::UnsafePath)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let context = SessionContext::new_with_config(SessionConfig::new().set_bool(
                "datafusion.execution.parquet.schema_force_view_types",
                false,
            ));
            let provider = context
                .read_parquet(
                    paths,
                    ParquetReadOptions::default().schema(transcript_schema().as_ref()),
                )
                .await?
                .into_view();
            session.register_table_provider(
                table,
                Arc::new(crate::recording_reader::SnapshotTable::new(provider, lease)),
            )?;
        }
        Ok(())
    }

    fn load(&self) -> Result<Manifest, RecordingError> {
        self.read_manifest("manifest.json")
    }

    fn read_manifest(&self, name: &str) -> Result<Manifest, RecordingError> {
        let path = self.root.join(name);
        reject_symlink(&path)?;
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(RecordingError::MissingManifest);
            }
            Err(error) => return Err(error.into()),
        };
        if file.metadata()?.len() > MAX_MANIFEST_BYTES {
            return Err(RecordingError::InvalidManifest);
        }
        let mut bytes = Vec::new();
        file.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut bytes)?;
        let version: ManifestVersion = serde_json::from_slice(&bytes)?;
        if version.format_version != FORMAT_VERSION {
            return Err(RecordingError::UnsupportedVersion(version.format_version));
        }
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        if manifest.recordings.len() > MAX_RECORDINGS {
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
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
                .map_err(|_| RecordingError::InvalidManifest)?;
            if reader.schema().as_ref() != transcript_schema().as_ref()
                || reader.metadata().file_metadata().num_rows() != snapshot.segments as i64
            {
                return Err(RecordingError::InvalidManifest);
            }
        }
        Ok(manifest)
    }

    fn publish(
        &self,
        manifest: &Manifest,
        previous: Option<&Manifest>,
    ) -> Result<(), RecordingError> {
        if let Some(previous) = previous {
            self.write_manifest("manifest.previous.json", previous, true)?;
            sync_directory(&self.root)?;
        }
        self.write_manifest("manifest.json", manifest, false)?;
        self.checkpoint(PublicationStage::DirectorySync)
            .and_then(|()| sync_directory(&self.root))
            .map_err(|error| match error {
                RecordingError::Io(error) => RecordingError::DurabilityUncertain(error),
                other => other,
            })?;
        Ok(())
    }

    fn write_manifest(
        &self,
        name: &str,
        manifest: &Manifest,
        backup: bool,
    ) -> Result<(), RecordingError> {
        if manifest.recordings.len() > MAX_RECORDINGS {
            return Err(RecordingError::DatasetLimit);
        }
        let bytes = serde_json::to_vec(manifest)?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(RecordingError::DatasetLimit);
        }
        reject_symlink(&self.root.join(name))?;
        let temporary = self.root.join(format!(".manifest-{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        self.checkpoint(if backup {
            PublicationStage::BackupWrite
        } else {
            PublicationStage::ManifestWrite
        })?;
        file.write_all(&bytes)?;
        self.checkpoint(if backup {
            PublicationStage::BackupSync
        } else {
            PublicationStage::ManifestSync
        })?;
        file.sync_all()?;
        drop(file);
        self.checkpoint(if backup {
            PublicationStage::BackupRename
        } else {
            PublicationStage::ManifestRename
        })?;
        fs::rename(temporary, self.root.join(name))?;
        Ok(())
    }
}

fn snapshot_file_name(name: &str) -> bool {
    name.strip_suffix(".parquet")
        .is_some_and(|stem| Uuid::parse_str(stem).is_ok())
}

fn sync_directory(root: &Path) -> Result<(), RecordingError> {
    #[cfg(unix)]
    File::open(root)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = root;
    Ok(())
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
            RecordingDataset::open(self.0.join("dataset [literal]")).unwrap()
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
            Err(RecordingError::UnsupportedVersion(999))
        ));
    }

    const FAULT_STAGES: [PublicationStage; 9] = [
        PublicationStage::SnapshotWrite,
        PublicationStage::SnapshotSync,
        PublicationStage::BackupWrite,
        PublicationStage::BackupSync,
        PublicationStage::BackupRename,
        PublicationStage::ManifestWrite,
        PublicationStage::ManifestSync,
        PublicationStage::ManifestRename,
        PublicationStage::DirectorySync,
    ];

    #[tokio::test]
    async fn vacuum_respects_lazy_plans_physical_plans_and_detached_streams() {
        let fixture = Fixture::new();
        let calls = AtomicUsize::new(0);
        let dataset = fixture.dataset();
        let source = fixture.0.join("source.wav");
        dataset
            .ingest(&source, lineage("v1"), &FixtureProvider(&calls))
            .unwrap();
        let physical = {
            let session = lakeprism_datafusion::MediaSession::new();
            dataset.register(&session, "transcripts").await.unwrap();
            let dataframe = session.sql("SELECT text FROM transcripts").await.unwrap();
            assert!(matches!(dataset.vacuum(), Err(RecordingError::ReadersBusy)));
            dataframe.create_physical_plan().await.unwrap()
        };
        dataset
            .ingest(&source, lineage("v2"), &FixtureProvider(&calls))
            .unwrap();
        let uri = dataset.recordings().unwrap()[0].source_uri.clone();
        assert!(dataset.remove(&uri).unwrap());
        assert!(!dataset.remove(&uri).unwrap());
        assert!(dataset.recordings().unwrap().is_empty());
        assert!(matches!(dataset.vacuum(), Err(RecordingError::ReadersBusy)));
        let stream = physical
            .execute(0, Arc::new(datafusion::execution::TaskContext::default()))
            .unwrap();
        drop(physical);
        assert!(matches!(dataset.vacuum(), Err(RecordingError::ReadersBusy)));
        use futures::TryStreamExt;
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let text = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.value(0), "v1");
        let vacuum = dataset.vacuum().unwrap();
        assert_eq!(vacuum.removed_files, 1);
        assert!(vacuum.reclaimed_bytes > 0);
        let previous = dataset.read_manifest("manifest.previous.json").unwrap();
        assert_eq!(
            previous
                .recordings
                .values()
                .next()
                .unwrap()
                .lineage
                .operator_version,
            "v2"
        );
    }

    #[test]
    fn vacuum_only_removes_unreferenced_owned_files() {
        let fixture = Fixture::new();
        let dataset = fixture.dataset();
        let snapshot = format!("{}.parquet", Uuid::new_v4());
        let temporary = format!(".manifest-{}.tmp", Uuid::new_v4());
        fs::write(dataset.root.join(&snapshot), b"orphan").unwrap();
        fs::write(dataset.root.join(&temporary), b"partial").unwrap();
        fs::write(dataset.root.join("notes.txt"), b"keep").unwrap();
        fs::write(dataset.root.join("custom.parquet"), b"keep").unwrap();
        let result = dataset.vacuum().unwrap();
        assert_eq!(
            result,
            VacuumResult {
                removed_files: 2,
                reclaimed_bytes: 13
            }
        );
        assert!(dataset.root.join("notes.txt").is_file());
        assert!(dataset.root.join("custom.parquet").is_file());
    }

    #[test]
    fn managed_inputs_are_rejected_and_legacy_inputs_cannot_be_vacuumed() {
        let fixture = Fixture::new();
        let dataset = fixture.dataset();
        let source = dataset.root.join(format!("{}.parquet", Uuid::new_v4()));
        fs::write(&source, b"legacy input").unwrap();
        let calls = AtomicUsize::new(0);
        let provider = FixtureProvider(&calls);
        assert!(matches!(
            dataset.ingest(&source, lineage("v1"), &provider),
            Err(RecordingError::InvalidSource)
        ));
        let uri = url::Url::from_file_path(&source).unwrap().to_string();
        let mut legacy_lineage = lineage("v1");
        legacy_lineage.source.media_uri = uri.clone();
        legacy_lineage.source.source_version = fingerprint(&source).unwrap();
        let rows = provider
            .transcribe(&TranscriptionRequest {
                media: MediaRef::new(uri.clone(), "audio", StorageMode::External).unwrap(),
                lineage: legacy_lineage.clone(),
                start_millis: 0,
                end_millis: u64::MAX,
                max_segments: MAX_SEGMENTS,
            })
            .unwrap();
        let batch = transcript_batch(&rows).unwrap();
        let snapshot = format!("{}.parquet", Uuid::new_v4());
        let mut writer = ArrowWriter::try_new(
            File::create(dataset.root.join(&snapshot)).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut legacy = Manifest::default();
        legacy.recordings.insert(
            uri.clone(),
            Snapshot {
                file: snapshot,
                lineage: legacy_lineage,
                segments: 1,
            },
        );
        dataset.publish(&legacy, None).unwrap();
        assert!(matches!(dataset.vacuum(), Err(RecordingError::UnsafePath)));
        assert!(matches!(
            dataset.remove(&uri),
            Err(RecordingError::UnsafePath)
        ));
        assert_eq!(fs::read(&source).unwrap(), b"legacy input");
        let moved = fixture.0.join("moved-input.parquet");
        fs::rename(source, &moved).unwrap();
        assert!(dataset.remove(&uri).unwrap());
        dataset.vacuum().unwrap();
        assert_eq!(fs::read(moved).unwrap(), b"legacy input");
    }

    #[cfg(unix)]
    #[test]
    fn vacuum_never_follows_managed_file_symlinks() {
        let fixture = Fixture::new();
        let dataset = fixture.dataset();
        let source = fixture.0.join("source.wav");
        std::os::unix::fs::symlink(
            &source,
            dataset.root.join(format!("{}.parquet", Uuid::new_v4())),
        )
        .unwrap();
        assert!(matches!(dataset.vacuum(), Err(RecordingError::UnsafePath)));
        assert_eq!(fs::read(source).unwrap(), b"fixture audio");
    }

    #[test]
    fn storage_failures_preserve_complete_publication_and_batch_state() {
        for stage in FAULT_STAGES {
            let fixture = Fixture::new();
            let calls = AtomicUsize::new(0);
            let provider = FixtureProvider(&calls);
            let source = fixture.0.join("source.wav");
            let mut dataset = fixture.dataset();
            dataset.ingest(&source, lineage("v1"), &provider).unwrap();
            let fail_once = Arc::new(std::sync::atomic::AtomicBool::new(true));
            dataset.fault = Some(Arc::new(move |current| {
                if current == stage && fail_once.swap(false, Ordering::SeqCst) {
                    Err(std::io::Error::from(std::io::ErrorKind::StorageFull))
                } else {
                    Ok(())
                }
            }));
            let mut writer = dataset.writer().unwrap();
            let error = writer
                .ingest(&source, lineage("v2"), &provider)
                .unwrap_err();
            let published = stage == PublicationStage::DirectorySync;
            assert_eq!(error.published(), published, "{stage:?}: {error}");
            let manifest = fixture.dataset().load().unwrap();
            assert_eq!(
                manifest
                    .recordings
                    .values()
                    .next()
                    .unwrap()
                    .lineage
                    .operator_version,
                if published { "v2" } else { "v1" },
                "{stage:?}",
            );
            assert_eq!(
                writer
                    .ingest(&source, lineage("v2"), &provider)
                    .unwrap()
                    .reused,
                published,
                "failed writes must never become warm-reuse entries: {stage:?}",
            );
        }
    }

    #[test]
    fn crash_publication_child() {
        let Ok(root) = std::env::var("LAKEPRISM_RECORDING_CRASH_ROOT") else {
            return;
        };
        let stage = std::env::var("LAKEPRISM_RECORDING_CRASH_STAGE").unwrap();
        let mut dataset = RecordingDataset::open(&root).unwrap();
        dataset.fault = Some(Arc::new(move |current| {
            if format!("{current:?}") == stage {
                std::process::exit(86);
            }
            Ok(())
        }));
        let calls = AtomicUsize::new(0);
        let source = Path::new(&root).parent().unwrap().join("source.wav");
        dataset
            .ingest(&source, lineage("v2"), &FixtureProvider(&calls))
            .unwrap();
        panic!("requested crash boundary was not reached");
    }

    #[test]
    fn process_death_at_every_publication_boundary_reopens_complete_snapshots() {
        for stage in FAULT_STAGES {
            let fixture = Fixture::new();
            let calls = AtomicUsize::new(0);
            let dataset = fixture.dataset();
            dataset
                .ingest(
                    &fixture.0.join("source.wav"),
                    lineage("v1"),
                    &FixtureProvider(&calls),
                )
                .unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "recordings::tests::crash_publication_child"])
                .env("LAKEPRISM_RECORDING_CRASH_ROOT", dataset.root())
                .env("LAKEPRISM_RECORDING_CRASH_STAGE", format!("{stage:?}"))
                .output()
                .unwrap();
            assert_eq!(status.status.code(), Some(86), "{stage:?}");
            let reopened = fixture.dataset();
            let manifest = reopened.load().unwrap();
            assert_eq!(
                manifest
                    .recordings
                    .values()
                    .next()
                    .unwrap()
                    .lineage
                    .operator_version,
                if stage == PublicationStage::DirectorySync {
                    "v2"
                } else {
                    "v1"
                },
                "{stage:?}",
            );
            drop(reopened.writer().unwrap());
        }
    }

    #[test]
    fn explicit_recovery_uses_the_previous_commit_not_orphan_files_or_future_versions() {
        let fixture = Fixture::new();
        let calls = AtomicUsize::new(0);
        let dataset = fixture.dataset();
        let source = fixture.0.join("source.wav");
        dataset
            .ingest(&source, lineage("v1"), &FixtureProvider(&calls))
            .unwrap();
        dataset
            .ingest(&source, lineage("v2"), &FixtureProvider(&calls))
            .unwrap();
        fs::remove_file(dataset.root.join("manifest.json")).unwrap();
        assert!(matches!(
            fixture.dataset_result(),
            Err(RecordingError::MissingManifest)
        ));
        let recovered = RecordingDataset::recover(dataset.root()).unwrap();
        assert_eq!(
            recovered,
            RecoveryResult {
                restored: true,
                recordings: 1
            }
        );
        assert_eq!(
            fixture
                .dataset()
                .load()
                .unwrap()
                .recordings
                .values()
                .next()
                .unwrap()
                .lineage
                .operator_version,
            "v1",
        );
        let future = br#"{"format_version":2,"recordings":{},"future_field":true}"#;
        fs::write(dataset.root.join("manifest.json"), future).unwrap();
        assert!(matches!(
            RecordingDataset::recover(dataset.root()),
            Err(RecordingError::UnsupportedVersion(2))
        ));
        assert_eq!(
            fs::read(dataset.root.join("manifest.json")).unwrap(),
            future
        );

        let orphan = Fixture::new();
        let root = orphan.0.join("dataset");
        fs::create_dir(&root).unwrap();
        fs::write(
            root.join(format!("{}.parquet", Uuid::new_v4())),
            b"partial file",
        )
        .unwrap();
        assert!(matches!(
            RecordingDataset::open(&root),
            Err(RecordingError::MissingManifest)
        ));
        assert!(matches!(
            RecordingDataset::recover(&root),
            Err(RecordingError::MissingManifest)
        ));
    }

    impl Fixture {
        fn dataset_result(&self) -> Result<RecordingDataset, RecordingError> {
            RecordingDataset::open(self.0.join("dataset [literal]"))
        }
    }
}
