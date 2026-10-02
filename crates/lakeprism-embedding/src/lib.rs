//! Optional adapter for an application-installed batch embedding command.
//! It never bundles, downloads, or fabricates a model or embedding.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use lakeprism_core::{FeatureLineage, SourceIdentity};
use lakeprism_index::{
    EmbeddingProvider, EmbeddingRecord, EmbeddingRequest, IndexError, MAX_EMBEDDING_DIMENSIONS,
};
use lakeprism_storage::{ExecutionGovernor, QueryControl, QueryControlState};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const MAX_ARGUMENTS: usize = 64;
const MAX_BATCH_ITEMS: usize = 1_024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddingSubprocessConfig {
    pub executable: PathBuf,
    /// Direct argv values. `{input}`, `{output}`, and `{model}` each occur
    /// once as a complete value and are never interpolated through a shell.
    pub arguments: Vec<String>,
    pub model_artifact: PathBuf,
    pub staging_directory: PathBuf,
    pub max_batch_items: usize,
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub timeout: Duration,
    pub operator_version: String,
    pub model: String,
    pub model_version: String,
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmbeddingAdapterCapabilities {
    pub batch_protocol: bool,
    pub governed_cpu: bool,
    pub governed_io: bool,
    pub cooperative_cancellation: bool,
    pub bundled_model: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum EmbeddingConfigError {
    #[error("executable must be an absolute regular file")]
    InvalidExecutable,
    #[error("model artifact must be an absolute regular file")]
    InvalidModelArtifact,
    #[error("staging directory must be an absolute existing directory")]
    InvalidStagingDirectory,
    #[error("batch bound, byte bounds, and timeout must be greater than zero")]
    InvalidLimits,
    #[error("batch item bound must not exceed {MAX_BATCH_ITEMS}")]
    BatchLimitExceeded,
    #[error("operator/model/version must not be empty")]
    MissingLineage,
    #[error("argument vector must contain no more than {MAX_ARGUMENTS} values")]
    TooManyArguments,
    #[error(
        "argument vector must contain each of {{input}}, {{output}}, and {{model}} exactly once"
    )]
    InvalidArgumentTemplate,
    #[error("argument contains an unsupported placeholder")]
    UnsupportedPlaceholder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmbeddingExecutionKind {
    EmptyBatch,
    BatchTooLarge,
    InputTooLarge,
    ResourceExhausted,
    Cancelled,
    DeadlineExceeded,
    TimedOut,
    SpawnFailed,
    ChildFailed,
    OutputTooLarge,
    InvalidOutput,
    Io,
    LineageMismatch,
}

/// Redacted structured error: it contains no paths, arguments, text, output,
/// stdout, or stderr and is safe for application telemetry.
#[derive(Debug, Error, Eq, PartialEq)]
#[error("embedding subprocess execution failed: {kind:?}")]
pub struct EmbeddingExecutionError {
    pub kind: EmbeddingExecutionKind,
}

impl EmbeddingSubprocessConfig {
    pub fn validate(&self) -> Result<(), EmbeddingConfigError> {
        if !valid_executable(&self.executable) {
            return Err(EmbeddingConfigError::InvalidExecutable);
        }
        if !self.model_artifact.is_absolute() || !self.model_artifact.is_file() {
            return Err(EmbeddingConfigError::InvalidModelArtifact);
        }
        if !self.staging_directory.is_absolute() || !self.staging_directory.is_dir() {
            return Err(EmbeddingConfigError::InvalidStagingDirectory);
        }
        if self.max_batch_items == 0
            || self.max_input_bytes == 0
            || self.max_output_bytes == 0
            || self.timeout.is_zero()
        {
            return Err(EmbeddingConfigError::InvalidLimits);
        }
        if self.max_batch_items > MAX_BATCH_ITEMS {
            return Err(EmbeddingConfigError::BatchLimitExceeded);
        }
        if self.operator_version.trim().is_empty()
            || self.model.trim().is_empty()
            || self.model_version.trim().is_empty()
        {
            return Err(EmbeddingConfigError::MissingLineage);
        }
        if self.arguments.len() > MAX_ARGUMENTS {
            return Err(EmbeddingConfigError::TooManyArguments);
        }
        let mut placeholders = BTreeMap::new();
        for argument in &self.arguments {
            if argument.contains('{')
                && !matches!(argument.as_str(), "{input}" | "{output}" | "{model}")
            {
                return Err(EmbeddingConfigError::UnsupportedPlaceholder);
            }
            if matches!(argument.as_str(), "{input}" | "{output}" | "{model}") {
                *placeholders.entry(argument.as_str()).or_insert(0_u8) += 1;
            }
        }
        if placeholders.get("{input}") != Some(&1)
            || placeholders.get("{output}") != Some(&1)
            || placeholders.get("{model}") != Some(&1)
        {
            return Err(EmbeddingConfigError::InvalidArgumentTemplate);
        }
        Ok(())
    }
}

pub struct EmbeddingSubprocessProvider {
    config: EmbeddingSubprocessConfig,
    governor: std::sync::Arc<ExecutionGovernor>,
}

impl EmbeddingSubprocessProvider {
    pub fn new(
        config: EmbeddingSubprocessConfig,
        governor: std::sync::Arc<ExecutionGovernor>,
    ) -> Result<Self, EmbeddingConfigError> {
        config.validate()?;
        Ok(Self { config, governor })
    }

    pub fn capabilities() -> EmbeddingAdapterCapabilities {
        EmbeddingAdapterCapabilities {
            batch_protocol: true,
            governed_cpu: true,
            governed_io: true,
            cooperative_cancellation: true,
            bundled_model: false,
        }
    }

    /// Embeds a bounded batch using the documented JSON-file protocol:
    /// input `{"requests":[{"id":"...","text":"..."}]}` and output
    /// `{"embeddings":[{"id":"...","values":[0.1,...]}]}`. IDs must exactly
    /// match once and output ordering is normalized to input ordering.
    pub fn embed_batch_governed(
        &self,
        requests: &[EmbeddingRequest],
        control: &QueryControl,
    ) -> Result<Vec<Vec<f32>>, EmbeddingExecutionError> {
        self.config
            .validate()
            .map_err(|_| execution(EmbeddingExecutionKind::Io))?;
        ensure_active(control)?;
        if requests.is_empty() {
            return Err(execution(EmbeddingExecutionKind::EmptyBatch));
        }
        if requests.len() > self.config.max_batch_items {
            return Err(execution(EmbeddingExecutionKind::BatchTooLarge));
        }
        let _cpu = self
            .governor
            .try_acquire_cpu()
            .ok_or_else(|| execution(EmbeddingExecutionKind::ResourceExhausted))?;
        let _io = self
            .governor
            .try_acquire_io()
            .ok_or_else(|| execution(EmbeddingExecutionKind::ResourceExhausted))?;
        for request in requests {
            if !request
                .lineage
                .is_compatible_with(&self.expected_lineage(request))
            {
                return Err(execution(EmbeddingExecutionKind::LineageMismatch));
            }
        }
        let ids = (0..requests.len())
            .map(|index| format!("r{index}"))
            .collect::<Vec<_>>();
        let payload = ProtocolInput {
            requests: ids
                .iter()
                .zip(requests)
                .map(|(id, request)| ProtocolInputRow {
                    id: id.clone(),
                    text: request.text.clone(),
                })
                .collect(),
        };
        let bytes =
            serde_json::to_vec(&payload).map_err(|_| execution(EmbeddingExecutionKind::Io))?;
        if bytes.len() as u64 > self.config.max_input_bytes {
            return Err(execution(EmbeddingExecutionKind::InputTooLarge));
        }
        let run_dir = self
            .config
            .staging_directory
            .join(format!("lp-embedding-{}", Uuid::new_v4()));
        fs::create_dir(&run_dir).map_err(|_| execution(EmbeddingExecutionKind::Io))?;
        restrict_directory(&run_dir);
        let result = self.embed_in_staging(control, &run_dir, &bytes, &ids);
        let _ = fs::remove_dir_all(&run_dir);
        result
    }

    pub fn embed_records_governed(
        &self,
        rows: &[(String, String, FeatureLineage)],
        control: &QueryControl,
    ) -> Result<Vec<EmbeddingRecord>, EmbeddingExecutionError> {
        let requests = rows
            .iter()
            .map(|(_, text, lineage)| EmbeddingRequest {
                text: text.clone(),
                lineage: lineage.clone(),
            })
            .collect::<Vec<_>>();
        let values = self.embed_batch_governed(&requests, control)?;
        Ok(rows
            .iter()
            .zip(values)
            .map(|((id, text, lineage), values)| EmbeddingRecord {
                id: id.clone(),
                text: text.clone(),
                values,
                lineage: lineage.clone(),
            })
            .collect())
    }

    fn expected_lineage(&self, request: &EmbeddingRequest) -> FeatureLineage {
        FeatureLineage {
            source: SourceIdentity {
                media_uri: request.lineage.source.media_uri.clone(),
                source_version: request.lineage.source.source_version.clone(),
            },
            operator_version: self.config.operator_version.clone(),
            model: Some(self.config.model.clone()),
            model_version: Some(self.config.model_version.clone()),
            parameters: self.config.parameters.clone(),
        }
    }

    fn embed_in_staging(
        &self,
        control: &QueryControl,
        run_dir: &Path,
        bytes: &[u8],
        ids: &[String],
    ) -> Result<Vec<Vec<f32>>, EmbeddingExecutionError> {
        let input = run_dir.join("input.json");
        write_private(&input, bytes)?;
        let output = run_dir.join("output.json");
        let arguments = self
            .config
            .arguments
            .iter()
            .map(|argument| match argument.as_str() {
                "{input}" => input.as_os_str().to_os_string(),
                "{output}" => output.as_os_str().to_os_string(),
                "{model}" => self.config.model_artifact.as_os_str().to_os_string(),
                _ => argument.clone().into(),
            });
        let mut child = Command::new(&self.config.executable)
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| execution(EmbeddingExecutionKind::SpawnFailed))?;
        let started = Instant::now();
        loop {
            ensure_active(control).inspect_err(|_| terminate_and_reap(&mut child))?;
            if started.elapsed() >= self.config.timeout {
                terminate_and_reap(&mut child);
                return Err(execution(EmbeddingExecutionKind::TimedOut));
            }
            match child.try_wait() {
                Ok(Some(status)) if status.success() => break,
                Ok(Some(_)) => return Err(execution(EmbeddingExecutionKind::ChildFailed)),
                Ok(None) => thread::sleep(POLL_INTERVAL),
                Err(_) => {
                    terminate_and_reap(&mut child);
                    return Err(execution(EmbeddingExecutionKind::Io));
                }
            }
        }
        let metadata = fs::symlink_metadata(&output)
            .map_err(|_| execution(EmbeddingExecutionKind::InvalidOutput))?;
        if !metadata.file_type().is_file() || metadata.len() > self.config.max_output_bytes {
            return Err(execution(EmbeddingExecutionKind::OutputTooLarge));
        }
        let output = read_bounded(&output, self.config.max_output_bytes)?;
        let protocol: ProtocolOutput = serde_json::from_slice(&output)
            .map_err(|_| execution(EmbeddingExecutionKind::InvalidOutput))?;
        if protocol.embeddings.len() != ids.len() {
            return Err(execution(EmbeddingExecutionKind::InvalidOutput));
        }
        let mut output_by_id = BTreeMap::new();
        for row in protocol.embeddings {
            if row.id.trim().is_empty()
                || row.values.is_empty()
                || row.values.len() > MAX_EMBEDDING_DIMENSIONS
                || row.values.iter().any(|value| !value.is_finite())
                || output_by_id.insert(row.id, row.values).is_some()
            {
                return Err(execution(EmbeddingExecutionKind::InvalidOutput));
            }
        }
        let expected = ids.iter().cloned().collect::<BTreeSet<_>>();
        if output_by_id.keys().cloned().collect::<BTreeSet<_>>() != expected {
            return Err(execution(EmbeddingExecutionKind::InvalidOutput));
        }
        let rows = ids
            .iter()
            .map(|id| output_by_id.remove(id).expect("validated output ID"))
            .collect::<Vec<_>>();
        if rows.windows(2).any(|pair| pair[0].len() != pair[1].len()) {
            return Err(execution(EmbeddingExecutionKind::InvalidOutput));
        }
        Ok(rows)
    }
}

impl EmbeddingProvider for EmbeddingSubprocessProvider {
    fn embed(&self, request: &EmbeddingRequest) -> Result<Vec<f32>, IndexError> {
        self.embed_batch_governed(std::slice::from_ref(request), &QueryControl::new(None))
            .map(|mut rows| rows.remove(0))
            .map_err(|error| IndexError::ProviderExecution {
                capability: "configured local embedding subprocess",
                kind: error.kind.as_str(),
            })
    }
}

#[derive(Serialize)]
struct ProtocolInput {
    requests: Vec<ProtocolInputRow>,
}
#[derive(Serialize)]
struct ProtocolInputRow {
    id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtocolOutput {
    embeddings: Vec<ProtocolOutputRow>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtocolOutputRow {
    id: String,
    values: Vec<f32>,
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<(), EmbeddingExecutionError> {
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|_| execution(EmbeddingExecutionKind::Io))?,
    );
    restrict_file(path);
    writer
        .write_all(bytes)
        .and_then(|_| writer.flush())
        .map_err(|_| execution(EmbeddingExecutionKind::Io))
}
fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, EmbeddingExecutionError> {
    let mut reader = BufReader::new(
        File::open(path).map_err(|_| execution(EmbeddingExecutionKind::InvalidOutput))?,
    );
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| execution(EmbeddingExecutionKind::InvalidOutput))?;
    if bytes.len() as u64 > maximum {
        Err(execution(EmbeddingExecutionKind::OutputTooLarge))
    } else {
        Ok(bytes)
    }
}
fn ensure_active(control: &QueryControl) -> Result<(), EmbeddingExecutionError> {
    match control.state() {
        QueryControlState::Active => Ok(()),
        QueryControlState::Cancelled => Err(execution(EmbeddingExecutionKind::Cancelled)),
        QueryControlState::DeadlineExceeded => {
            Err(execution(EmbeddingExecutionKind::DeadlineExceeded))
        }
    }
}
fn execution(kind: EmbeddingExecutionKind) -> EmbeddingExecutionError {
    EmbeddingExecutionError { kind }
}
impl EmbeddingExecutionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::EmptyBatch => "empty_batch",
            Self::BatchTooLarge => "batch_too_large",
            Self::InputTooLarge => "input_too_large",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::TimedOut => "timed_out",
            Self::SpawnFailed => "spawn_failed",
            Self::ChildFailed => "child_failed",
            Self::OutputTooLarge => "output_too_large",
            Self::InvalidOutput => "invalid_output",
            Self::Io => "io",
            Self::LineageMismatch => "lineage_mismatch",
        }
    }
}
fn valid_executable(path: &Path) -> bool {
    if !path.is_absolute() || !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}
fn terminate_and_reap(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}
#[cfg(unix)]
fn restrict_directory(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
}
#[cfg(not(unix))]
fn restrict_directory(_: &Path) {}
#[cfg(unix)]
fn restrict_file(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}
#[cfg(not(unix))]
fn restrict_file(_: &Path) {}
