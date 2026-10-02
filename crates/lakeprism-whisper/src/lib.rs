//! Explicit, optional adapter for a locally installed Whisper-compatible
//! command. It never downloads, bundles, discovers, or executes a model by
//! default. The configured command receives a private staged audio file and
//! must write the documented JSON result protocol.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use lakeprism_core::{FeatureLineage, SourceIdentity};
use lakeprism_index::{IndexError, TranscriptSegment, TranscriptionProvider, TranscriptionRequest};
use lakeprism_storage::{ExecutionGovernor, QueryControl, QueryControlState};
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

const MAX_ARGUMENTS: usize = 64;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhisperSubprocessConfig {
    /// Absolute path to an application-installed adapter executable.
    pub executable: PathBuf,
    /// Arguments passed without a shell. Exactly `{input}`, `{output}`, and
    /// `{model}` placeholders are required and replaced as individual argv
    /// values; no interpolation is performed.
    pub arguments: Vec<String>,
    /// Existing local model artifact; it is neither copied into nor packaged
    /// with LakePrism.
    pub model_artifact: PathBuf,
    /// A private application-owned directory for short-lived staged files.
    pub staging_directory: PathBuf,
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub timeout: Duration,
    pub operator_version: String,
    pub model: String,
    pub model_version: String,
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WhisperAdapterCapabilities {
    pub local_file_input_only: bool,
    pub governed_cpu: bool,
    pub governed_io: bool,
    pub cooperative_cancellation: bool,
    pub bundled_model: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum WhisperConfigError {
    #[error("executable must be an absolute regular file")]
    InvalidExecutable,
    #[error("model artifact must be an absolute regular file")]
    InvalidModelArtifact,
    #[error("staging directory must be an absolute existing directory")]
    InvalidStagingDirectory,
    #[error("input and output bounds and timeout must be greater than zero")]
    InvalidLimits,
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
pub enum WhisperExecutionKind {
    InputNotLocalFile,
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

/// Structured error fields contain no executable argument values, model path,
/// source path, stdout, stderr, or child output. This makes it suitable for
/// application error telemetry without exposing local topology or input text.
#[derive(Debug, Error, Eq, PartialEq)]
#[error("whisper subprocess execution failed: {kind:?}")]
pub struct WhisperExecutionError {
    pub kind: WhisperExecutionKind,
}

impl WhisperSubprocessConfig {
    pub fn validate(&self) -> Result<(), WhisperConfigError> {
        if !valid_executable(&self.executable) {
            return Err(WhisperConfigError::InvalidExecutable);
        }
        if !self.model_artifact.is_absolute() || !self.model_artifact.is_file() {
            return Err(WhisperConfigError::InvalidModelArtifact);
        }
        if !self.staging_directory.is_absolute() || !self.staging_directory.is_dir() {
            return Err(WhisperConfigError::InvalidStagingDirectory);
        }
        if self.max_input_bytes == 0 || self.max_output_bytes == 0 || self.timeout.is_zero() {
            return Err(WhisperConfigError::InvalidLimits);
        }
        if self.operator_version.trim().is_empty()
            || self.model.trim().is_empty()
            || self.model_version.trim().is_empty()
        {
            return Err(WhisperConfigError::MissingLineage);
        }
        if self.arguments.len() > MAX_ARGUMENTS {
            return Err(WhisperConfigError::TooManyArguments);
        }
        let mut placeholders = BTreeMap::new();
        for argument in &self.arguments {
            if argument.contains('{')
                && argument != "{input}"
                && argument != "{output}"
                && argument != "{model}"
            {
                return Err(WhisperConfigError::UnsupportedPlaceholder);
            }
            if matches!(argument.as_str(), "{input}" | "{output}" | "{model}") {
                *placeholders.entry(argument.as_str()).or_insert(0_u8) += 1;
            }
        }
        if placeholders.get("{input}") != Some(&1)
            || placeholders.get("{output}") != Some(&1)
            || placeholders.get("{model}") != Some(&1)
        {
            return Err(WhisperConfigError::InvalidArgumentTemplate);
        }
        Ok(())
    }
}

pub struct WhisperSubprocessProvider {
    config: WhisperSubprocessConfig,
    governor: std::sync::Arc<ExecutionGovernor>,
}

impl WhisperSubprocessProvider {
    pub fn new(
        config: WhisperSubprocessConfig,
        governor: std::sync::Arc<ExecutionGovernor>,
    ) -> Result<Self, WhisperConfigError> {
        config.validate()?;
        Ok(Self { config, governor })
    }

    pub fn capabilities() -> WhisperAdapterCapabilities {
        WhisperAdapterCapabilities {
            local_file_input_only: true,
            governed_cpu: true,
            governed_io: true,
            cooperative_cancellation: true,
            bundled_model: false,
        }
    }

    /// Runs a configured local adapter under explicit CPU, I/O, timeout, and
    /// cancellation bounds. The child is killed and reaped on every abnormal
    /// exit path.
    pub fn transcribe_governed(
        &self,
        request: &TranscriptionRequest,
        control: &QueryControl,
    ) -> Result<Vec<TranscriptSegment>, WhisperExecutionError> {
        self.config
            .validate()
            .map_err(|_| execution(WhisperExecutionKind::Io))?;
        ensure_active(control)?;
        let _cpu = self
            .governor
            .try_acquire_cpu()
            .ok_or_else(|| execution(WhisperExecutionKind::ResourceExhausted))?;
        let _io = self
            .governor
            .try_acquire_io()
            .ok_or_else(|| execution(WhisperExecutionKind::ResourceExhausted))?;
        let expected = self.expected_lineage(request);
        if !request.lineage.is_compatible_with(&expected) {
            return Err(execution(WhisperExecutionKind::LineageMismatch));
        }
        if request.end_millis < request.start_millis || request.max_segments == 0 {
            return Err(execution(WhisperExecutionKind::InvalidOutput));
        }
        let source = file_uri_path(&request.media.uri)
            .ok_or_else(|| execution(WhisperExecutionKind::InputNotLocalFile))?;
        let metadata = fs::metadata(&source).map_err(|_| execution(WhisperExecutionKind::Io))?;
        if !metadata.is_file() {
            return Err(execution(WhisperExecutionKind::InputNotLocalFile));
        }
        if metadata.len() > self.config.max_input_bytes {
            return Err(execution(WhisperExecutionKind::InputTooLarge));
        }
        let run_dir = self
            .config
            .staging_directory
            .join(format!("lp-whisper-{}", Uuid::new_v4()));
        fs::create_dir(&run_dir).map_err(|_| execution(WhisperExecutionKind::Io))?;
        restrict_directory(&run_dir);
        let result = self.transcribe_in_staging(request, control, &source, &run_dir, expected);
        let _ = fs::remove_dir_all(&run_dir);
        result
    }

    fn expected_lineage(&self, request: &TranscriptionRequest) -> FeatureLineage {
        FeatureLineage {
            source: SourceIdentity {
                media_uri: request.media.uri.clone(),
                source_version: request.lineage.source.source_version.clone(),
            },
            operator_version: self.config.operator_version.clone(),
            model: Some(self.config.model.clone()),
            model_version: Some(self.config.model_version.clone()),
            parameters: self.config.parameters.clone(),
        }
    }

    fn transcribe_in_staging(
        &self,
        request: &TranscriptionRequest,
        control: &QueryControl,
        source: &Path,
        run_dir: &Path,
        lineage: FeatureLineage,
    ) -> Result<Vec<TranscriptSegment>, WhisperExecutionError> {
        let input = run_dir.join("input.audio");
        bounded_copy(source, &input, self.config.max_input_bytes, control)?;
        let output = run_dir.join("result.json");
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
            .map_err(|_| execution(WhisperExecutionKind::SpawnFailed))?;
        let started = Instant::now();
        loop {
            ensure_active(control).inspect_err(|_| {
                terminate_and_reap(&mut child);
            })?;
            if started.elapsed() >= self.config.timeout {
                terminate_and_reap(&mut child);
                return Err(execution(WhisperExecutionKind::TimedOut));
            }
            let status = match child.try_wait() {
                Ok(status) => status,
                Err(_) => {
                    terminate_and_reap(&mut child);
                    return Err(execution(WhisperExecutionKind::Io));
                }
            };
            match status {
                Some(status) if status.success() => break,
                Some(_) => return Err(execution(WhisperExecutionKind::ChildFailed)),
                None => thread::sleep(POLL_INTERVAL),
            }
        }
        let output_meta = fs::symlink_metadata(&output)
            .map_err(|_| execution(WhisperExecutionKind::InvalidOutput))?;
        if !output_meta.file_type().is_file() || output_meta.len() > self.config.max_output_bytes {
            return Err(execution(WhisperExecutionKind::OutputTooLarge));
        }
        let rows: FixtureOutput = serde_json::from_reader(
            File::open(&output).map_err(|_| execution(WhisperExecutionKind::InvalidOutput))?,
        )
        .map_err(|_| execution(WhisperExecutionKind::InvalidOutput))?;
        if rows.segments.len() > request.max_segments {
            return Err(execution(WhisperExecutionKind::InvalidOutput));
        }
        rows.segments
            .into_iter()
            .filter(|segment| {
                segment.start_millis >= request.start_millis
                    && segment.end_millis <= request.end_millis
            })
            .map(|segment| {
                if segment.end_millis < segment.start_millis || segment.text.trim().is_empty() {
                    return Err(execution(WhisperExecutionKind::InvalidOutput));
                }
                Ok(TranscriptSegment {
                    media_id: request.media.uri.clone(),
                    start_millis: segment.start_millis,
                    end_millis: segment.end_millis,
                    text: segment.text,
                    confidence_millis: segment.confidence_millis,
                    lineage: lineage.clone(),
                    created_at_unix_millis: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64,
                })
            })
            .collect()
    }
}

impl TranscriptionProvider for WhisperSubprocessProvider {
    fn transcribe(
        &self,
        request: &TranscriptionRequest,
    ) -> Result<Vec<TranscriptSegment>, IndexError> {
        self.transcribe_governed(request, &QueryControl::new(None))
            .map_err(|error| IndexError::ProviderExecution {
                capability: "configured local Whisper subprocess",
                kind: error.kind.as_str(),
            })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureOutput {
    segments: Vec<FixtureSegment>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureSegment {
    start_millis: u64,
    end_millis: u64,
    text: String,
    confidence_millis: Option<u16>,
}

fn bounded_copy(
    source: &Path,
    destination: &Path,
    maximum: u64,
    control: &QueryControl,
) -> Result<(), WhisperExecutionError> {
    let mut reader =
        BufReader::new(File::open(source).map_err(|_| execution(WhisperExecutionKind::Io))?);
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .map_err(|_| execution(WhisperExecutionKind::Io))?,
    );
    restrict_file(destination);
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        ensure_active(control)?;
        let read = reader
            .read(&mut buffer)
            .map_err(|_| execution(WhisperExecutionKind::Io))?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(read as u64)
            .ok_or_else(|| execution(WhisperExecutionKind::InputTooLarge))?;
        if copied > maximum {
            return Err(execution(WhisperExecutionKind::InputTooLarge));
        }
        writer
            .write_all(&buffer[..read])
            .map_err(|_| execution(WhisperExecutionKind::Io))?;
    }
    writer
        .flush()
        .map_err(|_| execution(WhisperExecutionKind::Io))
}

fn file_uri_path(uri: &str) -> Option<PathBuf> {
    let path = uri.strip_prefix("file://")?;
    if path.contains(['?', '#']) {
        return None;
    }
    Some(PathBuf::from(path))
}
fn ensure_active(control: &QueryControl) -> Result<(), WhisperExecutionError> {
    match control.state() {
        QueryControlState::Active => Ok(()),
        QueryControlState::Cancelled => Err(execution(WhisperExecutionKind::Cancelled)),
        QueryControlState::DeadlineExceeded => {
            Err(execution(WhisperExecutionKind::DeadlineExceeded))
        }
    }
}
fn execution(kind: WhisperExecutionKind) -> WhisperExecutionError {
    WhisperExecutionError { kind }
}
impl WhisperExecutionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::InputNotLocalFile => "input_not_local_file",
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

#[cfg(test)]
mod tests {
    use super::*;
    use lakeprism_core::{MediaRef, StorageMode};
    use lakeprism_storage::{ExecutionGovernorConfig, QueryControl};

    #[test]
    fn rejects_non_explicit_templates() {
        let config = WhisperSubprocessConfig {
            executable: PathBuf::from("/bin/echo"),
            arguments: vec!["{input}".into()],
            model_artifact: PathBuf::from("/bin/echo"),
            staging_directory: PathBuf::from("/"),
            max_input_bytes: 1,
            max_output_bytes: 1,
            timeout: Duration::from_secs(1),
            operator_version: "v1".into(),
            model: "m".into(),
            model_version: "1".into(),
            parameters: BTreeMap::new(),
        };
        assert_eq!(
            config.validate(),
            Err(WhisperConfigError::InvalidArgumentTemplate)
        );
    }

    #[test]
    fn cancellation_is_reported_before_staging() {
        let root = std::env::current_dir().unwrap();
        let config = WhisperSubprocessConfig {
            executable: PathBuf::from("/bin/echo"),
            arguments: vec!["{input}".into(), "{output}".into(), "{model}".into()],
            model_artifact: root.join("Cargo.toml"),
            staging_directory: root,
            max_input_bytes: 1,
            max_output_bytes: 1,
            timeout: Duration::from_secs(1),
            operator_version: "v1".into(),
            model: "m".into(),
            model_version: "1".into(),
            parameters: BTreeMap::new(),
        };
        let provider = WhisperSubprocessProvider::new(
            config,
            std::sync::Arc::new(
                ExecutionGovernor::new(ExecutionGovernorConfig::default()).unwrap(),
            ),
        )
        .unwrap();
        let control = QueryControl::new(None);
        control.cancel();
        let request = TranscriptionRequest {
            media: MediaRef::new("file:///does-not-exist", "audio", StorageMode::External).unwrap(),
            lineage: FeatureLineage {
                source: SourceIdentity {
                    media_uri: "file:///does-not-exist".into(),
                    source_version: "1".into(),
                },
                operator_version: "v1".into(),
                model: Some("m".into()),
                model_version: Some("1".into()),
                parameters: BTreeMap::new(),
            },
            start_millis: 0,
            end_millis: 1,
            max_segments: 1,
        };
        assert_eq!(
            provider
                .transcribe_governed(&request, &control)
                .unwrap_err()
                .kind,
            WhisperExecutionKind::Cancelled
        );
    }
}
