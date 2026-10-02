use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use lakeprism_core::{AccessContext, ByteRange, LakePrismError, MediaRef};
#[cfg(feature = "s3")]
use object_store::ObjectStoreExt;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionGovernorConfig {
    pub max_concurrent_queries: usize,
    pub max_cpu_permits: usize,
    pub max_io_permits: usize,
}

impl Default for ExecutionGovernorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: 4,
            max_cpu_permits: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            max_io_permits: 16,
        }
    }
}

#[derive(Debug, Error)]
pub enum GovernorConfigError {
    #[error("max concurrent queries must be greater than zero")]
    ZeroConcurrentQueries,
    #[error("max CPU permits must be greater than zero")]
    ZeroCpuPermits,
    #[error("max I/O permits must be greater than zero")]
    ZeroIoPermits,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionCapability {
    Supported,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionCapabilities {
    pub deadlines: ExecutionCapability,
    pub cancellation: ExecutionCapability,
}

#[derive(Clone)]
pub struct ExecutionGovernor {
    query_slots: Arc<Semaphore>,
    cpu_slots: Arc<Semaphore>,
    io_slots: Arc<Semaphore>,
    config: ExecutionGovernorConfig,
}

/// Cooperative control shared by a query and practical local I/O paths.
/// Native synchronous decoders can only observe this control between decode
/// operations; async local storage checks it before and after every operation.
#[derive(Clone, Debug)]
pub struct QueryControl {
    cancellation: CancellationToken,
    deadline: Option<tokio::time::Instant>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryControlState {
    Active,
    Cancelled,
    DeadlineExceeded,
}

impl QueryControl {
    pub fn new(deadline: Option<tokio::time::Instant>) -> Self {
        Self {
            cancellation: CancellationToken::new(),
            deadline,
        }
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }

    pub fn state(&self) -> QueryControlState {
        if self.cancellation.is_cancelled() {
            QueryControlState::Cancelled
        } else if self
            .deadline
            .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
        {
            QueryControlState::DeadlineExceeded
        } else {
            QueryControlState::Active
        }
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
}

pub struct QueryPermit {
    _permit: OwnedSemaphorePermit,
}
pub struct CpuPermit {
    _permit: OwnedSemaphorePermit,
}
pub struct IoPermit {
    permit: OwnedSemaphorePermit,
}

impl IoPermit {
    fn into_inner(self) -> OwnedSemaphorePermit {
        self.permit
    }
}

impl ExecutionGovernor {
    pub fn new(config: ExecutionGovernorConfig) -> std::result::Result<Self, GovernorConfigError> {
        if config.max_concurrent_queries == 0 {
            return Err(GovernorConfigError::ZeroConcurrentQueries);
        }
        if config.max_cpu_permits == 0 {
            return Err(GovernorConfigError::ZeroCpuPermits);
        }
        if config.max_io_permits == 0 {
            return Err(GovernorConfigError::ZeroIoPermits);
        }

        Ok(Self {
            query_slots: Arc::new(Semaphore::new(config.max_concurrent_queries)),
            cpu_slots: Arc::new(Semaphore::new(config.max_cpu_permits)),
            io_slots: Arc::new(Semaphore::new(config.max_io_permits)),
            config,
        })
    }

    pub fn config(&self) -> ExecutionGovernorConfig {
        self.config
    }

    pub fn capabilities(&self) -> ExecutionCapabilities {
        ExecutionCapabilities {
            deadlines: ExecutionCapability::Supported,
            cancellation: ExecutionCapability::Supported,
        }
    }

    pub async fn acquire_query(&self) -> QueryPermit {
        QueryPermit {
            _permit: self
                .query_slots
                .clone()
                .acquire_owned()
                .await
                .expect("execution governor semaphore is never closed"),
        }
    }

    pub fn try_acquire_query(&self) -> Option<QueryPermit> {
        self.query_slots
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| QueryPermit { _permit: permit })
    }

    pub async fn acquire_cpu(&self) -> CpuPermit {
        CpuPermit {
            _permit: self
                .cpu_slots
                .clone()
                .acquire_owned()
                .await
                .expect("execution governor semaphore is never closed"),
        }
    }

    pub fn try_acquire_cpu(&self) -> Option<CpuPermit> {
        self.cpu_slots
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| CpuPermit { _permit: permit })
    }

    pub async fn acquire_io(&self) -> IoPermit {
        IoPermit {
            permit: self
                .io_slots
                .clone()
                .acquire_owned()
                .await
                .expect("execution governor semaphore is never closed"),
        }
    }

    pub fn try_acquire_io(&self) -> Option<IoPermit> {
        self.io_slots
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| IoPermit { permit })
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Core(#[from] LakePrismError),
    #[error("unsupported media URI scheme: {0}")]
    UnsupportedScheme(String),
    #[error("invalid local file URI: {0}")]
    InvalidFileUri(String),
    #[error("requested {requested_bytes} bytes exceeds the {maximum_bytes}-byte read budget")]
    ByteBudgetExceeded {
        requested_bytes: u64,
        maximum_bytes: u64,
    },
    #[error("query was cancelled")]
    Cancelled,
    #[error("query deadline was exceeded")]
    DeadlineExceeded,
    #[cfg(feature = "s3")]
    #[error(
        "remote media source is {source_bytes} bytes, exceeding the configured {maximum_bytes}-byte staging cap"
    )]
    StagingSizeExceeded {
        source_bytes: u64,
        maximum_bytes: u64,
    },
    #[cfg(feature = "s3")]
    #[error("invalid S3-compatible configuration: {0}")]
    InvalidS3Configuration(String),
    #[cfg(feature = "s3")]
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceMetadata {
    pub size_bytes: u64,
    /// An object-store ETag when the backend supplies one. It is metadata, not
    /// an authorization token, and is suitable only as a best-effort version.
    pub version: Option<String>,
}

#[async_trait]
pub trait MediaSource: Send + Sync {
    async fn metadata(&self) -> Result<SourceMetadata>;
    async fn read_range(&self, range: ByteRange) -> Result<Vec<u8>>;
}

#[async_trait]
pub trait MediaResolver: Send + Sync {
    async fn resolve(
        &self,
        media: &MediaRef,
        access: &AccessContext,
    ) -> Result<Arc<dyn MediaSource>>;
}

#[derive(Clone)]
pub struct ResourceManager {
    io_slots: Arc<Semaphore>,
    execution_governor: Option<Arc<ExecutionGovernor>>,
    query_control: Option<QueryControl>,
    byte_budget: u64,
}

impl ResourceManager {
    pub fn new(io_slots: usize, byte_budget: u64) -> std::result::Result<Self, LakePrismError> {
        if byte_budget == 0 {
            return Err(LakePrismError::ZeroByteBudget);
        }
        Ok(Self {
            io_slots: Arc::new(Semaphore::new(io_slots.max(1))),
            execution_governor: None,
            query_control: None,
            byte_budget,
        })
    }

    pub fn with_execution_governor(
        execution_governor: Arc<ExecutionGovernor>,
        byte_budget: u64,
    ) -> std::result::Result<Self, LakePrismError> {
        if byte_budget == 0 {
            return Err(LakePrismError::ZeroByteBudget);
        }
        Ok(Self {
            io_slots: Arc::new(Semaphore::new(1)),
            execution_governor: Some(execution_governor),
            query_control: None,
            byte_budget,
        })
    }

    pub async fn acquire_io_slot(&self) -> Result<OwnedSemaphorePermit> {
        self.check_control()?;
        if let Some(governor) = &self.execution_governor {
            let control = self.query_control.clone();
            let permit = async move { governor.acquire_io().await.into_inner() };
            return match control {
                Some(control) => tokio::select! {
                    permit = permit => {
                        control_state_result(control.state())?;
                        Ok(permit)
                    }
                    _ = control.cancelled() => Err(StorageError::Cancelled),
                },
                None => Ok(permit.await),
            };
        }
        let acquire = self.io_slots.clone().acquire_owned();
        let permit = match &self.query_control {
            Some(control) => tokio::select! {
                permit = acquire => permit.expect("resource manager semaphore is never closed"),
                _ = control.cancelled() => return Err(StorageError::Cancelled),
            },
            None => acquire
                .await
                .expect("resource manager semaphore is never closed"),
        };
        self.check_control()?;
        Ok(permit)
    }

    pub fn with_query_control(mut self, query_control: QueryControl) -> Self {
        self.query_control = Some(query_control);
        self
    }

    fn check_control(&self) -> Result<()> {
        self.query_control
            .as_ref()
            .map(|control| control_state_result(control.state()))
            .transpose()?;
        Ok(())
    }

    pub fn validate_read_size(&self, byte_count: u64) -> Result<()> {
        if byte_count > self.byte_budget {
            return Err(StorageError::ByteBudgetExceeded {
                requested_bytes: byte_count,
                maximum_bytes: self.byte_budget,
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LocalResolver {
    resources: ResourceManager,
}

impl LocalResolver {
    pub fn new(resources: ResourceManager) -> Self {
        Self { resources }
    }
}

#[async_trait]
impl MediaResolver for LocalResolver {
    async fn resolve(
        &self,
        media: &MediaRef,
        _access: &AccessContext,
    ) -> Result<Arc<dyn MediaSource>> {
        let parsed =
            Url::parse(&media.uri).map_err(|_| StorageError::InvalidFileUri(media.uri.clone()))?;
        if parsed.scheme() != "file" {
            return Err(StorageError::UnsupportedScheme(parsed.scheme().to_owned()));
        }
        let path = parsed
            .to_file_path()
            .map_err(|_| StorageError::InvalidFileUri(media.uri.clone()))?;
        Ok(Arc::new(LocalMediaSource {
            path,
            resources: self.resources.clone(),
        }))
    }
}

struct LocalMediaSource {
    path: PathBuf,
    resources: ResourceManager,
}

#[async_trait]
impl MediaSource for LocalMediaSource {
    async fn metadata(&self) -> Result<SourceMetadata> {
        let _permit = self.resources.acquire_io_slot().await?;
        let metadata = SourceMetadata {
            size_bytes: tokio::fs::metadata(&self.path).await?.len(),
            version: None,
        };
        self.resources.check_control()?;
        Ok(metadata)
    }

    async fn read_range(&self, range: ByteRange) -> Result<Vec<u8>> {
        self.resources.validate_read_size(range.len())?;
        let _permit = self.resources.acquire_io_slot().await?;
        let mut file = tokio::fs::File::open(&self.path).await?;
        let source_length = file.metadata().await?.len();
        ByteRange::new(range.start, range.end_exclusive, source_length)?;
        file.seek(std::io::SeekFrom::Start(range.start)).await?;
        let mut bytes = vec![0; range.len() as usize];
        file.read_exact(&mut bytes).await?;
        self.resources.check_control()?;
        Ok(bytes)
    }
}

/// Query-scoped credentials for an S3-compatible object store.
///
/// The credentials intentionally do not implement `Debug`, `Serialize`, or
/// `Deserialize`. Keep this value in the application request scope and drop it
/// after the query/session that needs remote reads completes.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct S3Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[cfg(feature = "s3")]
impl S3Credentials {
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
        }
    }

    pub fn with_session_token(mut self, session_token: impl Into<String>) -> Self {
        self.session_token = Some(session_token.into());
        self
    }
}

/// Application-owned source of request-scoped S3 credentials.
///
/// Invoke it when creating a query/session configuration, not during catalog
/// persistence or SQL planning. Implementations can vend short-lived STS or
/// SSO credentials without exposing a process-wide environment credential
/// chain to LakePrism.
#[cfg(feature = "s3")]
pub trait S3CredentialProvider: Send + Sync {
    fn provide(&self) -> Result<S3Credentials>;
}

/// Validated S3-compatible endpoint and identity configuration.
///
/// HTTP is accepted only for loopback endpoints, so local MinIO/MockServer
/// tests are possible without opening a general SSRF transport.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct S3CompatibleConfig {
    bucket: String,
    region: String,
    endpoint: Url,
    credentials: S3Credentials,
    virtual_hosted_style: bool,
}

#[cfg(feature = "s3")]
impl S3CompatibleConfig {
    pub fn from_provider(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl AsRef<str>,
        provider: &dyn S3CredentialProvider,
    ) -> Result<Self> {
        Self::new(bucket, region, endpoint, provider.provide()?)
    }

    pub fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl AsRef<str>,
        credentials: S3Credentials,
    ) -> Result<Self> {
        let bucket = bucket.into();
        if bucket.is_empty()
            || !bucket.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.' || byte == b'-'
            })
        {
            return Err(StorageError::InvalidS3Configuration(
                "bucket must be a non-empty lowercase DNS-compatible name".to_string(),
            ));
        }
        let endpoint = Url::parse(endpoint.as_ref()).map_err(|_| {
            StorageError::InvalidS3Configuration("endpoint must be an absolute URL".to_string())
        })?;
        if endpoint.username() != ""
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(StorageError::InvalidS3Configuration(
                "endpoint must not contain credentials, query parameters, or fragments".to_string(),
            ));
        }
        let is_loopback = endpoint
            .host_str()
            .is_some_and(|host| host == "localhost" || host == "127.0.0.1" || host == "::1");
        if endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && is_loopback) {
            return Err(StorageError::InvalidS3Configuration(
                "endpoint must use HTTPS (HTTP is allowed only for loopback test endpoints)"
                    .to_string(),
            ));
        }
        Ok(Self {
            bucket,
            region: region.into(),
            endpoint,
            credentials,
            virtual_hosted_style: false,
        })
    }

    pub fn with_virtual_hosted_style(mut self, enabled: bool) -> Self {
        self.virtual_hosted_style = enabled;
        self
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Produces delta-rs/object_store options only for the active query scope.
    /// Never write this map to a catalog, log, error, or Arrow payload.
    pub fn delta_storage_options(&self) -> std::collections::HashMap<String, String> {
        let mut options = std::collections::HashMap::from([
            ("AWS_REGION".to_string(), self.region.clone()),
            ("AWS_ENDPOINT_URL".to_string(), self.endpoint.to_string()),
            (
                "AWS_ACCESS_KEY_ID".to_string(),
                self.credentials.access_key_id.clone(),
            ),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                self.credentials.secret_access_key.clone(),
            ),
            (
                "AWS_S3_ADDRESSING_STYLE".to_string(),
                if self.virtual_hosted_style {
                    "virtual".to_string()
                } else {
                    "path".to_string()
                },
            ),
        ]);
        if let Some(token) = &self.credentials.session_token {
            options.insert("AWS_SESSION_TOKEN".to_string(), token.clone());
        }
        options
    }
}

/// Bounded range-capable resolver for `s3://bucket/key` references.
///
/// It accepts only the bucket configured for this query scope. Absolute HTTP
/// URLs and credential-bearing URIs remain rejected by `MediaRef`, avoiding
/// URL-based SSRF and signed-URL credential persistence.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct S3CompatibleResolver {
    config: S3CompatibleConfig,
    resources: ResourceManager,
}

#[cfg(feature = "s3")]
impl S3CompatibleResolver {
    pub fn new(config: S3CompatibleConfig, resources: ResourceManager) -> Self {
        Self { config, resources }
    }

    fn store(&self) -> Result<std::sync::Arc<dyn object_store::ObjectStore>> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(self.config.bucket())
            .with_region(&self.config.region)
            .with_endpoint(self.config.endpoint().as_str())
            .with_access_key_id(&self.config.credentials.access_key_id)
            .with_secret_access_key(&self.config.credentials.secret_access_key)
            .with_virtual_hosted_style_request(self.config.virtual_hosted_style);
        if self.config.endpoint().scheme() == "http" {
            builder = builder.with_allow_http(true);
        }
        if let Some(token) = &self.config.credentials.session_token {
            builder = builder.with_token(token);
        }
        Ok(std::sync::Arc::new(builder.build()?))
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl MediaResolver for S3CompatibleResolver {
    async fn resolve(
        &self,
        media: &MediaRef,
        _access: &AccessContext,
    ) -> Result<Arc<dyn MediaSource>> {
        let uri = Url::parse(&media.uri)
            .map_err(|_| StorageError::UnsupportedScheme(media.uri.clone()))?;
        if uri.scheme() != "s3" || uri.host_str() != Some(self.config.bucket()) {
            return Err(StorageError::UnsupportedScheme(
                "only the query-scoped configured s3 bucket is allowed".to_string(),
            ));
        }
        if uri.query().is_some() || uri.fragment().is_some() || uri.username() != "" {
            return Err(StorageError::InvalidS3Configuration(
                "s3 URI must not contain credentials, query parameters, or fragments".to_string(),
            ));
        }
        let key = uri.path().trim_start_matches('/');
        if key.is_empty() {
            return Err(StorageError::InvalidS3Configuration(
                "s3 URI must identify an object key".to_string(),
            ));
        }
        Ok(Arc::new(S3MediaSource {
            location: object_store::path::Path::from(key),
            store: self.store()?,
            resources: self.resources.clone(),
        }))
    }
}

#[cfg(feature = "s3")]
struct S3MediaSource {
    location: object_store::path::Path,
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    resources: ResourceManager,
}

#[cfg(feature = "s3")]
#[async_trait]
impl MediaSource for S3MediaSource {
    async fn metadata(&self) -> Result<SourceMetadata> {
        let _permit = self.resources.acquire_io_slot().await?;
        let metadata = self.store.head(&self.location).await?;
        self.resources.check_control()?;
        Ok(SourceMetadata {
            size_bytes: metadata.size as u64,
            version: metadata.e_tag,
        })
    }

    async fn read_range(&self, range: ByteRange) -> Result<Vec<u8>> {
        self.resources.validate_read_size(range.len())?;
        let _permit = self.resources.acquire_io_slot().await?;
        let bytes = self
            .store
            .get_range(&self.location, range.start..range.end_exclusive)
            .await?;
        self.resources.check_control()?;
        Ok(bytes.to_vec())
    }
}

/// Limits and destination for explicitly staged remote media.
///
/// Staging is intentionally opt-in: FFmpeg only receives a local path after
/// the object size is known and accepted. The destination must be
/// application-owned, private to the service account, and on storage with
/// enough capacity for the configured maximum.
#[cfg(feature = "s3")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaStagingConfig {
    pub max_source_bytes: u64,
    pub chunk_bytes: u64,
    pub directory: PathBuf,
}

#[cfg(feature = "s3")]
impl MediaStagingConfig {
    pub const DEFAULT_MAX_SOURCE_BYTES: u64 = 512 * 1024 * 1024;
    pub const DEFAULT_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

    pub fn new(
        max_source_bytes: u64,
        chunk_bytes: u64,
        directory: impl Into<PathBuf>,
    ) -> Result<Self> {
        if max_source_bytes == 0 {
            return Err(StorageError::InvalidS3Configuration(
                "remote media staging size cap must be greater than zero".to_string(),
            ));
        }
        if chunk_bytes == 0 || chunk_bytes > max_source_bytes {
            return Err(StorageError::InvalidS3Configuration(
                "remote media staging chunk size must be greater than zero and no larger than the size cap"
                    .to_string(),
            ));
        }
        Ok(Self {
            max_source_bytes,
            chunk_bytes,
            directory: directory.into(),
        })
    }
}

/// A locally staged remote object. Dropping the final clone removes the
/// private staging file; callers must not persist its path in a `MediaRef`.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct StagedMediaFile {
    file: Arc<StagedMediaFileInner>,
}

#[cfg(feature = "s3")]
struct StagedMediaFileInner {
    path: PathBuf,
}

#[cfg(feature = "s3")]
impl StagedMediaFile {
    pub fn path(&self) -> &std::path::Path {
        &self.file.path
    }
}

#[cfg(feature = "s3")]
impl Drop for StagedMediaFileInner {
    fn drop(&mut self) {
        // Best effort only: no object key, URI, or credentials are included in
        // cleanup diagnostics. A later service startup may safely sweep its
        // own private staging directory if the process was terminated.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Query-scoped S3 resolver plus a bounded, non-persistent local staging
/// policy for native decoders that cannot consume a custom async AVIO source.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct S3MediaStagingResolver {
    resolver: S3CompatibleResolver,
    config: MediaStagingConfig,
}

#[cfg(feature = "s3")]
impl S3MediaStagingResolver {
    pub fn new(resolver: S3CompatibleResolver, config: MediaStagingConfig) -> Self {
        Self { resolver, config }
    }

    /// Reads a known-size object in bounded ranges into a new owner-only file.
    ///
    /// The underlying S3 source acquires the shared `ResourceManager` I/O
    /// permit for every range and observes the query control before acquiring
    /// that permit and after the HTTP operation. This loop deliberately
    /// performs no full-object `get`.
    pub async fn stage(&self, media: &MediaRef, access: &AccessContext) -> Result<StagedMediaFile> {
        let source = self.resolver.resolve(media, access).await?;
        let metadata = source.metadata().await?;
        if metadata.size_bytes > self.config.max_source_bytes {
            return Err(StorageError::StagingSizeExceeded {
                source_bytes: metadata.size_bytes,
                maximum_bytes: self.config.max_source_bytes,
            });
        }

        ensure_private_staging_directory(&self.config.directory)?;
        let path = self
            .config
            .directory
            .join(format!("lakeprism-media-{}", uuid::Uuid::new_v4()));
        let file = create_private_staging_file(&path)?;
        let mut file = tokio::fs::File::from_std(file);
        let result = async {
            let mut start = 0_u64;
            while start < metadata.size_bytes {
                let end = start
                    .saturating_add(self.config.chunk_bytes)
                    .min(metadata.size_bytes);
                let range = ByteRange::new(start, end, metadata.size_bytes)?;
                let bytes = source.read_range(range).await?;
                if bytes.len() as u64 != end - start {
                    return Err(StorageError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "remote range response length did not match the requested range",
                    )));
                }
                tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
                start = end;
            }
            tokio::io::AsyncWriteExt::flush(&mut file).await?;
            Ok(())
        }
        .await;
        drop(file);
        if let Err(error) = result {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        Ok(StagedMediaFile {
            file: Arc::new(StagedMediaFileInner { path }),
        })
    }
}

#[cfg(feature = "s3")]
fn create_private_staging_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(feature = "s3")]
fn ensure_private_staging_directory(directory: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn control_state_result(state: QueryControlState) -> Result<()> {
    match state {
        QueryControlState::Active => Ok(()),
        QueryControlState::Cancelled => Err(StorageError::Cancelled),
        QueryControlState::DeadlineExceeded => Err(StorageError::DeadlineExceeded),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakeprism_core::StorageMode;
    use std::time::Duration;

    #[test]
    fn governor_rejects_zero_limits_and_reports_supported_controls() {
        let error = ExecutionGovernor::new(ExecutionGovernorConfig {
            max_concurrent_queries: 0,
            max_cpu_permits: 1,
            max_io_permits: 1,
        })
        .err()
        .unwrap();
        assert!(matches!(error, GovernorConfigError::ZeroConcurrentQueries));

        let governor = ExecutionGovernor::new(ExecutionGovernorConfig {
            max_concurrent_queries: 1,
            max_cpu_permits: 1,
            max_io_permits: 1,
        })
        .unwrap();
        assert_eq!(
            governor.capabilities(),
            ExecutionCapabilities {
                deadlines: ExecutionCapability::Supported,
                cancellation: ExecutionCapability::Supported,
            }
        );
    }

    #[tokio::test]
    async fn governor_bounds_query_slots_and_keeps_cpu_and_io_separate() {
        let governor = Arc::new(
            ExecutionGovernor::new(ExecutionGovernorConfig {
                max_concurrent_queries: 1,
                max_cpu_permits: 1,
                max_io_permits: 1,
            })
            .unwrap(),
        );
        let query_permit = governor.acquire_query().await;
        let waiting_governor = Arc::clone(&governor);
        let mut waiting_query = tokio::spawn(async move {
            waiting_governor.acquire_query().await;
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut waiting_query)
                .await
                .is_err()
        );
        let cpu_permit = governor.acquire_cpu().await;
        drop(cpu_permit);
        drop(query_permit);
        tokio::time::timeout(Duration::from_secs(1), waiting_query)
            .await
            .unwrap()
            .unwrap();

        let manager = ResourceManager::with_execution_governor(Arc::clone(&governor), 8).unwrap();
        let io_permit = manager.acquire_io_slot().await.unwrap();
        let waiting_manager = manager.clone();
        let mut waiting_io = tokio::spawn(async move {
            let _ = waiting_manager.acquire_io_slot().await.unwrap();
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut waiting_io)
                .await
                .is_err()
        );
        drop(io_permit);
        tokio::time::timeout(Duration::from_secs(1), waiting_io)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn local_resolver_reads_only_the_requested_range() {
        let path =
            std::env::temp_dir().join(format!("lakeprism-storage-{}.bin", std::process::id()));
        tokio::fs::write(&path, b"0123456789").await.unwrap();
        let media = MediaRef::new(
            Url::from_file_path(&path).unwrap().to_string(),
            "video",
            StorageMode::External,
        )
        .unwrap();
        let resolver = LocalResolver::new(ResourceManager::new(1, 4).unwrap());
        let source = resolver
            .resolve(&media, &AccessContext::default())
            .await
            .unwrap();

        assert_eq!(source.metadata().await.unwrap().size_bytes, 10);
        assert_eq!(
            source
                .read_range(ByteRange::new(2, 6, 10).unwrap())
                .await
                .unwrap(),
            b"2345"
        );
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn governed_io_permit_bounds_local_reads_and_preserves_bytes() {
        let governor = Arc::new(
            ExecutionGovernor::new(ExecutionGovernorConfig {
                max_concurrent_queries: 1,
                max_cpu_permits: 1,
                max_io_permits: 1,
            })
            .unwrap(),
        );
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let length = tokio::fs::metadata(&path).await.unwrap().len();
        let media = MediaRef::new(
            Url::from_file_path(&path).unwrap().to_string(),
            "document",
            StorageMode::External,
        )
        .unwrap();
        let resolver = LocalResolver::new(
            ResourceManager::with_execution_governor(Arc::clone(&governor), length).unwrap(),
        );
        let source = resolver
            .resolve(&media, &AccessContext::default())
            .await
            .unwrap();

        let held_io = governor.acquire_io().await;
        let waiting_source = Arc::clone(&source);
        let mut read = tokio::spawn(async move {
            waiting_source
                .read_range(ByteRange::new(0, 1, length).unwrap())
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut read)
                .await
                .is_err()
        );

        drop(held_io);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), read)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            b"["
        );
    }

    #[tokio::test]
    async fn resource_manager_rejects_reads_over_the_byte_budget() {
        let manager = ResourceManager::new(1, 3).unwrap();

        assert_eq!(
            manager.validate_read_size(4).unwrap_err().to_string(),
            "requested 4 bytes exceeds the 3-byte read budget"
        );
    }

    #[tokio::test]
    async fn controlled_io_wait_is_cancelled_without_consuming_a_permit() {
        let control = QueryControl::new(None);
        let manager = ResourceManager::new(1, 8)
            .unwrap()
            .with_query_control(control.clone());
        let held = manager.acquire_io_slot().await.unwrap();
        let waiting = manager.clone();
        let waiter = tokio::spawn(async move { waiting.acquire_io_slot().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        control.cancel();
        assert!(matches!(
            waiter.await.unwrap(),
            Err(StorageError::Cancelled)
        ));
        drop(held);
        assert!(matches!(
            manager.acquire_io_slot().await,
            Err(StorageError::Cancelled)
        ));
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn s3_resolver_uses_head_and_bounded_range_requests_against_a_local_mock() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/reports/sample.pdf"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "10")
                    .insert_header("etag", "\"version-1\""),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/test-bucket/reports/sample.pdf"))
            .and(header("range", "bytes=2-5"))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-range", "bytes 2-5/10")
                    .set_body_bytes(b"2345"),
            )
            .mount(&server)
            .await;

        let credentials = S3Credentials::new("test-access", "test-secret");
        let config =
            S3CompatibleConfig::new("test-bucket", "us-east-1", server.uri(), credentials).unwrap();
        let resolver = S3CompatibleResolver::new(config, ResourceManager::new(1, 4).unwrap());
        let media = MediaRef::new(
            "s3://test-bucket/reports/sample.pdf",
            "document",
            StorageMode::External,
        )
        .unwrap();
        let source = resolver
            .resolve(&media, &AccessContext::default())
            .await
            .unwrap();
        assert_eq!(
            source.metadata().await.unwrap(),
            SourceMetadata {
                size_bytes: 10,
                version: Some("\"version-1\"".to_string()),
            }
        );
        assert_eq!(
            source
                .read_range(ByteRange::new(2, 6, 10).unwrap())
                .await
                .unwrap(),
            b"2345"
        );
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn s3_media_staging_uses_bounded_ranges_and_removes_the_private_file() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/media/clip.mp4"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "10"))
            .mount(&server)
            .await;
        for (range, content_range, body) in [
            ("bytes=0-3", "bytes 0-3/10", b"0123".as_slice()),
            ("bytes=4-7", "bytes 4-7/10", b"4567".as_slice()),
            ("bytes=8-9", "bytes 8-9/10", b"89".as_slice()),
        ] {
            Mock::given(method("GET"))
                .and(path("/test-bucket/media/clip.mp4"))
                .and(header("range", range))
                .respond_with(
                    ResponseTemplate::new(206)
                        .insert_header("content-range", content_range)
                        .set_body_bytes(body),
                )
                .mount(&server)
                .await;
        }

        let resolver = S3CompatibleResolver::new(
            S3CompatibleConfig::new(
                "test-bucket",
                "us-east-1",
                server.uri(),
                S3Credentials::new("test-access", "test-secret"),
            )
            .unwrap(),
            ResourceManager::new(1, 4).unwrap(),
        );
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!(".lakeprism-stage-test-{}", uuid::Uuid::new_v4()));
        let stager = S3MediaStagingResolver::new(
            resolver,
            MediaStagingConfig::new(10, 4, &directory).unwrap(),
        );
        let media = MediaRef::new(
            "s3://test-bucket/media/clip.mp4",
            "video",
            StorageMode::External,
        )
        .unwrap();
        let staged = stager
            .stage(&media, &AccessContext::default())
            .await
            .unwrap();
        let staged_path = staged.path().to_path_buf();
        assert_eq!(tokio::fs::read(&staged_path).await.unwrap(), b"0123456789");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&staged_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        drop(staged);
        assert!(!staged_path.exists());
        tokio::fs::remove_dir(directory).await.unwrap();
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn s3_media_staging_rejects_objects_above_its_cap_before_get() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/test-bucket/media/large.mp4"))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "11"))
            .mount(&server)
            .await;
        let resolver = S3CompatibleResolver::new(
            S3CompatibleConfig::new(
                "test-bucket",
                "us-east-1",
                server.uri(),
                S3Credentials::new("test-access", "test-secret"),
            )
            .unwrap(),
            ResourceManager::new(1, 4).unwrap(),
        );
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(format!(".lakeprism-stage-test-{}", uuid::Uuid::new_v4()));
        let stager = S3MediaStagingResolver::new(
            resolver,
            MediaStagingConfig::new(10, 4, &directory).unwrap(),
        );
        let media = MediaRef::new(
            "s3://test-bucket/media/large.mp4",
            "video",
            StorageMode::External,
        )
        .unwrap();
        assert!(matches!(
            stager.stage(&media, &AccessContext::default()).await,
            Err(StorageError::StagingSizeExceeded {
                source_bytes: 11,
                maximum_bytes: 10
            })
        ));
        assert!(!directory.exists());
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_config_rejects_non_loopback_http_and_hides_credentials() {
        let error = S3CompatibleConfig::new(
            "bucket",
            "us-east-1",
            "http://example.com",
            S3Credentials::new("access", "secret"),
        )
        .err()
        .expect("public HTTP endpoint must be rejected");
        assert!(error.to_string().contains("HTTPS"));
    }
}
