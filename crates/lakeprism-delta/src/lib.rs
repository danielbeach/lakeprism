#[cfg(feature = "delta-rs")]
use std::collections::BTreeMap;

#[cfg(feature = "delta-rs")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "delta-rs")]
use thiserror::Error;

/// Explicit Delta save behavior; no implicit overwrite is possible.
#[cfg(feature = "delta-rs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaWriteMode {
    Create,
    Append,
    Overwrite,
}

/// Strict is the default and rejects schema drift. Evolution is opt-in.
#[cfg(feature = "delta-rs")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DeltaSchemaMode {
    #[default]
    Strict,
    Merge,
    Overwrite,
}

/// Credential-free audit metadata added to Delta's commitInfo.
#[cfg(feature = "delta-rs")]
#[derive(Clone, Default, Eq, PartialEq)]
pub struct DeltaCommitProperties {
    pub audit_metadata: BTreeMap<String, serde_json::Value>,
    /// `None` uses delta-rs' safe default; `Some(0)` disables its retry loop.
    pub max_commit_retries: Option<usize>,
}

#[cfg(feature = "delta-rs")]
#[derive(Clone, Eq, PartialEq)]
pub struct DeltaWriteOptions {
    pub mode: DeltaWriteMode,
    pub schema_mode: DeltaSchemaMode,
    pub partition_columns: Option<Vec<String>>,
    pub commit: DeltaCommitProperties,
}

#[cfg(feature = "delta-rs")]
impl Default for DeltaWriteOptions {
    fn default() -> Self {
        Self {
            mode: DeltaWriteMode::Append,
            schema_mode: DeltaSchemaMode::Strict,
            partition_columns: None,
            commit: DeltaCommitProperties::default(),
        }
    }
}

#[cfg(feature = "delta-rs")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaWriteResult {
    pub version: u64,
}

/// Explicit acknowledgement required for experimental remote Delta writes.
///
/// Delta log safety depends on atomic conditional creation of log objects.
/// Choose `SingleWriter` only when an external coordinator guarantees exactly
/// one active writer for this table. Choose `ConditionalPutVerified` only after
/// validating the deployed delta-rs/object-store backend against the target
/// bucket. This marker is intentionally required at every remote write call.
#[cfg(feature = "delta-rs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExperimentalRemoteWriteGuard {
    SingleWriter,
    ConditionalPutVerified,
}

/// Replay is only safe when the caller knows its input is idempotent.
#[cfg(feature = "delta-rs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryableCommitConflict {
    VersionAlreadyExists,
    ConcurrentAppend,
    ConcurrentDeleteRead,
    ConcurrentDeleteDelete,
    MetadataChanged,
    ConcurrentTransaction,
    ProtocolChanged,
}

#[cfg(feature = "delta-rs")]
#[derive(Debug, Error)]
pub enum DeltaAdapterError {
    #[error("Delta commit conflicted ({0:?}); refresh and retry only replay-safe input")]
    RetryableCommitConflict(RetryableCommitConflict),
    #[error("Delta write requires at least one RecordBatch")]
    EmptyWrite,
    #[error("all RecordBatches in a Delta write must have an identical Arrow schema")]
    InconsistentBatchSchemas,
    #[error("partition column `{0}` is absent from the Arrow schema")]
    UnknownPartitionColumn(String),
    #[error("schema overwrite is valid only with DeltaWriteMode::Overwrite")]
    InvalidSchemaMode,
    #[error("commit audit metadata must not contain credential-like keys or values")]
    UnsafeCommitMetadata,
    #[error("could not create local Delta table directory")]
    LocalDirectory(#[source] std::io::Error),
    #[error(transparent)]
    Delta(#[from] deltalake::errors::DeltaTableError),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

#[cfg(feature = "delta-rs")]
impl DeltaAdapterError {
    pub const fn is_retryable_commit_conflict(&self) -> bool {
        matches!(self, Self::RetryableCommitConflict(_))
    }
}

#[cfg(feature = "delta-rs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeltaReadCapabilities {
    pub deletion_vectors: bool,
}

#[cfg(feature = "delta-rs")]
pub const fn read_capabilities() -> DeltaReadCapabilities {
    DeltaReadCapabilities {
        deletion_vectors: true,
    }
}

/// Transactionally writes Arrow batches with delta-rs 1.1. The URI itself
/// must be credential-free; use [`write_s3_delta_table`] for query-scoped S3.
#[cfg(feature = "delta-rs")]
pub async fn write_delta_table(
    table_uri: impl AsRef<str>,
    batches: Vec<RecordBatch>,
    options: DeltaWriteOptions,
) -> Result<DeltaWriteResult, DeltaAdapterError> {
    write_at_url(parse_delta_uri(table_uri.as_ref())?, None, batches, options).await
}

/// Writes an application-owned in-memory Delta table. This is useful for
/// tests and ephemeral pipelines; persistence requires [`write_delta_table`].
#[cfg(feature = "delta-rs")]
pub async fn write_delta_memory_table(
    table: deltalake::DeltaTable,
    batches: Vec<RecordBatch>,
    options: DeltaWriteOptions,
) -> Result<(deltalake::DeltaTable, DeltaWriteResult), DeltaAdapterError> {
    write_to_table(table, batches, options).await
}

/// Retries only classified optimistic-concurrency conflicts. `attempts`
/// includes the first write and is made at least one.
#[cfg(feature = "delta-rs")]
pub async fn write_delta_table_with_retry(
    table_uri: impl AsRef<str>,
    batches: Vec<RecordBatch>,
    options: DeltaWriteOptions,
    attempts: usize,
) -> Result<DeltaWriteResult, DeltaAdapterError> {
    let url = parse_delta_uri(table_uri.as_ref())?;
    for attempt in 0..attempts.max(1) {
        match write_at_url(url.clone(), None, batches.clone(), options.clone()).await {
            Ok(result) => return Ok(result),
            Err(error) if error.is_retryable_commit_conflict() && attempt + 1 < attempts.max(1) => {
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("at least one write was attempted")
}

/// S3 credentials are converted to delta-rs options only at call time and are
/// neither persisted nor included in LakePrism diagnostics.
///
/// This experimental call requires an explicit safety acknowledgement.
/// `SingleWriter` requires an external single-writer coordinator;
/// `ConditionalPutVerified` requires live validation that the exact
/// delta-rs/object-store backend atomically conditionally creates Delta logs.
#[cfg(feature = "delta-rs")]
pub async fn write_s3_delta_table(
    table_uri: impl AsRef<str>,
    s3: &lakeprism_storage::S3CompatibleConfig,
    batches: Vec<RecordBatch>,
    options: DeltaWriteOptions,
    guard: ExperimentalRemoteWriteGuard,
) -> Result<DeltaWriteResult, DeltaAdapterError> {
    let _ = guard;
    write_at_url(
        parse_s3_delta_uri(table_uri.as_ref())?,
        Some(s3.delta_storage_options()),
        batches,
        options,
    )
    .await
}

#[cfg(feature = "delta-rs")]
async fn write_at_url(
    url: url::Url,
    storage_options: Option<std::collections::HashMap<String, String>>,
    batches: Vec<RecordBatch>,
    options: DeltaWriteOptions,
) -> Result<DeltaWriteResult, DeltaAdapterError> {
    validate_write(&batches, &options)?;
    if url.scheme() == "file" && options.mode == DeltaWriteMode::Create {
        let path = url.to_file_path().map_err(|_| {
            deltalake::errors::DeltaTableError::InvalidTableLocation(
                "invalid local Delta table URI".to_string(),
            )
        })?;
        std::fs::create_dir_all(path).map_err(DeltaAdapterError::LocalDirectory)?;
    }
    let table = match storage_options {
        Some(options) => {
            deltalake::DeltaTable::try_from_url_with_storage_options(url, options).await?
        }
        None => deltalake::DeltaTable::try_from_url(url).await?,
    };
    write_to_table(table, batches, options)
        .await
        .map(|(_, result)| result)
}

#[cfg(feature = "delta-rs")]
async fn write_to_table(
    table: deltalake::DeltaTable,
    batches: Vec<RecordBatch>,
    options: DeltaWriteOptions,
) -> Result<(deltalake::DeltaTable, DeltaWriteResult), DeltaAdapterError> {
    validate_write(&batches, &options)?;
    let commit = deltalake::kernel::transaction::CommitProperties::default()
        .with_metadata(options.commit.audit_metadata);
    let commit = match options.commit.max_commit_retries {
        Some(retries) => commit.with_max_retries(retries),
        None => commit,
    };
    let mut writer = table.write(batches).with_commit_properties(commit);
    writer = writer.with_save_mode(match options.mode {
        DeltaWriteMode::Create => deltalake::protocol::SaveMode::ErrorIfExists,
        DeltaWriteMode::Append => deltalake::protocol::SaveMode::Append,
        DeltaWriteMode::Overwrite => deltalake::protocol::SaveMode::Overwrite,
    });
    if let Some(columns) = options.partition_columns {
        writer = writer.with_partition_columns(columns);
    }
    writer = match options.schema_mode {
        DeltaSchemaMode::Strict => writer,
        DeltaSchemaMode::Merge => {
            writer.with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
        }
        DeltaSchemaMode::Overwrite => {
            writer.with_schema_mode(deltalake::operations::write::SchemaMode::Overwrite)
        }
    };
    let table = writer.await.map_err(classify_write_error)?;
    let result = DeltaWriteResult {
        version: table.version().unwrap_or_default(),
    };
    Ok((table, result))
}

#[cfg(feature = "delta-rs")]
fn validate_write(
    batches: &[RecordBatch],
    options: &DeltaWriteOptions,
) -> Result<(), DeltaAdapterError> {
    let Some(first) = batches.first() else {
        return Err(DeltaAdapterError::EmptyWrite);
    };
    if batches
        .iter()
        .skip(1)
        .any(|batch| batch.schema() != first.schema())
    {
        return Err(DeltaAdapterError::InconsistentBatchSchemas);
    }
    if options.schema_mode == DeltaSchemaMode::Overwrite
        && options.mode != DeltaWriteMode::Overwrite
    {
        return Err(DeltaAdapterError::InvalidSchemaMode);
    }
    if let Some(columns) = &options.partition_columns {
        for column in columns {
            if first.schema().column_with_name(column).is_none() {
                return Err(DeltaAdapterError::UnknownPartitionColumn(column.clone()));
            }
        }
    }
    if options.commit.audit_metadata.iter().any(|(key, value)| {
        let value = value.to_string();
        is_credential_like(key) || is_credential_like(&value) || value.len() > 4_096
    }) {
        return Err(DeltaAdapterError::UnsafeCommitMetadata);
    }
    Ok(())
}

#[cfg(feature = "delta-rs")]
fn is_credential_like(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "credential",
        "access_key",
        "authorization",
    ]
    .iter()
    .any(|needle| value.contains(needle))
}

#[cfg(feature = "delta-rs")]
fn classify_write_error(error: deltalake::errors::DeltaTableError) -> DeltaAdapterError {
    use deltalake::kernel::transaction::{CommitConflictError, TransactionError};
    let kind = match &error {
        deltalake::errors::DeltaTableError::VersionAlreadyExists(_) => {
            Some(RetryableCommitConflict::VersionAlreadyExists)
        }
        deltalake::errors::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(source),
        } => Some(match source {
            CommitConflictError::ConcurrentAppend => RetryableCommitConflict::ConcurrentAppend,
            CommitConflictError::ConcurrentDeleteRead => {
                RetryableCommitConflict::ConcurrentDeleteRead
            }
            CommitConflictError::ConcurrentDeleteDelete => {
                RetryableCommitConflict::ConcurrentDeleteDelete
            }
            CommitConflictError::MetadataChanged => RetryableCommitConflict::MetadataChanged,
            CommitConflictError::ConcurrentTransaction => {
                RetryableCommitConflict::ConcurrentTransaction
            }
            CommitConflictError::ProtocolChanged(_) => RetryableCommitConflict::ProtocolChanged,
            _ => return DeltaAdapterError::Delta(error),
        }),
        _ => None,
    };
    kind.map_or(
        DeltaAdapterError::Delta(error),
        DeltaAdapterError::RetryableCommitConflict,
    )
}

#[cfg(feature = "delta-rs")]
pub async fn register_delta_table(
    session: &lakeprism_datafusion::MediaSession,
    table_name: &str,
    table_uri: impl AsRef<str>,
) -> Result<(), DeltaAdapterError> {
    let table = deltalake::open_table(parse_delta_uri(table_uri.as_ref())?).await?;
    session.register_table_provider(table_name, table.table_provider().await?)?;
    Ok(())
}

#[cfg(feature = "delta-rs")]
pub async fn register_delta_table_in_schema(
    session: &lakeprism_datafusion::MediaSession,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    table_uri: impl AsRef<str>,
) -> Result<(), DeltaAdapterError> {
    let table = deltalake::open_table(parse_delta_uri(table_uri.as_ref())?).await?;
    session.register_table_provider_in_schema(
        catalog_name,
        schema_name,
        table_name,
        table.table_provider().await?,
    )?;
    Ok(())
}

#[cfg(feature = "delta-rs")]
pub async fn register_delta_table_version(
    session: &lakeprism_datafusion::MediaSession,
    table_name: &str,
    table_uri: impl AsRef<str>,
    version: u64,
) -> Result<(), DeltaAdapterError> {
    let mut table = deltalake::open_table(parse_delta_uri(table_uri.as_ref())?).await?;
    table.load_version(version).await?;
    session.register_table_provider(table_name, table.table_provider().await?)?;
    Ok(())
}

#[cfg(feature = "delta-rs")]
pub async fn register_s3_delta_table(
    session: &lakeprism_datafusion::MediaSession,
    table_name: &str,
    table_uri: impl AsRef<str>,
    s3: &lakeprism_storage::S3CompatibleConfig,
) -> Result<(), DeltaAdapterError> {
    let table = deltalake::open_table_with_storage_options(
        parse_s3_delta_uri(table_uri.as_ref())?,
        s3.delta_storage_options(),
    )
    .await?;
    session.register_table_provider(table_name, table.table_provider().await?)?;
    Ok(())
}

#[cfg(feature = "delta-rs")]
pub async fn register_s3_delta_table_version(
    session: &lakeprism_datafusion::MediaSession,
    table_name: &str,
    table_uri: impl AsRef<str>,
    version: u64,
    s3: &lakeprism_storage::S3CompatibleConfig,
) -> Result<(), DeltaAdapterError> {
    let mut table = deltalake::open_table_with_storage_options(
        parse_s3_delta_uri(table_uri.as_ref())?,
        s3.delta_storage_options(),
    )
    .await?;
    table.load_version(version).await?;
    session.register_table_provider(table_name, table.table_provider().await?)?;
    Ok(())
}

#[cfg(feature = "delta-rs")]
pub async fn register_s3_delta_table_in_schema(
    session: &lakeprism_datafusion::MediaSession,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    table_uri: impl AsRef<str>,
    s3: &lakeprism_storage::S3CompatibleConfig,
) -> Result<(), DeltaAdapterError> {
    let table = deltalake::open_table_with_storage_options(
        parse_s3_delta_uri(table_uri.as_ref())?,
        s3.delta_storage_options(),
    )
    .await?;
    session.register_table_provider_in_schema(
        catalog_name,
        schema_name,
        table_name,
        table.table_provider().await?,
    )?;
    Ok(())
}

#[cfg(feature = "delta-rs")]
fn parse_delta_uri(uri: &str) -> Result<url::Url, DeltaAdapterError> {
    let url = url::Url::parse(uri).map_err(|error| {
        deltalake::errors::DeltaTableError::InvalidTableLocation(error.to_string())
    })?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(deltalake::errors::DeltaTableError::InvalidTableLocation(
            "Delta table URI must not contain credentials, query parameters, or fragments"
                .to_string(),
        )
        .into());
    }
    Ok(url)
}

#[cfg(feature = "delta-rs")]
fn parse_s3_delta_uri(uri: &str) -> Result<url::Url, DeltaAdapterError> {
    let url = parse_delta_uri(uri)?;
    if url.scheme() != "s3" || url.host_str().is_none() {
        return Err(deltalake::errors::DeltaTableError::InvalidTableLocation(
            "remote Delta table URI must be credential-free s3://bucket/path".to_string(),
        )
        .into());
    }
    Ok(url)
}

#[cfg(all(test, feature = "delta-rs"))]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use deltalake::operations::collect_sendable_stream;
    use std::{path::PathBuf, sync::Arc};
    use uuid::Uuid;

    fn batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    }
    fn drift() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["drift"]))],
        )
        .unwrap()
    }
    fn uri(name: &str) -> (PathBuf, String) {
        let path = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!("lakeprism-delta-{name}-{}", Uuid::new_v4()));
        (
            path.clone(),
            url::Url::from_directory_path(path).unwrap().to_string(),
        )
    }
    async fn rows(uri: &str) -> usize {
        let table = deltalake::open_table(url::Url::parse(uri).unwrap())
            .await
            .unwrap();
        let (_, stream) = table.scan_table().await.unwrap();
        collect_sendable_stream(stream)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    #[tokio::test]
    async fn local_and_memory_create_append_read_round_trip() {
        let (path, local) = uri("roundtrip");
        write_delta_table(
            &local,
            vec![batch(vec![1, 2])],
            DeltaWriteOptions {
                mode: DeltaWriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        write_delta_table(&local, vec![batch(vec![3])], DeltaWriteOptions::default())
            .await
            .unwrap();
        assert_eq!(rows(&local).await, 3);
        let (table, _) = write_delta_memory_table(
            deltalake::DeltaTable::new_in_memory(),
            vec![batch(vec![1, 2])],
            DeltaWriteOptions {
                mode: DeltaWriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (table, _) =
            write_delta_memory_table(table, vec![batch(vec![3])], DeltaWriteOptions::default())
                .await
                .unwrap();
        let (_, stream) = table.scan_table().await.unwrap();
        assert_eq!(
            collect_sendable_stream(stream)
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn overwrite_schema_drift_and_append_only_behavior() {
        let (path, local) = uri("validation");
        write_delta_table(
            &local,
            vec![batch(vec![1, 2])],
            DeltaWriteOptions {
                mode: DeltaWriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        write_delta_table(
            &local,
            vec![batch(vec![9])],
            DeltaWriteOptions {
                mode: DeltaWriteMode::Overwrite,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(rows(&local).await, 1);
        assert!(
            write_delta_table(&local, vec![drift()], DeltaWriteOptions::default())
                .await
                .is_err()
        );
        let append_only = deltalake::DeltaTable::new_in_memory()
            .create()
            .with_columns(vec![deltalake::kernel::StructField::new(
                "id".to_string(),
                deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Integer),
                false,
            )])
            .with_configuration([("delta.appendOnly", Some("true"))])
            .await
            .unwrap();
        let (append_only, _) = write_delta_memory_table(
            append_only,
            vec![batch(vec![1])],
            DeltaWriteOptions::default(),
        )
        .await
        .unwrap();
        assert!(
            write_delta_memory_table(
                append_only,
                vec![batch(vec![2])],
                DeltaWriteOptions {
                    mode: DeltaWriteMode::Overwrite,
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn concurrent_append_is_safe_to_retry() {
        let (path, local) = uri("concurrent");
        write_delta_table(
            &local,
            vec![batch(vec![0])],
            DeltaWriteOptions {
                mode: DeltaWriteMode::Create,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (left, right) = tokio::join!(
            write_delta_table_with_retry(
                &local,
                vec![batch(vec![1])],
                DeltaWriteOptions::default(),
                3
            ),
            write_delta_table_with_retry(
                &local,
                vec![batch(vec![2])],
                DeltaWriteOptions::default(),
                3
            )
        );
        left.unwrap();
        right.unwrap();
        assert_eq!(rows(&local).await, 3);
        assert!(
            DeltaAdapterError::RetryableCommitConflict(
                RetryableCommitConflict::VersionAlreadyExists
            )
            .is_retryable_commit_conflict()
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}
