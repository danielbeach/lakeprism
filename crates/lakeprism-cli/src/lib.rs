use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Array;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use lakeprism_catalog::{CatalogDdlResult, LocalCatalog};
use lakeprism_core::{MediaRef, StorageMode};
use lakeprism_datafusion::{MediaSession, QueryAuditEvent, QueryInfo};
use lakeprism_flight::FlightSqlServer;
use tonic::transport::Server;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputFormat {
    Json,
    Csv,
    Arrow,
}

impl OutputFormat {
    pub fn parse(value: &str) -> Result<Self, CliError> {
        match value {
            "json" => Ok(Self::Json),
            "csv" => Ok(Self::Csv),
            "arrow" | "ipc" => Ok(Self::Arrow),
            _ => Err(CliError::Usage(format!(
                "unknown output format {value:?}; use json, csv, or arrow"
            ))),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
    #[error(transparent)]
    Catalog(#[from] lakeprism_catalog::CatalogError),
    #[error(transparent)]
    Core(#[from] lakeprism_core::LakePrismError),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

pub struct LocalCli {
    catalog_root: PathBuf,
    catalog: LocalCatalog,
    session: Arc<MediaSession>,
}

impl LocalCli {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, CliError> {
        let catalog_root = root.as_ref().to_path_buf();
        let catalog = LocalCatalog::open(&catalog_root)?;
        let session = Arc::new(MediaSession::new());
        catalog.register_in_session(&session).await?;
        Ok(Self {
            catalog_root,
            catalog,
            session,
        })
    }

    pub fn catalog_root(&self) -> &Path {
        &self.catalog_root
    }

    pub fn session(&self) -> &Arc<MediaSession> {
        &self.session
    }

    async fn reload_session(&mut self) -> Result<(), CliError> {
        self.catalog.refresh()?;
        let session = Arc::new(MediaSession::new());
        self.catalog.register_in_session(&session).await?;
        self.session = session;
        Ok(())
    }

    pub async fn register_media(
        &mut self,
        table: &str,
        uri: &str,
        media_type: &str,
        storage_mode: StorageMode,
    ) -> Result<(), CliError> {
        let media = MediaRef::new(uri, media_type, storage_mode)?;
        let mut values = self
            .catalog
            .media_table(table)
            .map(|items| items.to_vec())
            .unwrap_or_default();
        values.push(media);
        if self.catalog.media_table(table).is_ok() {
            self.catalog.drop_table(table)?;
        }
        self.catalog.register_media_table(table, values)?;
        self.reload_session().await
    }

    pub async fn ddl(&mut self, statement: &str) -> Result<CatalogDdlResult, CliError> {
        let result = self.catalog.execute_ddl(statement)?;
        self.reload_session().await?;
        Ok(result)
    }

    pub async fn query_batches(&self, sql: &str) -> Result<(String, Vec<RecordBatch>), CliError> {
        let execution = self.session.execute_stream_with_deadline(sql, None).await?;
        let id = execution.id.clone();
        let batches = execution.stream.try_collect().await?;
        Ok((id, batches))
    }

    pub fn query_status(&self, id: &str) -> Option<QueryInfo> {
        self.session.query(id)
    }

    pub fn query_audit(&self, id: &str) -> Option<QueryAuditEvent> {
        self.session.query_audit_event(id)
    }

    pub fn cancel_query(&self, id: &str) -> bool {
        self.session.cancel_query(id)
    }

    pub async fn explain_media(&self, sql: &str) -> Result<Vec<RecordBatch>, CliError> {
        let explain = format!("EXPLAIN {sql}");
        Ok(self.session.collect(&explain).await?)
    }

    pub async fn serve_flight(&self, address: SocketAddr) -> Result<(), CliError> {
        if !address.ip().is_loopback() {
            return Err(CliError::Usage(
                "Flight is local-only; --addr must use a loopback IP address".to_owned(),
            ));
        }
        Server::builder()
            .add_service(
                arrow_flight::flight_service_server::FlightServiceServer::new(
                    FlightSqlServer::new(Arc::clone(&self.session)),
                ),
            )
            .serve(address)
            .await?;
        Ok(())
    }
}

pub fn default_catalog_root() -> PathBuf {
    PathBuf::from(".lakeprism")
}

pub fn parse_loopback_address(value: &str) -> Result<SocketAddr, CliError> {
    let address: SocketAddr = value
        .parse()
        .map_err(|_| CliError::Usage(format!("invalid socket address: {value}")))?;
    if !matches!(address.ip(), IpAddr::V4(ip) if ip.is_loopback())
        && !matches!(address.ip(), IpAddr::V6(ip) if ip.is_loopback())
    {
        return Err(CliError::Usage(
            "Flight is local-only; --addr must use a loopback IP address".to_owned(),
        ));
    }
    Ok(address)
}

pub fn write_batches(
    output: &mut impl Write,
    batches: &[RecordBatch],
    format: OutputFormat,
) -> Result<(), CliError> {
    match format {
        OutputFormat::Json => write_json(output, batches),
        OutputFormat::Csv => write_csv(output, batches),
        OutputFormat::Arrow => write_arrow(output, batches),
    }
}

fn value(batch: &RecordBatch, column: usize, row: usize) -> Option<String> {
    let array = batch.column(column);
    (!array.is_null(row))
        .then(|| arrow::util::display::array_value_to_string(array.as_ref(), row))
        .and_then(Result::ok)
}

fn write_json(output: &mut impl Write, batches: &[RecordBatch]) -> Result<(), CliError> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut object = serde_json::Map::new();
            for (column, field) in batch.schema().fields().iter().enumerate() {
                let value = value(batch, column, row)
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null);
                object.insert(field.name().to_owned(), value);
            }
            rows.push(serde_json::Value::Object(object));
        }
    }
    serde_json::to_writer(&mut *output, &rows)?;
    writeln!(output)?;
    Ok(())
}

fn csv_escape(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn write_csv(output: &mut impl Write, batches: &[RecordBatch]) -> Result<(), CliError> {
    let Some(first) = batches.first() else {
        return Ok(());
    };
    writeln!(
        output,
        "{}",
        first
            .schema()
            .fields()
            .iter()
            .map(|field| csv_escape(field.name()))
            .collect::<Vec<_>>()
            .join(",")
    )?;
    for batch in batches {
        for row in 0..batch.num_rows() {
            let line = (0..batch.num_columns())
                .map(|column| csv_escape(&value(batch, column, row).unwrap_or_default()))
                .collect::<Vec<_>>()
                .join(",");
            writeln!(output, "{line}")?;
        }
    }
    Ok(())
}

fn write_arrow(output: &mut impl Write, batches: &[RecordBatch]) -> Result<(), CliError> {
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    let mut writer = StreamWriter::try_new(output, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(())
}

pub fn format_status(info: &QueryInfo) -> String {
    format!(
        "id={}; status={}; batches={}; rows={}; elapsed_ms={}",
        info.id,
        info.status.as_str(),
        info.metrics.batches,
        info.metrics.rows,
        info.metrics.elapsed.as_millis()
    )
}

/// Formats the credential-safe query lifecycle projection as one JSON object.
pub fn format_audit(event: &QueryAuditEvent) -> serde_json::Value {
    serde_json::json!({
        "schema_version": event.schema_version,
        "event_type": event.event_type,
        "query_id": event.query_id,
        "status": event.status.as_str(),
        "created_at_unix_millis": event.created_at_unix_millis,
        "started_at_unix_millis": event.started_at_unix_millis,
        "completed_at_unix_millis": event.completed_at_unix_millis,
        "elapsed_millis": event.elapsed_millis,
        "batches": event.batches,
        "rows": event.rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use uuid::Uuid;

    fn root() -> PathBuf {
        let root = PathBuf::from("target")
            .join("lakeprism-cli-tests")
            .join(Uuid::new_v4().to_string());
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[tokio::test]
    async fn durable_catalog_registration_and_sql_survive_reopen() {
        let root = root();
        let mut cli = LocalCli::open(&root).await.unwrap();
        cli.register_media(
            "media",
            "file:///local/video.mp4",
            "video",
            StorageMode::External,
        )
        .await
        .unwrap();
        let (_, batches) = cli
            .query_batches("SELECT lakeprism_media_uri(media) AS uri FROM media")
            .await
            .unwrap();
        let mut output = Vec::new();
        write_batches(&mut output, &batches, OutputFormat::Json).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "[{\"uri\":\"file:///local/video.mp4\"}]\n"
        );
        drop(cli);

        let reopened = LocalCli::open(&root).await.unwrap();
        let (_, batches) = reopened
            .query_batches("SELECT count(*) AS count FROM media")
            .await
            .unwrap();
        let mut output = Vec::new();
        write_batches(&mut output, &batches, OutputFormat::Csv).unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "\"count\"\n\"1\"\n");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn output_formats_are_escaped_and_flight_stays_loopback() {
        assert!(parse_loopback_address("127.0.0.1:0").is_ok());
        assert!(parse_loopback_address("[::1]:0").is_ok());
        assert!(parse_loopback_address("0.0.0.0:5005").is_err());
        assert_eq!(csv_escape("a,\"b\""), "\"a,\"\"b\"\"\"");
    }
}
