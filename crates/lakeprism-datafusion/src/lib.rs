use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use arrow::array::{
    Array, ArrayRef, BinaryArray, Float32Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "native-media")]
use datafusion::catalog::Session;
use datafusion::catalog::{CatalogProvider, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, Expr, Volatility, create_udf};
#[cfg(feature = "native-media")]
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown, TableType};
#[cfg(feature = "native-media")]
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionContext};
use datafusion::scalar::ScalarValue;
use futures::Stream;
use lakeprism_arrow::{media_ref_record_batch, media_ref_schema};
use lakeprism_core::{AccessContext, ByteRange, MediaRef};
use lakeprism_documents::{
    DocumentImage, DocumentSection, extract_docx_images_with_limits,
    extract_docx_sections_from_bytes_with_limits, extract_docx_sections_with_limits,
    extract_docx_tables_with_limits, extract_pdf_images_with_limits,
    extract_pdf_sections_from_bytes_with_limits, extract_pdf_sections_with_limits,
    extract_pdf_tables_with_limits,
};
use lakeprism_index::{
    CrossModalIndex, CrossModalRecord, CrossModalResult, EmbeddingIndex, EmbeddingProvider,
    EmbeddingRecord, EmbeddingRequest, IndexError, MediaSearchExplanation, MediaTranscriptSearch,
    MediaTranscriptSearchRequest, OcrProvider, OcrRequest, ProgressiveScheduleLimits, RankedResult,
    RankingOptions, TranscriptIndex, TranscriptSegment, TranscriptionProvider,
    TranscriptionRequest, UnavailableEmbeddingProvider, UnavailableOcrProvider,
    UnavailableTranscriptionProvider, run_ocr,
};
#[cfg(feature = "native-media")]
use lakeprism_media::{decode_audio_segments, decode_video_frames};
use lakeprism_storage::{
    CpuPermit, ExecutionGovernor, ExecutionGovernorConfig, GovernorConfigError, MediaResolver,
    QueryControl, QueryControlState, QueryPermit,
};
#[cfg(feature = "remote-media-s3")]
use lakeprism_storage::{S3MediaStagingResolver, StagedMediaFile};
use url::Url;
use uuid::Uuid;

/// Returns the URI field from a portable LakePrism `media` struct.
pub const MEDIA_URI_FUNCTION: &str = "lakeprism_media_uri";
/// Returns the media-type field from a portable LakePrism `media` struct.
pub const MEDIA_TYPE_FUNCTION: &str = "lakeprism_media_type";
/// Returns local PDF pages or DOCX paragraphs for a literal `file://` URI.
pub const DOCUMENT_SECTIONS_FUNCTION: &str = "lakeprism_document_sections";
/// Returns normalized DOCX table cells from a local document.
pub const DOCUMENT_TABLES_FUNCTION: &str = "lakeprism_document_tables";
/// Returns real embedded DOCX image bytes and metadata from a local document.
pub const DOCUMENT_IMAGES_FUNCTION: &str = "lakeprism_document_images";
/// Searches bounded document text, DOCX table cells, and image names.
pub const DOCUMENT_SEARCH_FUNCTION: &str = "lakeprism_document_search";
/// Explicitly reports that no OCR engine is compiled into this build.
pub const DOCUMENT_OCR_FUNCTION: &str = "lakeprism_document_ocr";
/// Returns bounded, native-decoded RGB24 video frames from a local URI.
pub const VIDEO_FRAMES_FUNCTION: &str = "lakeprism_video_frames";
/// Returns bounded timeline intervals from a local audio stream.
pub const AUDIO_SEGMENTS_FUNCTION: &str = "lakeprism_audio_segments";
/// Searches session-registered transcript index rows without cold extraction.
pub const TRANSCRIPT_SEARCH_FUNCTION: &str = "lakeprism_transcript_search";
/// Ranks compatible registered embeddings with explicit ranked/approximate output.
pub const SEMANTIC_SEARCH_FUNCTION: &str = "lakeprism_semantic_search";
/// Combines deterministic keyword and semantic scores for registered embeddings.
pub const HYBRID_SEARCH_FUNCTION: &str = "lakeprism_hybrid_search";

#[cfg(feature = "native-media")]
const MAX_MEDIA_INPUT_BYTES: u64 = 1 << 30;
const MAX_VIDEO_FRAMES: usize = 32;
const MAX_AUDIO_SEGMENTS: usize = 1_024;
const MAX_TRANSCRIPT_RESULTS: usize = 1_024;

/// Bounded decoding parameters for a MediaRef-backed local SQL relation.
///
/// DataFusion 55 table functions receive logical expressions rather than
/// input rows, so they cannot safely expand a `media` column into rows. Use
/// `register_video_frames` or `register_audio_chunks` to bind a credential-free
/// `MediaRef` to a normal, optimizer-visible SQL table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalMediaDecodeOptions {
    pub start_millis: u64,
    pub end_millis: u64,
    pub interval_millis: u64,
    pub limit: usize,
    pub include_payload: bool,
}

impl LocalMediaDecodeOptions {
    fn validate(&self, function_name: &str, maximum_limit: usize) -> Result<()> {
        if self.interval_millis == 0 || self.limit == 0 || self.end_millis < self.start_millis {
            return Err(DataFusionError::Plan(format!(
                "{function_name} requires a non-zero interval and limit, and end_millis >= start_millis"
            )));
        }
        if self.limit > maximum_limit {
            return Err(DataFusionError::Plan(format!(
                "{function_name} limit must not exceed {maximum_limit}"
            )));
        }
        Ok(())
    }
}

pub struct MediaSession {
    context: SessionContext,
    execution_governor: Arc<ExecutionGovernor>,
    transcript_index: Arc<Mutex<TranscriptIndex>>,
    media_transcript_search: Arc<Mutex<MediaTranscriptSearch>>,
    embedding_index: Arc<Mutex<EmbeddingIndex>>,
    embedding_lineage: Arc<Mutex<Option<lakeprism_core::FeatureLineage>>>,
    cross_modal_index: Arc<Mutex<CrossModalIndex>>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    transcription_provider: Arc<dyn TranscriptionProvider>,
    queries: Arc<Mutex<HashMap<String, QueryRecord>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    DeadlineExceeded,
}

impl QueryStatus {
    /// Stable, credential-free status name for local metrics and audit output.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryMetrics {
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub completed_at: Option<SystemTime>,
    pub elapsed: Duration,
    pub batches: u64,
    pub rows: u64,
}

#[derive(Clone, Debug)]
pub struct QueryInfo {
    pub id: String,
    pub status: QueryStatus,
    pub metrics: QueryMetrics,
}

/// A structured local audit projection of a query lifecycle record.
///
/// This deliberately excludes SQL text, media URIs, principals, catalog
/// identities, request metadata, and error text. It is safe to emit to a
/// local log or metrics sink without turning query inputs into telemetry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAuditEvent {
    pub schema_version: u16,
    pub event_type: &'static str,
    pub query_id: String,
    pub status: QueryStatus,
    pub created_at_unix_millis: u128,
    pub started_at_unix_millis: Option<u128>,
    pub completed_at_unix_millis: Option<u128>,
    pub elapsed_millis: u128,
    pub batches: u64,
    pub rows: u64,
}

#[derive(Clone)]
struct QueryRecord {
    control: QueryControl,
    status: QueryStatus,
    metrics: QueryMetrics,
}

pub struct QueryExecution {
    pub id: String,
    pub stream: SendableRecordBatchStream,
}

fn unix_millis(time: SystemTime) -> u128 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

struct GovernedRecordBatchStream {
    stream: SendableRecordBatchStream,
    _query_permit: QueryPermit,
    _cpu_permit: CpuPermit,
    id: String,
    queries: Arc<Mutex<HashMap<String, QueryRecord>>>,
    control: QueryControl,
    completed: bool,
}

impl Stream for GovernedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.control.state() {
            QueryControlState::Cancelled => {
                this.finish(QueryStatus::Cancelled);
                return Poll::Ready(Some(Err(DataFusionError::Execution(
                    "query was cancelled".to_string(),
                ))));
            }
            QueryControlState::DeadlineExceeded => {
                this.finish(QueryStatus::DeadlineExceeded);
                return Poll::Ready(Some(Err(DataFusionError::Execution(
                    "query deadline was exceeded".to_string(),
                ))));
            }
            QueryControlState::Active => {}
        }
        match this.stream.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(batch))) => {
                if let Ok(mut queries) = this.queries.lock()
                    && let Some(record) = queries.get_mut(&this.id)
                {
                    record.metrics.batches += 1;
                    record.metrics.rows += batch.num_rows() as u64;
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.finish(QueryStatus::Failed);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.finish(QueryStatus::Succeeded);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl GovernedRecordBatchStream {
    fn finish(&mut self, status: QueryStatus) {
        if self.completed {
            return;
        }
        self.completed = true;
        if let Ok(mut queries) = self.queries.lock()
            && let Some(record) = queries.get_mut(&self.id)
        {
            record.status = status;
            record.metrics.completed_at = Some(SystemTime::now());
            record.metrics.elapsed = record
                .metrics
                .started_at
                .and_then(|start| start.elapsed().ok())
                .unwrap_or_default();
        }
    }
}

impl Drop for GovernedRecordBatchStream {
    fn drop(&mut self) {
        if !self.completed {
            self.finish(match self.control.state() {
                QueryControlState::DeadlineExceeded => QueryStatus::DeadlineExceeded,
                QueryControlState::Active | QueryControlState::Cancelled => QueryStatus::Cancelled,
            });
        }
    }
}

impl RecordBatchStream for GovernedRecordBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.stream.schema()
    }
}

#[derive(Debug, Clone)]
pub struct CatalogTable {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_type: String,
    pub schema: arrow::datatypes::SchemaRef,
}

impl MediaSession {
    pub fn new() -> Self {
        Self::with_execution_governor_and_providers(
            Arc::new(
                ExecutionGovernor::new(ExecutionGovernorConfig::default())
                    .expect("default execution governor configuration is valid"),
            ),
            Arc::new(UnavailableEmbeddingProvider),
            Arc::new(UnavailableTranscriptionProvider),
            Arc::new(UnavailableOcrProvider),
        )
    }

    pub fn try_with_execution_governor(
        config: ExecutionGovernorConfig,
    ) -> std::result::Result<Self, GovernorConfigError> {
        Ok(Self::with_execution_governor(Arc::new(
            ExecutionGovernor::new(config)?,
        )))
    }

    pub fn with_execution_governor(execution_governor: Arc<ExecutionGovernor>) -> Self {
        Self::with_execution_governor_and_providers(
            execution_governor,
            Arc::new(UnavailableEmbeddingProvider),
            Arc::new(UnavailableTranscriptionProvider),
            Arc::new(UnavailableOcrProvider),
        )
    }

    /// Installs an application-owned embedding provider for this local session.
    /// The default constructor uses an explicit unavailable provider and never
    /// attempts model execution.
    pub fn with_embedding_provider(embedding_provider: Arc<dyn EmbeddingProvider>) -> Self {
        Self::with_embedding_provider_and_governor(
            Arc::new(
                ExecutionGovernor::new(ExecutionGovernorConfig::default())
                    .expect("default execution governor configuration is valid"),
            ),
            embedding_provider,
        )
    }

    /// Installs an application-owned embedding provider with the same
    /// governor that will constrain provider work and query execution.
    pub fn with_embedding_provider_and_governor(
        execution_governor: Arc<ExecutionGovernor>,
        embedding_provider: Arc<dyn EmbeddingProvider>,
    ) -> Self {
        Self::with_execution_governor_and_providers(
            execution_governor,
            embedding_provider,
            Arc::new(UnavailableTranscriptionProvider),
            Arc::new(UnavailableOcrProvider),
        )
    }

    /// Installs an application-owned bounded transcription provider for the
    /// explicit media-aware search API. Ordinary SQL transcript search remains
    /// persisted-index-only and never initiates cold work.
    pub fn with_transcription_provider(
        transcription_provider: Arc<dyn TranscriptionProvider>,
    ) -> Self {
        Self::with_transcription_provider_and_governor(
            Arc::new(
                ExecutionGovernor::new(ExecutionGovernorConfig::default())
                    .expect("default execution governor configuration is valid"),
            ),
            transcription_provider,
        )
    }

    /// Installs an application-owned bounded transcription provider with the
    /// same governor that constrains provider work and query execution.
    pub fn with_transcription_provider_and_governor(
        execution_governor: Arc<ExecutionGovernor>,
        transcription_provider: Arc<dyn TranscriptionProvider>,
    ) -> Self {
        Self::with_execution_governor_and_providers(
            execution_governor,
            Arc::new(UnavailableEmbeddingProvider),
            transcription_provider,
            Arc::new(UnavailableOcrProvider),
        )
    }

    /// Installs an application-owned OCR provider for the bounded local
    /// document OCR UDTF. The default provider returns ProviderUnavailable.
    pub fn with_ocr_provider(ocr_provider: Arc<dyn OcrProvider>) -> Self {
        Self::with_execution_governor_and_providers(
            Arc::new(
                ExecutionGovernor::new(ExecutionGovernorConfig::default())
                    .expect("default execution governor configuration is valid"),
            ),
            Arc::new(UnavailableEmbeddingProvider),
            Arc::new(UnavailableTranscriptionProvider),
            ocr_provider,
        )
    }

    fn with_execution_governor_and_providers(
        execution_governor: Arc<ExecutionGovernor>,
        embedding_provider: Arc<dyn EmbeddingProvider>,
        transcription_provider: Arc<dyn TranscriptionProvider>,
        ocr_provider: Arc<dyn OcrProvider>,
    ) -> Self {
        let context = SessionContext::new();
        let transcript_index = Arc::new(Mutex::new(TranscriptIndex::default()));
        let media_transcript_search = Arc::new(Mutex::new(MediaTranscriptSearch::new(Arc::clone(
            &transcription_provider,
        ))));
        let embedding_index = Arc::new(Mutex::new(EmbeddingIndex::default()));
        let embedding_lineage = Arc::new(Mutex::new(None));
        let cross_modal_index = Arc::new(Mutex::new(CrossModalIndex::default()));
        register_lakeprism_extensions(
            &context,
            Arc::clone(&execution_governor),
            Arc::clone(&transcript_index),
            Arc::clone(&embedding_index),
            Arc::clone(&embedding_lineage),
            Arc::clone(&embedding_provider),
            ocr_provider,
        );
        Self {
            context,
            execution_governor,
            transcript_index,
            media_transcript_search,
            embedding_index,
            embedding_lineage,
            cross_modal_index,
            embedding_provider,
            transcription_provider,
            queries: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn execution_governor(&self) -> &Arc<ExecutionGovernor> {
        &self.execution_governor
    }

    pub async fn register_media_refs(
        &self,
        table_name: &str,
        media_refs: &[MediaRef],
    ) -> Result<()> {
        let batch = media_ref_record_batch(media_refs).map_err(|error| {
            datafusion::error::DataFusionError::ArrowError(Box::new(error), None)
        })?;
        let table = MemTable::try_new(media_ref_schema(), vec![vec![batch]])?;
        self.context.register_table(table_name, Arc::new(table))?;
        Ok(())
    }

    pub async fn sql(&self, query: &str) -> Result<DataFrame> {
        self.context.sql(query).await
    }

    pub async fn register_parquet(&self, table_name: &str, table_path: &str) -> Result<()> {
        self.context
            .register_parquet(table_name, table_path, ParquetReadOptions::default())
            .await
    }

    /// Registers a lazy Parquet relation beneath an explicit catalog and
    /// schema. This is used by catalog adapters that must preserve qualified
    /// notebook names rather than flattening them into local aliases.
    pub async fn register_parquet_in_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_path: &str,
    ) -> Result<()> {
        let provider = self
            .context
            .read_parquet(table_path, ParquetReadOptions::default())
            .await?
            .into_view();
        self.register_table_provider_in_schema(catalog_name, schema_name, table_name, provider)
    }

    /// Fetches one bounded remote PDF or DOCX through an application-supplied
    /// range-capable resolver, then exposes its sections as an ordinary SQL
    /// table. This is deliberately explicit because DataFusion table functions
    /// are synchronous at planning time and cannot safely own request-scoped
    /// credentials.
    ///
    /// The entire object is held in memory only after its resolver metadata is
    /// checked against `DocumentLimits::max_input_bytes`; this is genuine remote
    /// read support, not an unbounded URL fetch. Media decoding UDTFs remain
    /// local-only because FFmpeg's current path API cannot consume a bounded
    /// `MediaSource` without staging or a custom AVIO bridge.
    pub async fn register_remote_document_sections(
        &self,
        table_name: &str,
        media: &MediaRef,
        access: &AccessContext,
        resolver: &dyn MediaResolver,
    ) -> Result<()> {
        let limits = lakeprism_documents::DocumentLimits::default();
        let source = resolver
            .resolve(media, access)
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let metadata = source
            .metadata()
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        if metadata.size_bytes > limits.max_input_bytes {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "remote document is {} bytes, exceeding the {}-byte limit",
                metadata.size_bytes, limits.max_input_bytes
            )));
        }
        let bytes = source
            .read_range(
                ByteRange::new(0, metadata.size_bytes, metadata.size_bytes)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?,
            )
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let extension = Url::parse(&media.uri).ok().and_then(|uri| {
            std::path::Path::new(uri.path())
                .extension()
                .and_then(|extension| extension.to_str())
                .map(str::to_ascii_lowercase)
        });
        let sections = match extension.as_deref() {
            Some("pdf") => extract_pdf_sections_from_bytes_with_limits(&bytes, limits),
            Some("docx") => extract_docx_sections_from_bytes_with_limits(&bytes, limits),
            _ => {
                return Err(DataFusionError::Plan(
                    "remote document sections supports only .pdf and .docx objects".to_string(),
                ));
            }
        }
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let version = metadata
            .version
            .unwrap_or_else(|| format!("size:{}", metadata.size_bytes));
        let batch = document_sections_record_batch(&media.uri, &version, &sections)?;
        self.register_table_provider(
            table_name,
            Arc::new(MemTable::try_new(
                document_sections_schema(),
                vec![vec![batch]],
            )?),
        )
    }

    pub fn register_table_provider(
        &self,
        table_name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        self.context.register_table(table_name, table)?;
        Ok(())
    }

    /// Registers a provider beneath an explicit catalog and schema. Catalog
    /// metadata is process-local and credential-free; providers retain their
    /// own execution-time access boundary.
    pub fn register_table_provider_in_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        use datafusion::catalog::{
            CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
        };

        let catalog = match self.context.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => {
                let catalog: Arc<dyn CatalogProvider> = Arc::new(MemoryCatalogProvider::new());
                self.context
                    .register_catalog(catalog_name, Arc::clone(&catalog));
                catalog
            }
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => {
                let schema: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
                catalog.register_schema(schema_name, Arc::clone(&schema))?;
                schema
            }
        };
        schema.register_table(table_name.to_owned(), table)?;
        Ok(())
    }

    /// Registers a catalog provider under a DataFusion catalog name. The
    /// provider owns resolution; this session retains no credentials.
    pub fn register_catalog_provider(
        &self,
        catalog_name: &str,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Result<()> {
        self.context.register_catalog(catalog_name, catalog);
        Ok(())
    }

    /// Registers a MediaRef-bound local video relation. FFmpeg decoding occurs
    /// only when DataFusion scans the relation, not during registration or SQL
    /// planning. The provider receives projection and SQL `LIMIT` pushdown.
    pub fn register_video_frames(
        &self,
        table_name: &str,
        media: &MediaRef,
        options: LocalMediaDecodeOptions,
    ) -> Result<()> {
        options.validate(VIDEO_FRAMES_FUNCTION, MAX_VIDEO_FRAMES)?;
        self.register_table_provider(
            table_name,
            video_frames_provider(
                &media.uri,
                options.start_millis,
                options.end_millis,
                options.interval_millis,
                options.limit,
                options.include_payload,
                Arc::clone(&self.execution_governor),
            )?,
        )
    }

    /// Registers a MediaRef-bound local normalized-audio relation. Each row
    /// contains mono 16 kHz f32-le samples when the query projects `f32le`;
    /// metadata-only projections do not materialize payload bytes.
    pub fn register_audio_chunks(
        &self,
        table_name: &str,
        media: &MediaRef,
        options: LocalMediaDecodeOptions,
    ) -> Result<()> {
        options.validate(AUDIO_SEGMENTS_FUNCTION, MAX_AUDIO_SEGMENTS)?;
        self.register_table_provider(
            table_name,
            audio_segments_provider(
                &media.uri,
                options.start_millis,
                options.end_millis,
                options.interval_millis,
                options.limit,
                options.include_payload,
                Arc::clone(&self.execution_governor),
            )?,
        )
    }

    /// Stages one bounded, credential-free S3 media reference and registers a
    /// video relation over it. The staging handle is retained by the relation
    /// and deletes its private local file after unregister/drop.
    ///
    /// This is the remote boundary for native FFmpeg. Literal
    /// `lakeprism_video_frames('s3://...')` calls remain intentionally
    /// unsupported because SQL planning must not own request credentials.
    #[cfg(feature = "remote-media-s3")]
    pub async fn register_s3_video_frames(
        &self,
        table_name: &str,
        media: &MediaRef,
        access: &AccessContext,
        stager: &S3MediaStagingResolver,
        options: LocalMediaDecodeOptions,
    ) -> Result<()> {
        options.validate(VIDEO_FRAMES_FUNCTION, MAX_VIDEO_FRAMES)?;
        let staged = stager
            .stage(media, access)
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        self.register_table_provider(
            table_name,
            video_frames_provider_from_path(
                &media.uri,
                staged.path().to_path_buf(),
                options,
                Arc::clone(&self.execution_governor),
                Some(staged),
            )?,
        )
    }

    /// Stages one bounded, credential-free S3 media reference and registers a
    /// normalized-audio relation over it. The source is fully staged before
    /// registration so FFmpeg never receives endpoint or credential data.
    #[cfg(feature = "remote-media-s3")]
    pub async fn register_s3_audio_chunks(
        &self,
        table_name: &str,
        media: &MediaRef,
        access: &AccessContext,
        stager: &S3MediaStagingResolver,
        options: LocalMediaDecodeOptions,
    ) -> Result<()> {
        options.validate(AUDIO_SEGMENTS_FUNCTION, MAX_AUDIO_SEGMENTS)?;
        let staged = stager
            .stage(media, access)
            .await
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        self.register_table_provider(
            table_name,
            audio_segments_provider_from_path(
                &media.uri,
                staged.path().to_path_buf(),
                options,
                Arc::clone(&self.execution_governor),
                Some(staged),
            )?,
        )
    }

    /// Adds verified, lineage-bearing transcript rows to the session-local
    /// search index. SQL search never falls back to cold extraction.
    pub fn register_transcript_segments(
        &self,
        segments: impl IntoIterator<Item = TranscriptSegment>,
    ) -> std::result::Result<(), IndexError> {
        let mut index = self
            .transcript_index
            .lock()
            .expect("transcript index mutex is never poisoned");
        for segment in segments {
            index.insert(segment)?;
        }
        Ok(())
    }

    /// Transcribes one bounded local media request through the configured
    /// provider, then registers the validated rows in the session transcript
    /// index. The default session returns ProviderUnavailable.
    pub fn transcribe_media(
        &self,
        request: TranscriptionRequest,
    ) -> std::result::Result<Vec<TranscriptSegment>, IndexError> {
        let _query_permit = self
            .execution_governor
            .try_acquire_query()
            .ok_or(IndexError::ResourceExhausted { resource: "query" })?;
        let rows = self.transcription_provider.transcribe(&request)?;
        for row in &rows {
            if !row.lineage.is_compatible_with(&request.lineage) {
                return Err(IndexError::IncompatibleProviderResult);
            }
        }
        self.register_transcript_segments(rows.clone())?;
        Ok(rows)
    }

    /// Registers rows loaded from an immutable persisted transcript index.
    /// The subsequent media-aware API substitutes these rows only if the
    /// request's full lineage exactly matches; no persisted payload is trusted
    /// by the ordinary SQL UDTF automatically.
    pub fn register_persisted_transcript_segments(
        &self,
        segments: impl IntoIterator<Item = TranscriptSegment>,
    ) -> std::result::Result<(), IndexError> {
        self.media_transcript_search
            .lock()
            .expect("media transcript search mutex is never poisoned")
            .register_persisted(segments)
    }

    /// Returns a media-aware execution explanation without parsing or running
    /// SQL. This is the supported DataFusion-55 integration point for exact
    /// persisted-index substitution and bounded cold transcription.
    pub fn explain_media_transcript_search(
        &self,
        request: &MediaTranscriptSearchRequest,
        limits: ProgressiveScheduleLimits,
    ) -> std::result::Result<MediaSearchExplanation, IndexError> {
        self.media_transcript_search
            .lock()
            .expect("media transcript search mutex is never poisoned")
            .explain(request, limits)
    }

    /// Executes the explicit media-aware search path. It selects a persisted
    /// index automatically only for exact lineage; otherwise it invokes the
    /// application-owned bounded provider according to the supplied global
    /// schedule limits.
    pub fn search_media_transcript(
        &self,
        request: &MediaTranscriptSearchRequest,
        limits: ProgressiveScheduleLimits,
    ) -> std::result::Result<Vec<TranscriptSegment>, IndexError> {
        let _query_permit = self
            .execution_governor
            .try_acquire_query()
            .ok_or(IndexError::ResourceExhausted { resource: "query" })?;
        let _cpu_permit = self
            .execution_governor
            .try_acquire_cpu()
            .ok_or(IndexError::ResourceExhausted { resource: "CPU" })?;
        self.media_transcript_search
            .lock()
            .expect("media transcript search mutex is never poisoned")
            .search(request, limits)
    }

    /// Registers one exact embedding-lineage family. A session refuses mixed
    /// lineage to prevent accidental compatible-index substitution.
    pub fn register_embedding_records(
        &self,
        records: impl IntoIterator<Item = EmbeddingRecord>,
    ) -> std::result::Result<(), IndexError> {
        let mut index = self
            .embedding_index
            .lock()
            .expect("embedding index mutex is never poisoned");
        let mut expected = self
            .embedding_lineage
            .lock()
            .expect("embedding lineage mutex is never poisoned");
        for record in records {
            if let Some(lineage) = expected.as_ref() {
                if !lineage.is_compatible_with(&record.lineage) {
                    return Err(IndexError::IncompatibleColdResult);
                }
            } else {
                *expected = Some(record.lineage.clone());
            }
            index.insert(record)?;
        }
        Ok(())
    }

    /// Registers bounded vectors from transcript, document, image, audio, or
    /// video feature extraction. Records retain their individual exact
    /// lineage; no modality-specific fallback or inference is performed.
    pub fn register_cross_modal_records(
        &self,
        records: impl IntoIterator<Item = CrossModalRecord>,
    ) -> std::result::Result<(), IndexError> {
        let mut index = self
            .cross_modal_index
            .lock()
            .expect("cross-modal index mutex is never poisoned");
        for record in records {
            index.insert(record)?;
        }
        Ok(())
    }

    /// Performs a governed exact-lineage cross-modal vector lookup. The
    /// embedding provider is application-owned; a default session returns
    /// ProviderUnavailable rather than generating a synthetic query vector.
    pub fn search_cross_modal(
        &self,
        query: impl Into<String>,
        lineage: &lakeprism_core::FeatureLineage,
        options: RankingOptions,
    ) -> std::result::Result<Vec<CrossModalResult>, IndexError> {
        let _query_permit = self
            .execution_governor
            .try_acquire_query()
            .ok_or(IndexError::ResourceExhausted { resource: "query" })?;
        let _cpu_permit = self
            .execution_governor
            .try_acquire_cpu()
            .ok_or(IndexError::ResourceExhausted { resource: "CPU" })?;
        let query = self.embedding_provider.embed(&EmbeddingRequest {
            text: query.into(),
            lineage: lineage.clone(),
        })?;
        self.cross_modal_index
            .lock()
            .expect("cross-modal index mutex is never poisoned")
            .rank(&query, lineage, options)
    }

    pub async fn collect(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let execution = self.execute_stream(query).await?;
        futures::TryStreamExt::try_collect(execution).await
    }

    pub async fn execute_stream(&self, query: &str) -> Result<SendableRecordBatchStream> {
        Ok(self.execute_stream_with_deadline(query, None).await?.stream)
    }

    pub async fn execute_stream_with_deadline(
        &self,
        query: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<QueryExecution> {
        let id = self.create_query(deadline);
        self.execute_registered_query(query, id).await
    }

    pub fn create_query(&self, deadline: Option<tokio::time::Instant>) -> String {
        let id = Uuid::new_v4().to_string();
        let now = SystemTime::now();
        self.queries
            .lock()
            .expect("query registry mutex is never poisoned")
            .insert(
                id.clone(),
                QueryRecord {
                    control: QueryControl::new(deadline),
                    status: QueryStatus::Pending,
                    metrics: QueryMetrics {
                        created_at: now,
                        started_at: None,
                        completed_at: None,
                        elapsed: Duration::default(),
                        batches: 0,
                        rows: 0,
                    },
                },
            );
        id
    }

    pub fn query(&self, id: &str) -> Option<QueryInfo> {
        self.queries.lock().ok()?.get(id).map(|record| QueryInfo {
            id: id.to_string(),
            status: record.status,
            metrics: record.metrics.clone(),
        })
    }

    /// Returns a credential-safe, structured local lifecycle event for a query.
    ///
    /// LakePrism stores query state only in this process. Callers that need
    /// durable audit retention must export this value to their own approved
    /// local sink; LakePrism intentionally does not create a telemetry daemon.
    pub fn query_audit_event(&self, id: &str) -> Option<QueryAuditEvent> {
        let record = self.queries.lock().ok()?.get(id)?.clone();
        Some(QueryAuditEvent {
            schema_version: 1,
            event_type: "lakeprism.query.lifecycle",
            query_id: id.to_owned(),
            status: record.status,
            created_at_unix_millis: unix_millis(record.metrics.created_at),
            started_at_unix_millis: record.metrics.started_at.map(unix_millis),
            completed_at_unix_millis: record.metrics.completed_at.map(unix_millis),
            elapsed_millis: record.metrics.elapsed.as_millis(),
            batches: record.metrics.batches,
            rows: record.metrics.rows,
        })
    }

    pub fn cancel_query(&self, id: &str) -> bool {
        let Ok(mut queries) = self.queries.lock() else {
            return false;
        };
        let Some(record) = queries.get_mut(id) else {
            return false;
        };
        if matches!(
            record.status,
            QueryStatus::Succeeded
                | QueryStatus::Failed
                | QueryStatus::Cancelled
                | QueryStatus::DeadlineExceeded
        ) {
            return false;
        }
        record.control.cancel();
        record.status = QueryStatus::Cancelled;
        record.metrics.completed_at = Some(SystemTime::now());
        record.metrics.elapsed = record.metrics.created_at.elapsed().unwrap_or_default();
        true
    }

    pub async fn execute_registered_query(
        &self,
        query: &str,
        id: String,
    ) -> Result<QueryExecution> {
        let control = self
            .queries
            .lock()
            .ok()
            .and_then(|queries| queries.get(&id).map(|record| record.control.clone()))
            .ok_or_else(|| DataFusionError::Execution("query does not exist".to_string()))?;
        if control.state() == QueryControlState::Cancelled {
            self.finish_unstarted_query(&id, QueryControlState::Cancelled);
            return Err(DataFusionError::Execution(
                "query was cancelled".to_string(),
            ));
        }
        if control.state() == QueryControlState::DeadlineExceeded {
            self.finish_unstarted_query(&id, QueryControlState::DeadlineExceeded);
            return Err(DataFusionError::Execution(
                "query deadline was exceeded".to_string(),
            ));
        }
        let query_permit = match acquire_query_permit(&self.execution_governor, &control).await {
            Ok(permit) => permit,
            Err(error) => {
                self.finish_unstarted_query(&id, control.state());
                return Err(error);
            }
        };
        if control.state() != QueryControlState::Active {
            drop(query_permit);
            return Err(control_error(control.state()));
        }
        let dataframe = match self.sql(query).await {
            Ok(dataframe) => dataframe,
            Err(error) => {
                self.finish_unstarted_query(&id, control.state());
                return Err(error);
            }
        };
        let cpu_permit = match acquire_cpu_permit(&self.execution_governor, &control).await {
            Ok(permit) => permit,
            Err(error) => {
                self.finish_unstarted_query(&id, control.state());
                return Err(error);
            }
        };
        let stream = match dataframe.execute_stream().await {
            Ok(stream) => stream,
            Err(error) => {
                self.finish_unstarted_query(&id, control.state());
                return Err(error);
            }
        };
        if let Some(record) = self
            .queries
            .lock()
            .expect("query registry mutex is never poisoned")
            .get_mut(&id)
        {
            record.status = QueryStatus::Running;
            record.metrics.started_at = Some(SystemTime::now());
        }

        fn control_error(state: QueryControlState) -> DataFusionError {
            match state {
                QueryControlState::Cancelled => {
                    DataFusionError::Execution("query was cancelled".to_string())
                }
                QueryControlState::DeadlineExceeded => {
                    DataFusionError::Execution("query deadline was exceeded".to_string())
                }
                QueryControlState::Active => {
                    DataFusionError::Execution("query control stopped execution".to_string())
                }
            }
        }

        async fn acquire_query_permit(
            governor: &ExecutionGovernor,
            control: &QueryControl,
        ) -> Result<QueryPermit> {
            let acquire = governor.acquire_query();
            match control.deadline() {
                Some(deadline) => tokio::select! {
                    permit = acquire => Ok(permit),
                    _ = control.cancelled() => Err(control_error(QueryControlState::Cancelled)),
                    _ = tokio::time::sleep_until(deadline) => Err(control_error(QueryControlState::DeadlineExceeded)),
                },
                None => tokio::select! {
                    permit = acquire => Ok(permit),
                    _ = control.cancelled() => Err(control_error(QueryControlState::Cancelled)),
                },
            }
        }

        async fn acquire_cpu_permit(
            governor: &ExecutionGovernor,
            control: &QueryControl,
        ) -> Result<CpuPermit> {
            let acquire = governor.acquire_cpu();
            match control.deadline() {
                Some(deadline) => tokio::select! {
                    permit = acquire => Ok(permit),
                    _ = control.cancelled() => Err(control_error(QueryControlState::Cancelled)),
                    _ = tokio::time::sleep_until(deadline) => Err(control_error(QueryControlState::DeadlineExceeded)),
                },
                None => tokio::select! {
                    permit = acquire => Ok(permit),
                    _ = control.cancelled() => Err(control_error(QueryControlState::Cancelled)),
                },
            }
        }
        Ok(QueryExecution {
            id: id.clone(),
            stream: Box::pin(GovernedRecordBatchStream {
                stream,
                _query_permit: query_permit,
                _cpu_permit: cpu_permit,
                id,
                queries: Arc::clone(&self.queries),
                control,
                completed: false,
            }),
        })
    }

    fn finish_unstarted_query(&self, id: &str, state: QueryControlState) {
        if let Some(record) = self
            .queries
            .lock()
            .expect("query registry mutex is never poisoned")
            .get_mut(id)
        {
            record.status = match state {
                QueryControlState::Cancelled => QueryStatus::Cancelled,
                QueryControlState::DeadlineExceeded => QueryStatus::DeadlineExceeded,
                QueryControlState::Active => QueryStatus::Failed,
            };
            record.metrics.completed_at = Some(SystemTime::now());
            record.metrics.elapsed = record.metrics.created_at.elapsed().unwrap_or_default();
        }
    }

    pub fn catalog_names(&self) -> Vec<String> {
        self.context.catalog_names()
    }

    pub fn schema_names(&self) -> Vec<(String, String)> {
        let mut schemas = Vec::new();
        for catalog_name in self.context.catalog_names() {
            if let Some(catalog) = self.context.catalog(&catalog_name) {
                schemas.extend(
                    catalog
                        .schema_names()
                        .into_iter()
                        .map(|schema_name| (catalog_name.clone(), schema_name)),
                );
            }
        }
        schemas
    }

    pub async fn catalog_tables(&self) -> Result<Vec<CatalogTable>> {
        let mut tables = Vec::new();
        for (catalog_name, schema_name) in self.schema_names() {
            let Some(catalog) = self.context.catalog(&catalog_name) else {
                continue;
            };
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };
            for table_name in schema.table_names() {
                let Some(table) = schema.table(&table_name).await? else {
                    continue;
                };
                tables.push(CatalogTable {
                    catalog_name: catalog_name.clone(),
                    schema_name: schema_name.clone(),
                    table_name,
                    table_type: match table.table_type() {
                        datafusion::logical_expr::TableType::Base => "TABLE",
                        datafusion::logical_expr::TableType::View => "VIEW",
                        datafusion::logical_expr::TableType::Temporary => "TEMPORARY",
                    }
                    .to_string(),
                    schema: table.schema(),
                });
            }
        }
        Ok(tables)
    }
}

impl Default for MediaSession {
    fn default() -> Self {
        Self::new()
    }
}

fn register_lakeprism_extensions(
    context: &SessionContext,
    execution_governor: Arc<ExecutionGovernor>,
    transcript_index: Arc<Mutex<TranscriptIndex>>,
    embedding_index: Arc<Mutex<EmbeddingIndex>>,
    embedding_lineage: Arc<Mutex<Option<lakeprism_core::FeatureLineage>>>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    ocr_provider: Arc<dyn OcrProvider>,
) {
    let media_type = lakeprism_arrow::media_ref_data_type();
    context.register_udf(create_udf(
        MEDIA_URI_FUNCTION,
        vec![media_type.clone()],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| media_string_field(args, "uri")),
    ));
    context.register_udf(create_udf(
        MEDIA_TYPE_FUNCTION,
        vec![media_type],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| media_string_field(args, "media_type")),
    ));
    context.register_udtf(
        DOCUMENT_SECTIONS_FUNCTION,
        Arc::new(DocumentSections {
            execution_governor: Arc::clone(&execution_governor),
        }),
    );
    context.register_udtf(
        DOCUMENT_TABLES_FUNCTION,
        Arc::new(DocumentTables {
            execution_governor: Arc::clone(&execution_governor),
        }),
    );
    context.register_udtf(
        DOCUMENT_IMAGES_FUNCTION,
        Arc::new(DocumentImages {
            execution_governor: Arc::clone(&execution_governor),
        }),
    );
    context.register_udtf(
        DOCUMENT_SEARCH_FUNCTION,
        Arc::new(DocumentSearch {
            execution_governor: Arc::clone(&execution_governor),
        }),
    );
    context.register_udtf(
        DOCUMENT_OCR_FUNCTION,
        Arc::new(DocumentOcr {
            execution_governor: Arc::clone(&execution_governor),
            provider: ocr_provider,
        }),
    );
    context.register_udtf(
        VIDEO_FRAMES_FUNCTION,
        Arc::new(VideoFrames {
            execution_governor: Arc::clone(&execution_governor),
        }),
    );
    context.register_udtf(
        AUDIO_SEGMENTS_FUNCTION,
        Arc::new(AudioSegments { execution_governor }),
    );
    context.register_udtf(
        TRANSCRIPT_SEARCH_FUNCTION,
        Arc::new(TranscriptSearch { transcript_index }),
    );
    context.register_udtf(
        SEMANTIC_SEARCH_FUNCTION,
        Arc::new(EmbeddingSearch {
            embedding_index: Arc::clone(&embedding_index),
            embedding_lineage: Arc::clone(&embedding_lineage),
            embedding_provider: Arc::clone(&embedding_provider),
            hybrid: false,
        }),
    );
    context.register_udtf(
        HYBRID_SEARCH_FUNCTION,
        Arc::new(EmbeddingSearch {
            embedding_index,
            embedding_lineage,
            embedding_provider,
            hybrid: true,
        }),
    );
}

fn media_string_field(args: &[ColumnarValue], field_name: &str) -> Result<ColumnarValue> {
    let [ColumnarValue::Array(values)] = args else {
        return Err(DataFusionError::Execution(format!(
            "{field_name} accessor requires one media struct column"
        )));
    };
    let media = values
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{field_name} accessor requires a LakePrism media struct"
            ))
        })?;
    let strings = media
        .column_by_name(field_name)
        .and_then(|field| field.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "LakePrism media struct is missing its {field_name} field"
            ))
        })?;
    let values = StringArray::from_iter((0..media.len()).map(|index| {
        (!media.is_null(index) && !strings.is_null(index)).then(|| strings.value(index))
    }));
    Ok(ColumnarValue::Array(Arc::new(values)))
}

struct DocumentSections {
    execution_governor: Arc<ExecutionGovernor>,
}

impl std::fmt::Debug for DocumentSections {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DocumentSections")
    }
}

impl TableFunctionImpl for DocumentSections {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let [Expr::Literal(ScalarValue::Utf8(Some(uri)), _)] = args.exprs() else {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_SECTIONS_FUNCTION} requires exactly one literal file:// URI"
            )));
        };
        let _cpu_permit = self.execution_governor.try_acquire_cpu().ok_or_else(|| {
            DataFusionError::ResourcesExhausted(format!(
                "{DOCUMENT_SECTIONS_FUNCTION} cannot extract a document while all CPU permits are in use"
            ))
        })?;
        document_sections_provider(uri)
    }
}

fn document_sections_provider(uri: &str) -> Result<Arc<dyn TableProvider>> {
    let url = Url::parse(uri).map_err(|_| {
        DataFusionError::Plan(format!(
            "{DOCUMENT_SECTIONS_FUNCTION} requires an absolute file:// URI"
        ))
    })?;
    if url.scheme() != "file" {
        return Err(DataFusionError::Plan(format!(
            "{DOCUMENT_SECTIONS_FUNCTION} only supports local file:// URIs"
        )));
    }
    let path = url.to_file_path().map_err(|_| {
        DataFusionError::Plan(format!(
            "{DOCUMENT_SECTIONS_FUNCTION} received an invalid file URI"
        ))
    })?;
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    let sections = match extension.as_deref() {
        Some("pdf") => extract_pdf_sections_with_limits(&path, Default::default()),
        Some("docx") => extract_docx_sections_with_limits(&path, Default::default()),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_SECTIONS_FUNCTION} supports only .pdf and .docx files"
            )));
        }
    }
    .map_err(|error| DataFusionError::Execution(error.to_string()))?;

    let batch = document_sections_record_batch(uri, &local_document_version(&path)?, &sections)?;
    let table = MemTable::try_new(document_sections_schema(), vec![vec![batch]])?;
    Ok(Arc::new(table))
}

fn document_sections_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("heading", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, false),
    ]))
}

fn document_sections_record_batch(
    uri: &str,
    source_version: &str,
    sections: &[DocumentSection],
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        document_sections_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                uri,
                sections.len(),
            ))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                source_version,
                sections.len(),
            ))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                sections.iter().map(|section| section.ordinal),
            )),
            Arc::new(StringArray::from_iter(
                sections.iter().map(|section| section.heading.as_deref()),
            )),
            Arc::new(StringArray::from_iter_values(
                sections.iter().map(|section| section.text.as_str()),
            )),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn local_document_version(path: &std::path::Path) -> Result<String> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        DataFusionError::Execution(format!("cannot stat local document: {error}"))
    })?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    Ok(format!("size:{};mtime_ns:{modified}", metadata.len()))
}

struct DocumentTables {
    execution_governor: Arc<ExecutionGovernor>,
}

impl std::fmt::Debug for DocumentTables {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DocumentTables")
    }
}

impl TableFunctionImpl for DocumentTables {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let uri = document_uri_argument(args.exprs(), DOCUMENT_TABLES_FUNCTION)?;
        let _cpu_permit = document_cpu_permit(&self.execution_governor, DOCUMENT_TABLES_FUNCTION)?;
        document_tables_provider(uri)
    }
}

struct DocumentImages {
    execution_governor: Arc<ExecutionGovernor>,
}

impl std::fmt::Debug for DocumentImages {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DocumentImages")
    }
}

impl TableFunctionImpl for DocumentImages {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let [
            Expr::Literal(ScalarValue::Utf8(Some(uri)), _),
            Expr::Literal(ScalarValue::Boolean(Some(include_bytes)), _),
        ] = args.exprs()
        else {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_IMAGES_FUNCTION} requires a literal file:// URI and literal include_bytes boolean"
            )));
        };
        let _cpu_permit = document_cpu_permit(&self.execution_governor, DOCUMENT_IMAGES_FUNCTION)?;
        document_images_provider(uri, *include_bytes)
    }
}

struct DocumentSearch {
    execution_governor: Arc<ExecutionGovernor>,
}

impl std::fmt::Debug for DocumentSearch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DocumentSearch")
    }
}

impl TableFunctionImpl for DocumentSearch {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let [
            Expr::Literal(ScalarValue::Utf8(Some(uri)), _),
            Expr::Literal(ScalarValue::Utf8(Some(query)), _),
            Expr::Literal(limit, _),
        ] = args.exprs()
        else {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_SEARCH_FUNCTION} requires literal uri, query, and limit arguments"
            )));
        };
        let limit = match limit {
            ScalarValue::UInt64(Some(limit)) => *limit,
            ScalarValue::Int64(Some(limit)) if *limit >= 0 => *limit as u64,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{DOCUMENT_SEARCH_FUNCTION} limit must be a non-negative integer literal"
                )));
            }
        };
        let limit = usize::try_from(limit).map_err(|_| {
            DataFusionError::Plan(format!("{DOCUMENT_SEARCH_FUNCTION} limit is too large"))
        })?;
        if limit > MAX_TRANSCRIPT_RESULTS {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_SEARCH_FUNCTION} limit must not exceed {MAX_TRANSCRIPT_RESULTS}"
            )));
        }
        let _cpu_permit = document_cpu_permit(&self.execution_governor, DOCUMENT_SEARCH_FUNCTION)?;
        document_search_provider(uri, query, limit)
    }
}

struct DocumentOcr {
    execution_governor: Arc<ExecutionGovernor>,
    provider: Arc<dyn OcrProvider>,
}

impl std::fmt::Debug for DocumentOcr {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DocumentOcr")
    }
}

impl TableFunctionImpl for DocumentOcr {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let [
            Expr::Literal(ScalarValue::Utf8(Some(uri)), _),
            Expr::Literal(limit, _),
        ] = args.exprs()
        else {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_OCR_FUNCTION} requires literal file:// URI and result limit arguments"
            )));
        };
        let limit = match limit {
            ScalarValue::UInt64(Some(limit)) => usize::try_from(*limit),
            ScalarValue::Int64(Some(limit)) if *limit >= 0 => usize::try_from(*limit as u64),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{DOCUMENT_OCR_FUNCTION} limit must be a non-negative integer literal"
                )));
            }
        }
        .map_err(|_| {
            DataFusionError::Plan(format!("{DOCUMENT_OCR_FUNCTION} limit is too large"))
        })?;
        if limit > lakeprism_index::MAX_OCR_RESULTS {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_OCR_FUNCTION} limit must not exceed {}",
                lakeprism_index::MAX_OCR_RESULTS
            )));
        }
        let _cpu_permit = document_cpu_permit(&self.execution_governor, DOCUMENT_OCR_FUNCTION)?;
        document_ocr_provider(uri, limit, self.provider.as_ref())
    }
}

fn document_ocr_provider(
    uri: &str,
    limit: usize,
    provider: &dyn OcrProvider,
) -> Result<Arc<dyn TableProvider>> {
    let path = document_path(uri, DOCUMENT_OCR_FUNCTION)?;
    let images = match path.extension().and_then(|extension| extension.to_str()) {
        Some(extension) if extension.eq_ignore_ascii_case("docx") => {
            extract_docx_images_with_limits(&path, Default::default())
        }
        Some(extension) if extension.eq_ignore_ascii_case("pdf") => {
            extract_pdf_images_with_limits(&path, Default::default())
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_OCR_FUNCTION} supports only .pdf and .docx files"
            )));
        }
    }
    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let source_version = local_document_version(&path)?;
    let lineage = lakeprism_core::FeatureLineage {
        source: lakeprism_core::SourceIdentity {
            media_uri: uri.to_owned(),
            source_version: source_version.clone(),
        },
        operator_version: "document-ocr-v1".to_owned(),
        model: None,
        model_version: None,
        parameters: BTreeMap::new(),
    };
    let mut image_ordinal = Vec::new();
    let mut ordinal = Vec::new();
    let mut text = Vec::new();
    let mut confidence_millis = Vec::new();
    for image in images {
        if text.len() == limit {
            break;
        }
        let remaining = limit.saturating_sub(text.len());
        if remaining == 0 {
            break;
        }
        let rows = run_ocr(
            provider,
            &OcrRequest {
                bytes: Arc::from(image.bytes),
                media_type: image.media_type,
                lineage: lineage.clone(),
                max_results: remaining,
            },
        )
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        for row in rows {
            image_ordinal.push(image.ordinal);
            ordinal.push(row.ordinal);
            text.push(row.text);
            confidence_millis.push(row.confidence_millis);
        }
    }
    let count = text.len();
    let batch = RecordBatch::try_new(
        document_ocr_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                uri, count,
            ))),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                source_version.as_str(),
                count,
            ))),
            Arc::new(UInt32Array::from(image_ordinal)),
            Arc::new(UInt32Array::from(ordinal)),
            Arc::new(StringArray::from(text)),
            Arc::new(UInt16Array::from(confidence_millis)),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok(Arc::new(MemTable::try_new(
        document_ocr_schema(),
        vec![vec![batch]],
    )?))
}

fn document_ocr_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("image_ordinal", DataType::UInt32, false),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("confidence_millis", DataType::UInt16, true),
    ]))
}

fn document_cpu_permit(governor: &ExecutionGovernor, function: &str) -> Result<CpuPermit> {
    governor.try_acquire_cpu().ok_or_else(|| {
        DataFusionError::ResourcesExhausted(format!(
            "{function} cannot extract a document while all CPU permits are in use"
        ))
    })
}

fn document_uri_argument<'a>(expressions: &'a [Expr], function: &str) -> Result<&'a str> {
    let [Expr::Literal(ScalarValue::Utf8(Some(uri)), _)] = expressions else {
        return Err(DataFusionError::Plan(format!(
            "{function} requires exactly one literal file:// URI"
        )));
    };
    Ok(uri)
}

fn document_path(uri: &str, function: &str) -> Result<std::path::PathBuf> {
    let url = Url::parse(uri).map_err(|_| {
        DataFusionError::Plan(format!("{function} requires an absolute file:// URI"))
    })?;
    if url.scheme() != "file" {
        return Err(DataFusionError::Plan(format!(
            "{function} only supports local file:// URIs"
        )));
    }
    url.to_file_path()
        .map_err(|_| DataFusionError::Plan(format!("{function} received an invalid file URI")))
}

fn document_tables_provider(uri: &str) -> Result<Arc<dyn TableProvider>> {
    let path = document_path(uri, DOCUMENT_TABLES_FUNCTION)?;
    let tables = match path.extension().and_then(|extension| extension.to_str()) {
        Some(extension) if extension.eq_ignore_ascii_case("docx") => {
            extract_docx_tables_with_limits(&path, Default::default())
        }
        Some(extension) if extension.eq_ignore_ascii_case("pdf") => {
            extract_pdf_tables_with_limits(&path, Default::default())
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_TABLES_FUNCTION} supports only .pdf and .docx files"
            )));
        }
    }
    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let source_version = local_document_version(&path)?;
    let mut table_ordinal = Vec::new();
    let mut row_ordinal = Vec::new();
    let mut column_ordinal = Vec::new();
    let mut text = Vec::new();
    for table in &tables {
        for (row, cells) in table.rows.iter().enumerate() {
            for (column, cell) in cells.iter().enumerate() {
                table_ordinal.push(table.ordinal);
                row_ordinal.push(row as u32);
                column_ordinal.push(column as u32);
                text.push(cell.as_str());
            }
        }
    }
    let row_count = text.len();
    let batch = RecordBatch::try_new(
        document_tables_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                uri, row_count,
            ))),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                source_version.as_str(),
                row_count,
            ))),
            Arc::new(UInt32Array::from(table_ordinal)),
            Arc::new(UInt32Array::from(row_ordinal)),
            Arc::new(UInt32Array::from(column_ordinal)),
            Arc::new(StringArray::from_iter_values(text)),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok(Arc::new(MemTable::try_new(
        document_tables_schema(),
        vec![vec![batch]],
    )?))
}

fn document_tables_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("table_ordinal", DataType::UInt32, false),
        Field::new("row_ordinal", DataType::UInt32, false),
        Field::new("column_ordinal", DataType::UInt32, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

fn document_images_provider(uri: &str, include_bytes: bool) -> Result<Arc<dyn TableProvider>> {
    let path = document_path(uri, DOCUMENT_IMAGES_FUNCTION)?;
    let images = match path.extension().and_then(|extension| extension.to_str()) {
        Some(extension) if extension.eq_ignore_ascii_case("docx") => {
            extract_docx_images_with_limits(&path, Default::default())
        }
        Some(extension) if extension.eq_ignore_ascii_case("pdf") => {
            extract_pdf_images_with_limits(&path, Default::default())
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{DOCUMENT_IMAGES_FUNCTION} supports only .pdf and .docx files"
            )));
        }
    }
    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let source_version = local_document_version(&path)?;
    let batch = document_images_record_batch(uri, &source_version, &images, include_bytes)?;
    Ok(Arc::new(MemTable::try_new(
        document_images_schema(),
        vec![vec![batch]],
    )?))
}

fn document_images_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("media_type", DataType::Utf8, true),
        Field::new("bytes", DataType::Binary, true),
    ]))
}

fn document_images_record_batch(
    uri: &str,
    source_version: &str,
    images: &[DocumentImage],
    include_bytes: bool,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        document_images_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                uri,
                images.len(),
            ))),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                source_version,
                images.len(),
            ))),
            Arc::new(UInt32Array::from_iter_values(
                images.iter().map(|image| image.ordinal),
            )),
            Arc::new(StringArray::from_iter_values(
                images.iter().map(|image| image.name.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                images.iter().map(|image| image.media_type.as_deref()),
            )),
            Arc::new(BinaryArray::from_iter(
                images
                    .iter()
                    .map(|image| include_bytes.then_some(image.bytes.as_slice())),
            )),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

fn document_search_provider(
    uri: &str,
    query: &str,
    limit: usize,
) -> Result<Arc<dyn TableProvider>> {
    if query.trim().is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{DOCUMENT_SEARCH_FUNCTION} query must not be empty"
        )));
    }
    let path = document_path(uri, DOCUMENT_SEARCH_FUNCTION)?;
    let source_version = local_document_version(&path)?;
    let mut matches = Vec::<(&str, u32, String)>::new();
    if limit > 0 {
        let needle = query.to_lowercase();
        match path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("pdf") => {
                for section in extract_pdf_sections_with_limits(&path, Default::default())
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
                {
                    if section.text.to_lowercase().contains(&needle) {
                        matches.push(("section", section.ordinal, section.text));
                        if matches.len() == limit {
                            break;
                        }
                    }
                }
            }
            Some("docx") => {
                for section in extract_docx_sections_with_limits(&path, Default::default())
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
                {
                    if section.text.to_lowercase().contains(&needle) {
                        matches.push(("section", section.ordinal, section.text));
                        if matches.len() == limit {
                            break;
                        }
                    }
                }
                if matches.len() < limit {
                    for table in extract_docx_tables_with_limits(&path, Default::default())
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?
                    {
                        for row in table.rows {
                            for cell in row {
                                if cell.to_lowercase().contains(&needle) {
                                    matches.push(("table_cell", table.ordinal, cell));
                                    if matches.len() == limit {
                                        break;
                                    }
                                }
                            }
                            if matches.len() == limit {
                                break;
                            }
                        }
                        if matches.len() == limit {
                            break;
                        }
                    }
                }
                if matches.len() < limit {
                    for image in extract_docx_images_with_limits(&path, Default::default())
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?
                    {
                        if image.name.to_lowercase().contains(&needle) {
                            matches.push(("image_name", image.ordinal, image.name));
                            if matches.len() == limit {
                                break;
                            }
                        }
                    }
                }
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "{DOCUMENT_SEARCH_FUNCTION} supports only .pdf and .docx files"
                )));
            }
        }
    }
    let count = matches.len();
    let batch = RecordBatch::try_new(
        document_search_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                uri, count,
            ))),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                source_version.as_str(),
                count,
            ))),
            Arc::new(StringArray::from_iter_values(
                matches.iter().map(|entry| entry.0),
            )),
            Arc::new(UInt32Array::from_iter_values(
                matches.iter().map(|entry| entry.1),
            )),
            Arc::new(StringArray::from_iter_values(
                matches.iter().map(|entry| entry.2.as_str()),
            )),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok(Arc::new(MemTable::try_new(
        document_search_schema(),
        vec![vec![batch]],
    )?))
}

fn document_search_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("ordinal", DataType::UInt32, false),
        Field::new("text", DataType::Utf8, false),
    ]))
}

struct VideoFrames {
    execution_governor: Arc<ExecutionGovernor>,
}

impl std::fmt::Debug for VideoFrames {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("VideoFrames")
    }
}

impl TableFunctionImpl for VideoFrames {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let (uri, start, end, every, limit, include_rgb24) = media_function_arguments(
            args.exprs(),
            VIDEO_FRAMES_FUNCTION,
            "uri, start_millis, end_millis, every_millis, limit, include_rgb24",
        )?;
        if limit > MAX_VIDEO_FRAMES {
            return Err(DataFusionError::Plan(format!(
                "{VIDEO_FRAMES_FUNCTION} limit must not exceed {MAX_VIDEO_FRAMES}"
            )));
        }
        video_frames_provider(
            &uri,
            start,
            end,
            every,
            limit,
            include_rgb24,
            Arc::clone(&self.execution_governor),
        )
    }
}

struct AudioSegments {
    execution_governor: Arc<ExecutionGovernor>,
}

impl std::fmt::Debug for AudioSegments {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AudioSegments")
    }
}

impl TableFunctionImpl for AudioSegments {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let (uri, start, end, segment, limit, include_payload) = media_function_arguments(
            args.exprs(),
            AUDIO_SEGMENTS_FUNCTION,
            "uri, start_millis, end_millis, segment_millis, limit, include_payload",
        )?;
        if limit > MAX_AUDIO_SEGMENTS {
            return Err(DataFusionError::Plan(format!(
                "{AUDIO_SEGMENTS_FUNCTION} limit must not exceed {MAX_AUDIO_SEGMENTS}"
            )));
        }
        audio_segments_provider(
            &uri,
            start,
            end,
            segment,
            limit,
            include_payload,
            Arc::clone(&self.execution_governor),
        )
    }
}

fn media_function_arguments(
    expressions: &[Expr],
    function_name: &str,
    signature: &str,
) -> Result<(String, u64, u64, u64, usize, bool)> {
    let [
        Expr::Literal(ScalarValue::Utf8(Some(uri)), _),
        Expr::Literal(ScalarValue::Int64(Some(start)), _),
        Expr::Literal(ScalarValue::Int64(Some(end)), _),
        Expr::Literal(ScalarValue::Int64(Some(interval)), _),
        Expr::Literal(ScalarValue::Int64(Some(limit)), _),
        Expr::Literal(ScalarValue::Boolean(Some(include_payload)), _),
    ] = expressions
    else {
        return Err(DataFusionError::Plan(format!(
            "{function_name} requires literal arguments: {signature}"
        )));
    };
    let (start, end, interval, limit) = (*start, *end, *interval, *limit);
    let (start, end, interval, limit) = (
        u64::try_from(start),
        u64::try_from(end),
        u64::try_from(interval),
        usize::try_from(limit),
    );
    let (start, end, interval, limit) = match (start, end, interval, limit) {
        (Ok(start), Ok(end), Ok(interval), Ok(limit)) => (start, end, interval, limit),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{function_name} interval and limit must be non-negative integers"
            )));
        }
    };
    if interval == 0 || limit == 0 || end < start {
        return Err(DataFusionError::Plan(format!(
            "{function_name} requires a non-zero interval and limit, and end_millis >= start_millis"
        )));
    }
    Ok((uri.clone(), start, end, interval, limit, *include_payload))
}

#[cfg(feature = "native-media")]
fn local_media_path(uri: &str, function_name: &str) -> Result<std::path::PathBuf> {
    let url = Url::parse(uri).map_err(|_| {
        DataFusionError::Plan(format!("{function_name} requires an absolute file:// URI"))
    })?;
    if url.scheme() != "file" {
        return Err(DataFusionError::Plan(format!(
            "{function_name} only supports local file:// URIs"
        )));
    }
    let path = url.to_file_path().map_err(|_| {
        DataFusionError::Plan(format!("{function_name} received an invalid file URI"))
    })?;
    let size = std::fs::metadata(&path)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
        .len();
    if size > MAX_MEDIA_INPUT_BYTES {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "{function_name} rejects sources larger than {MAX_MEDIA_INPUT_BYTES} bytes"
        )));
    }
    Ok(path)
}

#[cfg(feature = "native-media")]
fn video_frames_provider(
    uri: &str,
    start: u64,
    end: u64,
    every: u64,
    limit: usize,
    include_rgb24: bool,
    execution_governor: Arc<ExecutionGovernor>,
) -> Result<Arc<dyn TableProvider>> {
    let path = local_media_path(uri, VIDEO_FRAMES_FUNCTION)?;
    video_frames_provider_from_path(
        uri,
        path,
        LocalMediaDecodeOptions {
            start_millis: start,
            end_millis: end,
            interval_millis: every,
            limit,
            include_payload: include_rgb24,
        },
        execution_governor,
        #[cfg(feature = "remote-media-s3")]
        None,
    )
}

#[cfg(feature = "native-media")]
fn video_frames_provider_from_path(
    uri: &str,
    path: std::path::PathBuf,
    options: LocalMediaDecodeOptions,
    execution_governor: Arc<ExecutionGovernor>,
    #[cfg(feature = "remote-media-s3")] staged_media: Option<StagedMediaFile>,
) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(VideoFramesTable {
        uri: uri.to_string(),
        path,
        start: options.start_millis,
        end: options.end_millis,
        every: options.interval_millis,
        limit: options.limit,
        include_rgb24: options.include_payload,
        execution_governor,
        #[cfg(feature = "remote-media-s3")]
        _staged_media: staged_media,
    }))
}

#[cfg(feature = "native-media")]
struct VideoFramesTable {
    uri: String,
    path: std::path::PathBuf,
    start: u64,
    end: u64,
    every: u64,
    limit: usize,
    include_rgb24: bool,
    execution_governor: Arc<ExecutionGovernor>,
    #[cfg(feature = "remote-media-s3")]
    _staged_media: Option<StagedMediaFile>,
}

#[cfg(feature = "native-media")]
impl std::fmt::Debug for VideoFramesTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VideoFramesTable")
            .field("uri", &self.uri)
            .field("start", &self.start)
            .field("end", &self.end)
            .field("every", &self.every)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "native-media")]
#[async_trait::async_trait]
impl TableProvider for VideoFramesTable {
    fn schema(&self) -> SchemaRef {
        video_frames_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _io_permit = self.execution_governor.try_acquire_io().ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "lakeprism_video_frames cannot decode media while all I/O permits are in use"
                    .to_string(),
            )
        })?;
        let limit = self.limit.min(limit.unwrap_or(self.limit));
        let include_rgb24 =
            self.include_rgb24 && projection.is_none_or(|columns| columns.contains(&4));
        let (start, end) = pushed_time_range(self.start, self.end, filters);
        let batch = video_frames_batch(
            &self.uri,
            &self.path,
            start,
            end,
            self.every,
            limit,
            include_rgb24,
        )?;
        MemTable::try_new(video_frames_schema(), vec![vec![batch]])?
            .scan(state, projection, &[], None)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if media_time_filter(filter) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[cfg(feature = "native-media")]
fn video_frames_batch(
    uri: &str,
    path: &std::path::Path,
    start: u64,
    end: u64,
    every: u64,
    limit: usize,
    include_rgb24: bool,
) -> Result<RecordBatch> {
    let frames = decode_video_frames(path, start, end, every, limit)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let total_bytes = frames.iter().try_fold(0usize, |total, frame| {
        total.checked_add(frame.rgb24_bytes.len()).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("decoded frame payload overflow".into())
        })
    })?;
    if include_rgb24 && total_bytes > 64 * 1024 * 1024 {
        return Err(DataFusionError::ResourcesExhausted(
            "decoded frame payload exceeds the 64 MiB SQL table-function budget".into(),
        ));
    }
    RecordBatch::try_new(
        video_frames_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                uri,
                frames.len(),
            ))),
            Arc::new(UInt64Array::from_iter(
                frames.iter().map(|frame| frame.timestamp_millis),
            )),
            Arc::new(UInt32Array::from_iter_values(
                frames.iter().map(|frame| frame.width),
            )),
            Arc::new(UInt32Array::from_iter_values(
                frames.iter().map(|frame| frame.height),
            )),
            Arc::new(BinaryArray::from_iter(frames.iter().map(|frame| {
                include_rgb24.then_some(frame.rgb24_bytes.as_slice())
            }))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[cfg(not(feature = "native-media"))]
fn video_frames_provider(
    _uri: &str,
    _start: u64,
    _end: u64,
    _every: u64,
    _limit: usize,
    _include_rgb24: bool,
    _execution_governor: Arc<ExecutionGovernor>,
) -> Result<Arc<dyn TableProvider>> {
    Err(DataFusionError::NotImplemented(format!(
        "{VIDEO_FRAMES_FUNCTION} requires lakeprism-datafusion's native-media feature and target-matched FFmpeg libraries"
    )))
}

#[cfg(feature = "native-media")]
fn audio_segments_provider(
    uri: &str,
    start: u64,
    end: u64,
    segment: u64,
    limit: usize,
    include_payload: bool,
    execution_governor: Arc<ExecutionGovernor>,
) -> Result<Arc<dyn TableProvider>> {
    let path = local_media_path(uri, AUDIO_SEGMENTS_FUNCTION)?;
    audio_segments_provider_from_path(
        uri,
        path,
        LocalMediaDecodeOptions {
            start_millis: start,
            end_millis: end,
            interval_millis: segment,
            limit,
            include_payload,
        },
        execution_governor,
        #[cfg(feature = "remote-media-s3")]
        None,
    )
}

#[cfg(feature = "native-media")]
fn audio_segments_provider_from_path(
    uri: &str,
    path: std::path::PathBuf,
    options: LocalMediaDecodeOptions,
    execution_governor: Arc<ExecutionGovernor>,
    #[cfg(feature = "remote-media-s3")] staged_media: Option<StagedMediaFile>,
) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(AudioSegmentsTable {
        uri: uri.to_string(),
        path,
        start: options.start_millis,
        end: options.end_millis,
        segment: options.interval_millis,
        limit: options.limit,
        include_payload: options.include_payload,
        execution_governor,
        #[cfg(feature = "remote-media-s3")]
        _staged_media: staged_media,
    }))
}

#[cfg(feature = "native-media")]
struct AudioSegmentsTable {
    uri: String,
    path: std::path::PathBuf,
    start: u64,
    end: u64,
    segment: u64,
    limit: usize,
    include_payload: bool,
    execution_governor: Arc<ExecutionGovernor>,
    #[cfg(feature = "remote-media-s3")]
    _staged_media: Option<StagedMediaFile>,
}

#[cfg(feature = "native-media")]
impl std::fmt::Debug for AudioSegmentsTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AudioSegmentsTable")
            .field("uri", &self.uri)
            .field("start", &self.start)
            .field("end", &self.end)
            .field("segment", &self.segment)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "native-media")]
#[async_trait::async_trait]
impl TableProvider for AudioSegmentsTable {
    fn schema(&self) -> SchemaRef {
        audio_segments_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _io_permit = self.execution_governor.try_acquire_io().ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "lakeprism_audio_segments cannot decode media while all I/O permits are in use"
                    .to_string(),
            )
        })?;
        let limit = self.limit.min(limit.unwrap_or(self.limit));
        let include_payload =
            self.include_payload && projection.is_none_or(|columns| columns.contains(&5));
        let (start, end) = pushed_time_range(self.start, self.end, filters);
        let batch = audio_segments_batch(
            &self.uri,
            &self.path,
            start,
            end,
            self.segment,
            limit,
            include_payload,
        )?;
        MemTable::try_new(audio_segments_schema(), vec![vec![batch]])?
            .scan(state, projection, &[], None)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if media_time_filter(filter) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[cfg(feature = "native-media")]
fn pushed_time_range(start: u64, end: u64, filters: &[Expr]) -> (u64, u64) {
    filters.iter().fold((start, end), |(start, end), filter| {
        let Some((column, operator, value)) = media_time_filter_parts(filter) else {
            return (start, end);
        };
        match (column, operator) {
            ("start_millis", Operator::GtEq) => (start.max(value), end),
            ("start_millis", Operator::Gt) => (start.max(value.saturating_add(1)), end),
            ("end_millis", Operator::LtEq) => (start, end.min(value)),
            ("end_millis", Operator::Lt) => (start, end.min(value.saturating_sub(1))),
            _ => (start, end),
        }
    })
}

#[cfg(feature = "native-media")]
fn media_time_filter(filter: &Expr) -> bool {
    media_time_filter_parts(filter).is_some()
}

#[cfg(feature = "native-media")]
fn media_time_filter_parts(filter: &Expr) -> Option<(&str, Operator, u64)> {
    let Expr::BinaryExpr(binary) = filter else {
        return None;
    };
    let Expr::Column(column) = binary.left.as_ref() else {
        return None;
    };
    let Expr::Literal(ScalarValue::Int64(Some(value)), _) = binary.right.as_ref() else {
        return None;
    };
    let value = u64::try_from(*value).ok()?;
    matches!(
        binary.op,
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
    )
    .then_some((column.name.as_str(), binary.op, value))
}

#[cfg(feature = "native-media")]
fn audio_segments_batch(
    uri: &str,
    path: &std::path::Path,
    start: u64,
    end: u64,
    segment: u64,
    limit: usize,
    include_payload: bool,
) -> Result<RecordBatch> {
    let segments = decode_audio_segments(path, start, end, segment, limit)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    RecordBatch::try_new(
        audio_segments_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                uri,
                segments.len(),
            ))),
            Arc::new(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.start_millis),
            )),
            Arc::new(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.end_millis),
            )),
            Arc::new(UInt32Array::from_iter_values(
                segments.iter().map(|segment| segment.sample_rate_hz),
            )),
            Arc::new(UInt16Array::from_iter_values(
                segments.iter().map(|segment| segment.channel_count),
            )),
            Arc::new(BinaryArray::from_iter(segments.iter().map(|segment| {
                include_payload.then_some(segment.f32le_bytes.as_slice())
            }))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[cfg(not(feature = "native-media"))]
fn audio_segments_provider(
    _uri: &str,
    _start: u64,
    _end: u64,
    _segment: u64,
    _limit: usize,
    _include_payload: bool,
    _execution_governor: Arc<ExecutionGovernor>,
) -> Result<Arc<dyn TableProvider>> {
    Err(DataFusionError::NotImplemented(format!(
        "{AUDIO_SEGMENTS_FUNCTION} requires lakeprism-datafusion's native-media feature and target-matched FFmpeg libraries"
    )))
}

#[cfg(feature = "native-media")]
fn video_frames_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("timestamp_millis", DataType::UInt64, true),
        Field::new("width", DataType::UInt32, false),
        Field::new("height", DataType::UInt32, false),
        Field::new("rgb24", DataType::Binary, true),
    ]))
}

#[cfg(feature = "native-media")]
fn audio_segments_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("start_millis", DataType::UInt64, false),
        Field::new("end_millis", DataType::UInt64, false),
        Field::new("sample_rate_hz", DataType::UInt32, false),
        Field::new("channel_count", DataType::UInt16, false),
        Field::new("f32le", DataType::Binary, true),
    ]))
}

struct TranscriptSearch {
    transcript_index: Arc<Mutex<TranscriptIndex>>,
}

impl std::fmt::Debug for TranscriptSearch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TranscriptSearch")
    }
}

impl TableFunctionImpl for TranscriptSearch {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let [
            Expr::Literal(ScalarValue::Utf8(Some(query)), _),
            Expr::Literal(ScalarValue::Int64(Some(limit)), _),
        ] = args.exprs()
        else {
            return Err(DataFusionError::Plan(format!(
                "{TRANSCRIPT_SEARCH_FUNCTION} requires literal query and limit arguments"
            )));
        };
        let limit = usize::try_from(*limit).map_err(|_| {
            DataFusionError::Plan(format!(
                "{TRANSCRIPT_SEARCH_FUNCTION} limit does not fit this platform"
            ))
        })?;
        if limit > MAX_TRANSCRIPT_RESULTS {
            return Err(DataFusionError::Plan(format!(
                "{TRANSCRIPT_SEARCH_FUNCTION} limit must not exceed {MAX_TRANSCRIPT_RESULTS}"
            )));
        }
        let index = self.transcript_index.lock().map_err(|_| {
            DataFusionError::Execution("transcript index state is unavailable".into())
        })?;
        let normalized_query = query.to_lowercase();
        let matching = index
            .all_matching(&normalized_query, limit)
            .cloned()
            .collect::<Vec<_>>();
        transcript_search_provider(&matching)
    }
}

fn transcript_search_provider(segments: &[TranscriptSegment]) -> Result<Arc<dyn TableProvider>> {
    let batch = RecordBatch::try_new(
        transcript_search_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.media_id.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.start_millis),
            )),
            Arc::new(UInt64Array::from_iter_values(
                segments.iter().map(|segment| segment.end_millis),
            )),
            Arc::new(StringArray::from_iter_values(
                segments.iter().map(|segment| segment.text.as_str()),
            )),
            Arc::new(UInt16Array::from_iter(
                segments.iter().map(|segment| segment.confidence_millis),
            )),
            Arc::new(StringArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.lineage.source.media_uri.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                segments
                    .iter()
                    .map(|segment| segment.lineage.source.source_version.as_str()),
            )),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok(Arc::new(MemTable::try_new(
        transcript_search_schema(),
        vec![vec![batch]],
    )?))
}

fn transcript_search_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("media_id", DataType::Utf8, false),
        Field::new("start_millis", DataType::UInt64, false),
        Field::new("end_millis", DataType::UInt64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("confidence_millis", DataType::UInt16, true),
        Field::new("source_uri", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
    ]))
}

struct EmbeddingSearch {
    embedding_index: Arc<Mutex<EmbeddingIndex>>,
    embedding_lineage: Arc<Mutex<Option<lakeprism_core::FeatureLineage>>>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    hybrid: bool,
}

impl std::fmt::Debug for EmbeddingSearch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EmbeddingSearch")
    }
}

impl TableFunctionImpl for EmbeddingSearch {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let (query, limit, candidate_limit, semantic_weight) = if self.hybrid {
            let [
                Expr::Literal(ScalarValue::Utf8(Some(query)), _),
                Expr::Literal(ScalarValue::Int64(Some(limit)), _),
                Expr::Literal(ScalarValue::Int64(Some(candidate_limit)), _),
                Expr::Literal(ScalarValue::Float64(Some(weight)), _),
            ] = args.exprs()
            else {
                return Err(DataFusionError::Plan(format!(
                    "{HYBRID_SEARCH_FUNCTION} requires literal query, limit, candidate_limit, and semantic_weight"
                )));
            };
            (query.as_str(), *limit, *candidate_limit, *weight as f32)
        } else {
            let [
                Expr::Literal(ScalarValue::Utf8(Some(query)), _),
                Expr::Literal(ScalarValue::Int64(Some(limit)), _),
                Expr::Literal(ScalarValue::Int64(Some(candidate_limit)), _),
            ] = args.exprs()
            else {
                return Err(DataFusionError::Plan(format!(
                    "{SEMANTIC_SEARCH_FUNCTION} requires literal query, limit, and candidate_limit"
                )));
            };
            (query.as_str(), *limit, *candidate_limit, 1.0)
        };
        let function = if self.hybrid {
            HYBRID_SEARCH_FUNCTION
        } else {
            SEMANTIC_SEARCH_FUNCTION
        };
        let limit = usize::try_from(limit)
            .map_err(|_| DataFusionError::Plan(format!("{function} limit must be non-negative")))?;
        let candidate_limit = usize::try_from(candidate_limit).map_err(|_| {
            DataFusionError::Plan(format!("{function} candidate_limit must be non-negative"))
        })?;
        let lineage = self
            .embedding_lineage
            .lock()
            .map_err(|_| {
                DataFusionError::Execution("embedding lineage state is unavailable".into())
            })?
            .clone()
            .ok_or_else(|| DataFusionError::Execution("no embedding index is registered".into()))?;
        let query_values = self
            .embedding_provider
            .embed(&EmbeddingRequest {
                text: query.to_string(),
                lineage: lineage.clone(),
            })
            .map_err(|error| DataFusionError::NotImplemented(error.to_string()))?;
        let options = RankingOptions {
            limit,
            candidate_limit: (candidate_limit != 0).then_some(candidate_limit),
        };
        let index = self.embedding_index.lock().map_err(|_| {
            DataFusionError::Execution("embedding index state is unavailable".into())
        })?;
        let rows = if self.hybrid {
            index.rank_hybrid(query, &query_values, &lineage, options, semantic_weight)
        } else {
            index.rank(&query_values, &lineage, options)
        }
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        embedding_search_provider(&rows)
    }
}

fn embedding_search_provider(rows: &[RankedResult]) -> Result<Arc<dyn TableProvider>> {
    let batch = RecordBatch::try_new(
        embedding_search_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.text.as_str()),
            )),
            Arc::new(Float32Array::from_iter_values(
                rows.iter().map(|row| row.score),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(
                |row| match row.semantics {
                    lakeprism_index::RankingSemantics::Approximate => "approximate",
                    lakeprism_index::RankingSemantics::Ranked => "ranked",
                },
            ))),
        ],
    )
    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
    Ok(Arc::new(MemTable::try_new(
        embedding_search_schema(),
        vec![vec![batch]],
    )?))
}

fn embedding_search_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
        Field::new("ranking_semantics", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use lakeprism_core::{SourceIdentity, StorageMode};
    use lakeprism_index::{
        CrossModalKind, CrossModalRecord, DeterministicMockEmbeddingProvider,
        DeterministicMockOcrProvider, DeterministicMockTranscriptionProvider, EmbeddingRequest,
        MediaCostModel, MediaSearchStrategy, OcrResult, RankingOptions,
    };
    use lakeprism_storage::{ExecutionGovernorConfig, MediaSource, SourceMetadata, StorageError};
    use std::collections::BTreeMap;
    use std::io::{Cursor, Write};
    use std::time::Duration;

    #[derive(Clone)]
    struct FixtureRemoteSource {
        bytes: Arc<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl MediaSource for FixtureRemoteSource {
        async fn metadata(&self) -> lakeprism_storage::Result<SourceMetadata> {
            Ok(SourceMetadata {
                size_bytes: self.bytes.len() as u64,
                version: Some("fixture-etag".to_string()),
            })
        }

        async fn read_range(&self, range: ByteRange) -> lakeprism_storage::Result<Vec<u8>> {
            let start = usize::try_from(range.start)
                .map_err(|error| StorageError::InvalidFileUri(error.to_string()))?;
            let end = usize::try_from(range.end_exclusive)
                .map_err(|error| StorageError::InvalidFileUri(error.to_string()))?;
            Ok(self.bytes[start..end].to_vec())
        }
    }

    struct FixtureRemoteResolver {
        source: Arc<FixtureRemoteSource>,
    }

    #[async_trait::async_trait]
    impl MediaResolver for FixtureRemoteResolver {
        async fn resolve(
            &self,
            _media: &MediaRef,
            _access: &AccessContext,
        ) -> lakeprism_storage::Result<Arc<dyn MediaSource>> {
            Ok(self.source.clone())
        }
    }

    #[tokio::test]
    async fn remote_document_sections_registers_a_bounded_sql_relation() {
        let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
        archive
            .start_file(
                "word/document.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        archive
            .write_all(
                br#"<w:document xmlns:w="w"><w:body><w:p><w:r><w:t>Remote report</w:t></w:r></w:p></w:body></w:document>"#,
            )
            .unwrap();
        let bytes = archive.finish().unwrap().into_inner();
        let resolver = FixtureRemoteResolver {
            source: Arc::new(FixtureRemoteSource {
                bytes: Arc::new(bytes),
            }),
        };
        let session = MediaSession::new();
        let media = MediaRef::new(
            "s3://test-bucket/reports/report.docx",
            "document",
            StorageMode::External,
        )
        .unwrap();
        session
            .register_remote_document_sections(
                "remote_report",
                &media,
                &AccessContext::default(),
                &resolver,
            )
            .await
            .unwrap();
        let batches = session
            .collect("SELECT source_version, text FROM remote_report")
            .await
            .unwrap();
        let version = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let text = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(version.value(0), "fixture-etag");
        assert_eq!(text.value(0), "Remote report");
    }

    #[tokio::test]
    async fn concurrent_collects_wait_for_the_configured_query_slot_and_remain_correct() {
        let session = Arc::new(
            MediaSession::try_with_execution_governor(ExecutionGovernorConfig {
                max_concurrent_queries: 1,
                max_cpu_permits: 2,
                max_io_permits: 1,
            })
            .unwrap(),
        );
        let held_query = session.execution_governor().acquire_query().await;
        let first_session = Arc::clone(&session);
        let second_session = Arc::clone(&session);
        let mut first_query =
            tokio::spawn(async move { first_session.collect("SELECT 40 + 2 AS answer").await });
        let mut second_query =
            tokio::spawn(async move { second_session.collect("SELECT 40 + 2 AS answer").await });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut first_query)
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut second_query)
                .await
                .is_err()
        );
        drop(held_query);

        for query in [first_query, second_query] {
            let batches = tokio::time::timeout(Duration::from_secs(1), query)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let answer = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(answer.value(0), 42);
        }
    }

    #[tokio::test]
    async fn cancellation_is_explicit_and_releases_governor_permits() {
        let session = MediaSession::try_with_execution_governor(ExecutionGovernorConfig {
            max_concurrent_queries: 1,
            max_cpu_permits: 1,
            max_io_permits: 1,
        })
        .unwrap();
        let execution = session
            .execute_stream_with_deadline("SELECT 1", None)
            .await
            .unwrap();
        let id = execution.id.clone();
        assert!(session.cancel_query(&id));
        let error = futures::TryStreamExt::try_collect::<Vec<RecordBatch>>(execution.stream)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert_eq!(session.query(&id).unwrap().status, QueryStatus::Cancelled);
        assert_eq!(
            session.collect("SELECT 42 AS answer").await.unwrap()[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            42
        );
    }

    #[tokio::test]
    async fn planning_errors_finish_registered_queries_as_failed() {
        let session = MediaSession::new();
        let id = session.create_query(None);

        let error = match session
            .execute_registered_query("SELECT * FROM missing_lakeprism_table", id.clone())
            .await
        {
            Ok(_) => panic!("missing table query unexpectedly started"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("missing_lakeprism_table"));
        let info = session.query(&id).unwrap();
        assert_eq!(info.status, QueryStatus::Failed);
        assert!(info.metrics.completed_at.is_some());
    }

    #[tokio::test]
    async fn expired_deadlines_finish_registered_queries() {
        let session = MediaSession::new();
        let id = session.create_query(Some(tokio::time::Instant::now()));

        let error = match session
            .execute_registered_query("SELECT 1", id.clone())
            .await
        {
            Ok(_) => panic!("expired query unexpectedly started"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("deadline"));
        let info = session.query(&id).unwrap();
        assert_eq!(info.status, QueryStatus::DeadlineExceeded);
        assert!(info.metrics.completed_at.is_some());
    }

    #[tokio::test]
    async fn audit_events_are_structured_and_never_include_query_text() {
        let session = MediaSession::new();
        let id = session.create_query(None);
        let execution = session
            .execute_registered_query("SELECT 'token=not-for-audit' AS value", id.clone())
            .await
            .unwrap();
        futures::TryStreamExt::try_collect::<Vec<RecordBatch>>(execution.stream)
            .await
            .unwrap();

        let event = session.query_audit_event(&id).unwrap();
        assert_eq!(event.schema_version, 1);
        assert_eq!(event.event_type, "lakeprism.query.lifecycle");
        assert_eq!(event.status, QueryStatus::Succeeded);
        assert_eq!(event.rows, 1);
        assert!(event.started_at_unix_millis.is_some());
        assert!(event.completed_at_unix_millis.is_some());
        assert!(!format!("{event:?}").contains("token=not-for-audit"));
    }

    #[tokio::test]
    async fn mixed_workload_stress_keeps_statuses_and_permits_consistent() {
        let session = Arc::new(
            MediaSession::try_with_execution_governor(ExecutionGovernorConfig {
                max_concurrent_queries: 2,
                max_cpu_permits: 2,
                max_io_permits: 2,
            })
            .unwrap(),
        );
        let mut tasks = Vec::new();
        for index in 0..24 {
            let session = Arc::clone(&session);
            tasks.push(tokio::spawn(async move {
                let id = session.create_query(None);
                if index % 3 == 0 {
                    assert!(session.cancel_query(&id));
                }
                let result = session
                    .execute_registered_query("SELECT 1 AS value", id.clone())
                    .await;
                if let Ok(execution) = result {
                    let _ =
                        futures::TryStreamExt::try_collect::<Vec<RecordBatch>>(execution.stream)
                            .await;
                }
                (id, index % 3 == 0)
            }));
        }
        for task in tasks {
            let (id, cancelled) = task.await.unwrap();
            let status = session.query(&id).unwrap().status;
            if cancelled {
                assert_eq!(status, QueryStatus::Cancelled);
            } else {
                assert_eq!(status, QueryStatus::Succeeded);
            }
        }
        assert_eq!(
            session.collect("SELECT 99 AS value").await.unwrap()[0].num_rows(),
            1
        );
    }

    #[tokio::test]
    async fn stream_holds_the_cpu_permit_until_dropped() {
        let session = Arc::new(
            MediaSession::try_with_execution_governor(ExecutionGovernorConfig {
                max_concurrent_queries: 2,
                max_cpu_permits: 1,
                max_io_permits: 1,
            })
            .unwrap(),
        );
        let stream = session.execute_stream("SELECT 1").await.unwrap();
        let waiting_session = Arc::clone(&session);
        let mut query = tokio::spawn(async move { waiting_session.collect("SELECT 1").await });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut query)
                .await
                .is_err()
        );
        drop(stream);

        let batches = tokio::time::timeout(Duration::from_secs(1), query)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn governed_cpu_permit_bounds_media_udf_execution_and_preserves_values() {
        let session = Arc::new(
            MediaSession::try_with_execution_governor(ExecutionGovernorConfig {
                max_concurrent_queries: 2,
                max_cpu_permits: 1,
                max_io_permits: 1,
            })
            .unwrap(),
        );
        let media = MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap();
        session
            .register_media_refs("media_objects", &[media])
            .await
            .unwrap();

        let held_cpu = session.execution_governor().acquire_cpu().await;
        let waiting_session = Arc::clone(&session);
        let mut query = tokio::spawn(async move {
            waiting_session
                .collect(&format!(
                    "SELECT {MEDIA_URI_FUNCTION}(media) AS uri FROM media_objects"
                ))
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut query)
                .await
                .is_err()
        );

        drop(held_cpu);
        let batches = tokio::time::timeout(Duration::from_secs(1), query)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let uri = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(uri.value(0), "file:///media/a.mp4");
    }

    #[tokio::test]
    async fn media_refs_are_queryable_through_datafusion() {
        let session = MediaSession::new();
        let media = MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap();
        session
            .register_media_refs("media_objects", &[media])
            .await
            .unwrap();

        let batches = session
            .collect("SELECT media.uri, media.media_type FROM media_objects")
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn media_accessor_udfs_use_portable_media_structs() {
        let session = MediaSession::new();
        let media = MediaRef::new(
            "file:///media/a.mp4",
            "video",
            lakeprism_core::StorageMode::External,
        )
        .unwrap();
        session
            .register_media_refs("media_objects", &[media])
            .await
            .unwrap();

        let batches = session
            .collect(&format!(
                "SELECT {MEDIA_URI_FUNCTION}(media) AS uri, \
                 {MEDIA_TYPE_FUNCTION}(media) AS media_type FROM media_objects"
            ))
            .await
            .unwrap();
        let uri = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let media_type = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(uri.value(0), "file:///media/a.mp4");
        assert_eq!(media_type.value(0), "video");
    }

    #[tokio::test]
    async fn document_sections_udtf_extracts_local_docx_at_planning_time() {
        let path = std::env::current_dir().unwrap().join(format!(
            "lakeprism-document-sections-{}.docx",
            std::process::id()
        ));
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "word/document.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer
            .write_all(
                br#"<w:document xmlns:w="urn:test"><w:body><w:p><w:r><w:t>Local text</w:t></w:r></w:p></w:body></w:document>"#,
            )
            .unwrap();
        std::fs::write(&path, writer.finish().unwrap().into_inner()).unwrap();
        let uri = Url::from_file_path(&path).unwrap().to_string();
        let session = MediaSession::new();

        let batches = session
            .collect(&format!(
                "SELECT ordinal, text FROM {DOCUMENT_SECTIONS_FUNCTION}('{uri}')"
            ))
            .await
            .unwrap();

        std::fs::remove_file(path).unwrap();
        let text = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.value(0), "Local text");
    }

    #[tokio::test]
    async fn document_feature_udtfs_expose_bounded_tables_images_search_and_lineage() {
        let path = std::env::current_dir().unwrap().join(format!(
            "lakeprism-document-features-{}.docx",
            std::process::id()
        ));
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "word/document.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer
            .write_all(
                br#"<w:document xmlns:w="urn:test"><w:body><w:p><w:r><w:t>Budget Report</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>Revenue</w:t></w:r></w:p></w:tc></w:tr></w:tbl></w:body></w:document>"#,
            )
            .unwrap();
        writer
            .start_file(
                "word/media/chart.png",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer.write_all(b"actual image payload").unwrap();
        std::fs::write(&path, writer.finish().unwrap().into_inner()).unwrap();
        let uri = Url::from_file_path(&path).unwrap().to_string();
        let session = MediaSession::new();

        let tables = session
            .collect(&format!(
                "SELECT source_version, text FROM {DOCUMENT_TABLES_FUNCTION}('{uri}')"
            ))
            .await
            .unwrap();
        let images = session
            .collect(&format!(
                "SELECT name, bytes FROM {DOCUMENT_IMAGES_FUNCTION}('{uri}', true)"
            ))
            .await
            .unwrap();
        let search = session
            .collect(&format!(
                "SELECT kind, text FROM {DOCUMENT_SEARCH_FUNCTION}('{uri}', 'chart', 2)"
            ))
            .await
            .unwrap();
        let ocr_error = session
            .collect(&format!(
                "SELECT * FROM {DOCUMENT_OCR_FUNCTION}('{uri}', 1)"
            ))
            .await
            .unwrap_err();
        std::fs::remove_file(path).unwrap();

        assert!(
            tables[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .starts_with("size:")
        );
        assert_eq!(
            images[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "word/media/chart.png"
        );
        let image_bytes = images[0]
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(image_bytes.value(0), b"actual image payload");
        assert_eq!(
            search[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "image_name"
        );
        assert!(matches!(ocr_error, DataFusionError::Execution(_)));
    }

    #[tokio::test]
    async fn document_ocr_udtf_uses_a_registered_fixture_provider_and_exact_lineage() {
        let path = std::env::current_dir().unwrap().join(format!(
            "lakeprism-document-ocr-{}.docx",
            std::process::id()
        ));
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file(
                "word/document.xml",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer
            .write_all(br#"<w:document xmlns:w="urn:test"><w:body/></w:document>"#)
            .unwrap();
        writer
            .start_file(
                "word/media/scan.png",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer.write_all(b"fixture image bytes").unwrap();
        std::fs::write(&path, writer.finish().unwrap().into_inner()).unwrap();
        let uri = Url::from_file_path(&path).unwrap().to_string();
        let lineage = lakeprism_core::FeatureLineage {
            source: SourceIdentity {
                media_uri: uri.clone(),
                source_version: local_document_version(&path).unwrap(),
            },
            operator_version: "document-ocr-v1".into(),
            model: None,
            model_version: None,
            parameters: BTreeMap::new(),
        };
        let session =
            MediaSession::with_ocr_provider(Arc::new(DeterministicMockOcrProvider::new([
                OcrResult {
                    ordinal: 7,
                    text: "registered fixture text".into(),
                    confidence_millis: Some(999),
                    lineage,
                },
            ])));
        let batches = session
            .collect(&format!(
                "SELECT image_ordinal, ordinal, text FROM {DOCUMENT_OCR_FUNCTION}('{uri}', 1)"
            ))
            .await
            .unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "registered fixture text"
        );
    }

    #[tokio::test]
    async fn document_sections_udtf_respects_the_media_session_cpu_governor() {
        let session = MediaSession::try_with_execution_governor(ExecutionGovernorConfig {
            max_concurrent_queries: 1,
            max_cpu_permits: 1,
            max_io_permits: 1,
        })
        .unwrap();
        let _cpu_permit = session.execution_governor().acquire_cpu().await;

        let error = session
            .collect(&format!(
                "SELECT * FROM {DOCUMENT_SECTIONS_FUNCTION}('file:///unread.docx')"
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    }

    #[tokio::test]
    async fn transcript_search_udtf_returns_only_registered_index_rows_within_its_limit() {
        let session = MediaSession::new();
        session
            .register_transcript_segments([TranscriptSegment {
                media_id: "clip-1".to_string(),
                start_millis: 100,
                end_millis: 400,
                text: "Local multimodal SQL search".to_string(),
                confidence_millis: Some(987),
                lineage: lakeprism_core::FeatureLineage {
                    source: lakeprism_core::SourceIdentity {
                        media_uri: "file:///media/clip.mp4".to_string(),
                        source_version: "etag-1".to_string(),
                    },
                    operator_version: "transcribe-v1".to_string(),
                    model: Some("local".to_string()),
                    model_version: Some("1".to_string()),
                    parameters: Default::default(),
                },
                created_at_unix_millis: 1,
            }])
            .unwrap();

        let batches = session
            .collect(&format!(
                "SELECT media_id, text FROM {TRANSCRIPT_SEARCH_FUNCTION}('MULTIMODAL', 1)"
            ))
            .await
            .unwrap();
        let text = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.value(0), "Local multimodal SQL search");
    }

    #[cfg(not(feature = "native-media"))]
    #[tokio::test]
    async fn native_media_udtfs_report_their_required_feature() {
        let session = MediaSession::new();
        let error = session
            .collect(&format!(
                "SELECT * FROM {VIDEO_FRAMES_FUNCTION}('file:///missing.mp4', 0, 1, 1, 1, false)"
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
    }

    #[cfg(not(feature = "native-media"))]
    #[test]
    fn mediaref_bound_relations_report_missing_native_support_without_fake_rows() {
        let session = MediaSession::new();
        let media = MediaRef::new("file:///missing.wav", "audio", StorageMode::External).unwrap();
        let error = session
            .register_audio_chunks(
                "audio",
                &media,
                LocalMediaDecodeOptions {
                    start_millis: 0,
                    end_millis: 100,
                    interval_millis: 20,
                    limit: 1,
                    include_payload: true,
                },
            )
            .unwrap_err();
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
    }

    #[cfg(not(feature = "native-media"))]
    #[tokio::test]
    async fn unavailable_native_audio_reports_explicit_unsupported_decode() {
        let session = MediaSession::try_with_execution_governor(ExecutionGovernorConfig {
            max_concurrent_queries: 1,
            max_cpu_permits: 1,
            max_io_permits: 1,
        })
        .unwrap();
        let error = session
            .collect(&format!(
                "SELECT * FROM {AUDIO_SEGMENTS_FUNCTION}('file:///missing.wav', 0, 100, 20, 2, false)"
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
    }

    #[tokio::test]
    async fn parquet_tables_are_registered_lazily() {
        let media = MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap();
        let batch = media_ref_record_batch(&[media]).unwrap();
        let path = std::env::temp_dir().join(format!(
            "lakeprism-datafusion-{}.parquet",
            std::process::id()
        ));
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let session = MediaSession::new();
        session
            .register_parquet("media_parquet", path.to_str().unwrap())
            .await
            .unwrap();
        let batches = session
            .collect("SELECT media FROM media_parquet")
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 1);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn semantic_and_hybrid_sql_expose_explicit_ranking_semantics() {
        use lakeprism_core::{FeatureLineage, SourceIdentity};
        use lakeprism_index::{
            DeterministicMockEmbeddingProvider, EmbeddingRecord, EmbeddingRequest,
        };
        use std::collections::BTreeMap;

        let lineage = FeatureLineage {
            source: SourceIdentity {
                media_uri: "file:///media/a.wav".into(),
                source_version: "v1".into(),
            },
            operator_version: "embed-mock-v1".into(),
            model: Some("deterministic-mock".into()),
            model_version: Some("1".into()),
            parameters: BTreeMap::new(),
        };
        let provider = Arc::new(DeterministicMockEmbeddingProvider::new(16).unwrap());
        let session = MediaSession::with_embedding_provider(provider.clone());
        let records = ["lake data", "ocean water"]
            .into_iter()
            .enumerate()
            .map(|(index, text)| EmbeddingRecord {
                id: format!("row-{index}"),
                text: text.into(),
                values: provider
                    .embed(&EmbeddingRequest {
                        text: text.into(),
                        lineage: lineage.clone(),
                    })
                    .unwrap(),
                lineage: lineage.clone(),
            });
        session.register_embedding_records(records).unwrap();

        let batches = session
            .collect(&format!(
                "SELECT id, ranking_semantics FROM {SEMANTIC_SEARCH_FUNCTION}('lake', 2, 0)"
            ))
            .await
            .unwrap();
        let semantics = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(semantics.value(0), "ranked");
        let approximate = session
            .collect(&format!(
                "SELECT ranking_semantics FROM {HYBRID_SEARCH_FUNCTION}('lake', 2, 1, 0.5)"
            ))
            .await
            .unwrap();
        assert_eq!(
            approximate[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "approximate"
        );
    }

    #[test]
    fn cross_modal_search_is_governed_and_requires_exact_lineage() {
        let lineage = lakeprism_core::FeatureLineage {
            source: SourceIdentity {
                media_uri: "file:///media/clip.mp4".into(),
                source_version: "v1".into(),
            },
            operator_version: "multimodal-embed-v1".into(),
            model: Some("fixture".into()),
            model_version: Some("1".into()),
            parameters: BTreeMap::new(),
        };
        let provider = Arc::new(DeterministicMockEmbeddingProvider::new(8).unwrap());
        let session = MediaSession::with_embedding_provider(provider.clone());
        let values = provider
            .embed(&EmbeddingRequest {
                text: "lake view".into(),
                lineage: lineage.clone(),
            })
            .unwrap();
        session
            .register_cross_modal_records([
                CrossModalRecord {
                    id: "frame-1".into(),
                    kind: CrossModalKind::Image,
                    text: "lake view".into(),
                    values,
                    lineage: lineage.clone(),
                },
                CrossModalRecord {
                    id: "transcript-1".into(),
                    kind: CrossModalKind::Transcript,
                    text: "lake discussion".into(),
                    values: provider
                        .embed(&EmbeddingRequest {
                            text: "lake discussion".into(),
                            lineage: lineage.clone(),
                        })
                        .unwrap(),
                    lineage: lineage.clone(),
                },
            ])
            .unwrap();
        assert_eq!(
            session
                .search_cross_modal(
                    "lake",
                    &lineage,
                    RankingOptions {
                        limit: 2,
                        candidate_limit: None,
                    },
                )
                .unwrap()
                .len(),
            2
        );
        let changed = lakeprism_core::FeatureLineage {
            source: SourceIdentity {
                source_version: "v2".into(),
                ..lineage.source.clone()
            },
            ..lineage
        };
        assert!(
            session
                .search_cross_modal(
                    "lake",
                    &changed,
                    RankingOptions {
                        limit: 2,
                        candidate_limit: None,
                    },
                )
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn media_search_api_automatically_uses_exact_persisted_lineage() {
        let lineage = lakeprism_core::FeatureLineage {
            source: SourceIdentity {
                media_uri: "file:///media/meeting.wav".into(),
                source_version: "v1".into(),
            },
            operator_version: "transcribe-v1".into(),
            model: Some("fixture".into()),
            model_version: Some("1".into()),
            parameters: BTreeMap::new(),
        };
        let row = TranscriptSegment {
            media_id: "meeting".into(),
            start_millis: 0,
            end_millis: 1_000,
            text: "meeting action".into(),
            confidence_millis: Some(1_000),
            lineage: lineage.clone(),
            created_at_unix_millis: 0,
        };
        let session = MediaSession::with_transcription_provider(Arc::new(
            DeterministicMockTranscriptionProvider::new([row.clone()]),
        ));
        session
            .register_persisted_transcript_segments([row])
            .unwrap();
        let request = MediaTranscriptSearchRequest {
            media: MediaRef::new(
                lineage.source.media_uri.clone(),
                "audio",
                StorageMode::External,
            )
            .unwrap(),
            lineage,
            query: "action".into(),
            limit: 1,
            max_cold_segments: 8,
            cost_model: MediaCostModel {
                persisted_index_lookup_units: 1,
                cold_transcription_units_per_segment: 10,
            },
        };
        let limits = ProgressiveScheduleLimits {
            max_work_items: 1,
            max_total_cost_units: 80,
            max_total_segments: 8,
            max_parallelism: 1,
        };
        let explain = session
            .explain_media_transcript_search(&request, limits)
            .unwrap();
        assert_eq!(explain.strategy, MediaSearchStrategy::PersistedIndex);
        assert_eq!(explain.estimated_cost_units, 1);
        assert_eq!(
            session.search_media_transcript(&request, limits).unwrap()[0].text,
            "meeting action"
        );
    }
}
