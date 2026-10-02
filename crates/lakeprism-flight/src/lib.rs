use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCancelQueryRequest,
    ActionCancelQueryResult, ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    ActionEndTransactionRequest, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo,
    CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery, CommandStatementQuery,
    DoPutPreparedStatementResult, ProstMessageExt, SqlInfo, SqlSupportedTransaction,
    TicketStatementQuery,
};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, IpcMessage, SchemaAsIpc, Ticket,
};
use futures::{Stream, TryStreamExt, stream};
use lakeprism_datafusion::{
    AUDIO_SEGMENTS_FUNCTION, DOCUMENT_IMAGES_FUNCTION, DOCUMENT_OCR_FUNCTION,
    DOCUMENT_SEARCH_FUNCTION, DOCUMENT_SECTIONS_FUNCTION, DOCUMENT_TABLES_FUNCTION,
    HYBRID_SEARCH_FUNCTION, MEDIA_TYPE_FUNCTION, MEDIA_URI_FUNCTION, MediaSession,
    SEMANTIC_SEARCH_FUNCTION, TRANSCRIPT_SEARCH_FUNCTION, VIDEO_FRAMES_FUNCTION,
};
use prost::Message;
use thiserror::Error;
use tonic::{Request, Response, Status};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum FlightQueryError {
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

pub struct FlightStatementService<'a> {
    session: &'a MediaSession,
}

impl<'a> FlightStatementService<'a> {
    pub fn new(session: &'a MediaSession) -> Self {
        Self { session }
    }

    pub async fn execute_statement(
        &self,
        statement: &str,
    ) -> Result<Vec<FlightData>, FlightQueryError> {
        let batches = self.session.collect(statement).await?;
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
        Ok(arrow_flight::utils::batches_to_flight_data(
            &schema, batches,
        )?)
    }
}

type FlightDataStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

const MAX_PREPARED_STATEMENTS: usize = 1_024;
/// Vendor SQL-info key containing the local LakePrism SQL function contracts.
pub const LAKEPRISM_SQL_INFO_LOCAL_FUNCTIONS: u32 = 10_000;
/// Vendor SQL-info key containing process-local execution-governor capabilities and limits.
pub const LAKEPRISM_SQL_INFO_EXECUTION_GOVERNOR: u32 = 10_001;

struct ActiveFlightDataStream {
    inner: FlightDataStream,
    active_queries: Arc<Mutex<HashMap<Vec<u8>, String>>>,
    handle: Vec<u8>,
    completed: bool,
}

impl ActiveFlightDataStream {
    fn finish(&mut self) {
        if !self.completed {
            self.completed = true;
            if let Ok(mut queries) = self.active_queries.lock() {
                queries.remove(&self.handle);
            }
        }
    }
}

impl Stream for ActiveFlightDataStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            std::task::Poll::Ready(None) => {
                self.finish();
                std::task::Poll::Ready(None)
            }
            poll => poll,
        }
    }
}

impl Drop for ActiveFlightDataStream {
    fn drop(&mut self) {
        self.finish();
    }
}

pub struct FlightSqlServer {
    session: Arc<MediaSession>,
    statements: Mutex<HashMap<Vec<u8>, StatementHandle>>,
    active_queries: Arc<Mutex<HashMap<Vec<u8>, String>>>,
    bearer_token: Option<String>,
}

enum StatementHandle {
    Ticket(String),
    Prepared(String),
}

impl FlightSqlServer {
    pub fn new(session: Arc<MediaSession>) -> Self {
        Self {
            session,
            statements: Mutex::new(HashMap::new()),
            active_queries: Arc::new(Mutex::new(HashMap::new())),
            bearer_token: None,
        }
    }

    pub fn with_optional_bearer_token(session: Arc<MediaSession>, bearer_token: String) -> Self {
        Self {
            session,
            statements: Mutex::new(HashMap::new()),
            active_queries: Arc::new(Mutex::new(HashMap::new())),
            bearer_token: Some(bearer_token),
        }
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(expected_token) = &self.bearer_token else {
            return Ok(());
        };
        let Some(value) = request.metadata().get("authorization") else {
            return Err(Status::unauthenticated("authorization is required"));
        };
        let value = value
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is invalid"))?;
        let token = value
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("authorization must use Bearer"))?;
        if token != expected_token {
            return Err(Status::unauthenticated("bearer token is invalid"));
        }
        Ok(())
    }

    async fn flight_info(
        &self,
        statement: &str,
        handle: Vec<u8>,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let dataframe = self
            .session
            .sql(statement)
            .await
            .map_err(datafusion_status)?;
        let ticket = TicketStatementQuery {
            statement_handle: handle.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });

        FlightInfo::new()
            .try_with_schema(dataframe.schema().as_arrow())
            .map_err(arrow_status)
            .map(|info| info.with_endpoint(endpoint).with_descriptor(descriptor))
    }

    fn metadata_flight_info<M: ProstMessageExt>(
        &self,
        query: &M,
        schema: arrow::datatypes::SchemaRef,
        descriptor: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        });
        FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(arrow_status)
            .map(|info| Response::new(info.with_endpoint(endpoint).with_descriptor(descriptor)))
    }

    fn metadata_stream(
        batch: arrow_flight::error::Result<arrow::record_batch::RecordBatch>,
    ) -> Response<FlightDataStream> {
        let schema = batch
            .as_ref()
            .map(|batch| batch.schema())
            .unwrap_or_else(|_| Arc::new(arrow::datatypes::Schema::empty()));
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async move { batch }))
            .map_err(Status::from);
        Response::new(Box::pin(stream))
    }

    fn sql_info(&self) -> Result<arrow_flight::sql::metadata::SqlInfoData, Status> {
        let governor = self.session.execution_governor();
        let governor_config = governor.config();
        let governor_capabilities = governor.capabilities();
        let mut builder = SqlInfoDataBuilder::new();
        builder.append(SqlInfo::FlightSqlServerName, "LakePrism");
        builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        builder.append(SqlInfo::FlightSqlServerArrowVersion, "59");
        builder.append(SqlInfo::FlightSqlServerReadOnly, true);
        builder.append(SqlInfo::FlightSqlServerSql, true);
        builder.append(SqlInfo::FlightSqlServerSubstrait, false);
        builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::None as i32,
        );
        builder.append(SqlInfo::FlightSqlServerCancel, true);
        builder.append(SqlInfo::FlightSqlServerBulkIngestion, false);
        builder.append(
            LAKEPRISM_SQL_INFO_LOCAL_FUNCTIONS,
            vec![
                format!("{MEDIA_URI_FUNCTION}(media struct) -> UTF8"),
                format!("{MEDIA_TYPE_FUNCTION}(media struct) -> UTF8"),
                format!(
                    "{DOCUMENT_SECTIONS_FUNCTION}(literal file:// .pdf|.docx) \
                     -> TABLE(source_uri UTF8, source_version UTF8, ordinal UINT32, heading UTF8 NULL, text UTF8)"
                ),
                format!(
                    "{DOCUMENT_TABLES_FUNCTION}(literal file:// .pdf|.docx) \
                     -> TABLE(...); DOCX cells or conservative PDF text-layout tables"
                ),
                format!(
                    "{DOCUMENT_IMAGES_FUNCTION}(literal file:// .pdf|.docx, literal include_bytes) \
                     -> TABLE(...); embedded DOCX bytes or PDF image-XObject streams"
                ),
                format!(
                    "{DOCUMENT_SEARCH_FUNCTION}(literal file:// .pdf|.docx, literal query, literal limit) \
                     -> TABLE(...); text, DOCX table cells, and image names only"
                ),
                format!(
                    "{DOCUMENT_OCR_FUNCTION}(literal file:// .pdf|.docx, literal limit) \
                     -> TABLE(...); application OCR provider or ProviderUnavailable"
                ),
                format!(
                    "{VIDEO_FRAMES_FUNCTION}(literal uri, start, end, every, limit, include_rgb24) \
                     -> TABLE(...); native FFmpeg feature required"
                ),
                format!(
                    "{AUDIO_SEGMENTS_FUNCTION}(literal uri, start, end, segment, limit, false) \
                     -> TABLE(...); native FFmpeg feature required"
                ),
                format!(
                    "{TRANSCRIPT_SEARCH_FUNCTION}(literal query, limit) \
                     -> TABLE(...); registered transcript index only"
                ),
                format!(
                    "{SEMANTIC_SEARCH_FUNCTION}(literal query, limit, candidate_limit) \
                     -> TABLE(id, text, score, ranking_semantics); 0 candidate_limit is exact ranked, positive is approximate"
                ),
                format!(
                    "{HYBRID_SEARCH_FUNCTION}(literal query, limit, candidate_limit, semantic_weight) \
                     -> TABLE(id, text, score, ranking_semantics); only registered exact-lineage embeddings"
                ),
            ],
        );
        builder.append(
            LAKEPRISM_SQL_INFO_EXECUTION_GOVERNOR,
            vec![
                "scope=process-local MediaSession".to_string(),
                format!(
                    "max_concurrent_queries={}",
                    governor_config.max_concurrent_queries
                ),
                format!("max_cpu_permits={}", governor_config.max_cpu_permits),
                format!("max_io_permits={}", governor_config.max_io_permits),
                format!("deadlines={:?}", governor_capabilities.deadlines).to_ascii_lowercase(),
                format!("cancellation={:?}", governor_capabilities.cancellation)
                    .to_ascii_lowercase(),
            ],
        );
        builder.build().map_err(flight_status)
    }

    async fn execute(
        &self,
        statement: String,
        ticket_handle: Option<Vec<u8>>,
    ) -> Result<Response<FlightDataStream>, Status> {
        let query_id = self.session.create_query(None);
        if let Some(handle) = ticket_handle.as_ref() {
            self.active_queries
                .lock()
                .map_err(|_| Status::internal("query state is unavailable"))?
                .insert(handle.clone(), query_id.clone());
        }
        let execution = self
            .session
            .execute_registered_query(&statement, query_id)
            .await
            .map_err(datafusion_status)?;
        let schema = execution.stream.schema();
        let stream =
            FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(execution.stream.map_err(|error| {
                    arrow_flight::error::FlightError::from(datafusion_status(error))
                }))
                .map_err(Status::from);
        let stream: FlightDataStream = Box::pin(stream);
        if let Some(handle) = ticket_handle {
            return Ok(Response::new(Box::pin(ActiveFlightDataStream {
                inner: stream,
                active_queries: Arc::clone(&self.active_queries),
                handle,
                completed: false,
            })));
        }
        Ok(Response::new(stream))
    }

    async fn prepared_flight_info(
        &self,
        statement: &str,
        handle: Vec<u8>,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let dataframe = self
            .session
            .sql(statement)
            .await
            .map_err(datafusion_status)?;
        let ticket = CommandPreparedStatementQuery {
            prepared_statement_handle: handle.into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });
        FlightInfo::new()
            .try_with_schema(dataframe.schema().as_arrow())
            .map_err(arrow_status)
            .map(|info| info.with_endpoint(endpoint).with_descriptor(descriptor))
    }

    fn prepared_statement(&self, handle: &[u8]) -> Result<String, Status> {
        self.statements
            .lock()
            .map_err(|_| Status::internal("prepared statement state is unavailable"))?
            .get(handle)
            .and_then(|entry| match entry {
                StatementHandle::Prepared(statement) => Some(statement.clone()),
                StatementHandle::Ticket(_) => None,
            })
            .ok_or_else(|| Status::not_found("prepared statement does not exist"))
    }

    fn insert_statement(
        &self,
        statement: String,
        handle_type: StatementHandleType,
    ) -> Result<Vec<u8>, Status> {
        let mut statements = self
            .statements
            .lock()
            .map_err(|_| Status::internal("prepared statement state is unavailable"))?;
        if statements.len() >= MAX_PREPARED_STATEMENTS {
            return Err(Status::resource_exhausted(
                "prepared statement limit reached",
            ));
        }
        let handle = Uuid::new_v4().as_bytes().to_vec();
        let statement = match handle_type {
            StatementHandleType::Ticket => StatementHandle::Ticket(statement),
            StatementHandleType::Prepared => StatementHandle::Prepared(statement),
        };
        statements.insert(handle.clone(), statement);
        Ok(handle)
    }

    fn take_ticket(&self, handle: &[u8]) -> Result<String, Status> {
        let mut statements = self
            .statements
            .lock()
            .map_err(|_| Status::internal("prepared statement state is unavailable"))?;
        match statements.get(handle) {
            Some(StatementHandle::Ticket(_)) => match statements.remove(handle) {
                Some(StatementHandle::Ticket(statement)) => Ok(statement),
                _ => Err(Status::internal(
                    "statement ticket state changed unexpectedly",
                )),
            },
            _ => Err(Status::not_found("statement ticket does not exist")),
        }
    }

    async fn prepared_statement_result(
        &self,
        statement: String,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let dataframe = self
            .session
            .sql(&statement)
            .await
            .map_err(datafusion_status)?;
        let options = IpcWriteOptions::default();
        let message: IpcMessage = SchemaAsIpc::new(dataframe.schema().as_arrow(), &options)
            .try_into()
            .map_err(arrow_status)?;
        let handle = self.insert_statement(statement, StatementHandleType::Prepared)?;
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: message.0,
            // DataFusion 55 has no safe Flight SQL parameter-binding API for
            // this session. DoPut reports Unimplemented rather than applying
            // client values through SQL text interpolation.
            parameter_schema: Vec::new().into(),
        })
    }
}

enum StatementHandleType {
    Ticket,
    Prepared,
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServer {
    type FlightService = Self;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
        let handle = self.insert_statement(query.query.clone(), StatementHandleType::Ticket)?;
        let info = self
            .flight_info(&query.query, handle.clone(), request.into_inner())
            .await;
        if info.is_err() {
            self.statements
                .lock()
                .map_err(|_| Status::internal("prepared statement state is unavailable"))?
                .remove(&handle);
        }
        info.map(Response::new)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
        self.metadata_flight_info(&query, query.into_builder().schema(), request.into_inner())
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
        self.metadata_flight_info(
            &query,
            query.clone().into_builder().schema(),
            request.into_inner(),
        )
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
        self.metadata_flight_info(
            &query,
            query.clone().into_builder().schema(),
            request.into_inner(),
        )
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
        self.metadata_flight_info(&query, query.into_builder().schema(), request.into_inner())
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
        let info = self.sql_info()?;
        self.metadata_flight_info(
            &query,
            query.clone().into_builder(&info).schema(),
            request.into_inner(),
        )
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authorize(&_request)?;
        let handle = ticket.statement_handle.to_vec();
        self.execute(self.take_ticket(&handle)?, Some(handle)).await
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authorize(&request)?;
        let mut builder = query.into_builder();
        for catalog_name in self.session.catalog_names() {
            builder.append(catalog_name);
        }
        let batch = builder.build();
        Ok(Self::metadata_stream(batch))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authorize(&request)?;
        let mut builder = query.into_builder();
        for (catalog_name, schema_name) in self.session.schema_names() {
            builder.append(catalog_name, schema_name);
        }
        Ok(Self::metadata_stream(builder.build()))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authorize(&request)?;
        let mut builder = query.into_builder();
        for table in self
            .session
            .catalog_tables()
            .await
            .map_err(datafusion_status)?
        {
            builder
                .append(
                    table.catalog_name,
                    table.schema_name,
                    table.table_name,
                    table.table_type,
                    table.schema.as_ref(),
                )
                .map_err(flight_status)?;
        }
        Ok(Self::metadata_stream(builder.build()))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authorize(&request)?;
        let mut builder = query.into_builder();
        builder.append("TABLE");
        builder.append("TEMPORARY");
        builder.append("VIEW");
        Ok(Self::metadata_stream(builder.build()))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authorize(&request)?;
        let info = self.sql_info()?;
        Ok(Self::metadata_stream(query.into_builder(&info).build()))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.authorize(&request)?;
        if query.transaction_id.is_some() {
            return Err(Status::unimplemented(
                "transactions are not supported because MediaSession has no isolation primitive",
            ));
        }
        self.prepared_statement_result(query.query).await
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
        let statement = self.prepared_statement(&query.prepared_statement_handle)?;
        self.prepared_flight_info(
            &statement,
            query.prepared_statement_handle.to_vec(),
            request.into_inner(),
        )
        .await
        .map(Response::new)
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.authorize(&_request)?;
        let handle = query.prepared_statement_handle.to_vec();
        self.execute(self.prepared_statement(&handle)?, Some(handle))
            .await
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        self.authorize(&_request)?;
        let removed = self
            .statements
            .lock()
            .map_err(|_| Status::internal("prepared statement state is unavailable"))?
            .remove(query.prepared_statement_handle.as_ref());
        if removed.is_none() {
            return Err(Status::not_found("prepared statement does not exist"));
        }
        Ok(())
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        self.authorize(&request)?;
        Err(Status::unimplemented(
            "prepared statement parameter binding is not supported",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        self.authorize(&request)?;
        Err(Status::unimplemented(
            "transactions are not supported because MediaSession has no isolation primitive",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        self.authorize(&request)?;
        Err(Status::unimplemented(
            "transactions are not supported because MediaSession has no isolation primitive",
        ))
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        self.authorize(&request)?;
        let info = FlightInfo::decode(query.info.as_ref())
            .map_err(|_| Status::invalid_argument("cancel request has invalid FlightInfo"))?;
        let handle = info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.as_ref())
            .and_then(|ticket| {
                let any = arrow_flight::sql::Any::decode(ticket.ticket.as_ref()).ok()?;
                match arrow_flight::sql::Command::try_from(any).ok()? {
                    arrow_flight::sql::Command::TicketStatementQuery(ticket) => {
                        Some(ticket.statement_handle.to_vec())
                    }
                    arrow_flight::sql::Command::CommandPreparedStatementQuery(ticket) => {
                        Some(ticket.prepared_statement_handle.to_vec())
                    }
                    _ => None,
                }
            })
            .ok_or_else(|| {
                Status::invalid_argument("cancel request has no LakePrism query ticket")
            })?;
        let query_id = self
            .active_queries
            .lock()
            .map_err(|_| Status::internal("query state is unavailable"))?
            .get(&handle)
            .cloned()
            .ok_or_else(|| Status::not_found("query is not active"))?;
        if self.session.cancel_query(&query_id) {
            Ok(ActionCancelQueryResult {
                // Protocol enum value `Cancelled`; Arrow Flight 59 does not
                // re-export the generated nested enum from its SQL module.
                result: 1,
            })
        } else {
            Err(Status::not_found("query is no longer cancellable"))
        }
    }
}

fn datafusion_status(error: datafusion::error::DataFusionError) -> Status {
    let message = error.to_string();
    if message == "Execution error: query was cancelled" {
        Status::cancelled("query was cancelled")
    } else if message == "Execution error: query deadline was exceeded" {
        Status::deadline_exceeded("query deadline was exceeded")
    } else {
        Status::invalid_argument(message)
    }
}

fn arrow_status(error: arrow::error::ArrowError) -> Status {
    Status::internal(error.to_string())
}

fn flight_status(error: arrow_flight::error::FlightError) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, UInt32Array};
    use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::flight_service_server::FlightServiceServer;
    use futures::StreamExt;
    use futures::TryStreamExt;
    use lakeprism_core::{MediaRef, StorageMode};

    #[tokio::test]
    async fn statement_results_are_encoded_as_arrow_flight_data() {
        let session = MediaSession::new();
        let media = MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap();
        session
            .register_media_refs("media", &[media])
            .await
            .unwrap();

        let data = FlightStatementService::new(&session)
            .execute_statement("SELECT media FROM media")
            .await
            .unwrap();

        assert!(!data.is_empty());
    }

    #[tokio::test]
    async fn prepared_statements_stream_results_and_are_closed_explicitly() {
        let server = FlightSqlServer::new(Arc::new(MediaSession::new()));
        let prepared = server
            .do_action_create_prepared_statement(
                ActionCreatePreparedStatementRequest {
                    query: "SELECT 42 AS answer".to_string(),
                    transaction_id: None,
                },
                Request::new(arrow_flight::Action::default()),
            )
            .await
            .unwrap();
        assert!(!prepared.dataset_schema.is_empty());
        assert!(prepared.parameter_schema.is_empty());

        let query = CommandPreparedStatementQuery {
            prepared_statement_handle: prepared.prepared_statement_handle.clone(),
        };
        let info = server
            .get_flight_info_prepared_statement(
                query.clone(),
                Request::new(FlightDescriptor::new_cmd(Vec::new())),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(info.endpoint.len(), 1);

        let response = server
            .do_get_prepared_statement(query.clone(), Request::new(Ticket::default()))
            .await
            .unwrap();
        let data = response.into_inner().collect::<Vec<_>>().await;
        assert!(data.len() >= 2);
        assert!(data.into_iter().all(|item| item.is_ok()));

        server
            .do_action_close_prepared_statement(
                ActionClosePreparedStatementRequest {
                    prepared_statement_handle: query.prepared_statement_handle.clone(),
                },
                Request::new(arrow_flight::Action::default()),
            )
            .await
            .unwrap();

        let error = match server
            .do_get_prepared_statement(query, Request::new(Ticket::default()))
            .await
        {
            Ok(_) => panic!("closed prepared statement was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[test]
    fn statement_tickets_are_consumed_once() {
        let server = FlightSqlServer::new(Arc::new(MediaSession::new()));
        let handle = server
            .insert_statement("SELECT 1".to_string(), StatementHandleType::Ticket)
            .unwrap();

        assert_eq!(server.take_ticket(&handle).unwrap(), "SELECT 1");
        assert_eq!(
            server.take_ticket(&handle).unwrap_err().code(),
            tonic::Code::NotFound
        );
    }

    #[tokio::test]
    async fn flight_cancel_marks_the_active_query_cancelled() {
        let server = FlightSqlServer::new(Arc::new(MediaSession::new()));
        let info = server
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "SELECT 1 AS answer".to_string(),
                    transaction_id: None,
                },
                Request::new(FlightDescriptor::new_cmd(Vec::new())),
            )
            .await
            .unwrap()
            .into_inner();
        let handle = server
            .statements
            .lock()
            .unwrap()
            .keys()
            .next()
            .cloned()
            .unwrap();
        let response = server
            .do_get_statement(
                TicketStatementQuery {
                    statement_handle: handle.into(),
                },
                Request::new(Ticket::default()),
            )
            .await
            .unwrap();
        let result = server
            .do_action_cancel_query(
                ActionCancelQueryRequest {
                    info: info.encode_to_vec().into(),
                },
                Request::new(arrow_flight::Action::default()),
            )
            .await
            .unwrap();
        assert_eq!(result.result, 1);
        assert_eq!(
            response
                .into_inner()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Cancelled
        );
        assert!(
            server
                .active_queries
                .lock()
                .unwrap()
                .values()
                .all(|id| server.session.query(id).unwrap().status
                    == lakeprism_datafusion::QueryStatus::Cancelled)
        );
    }

    #[tokio::test]
    async fn dropping_a_flight_stream_cancels_and_forgets_the_query() {
        let server = FlightSqlServer::new(Arc::new(MediaSession::new()));
        let info = server
            .get_flight_info_statement(
                CommandStatementQuery {
                    query: "SELECT 1 AS answer".to_string(),
                    transaction_id: None,
                },
                Request::new(FlightDescriptor::new_cmd(Vec::new())),
            )
            .await
            .unwrap()
            .into_inner();
        let handle = server
            .statements
            .lock()
            .unwrap()
            .keys()
            .next()
            .cloned()
            .unwrap();
        let response = server
            .do_get_statement(
                TicketStatementQuery {
                    statement_handle: handle.clone().into(),
                },
                Request::new(Ticket::default()),
            )
            .await
            .unwrap();
        let query_id = server
            .active_queries
            .lock()
            .unwrap()
            .get(&handle)
            .cloned()
            .unwrap();

        drop(response);

        assert_eq!(
            server.session.query(&query_id).unwrap().status,
            lakeprism_datafusion::QueryStatus::Cancelled
        );
        assert!(!server.active_queries.lock().unwrap().contains_key(&handle));
        assert_eq!(info.endpoint.len(), 1);
    }

    #[tokio::test]
    async fn catalogs_are_streamed_from_the_datafusion_session() {
        let server = FlightSqlServer::new(Arc::new(MediaSession::new()));
        let response = server
            .do_get_catalogs(CommandGetCatalogs {}, Request::new(Ticket::default()))
            .await
            .unwrap();
        let data = response.into_inner().collect::<Vec<_>>().await;

        assert!(data.len() >= 2);
        assert!(data.into_iter().all(|item| item.is_ok()));
    }

    #[tokio::test]
    async fn metadata_uses_live_datafusion_catalogs() {
        let session = Arc::new(MediaSession::new());
        let media = MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap();
        session
            .register_media_refs("media", &[media])
            .await
            .unwrap();
        let server = FlightSqlServer::new(session);

        let tables = server
            .do_get_tables(
                CommandGetTables {
                    catalog: None,
                    db_schema_filter_pattern: None,
                    table_name_filter_pattern: Some("media".to_string()),
                    table_types: vec!["TABLE".to_string()],
                    include_schema: true,
                },
                Request::new(Ticket::default()),
            )
            .await
            .unwrap()
            .into_inner()
            .map_err(Into::into);
        let decoded = FlightDataDecoder::new(tables)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(decoded.iter().any(|message| matches!(
            message.payload,
            DecodedPayload::RecordBatch(ref batch) if batch.num_rows() == 1
        )));

        let sql_info = server
            .do_get_sql_info(
                CommandGetSqlInfo {
                    info: vec![SqlInfo::FlightSqlServerCancel as u32],
                },
                Request::new(Ticket::default()),
            )
            .await
            .unwrap();
        assert!(sql_info.into_inner().collect::<Vec<_>>().await.len() >= 2);
    }

    #[tokio::test]
    async fn unsupported_operations_and_invalid_cancellation_are_reported() {
        let server = FlightSqlServer::new(Arc::new(MediaSession::new()));
        let error = server
            .do_action_begin_transaction(
                ActionBeginTransactionRequest {},
                Request::new(arrow_flight::Action::default()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unimplemented);

        let error = server
            .do_action_cancel_query(
                ActionCancelQueryRequest {
                    info: Vec::new().into(),
                },
                Request::new(arrow_flight::Action::default()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn configured_bearer_authentication_is_required_and_exact() {
        let server = FlightSqlServer::with_optional_bearer_token(
            Arc::new(MediaSession::new()),
            "expected-token".to_string(),
        );
        assert_eq!(
            server.authorize(&Request::new(())).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer expected-token".parse().unwrap());
        assert!(server.authorize(&request).is_ok());
    }

    #[tokio::test]
    async fn tonic_client_executes_and_decodes_a_statement_ticket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let session = Arc::new(MediaSession::new());
        session
            .register_media_refs(
                "media",
                &[MediaRef::new("file:///media/a.mp4", "video", StorageMode::External).unwrap()],
            )
            .await
            .unwrap();
        let server = FlightSqlServer::new(session);
        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(server))
                .serve(address)
                .await
        });
        tokio::task::yield_now().await;

        let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightServiceClient::new(channel);
        let query = CommandStatementQuery {
            query: format!("SELECT {MEDIA_URI_FUNCTION}(media) AS uri FROM media"),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(query.as_any().encode_to_vec());
        let info = client
            .get_flight_info(descriptor)
            .await
            .unwrap()
            .into_inner();
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        let stream = client.do_get(ticket).await.unwrap().into_inner();
        let decoded = FlightDataDecoder::new(stream.map_err(Into::into))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = decoded
            .iter()
            .find_map(|message| match &message.payload {
                DecodedPayload::RecordBatch(batch) => Some(batch),
                _ => None,
            })
            .unwrap();
        let uri = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(uri.value(0), "file:///media/a.mp4");

        let metadata_query = CommandGetSqlInfo {
            info: vec![
                LAKEPRISM_SQL_INFO_LOCAL_FUNCTIONS,
                LAKEPRISM_SQL_INFO_EXECUTION_GOVERNOR,
            ],
        };
        let metadata_info = client
            .get_flight_info(FlightDescriptor::new_cmd(
                metadata_query.as_any().encode_to_vec(),
            ))
            .await
            .unwrap()
            .into_inner();
        let metadata_ticket = metadata_info.endpoint[0].ticket.clone().unwrap();
        let metadata = FlightDataDecoder::new(
            client
                .do_get(metadata_ticket)
                .await
                .unwrap()
                .into_inner()
                .map_err(Into::into),
        )
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
        let metadata_batch = metadata
            .iter()
            .find_map(|message| match &message.payload {
                DecodedPayload::RecordBatch(batch) => Some(batch),
                _ => None,
            })
            .unwrap();
        let keys = metadata_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(
            keys.values(),
            &[
                LAKEPRISM_SQL_INFO_LOCAL_FUNCTIONS,
                LAKEPRISM_SQL_INFO_EXECUTION_GOVERNOR,
            ]
        );
        server_task.abort();
    }
}
