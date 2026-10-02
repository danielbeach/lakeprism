use lakeprism_core::{
    FeatureLineage, MediaConstraints, MediaProjection, MediaRef, SourceIdentity, StorageMode,
};
use lakeprism_datafusion::MediaSession;
use lakeprism_derived::{DerivedIndexRow, refresh_embedding_index_parquet};
#[cfg(feature = "embedding-subprocess")]
use lakeprism_embedding::{EmbeddingSubprocessConfig, EmbeddingSubprocessProvider};
use lakeprism_index::{DeterministicMockEmbeddingProvider, EmbeddingRecord, TranscriptSegment};
use lakeprism_media::plan_frames;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict, PyModule};
use std::collections::{BTreeMap, HashSet};
use std::ffi::c_void;
#[cfg(feature = "delta-rs")]
use std::io::Cursor;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;

#[pyclass(name = "MediaRef", skip_from_py_object)]
#[derive(Clone)]
struct PythonMediaRef {
    inner: MediaRef,
}

#[pyclass(name = "TranscriptSegment", skip_from_py_object)]
#[derive(Clone)]
struct PythonTranscriptSegment {
    inner: TranscriptSegment,
}

#[pyclass(name = "EmbeddingRecord", skip_from_py_object)]
#[derive(Clone)]
struct PythonEmbeddingRecord {
    inner: EmbeddingRecord,
}

#[pyclass(name = "DerivedIndexRow", skip_from_py_object)]
#[derive(Clone)]
struct PythonDerivedIndexRow {
    inner: DerivedIndexRow,
}

/// Optional, explicit configuration for the local subprocess embedding
/// adapter. It is feature-gated so ordinary portable wheels have no provider
/// runtime dependency and no model is included.
#[cfg(feature = "embedding-subprocess")]
#[pyclass(name = "EmbeddingSubprocessConfig", skip_from_py_object)]
#[derive(Clone)]
struct PythonEmbeddingSubprocessConfig {
    inner: EmbeddingSubprocessConfig,
}

#[pyclass(name = "LocalCatalog")]
struct PythonLocalCatalog {
    inner: Mutex<lakeprism_catalog::LocalCatalog>,
}

#[cfg(feature = "unity")]
#[pyclass(name = "UnityQueryContext", skip_from_py_object)]
#[derive(Clone)]
struct PythonUnityQueryContext {
    inner: lakeprism_unity::enabled::UnityQueryContext,
}

/// Keeps only an application callback. Tokens returned by it are converted
/// directly into a request-local Rust credential and are never retained in a
/// session, catalog, Python binding, or diagnostic object.
#[cfg(feature = "unity")]
struct PythonUnityCredentialProvider {
    supplier: Py<PyAny>,
}

#[cfg(feature = "unity")]
#[async_trait::async_trait]
impl lakeprism_unity::enabled::CredentialProvider for PythonUnityCredentialProvider {
    async fn credential_for(
        &self,
        context: &lakeprism_unity::enabled::UnityQueryContext,
    ) -> Result<lakeprism_unity::enabled::VendedCredential, lakeprism_unity::enabled::UnityError>
    {
        let result = Python::attach(|py| -> PyResult<String> {
            self.supplier
                .bind(py)
                .call1((
                    context.query_id(),
                    &context.access_context().principal,
                    context.access_context().catalog_identity.as_deref(),
                ))?
                .extract()
        });
        let token = result.map_err(|_| lakeprism_unity::enabled::UnityError::CredentialProvider)?;
        if token.trim().is_empty() || token.contains(['\r', '\n']) {
            return Err(lakeprism_unity::enabled::UnityError::CredentialProvider);
        }
        Ok(lakeprism_unity::enabled::VendedCredential::new(token))
    }
}

#[cfg(feature = "unity")]
#[pyclass(name = "UnityCatalog")]
struct PythonUnityCatalog {
    client: lakeprism_unity::enabled::UnityCatalogClient<PythonUnityCredentialProvider>,
    runtime: Arc<Runtime>,
}

#[cfg(feature = "flight")]
#[pyclass(name = "FlightServer")]
struct PythonFlightServer {
    session: Arc<MediaSession>,
    runtime: Arc<Runtime>,
    endpoint: Mutex<Option<String>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[cfg(feature = "flight")]
#[pyclass(name = "FlightClient")]
struct PythonFlightClient {
    endpoint: String,
    runtime: Arc<Runtime>,
    /// A Python callable, never a token. It is invoked for each RPC while the
    /// token itself remains a stack-local value in `execute`.
    auth_supplier: Option<Py<PyAny>>,
}

#[pymethods]
impl PythonDerivedIndexRow {
    #[new]
    #[pyo3(signature = (id, text, values, source_uri, source_version, operator_version, model = None, model_version = None, parameters = None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: String,
        text: String,
        values: Vec<f32>,
        source_uri: String,
        source_version: String,
        operator_version: String,
        model: Option<String>,
        model_version: Option<String>,
        parameters: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        if id.trim().is_empty() {
            return Err(PyValueError::new_err("id must not be empty"));
        }
        let values_json = serde_json::to_string(&values)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self {
            inner: DerivedIndexRow {
                id,
                text,
                values_json,
                lineage: FeatureLineage {
                    source: SourceIdentity {
                        media_uri: source_uri,
                        source_version,
                    },
                    operator_version,
                    model,
                    model_version,
                    parameters: parameters.unwrap_or_default(),
                },
            },
        })
    }
}

#[cfg(feature = "embedding-subprocess")]
#[pymethods]
impl PythonEmbeddingSubprocessConfig {
    #[new]
    #[pyo3(signature = (executable, arguments, model_artifact, staging_directory, max_batch_items=64, max_input_bytes=8*1024*1024, max_output_bytes=8*1024*1024, timeout_seconds=60.0, operator_version="embedding-subprocess-v1".to_string(), model="local".to_string(), model_version="unknown".to_string(), parameters=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        executable: String,
        arguments: Vec<String>,
        model_artifact: String,
        staging_directory: String,
        max_batch_items: usize,
        max_input_bytes: u64,
        max_output_bytes: u64,
        timeout_seconds: f64,
        operator_version: String,
        model: String,
        model_version: String,
        parameters: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(PyValueError::new_err(
                "timeout_seconds must be finite and greater than zero",
            ));
        }
        let inner = EmbeddingSubprocessConfig {
            executable: executable.into(),
            arguments,
            model_artifact: model_artifact.into(),
            staging_directory: staging_directory.into(),
            max_batch_items,
            max_input_bytes,
            max_output_bytes,
            timeout: Duration::from_secs_f64(timeout_seconds),
            operator_version,
            model,
            model_version,
            parameters: parameters.unwrap_or_default(),
        };
        inner
            .validate()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { inner })
    }
}

#[pymethods]
impl PythonLocalCatalog {
    #[new]
    #[pyo3(signature = (path = None))]
    fn new(path: Option<String>) -> PyResult<Self> {
        let catalog = match path {
            Some(path) => lakeprism_catalog::LocalCatalog::open(path),
            None => Ok(lakeprism_catalog::LocalCatalog::default()),
        }
        .map_err(catalog_error)?;
        Ok(Self {
            inner: Mutex::new(catalog),
        })
    }

    fn register_media_table(
        &self,
        py: Python<'_>,
        table_name: String,
        media_refs: Vec<Py<PythonMediaRef>>,
    ) -> PyResult<()> {
        let media_refs = media_refs
            .iter()
            .map(|media| media.bind(py).borrow().inner.clone())
            .collect();
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("catalog state is unavailable"))?
            .register_media_table(table_name, media_refs)
            .map_err(catalog_error)
    }

    /// Execute the intentionally small durable DDL surface.
    ///
    /// Returns ``("created"|"dropped", table_name)`` or a table-name list for
    /// ``SHOW TABLES``.
    fn execute_ddl(&self, py: Python<'_>, statement: &str) -> PyResult<Py<PyAny>> {
        let result = self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("catalog state is unavailable"))?
            .execute_ddl(statement)
            .map_err(catalog_error)?;
        match result {
            lakeprism_catalog::CatalogDdlResult::CreatedTable(name) => ("created", name)
                .into_pyobject(py)
                .map(|value| value.into_any().unbind()),
            lakeprism_catalog::CatalogDdlResult::DroppedTable(name) => ("dropped", name)
                .into_pyobject(py)
                .map(|value| value.into_any().unbind()),
            lakeprism_catalog::CatalogDdlResult::Tables(names) => names
                .into_pyobject(py)
                .map(|value| value.into_any().unbind()),
        }
    }

    fn table_names(&self) -> PyResult<Vec<String>> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("catalog state is unavailable"))?
            .table_names()
            .map(str::to_owned)
            .collect())
    }

    fn refresh(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| PyRuntimeError::new_err("catalog state is unavailable"))?
            .refresh()
            .map_err(catalog_error)
    }

    /// Register a snapshot of the durable catalog in a local MediaSession.
    fn register_in_session(&self, py: Python<'_>, session: Py<PythonMediaSession>) -> PyResult<()> {
        let tables = {
            let catalog = self
                .inner
                .lock()
                .map_err(|_| PyRuntimeError::new_err("catalog state is unavailable"))?;
            catalog
                .table_names()
                .map(|name| {
                    catalog
                        .media_table(name)
                        .map(|media| (name.to_owned(), media.to_vec()))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(catalog_error)?
        };
        let binding = session.bind(py);
        let session = Arc::clone(&binding.borrow().inner);
        let runtime = Arc::clone(&binding.borrow().runtime);
        py.detach(|| {
            runtime
                .block_on(async {
                    for (name, media) in tables {
                        session.register_media_refs(&name, &media).await?;
                    }
                    Ok::<_, datafusion::error::DataFusionError>(())
                })
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }
}

#[cfg(feature = "unity")]
#[pymethods]
impl PythonUnityQueryContext {
    #[new]
    #[pyo3(signature = (query_id, principal, catalog_identity = None))]
    fn new(
        query_id: String,
        principal: String,
        catalog_identity: Option<String>,
    ) -> PyResult<Self> {
        let inner = lakeprism_unity::enabled::UnityQueryContext::new(
            query_id,
            lakeprism_core::AccessContext {
                principal,
                catalog_identity,
            },
        )
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { inner })
    }

    #[getter]
    fn query_id(&self) -> &str {
        self.inner.query_id()
    }
}

#[cfg(feature = "unity")]
#[pymethods]
impl PythonUnityCatalog {
    /// Construct a Unity REST client with an application-owned fresh-token
    /// callback. The callback receives `(query_id, principal,
    /// catalog_identity)` and must return a bearer token string. LakePrism
    /// retains only the callback, never its return value.
    #[new]
    fn new(base_url: String, token_supplier: Py<PyAny>) -> PyResult<Self> {
        let base_url =
            url::Url::parse(&base_url).map_err(|error| PyValueError::new_err(error.to_string()))?;
        let client = lakeprism_unity::enabled::UnityCatalogClient::new(
            base_url,
            PythonUnityCredentialProvider {
                supplier: token_supplier,
            },
        )
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let runtime = Runtime::new().map_err(|error| {
            PyRuntimeError::new_err(format!("could not start runtime: {error}"))
        })?;
        Ok(Self {
            client,
            runtime: Arc::new(runtime),
        })
    }

    /// Resolve and register a Unity table. The callback is invoked only for
    /// the REST request; remote formats unsupported by the Rust adapter remain
    /// explicit errors instead of unsafe credential fallback.
    fn resolve_and_register(
        &self,
        py: Python<'_>,
        source_table: &str,
        datafusion_table: &str,
        context: Py<PythonUnityQueryContext>,
        session: Py<PythonMediaSession>,
    ) -> PyResult<String> {
        let context = context.bind(py).borrow().inner.clone();
        let session = Arc::clone(&session.bind(py).borrow().inner);
        let source_table = source_table.to_owned();
        let datafusion_table = datafusion_table.to_owned();
        py.detach(|| {
            self.runtime
                .block_on(self.client.resolve_and_register_table(
                    &source_table,
                    &datafusion_table,
                    &context,
                    &session,
                ))
                .map(|registration| format!("{registration:?}"))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Resolve a governed Unity Delta table and register its lazy provider.
    ///
    /// Unity's temporary S3 envelope is vended and consumed entirely within
    /// this call. Python receives neither the envelope nor object-store
    /// credentials, while the native Delta provider remains lazy.
    #[cfg(feature = "delta-rs")]
    fn resolve_and_register_managed_delta(
        &self,
        py: Python<'_>,
        source_table: &str,
        datafusion_table: &str,
        context: Py<PythonUnityQueryContext>,
        session: Py<PythonMediaSession>,
    ) -> PyResult<String> {
        let context = context.bind(py).borrow().inner.clone();
        let session = Arc::clone(&session.bind(py).borrow().inner);
        let source_table = source_table.to_owned();
        let datafusion_table = datafusion_table.to_owned();
        py.detach(|| {
            self.runtime
                .block_on(self.client.resolve_and_register_managed_delta_table(
                    &source_table,
                    &datafusion_table,
                    &context,
                    &session,
                ))
                .map(|registration| format!("{registration:?}"))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }
}

#[cfg(feature = "flight")]
#[pymethods]
impl PythonFlightServer {
    #[new]
    fn new(session: Py<PythonMediaSession>, py: Python<'_>) -> Self {
        let binding = session.bind(py);
        Self {
            session: Arc::clone(&binding.borrow().inner),
            runtime: Arc::clone(&binding.borrow().runtime),
            endpoint: Mutex::new(None),
            task: Mutex::new(None),
        }
    }

    /// Start a loopback-only Flight SQL server. ``port=0`` chooses an
    /// available local port. Authentication configuration remains a Rust
    /// application-boundary concern and is intentionally not accepted here.
    #[pyo3(signature = (port = 0))]
    fn start(&self, port: u16) -> PyResult<String> {
        let mut task = self
            .task
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Flight server state is unavailable"))?;
        if task.is_some() {
            return Err(PyRuntimeError::new_err("Flight server is already running"));
        }
        let listener = std::net::TcpListener::bind(("127.0.0.1", port))
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let listener = tokio::net::TcpListener::from_std(listener)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let service = lakeprism_flight::FlightSqlServer::new(Arc::clone(&self.session));
        let endpoint = format!("http://{address}");
        *task = Some(self.runtime.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        }));
        *self
            .endpoint
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Flight server state is unavailable"))? =
            Some(endpoint.clone());
        Ok(endpoint)
    }

    #[getter]
    fn endpoint(&self) -> PyResult<Option<String>> {
        self.endpoint
            .lock()
            .map(|endpoint| endpoint.clone())
            .map_err(|_| PyRuntimeError::new_err("Flight server state is unavailable"))
    }

    fn stop(&self) -> PyResult<()> {
        if let Some(task) = self
            .task
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Flight server state is unavailable"))?
            .take()
        {
            task.abort();
        }
        *self
            .endpoint
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Flight server state is unavailable"))? = None;
        Ok(())
    }
}

#[cfg(feature = "flight")]
#[pymethods]
impl PythonFlightClient {
    #[new]
    #[pyo3(signature = (endpoint, auth_supplier = None))]
    fn new(endpoint: String, auth_supplier: Option<Py<PyAny>>) -> PyResult<Self> {
        validate_loopback_flight_endpoint(&endpoint)?;
        let runtime = Runtime::new().map_err(|error| {
            PyRuntimeError::new_err(format!("could not start runtime: {error}"))
        })?;
        Ok(Self {
            endpoint,
            runtime: Arc::new(runtime),
            auth_supplier,
        })
    }

    /// Execute a statement through Flight SQL and return its decoded local rows.
    ///
    /// This local-only convenience client intentionally has no bearer-token
    /// argument, so Python never stores or forwards cloud credentials.
    fn execute(&self, py: Python<'_>, query: String) -> PyResult<Vec<Py<PyDict>>> {
        let authorization = self
            .auth_supplier
            .as_ref()
            .map(|supplier| flight_authorization(py, supplier))
            .transpose()?;
        let batches = py.detach(|| {
            self.runtime.block_on(async {
                use futures::TryStreamExt;
                let channel = tonic::transport::Channel::from_shared(self.endpoint.clone())
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                    .connect()
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                let mut client = arrow_flight::sql::client::FlightSqlServiceClient::new(channel);
                if let Some(authorization) = authorization {
                    // `set_header` copies this transient value only into the
                    // short-lived client. Do not use `set_token`: Arrow
                    // Flight 59 redacts that API's wire value.
                    client.set_header("authorization", authorization);
                }
                let info = client
                    .execute(query, None)
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
                let ticket = info
                    .endpoint
                    .first()
                    .and_then(|endpoint| endpoint.ticket.clone())
                    .ok_or_else(|| {
                        PyRuntimeError::new_err("Flight response has no endpoint ticket")
                    })?;
                client
                    .do_get(ticket)
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                    .try_collect()
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))
            })
        })?;
        batches_to_python_rows(py, batches)
    }
}

#[pymethods]
impl PythonEmbeddingRecord {
    #[new]
    #[pyo3(signature = (id, text, values, source_uri, source_version, operator_version, model = None, model_version = None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: String,
        text: String,
        values: Vec<f32>,
        source_uri: String,
        source_version: String,
        operator_version: String,
        model: Option<String>,
        model_version: Option<String>,
    ) -> Self {
        Self {
            inner: EmbeddingRecord {
                id,
                text,
                values,
                lineage: FeatureLineage {
                    source: SourceIdentity {
                        media_uri: source_uri,
                        source_version,
                    },
                    operator_version,
                    model,
                    model_version,
                    parameters: Default::default(),
                },
            },
        }
    }
}

#[pymethods]
impl PythonTranscriptSegment {
    #[new]
    #[pyo3(signature = (media_id, start_millis, end_millis, text, source_uri, source_version, operator_version, confidence_millis = None, model = None, model_version = None, created_at_unix_millis = 0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        media_id: String,
        start_millis: u64,
        end_millis: u64,
        text: String,
        source_uri: String,
        source_version: String,
        operator_version: String,
        confidence_millis: Option<u16>,
        model: Option<String>,
        model_version: Option<String>,
        created_at_unix_millis: i64,
    ) -> PyResult<Self> {
        if end_millis < start_millis {
            return Err(PyValueError::new_err(
                "end_millis must be greater than or equal to start_millis",
            ));
        }
        Ok(Self {
            inner: TranscriptSegment {
                media_id,
                start_millis,
                end_millis,
                text,
                confidence_millis,
                lineage: FeatureLineage {
                    source: SourceIdentity {
                        media_uri: source_uri,
                        source_version,
                    },
                    operator_version,
                    model,
                    model_version,
                    parameters: Default::default(),
                },
                created_at_unix_millis,
            },
        })
    }
}

#[pymethods]
impl PythonMediaRef {
    #[new]
    #[pyo3(signature = (uri, media_type, storage_mode = "external"))]
    fn new(uri: String, media_type: String, storage_mode: &str) -> PyResult<Self> {
        let storage_mode = match storage_mode {
            "external" => StorageMode::External,
            "managed" => StorageMode::Managed,
            "inline" => StorageMode::Inline,
            _ => {
                return Err(PyValueError::new_err(
                    "storage_mode must be external, managed, or inline",
                ));
            }
        };
        let inner = MediaRef::new(uri, media_type, storage_mode)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { inner })
    }

    #[getter]
    fn uri(&self) -> &str {
        &self.inner.uri
    }

    #[getter]
    fn media_type(&self) -> &str {
        &self.inner.media_type
    }

    #[getter]
    fn storage_mode(&self) -> &str {
        match self.inner.storage_mode {
            StorageMode::External => "external",
            StorageMode::Managed => "managed",
            StorageMode::Inline => "inline",
        }
    }
}

#[pyclass(name = "MediaSession")]
struct PythonMediaSession {
    inner: Arc<MediaSession>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl PythonMediaSession {
    #[new]
    #[pyo3(signature = (embedding_mock_dimensions = None))]
    fn new(embedding_mock_dimensions: Option<usize>) -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|error| {
            PyRuntimeError::new_err(format!("could not start runtime: {error}"))
        })?;
        let inner = match embedding_mock_dimensions {
            Some(dimensions) => Arc::new(MediaSession::with_embedding_provider(Arc::new(
                DeterministicMockEmbeddingProvider::new(dimensions)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            ))),
            None => Arc::new(MediaSession::new()),
        };
        Ok(Self {
            inner,
            runtime: Arc::new(runtime),
        })
    }

    /// Construct a session with an explicit, locally installed batch embedding
    /// adapter. The adapter executes direct argv only; neither a model nor
    /// executable is bundled by LakePrism.
    #[cfg(feature = "embedding-subprocess")]
    #[staticmethod]
    fn with_embedding_subprocess(
        config: PyRef<'_, PythonEmbeddingSubprocessConfig>,
    ) -> PyResult<Self> {
        let runtime =
            Runtime::new().map_err(|_| PyRuntimeError::new_err("could not start runtime"))?;
        let governor = Arc::new(
            lakeprism_storage::ExecutionGovernor::new(
                lakeprism_storage::ExecutionGovernorConfig::default(),
            )
            .map_err(|_| PyRuntimeError::new_err("could not configure execution governor"))?,
        );
        let provider =
            EmbeddingSubprocessProvider::new(config.inner.clone(), Arc::clone(&governor))
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(MediaSession::with_embedding_provider_and_governor(
                governor,
                Arc::new(provider),
            )),
            runtime: Arc::new(runtime),
        })
    }

    fn register_media_refs(
        &self,
        py: Python<'_>,
        table_name: &str,
        media_refs: Vec<Py<PythonMediaRef>>,
    ) -> PyResult<()> {
        let media_refs = media_refs
            .iter()
            .map(|media| media.bind(py).borrow().inner.clone())
            .collect::<Vec<_>>();
        py.detach(|| {
            self.runtime
                .block_on(self.inner.register_media_refs(table_name, &media_refs))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Register a local Parquet path through DataFusion's native lazy reader.
    fn register_parquet(&self, py: Python<'_>, table_name: &str, path: &str) -> PyResult<()> {
        validate_local_path_or_file_uri(path)?;
        py.detach(|| {
            self.runtime
                .block_on(self.inner.register_parquet(table_name, path))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Register an immutable derived-feature or derived-index Parquet snapshot.
    ///
    /// This only exposes the stored table. It never promotes persisted vectors
    /// into a semantic index without explicit application lineage validation.
    fn register_derived_snapshot(
        &self,
        py: Python<'_>,
        table_name: &str,
        snapshot_path: &str,
    ) -> PyResult<()> {
        validate_local_path_or_file_uri(snapshot_path)?;
        py.detach(|| {
            self.runtime
                .block_on(lakeprism_derived::register_parquet_snapshot(
                    &self.inner,
                    table_name,
                    snapshot_path,
                ))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Register a local Delta table with delta-rs' native lazy provider.
    ///
    /// Python deliberately accepts only local file URLs/paths: remote object
    /// store credentials belong to an application-owned Rust integration.
    #[cfg(feature = "delta-rs")]
    fn register_delta(
        &self,
        py: Python<'_>,
        table_name: &str,
        table_uri: &str,
        version: Option<u64>,
    ) -> PyResult<()> {
        validate_local_file_uri(table_uri)?;
        py.detach(|| {
            let result = match version {
                Some(version) => {
                    self.runtime
                        .block_on(lakeprism_delta::register_delta_table_version(
                            &self.inner,
                            table_name,
                            table_uri,
                            version,
                        ))
                }
                None => self.runtime.block_on(lakeprism_delta::register_delta_table(
                    &self.inner,
                    table_name,
                    table_uri,
                )),
            };
            result.map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Register a resolved physical Unity table only when its location is a
    /// credential-free local Parquet path. No Unity credential is accepted or
    /// retained by this Python API.
    #[cfg(feature = "unity")]
    #[pyo3(signature = (table_name, source_table, table_type, storage_location, context))]
    fn register_unity_local_parquet(
        &self,
        py: Python<'_>,
        table_name: &str,
        source_table: &str,
        table_type: &str,
        storage_location: &str,
        context: Py<PythonUnityQueryContext>,
    ) -> PyResult<()> {
        validate_local_path_or_file_uri(storage_location)?;
        let context = context.bind(py).borrow().inner.clone();
        let resolved = lakeprism_unity::enabled::ResolvedUnityTable {
            table_id: source_table.to_owned(),
            full_name: source_table.to_owned(),
            table_type: table_type.to_owned(),
            storage_location: Some(storage_location.to_owned()),
            data_source_format: Some("PARQUET".to_owned()),
            properties: BTreeMap::new(),
        };
        py.detach(|| {
            let _query_context = context;
            self.runtime
                .block_on(lakeprism_unity::enabled::register_resolved_table(
                    &self.inner,
                    table_name,
                    &resolved,
                ))
                .map(|_| ())
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Attach resolved Unity schema metadata in one notebook call.
    ///
    /// `tables` is a list of `(source_table, table_type, storage_location,
    /// data_source_format)`. The Python boundary accepts no credentials and
    /// therefore only attaches locations supported by the Rust Unity adapter;
    /// callers embedding a `CredentialProvider` use Rust
    /// `UnityCatalogClient::attach_schema` for REST resolution.
    #[cfg(feature = "unity")]
    #[pyo3(signature = (catalog_name, schema_name, tables, context))]
    #[allow(clippy::type_complexity)]
    fn attach_unity_schema(
        &self,
        py: Python<'_>,
        catalog_name: &str,
        schema_name: &str,
        tables: Vec<(String, String, String, String)>,
        context: Py<PythonUnityQueryContext>,
    ) -> PyResult<(Vec<String>, Vec<(String, String)>)> {
        let _context = context.bind(py).borrow().inner.clone();
        let catalog_name = catalog_name.to_owned();
        let schema_name = schema_name.to_owned();
        let session = Arc::clone(&self.inner);
        let runtime = Arc::clone(&self.runtime);
        py.detach(|| {
            runtime.block_on(async {
                let mut attached = Vec::new();
                let mut unsupported = Vec::new();
                for (source_table, table_type, storage_location, data_source_format) in tables {
                    let table_name = source_table
                        .rsplit('.')
                        .next()
                        .ok_or_else(|| PyValueError::new_err("source_table must not be empty"))?;
                    let table = lakeprism_unity::enabled::ResolvedUnityTable {
                        table_id: source_table.clone(),
                        full_name: source_table.clone(),
                        table_type,
                        storage_location: Some(storage_location),
                        data_source_format: Some(data_source_format),
                        properties: BTreeMap::new(),
                    };
                    match lakeprism_unity::enabled::register_resolved_table_in_schema(
                        &session,
                        &catalog_name,
                        &schema_name,
                        table_name,
                        &table,
                    )
                    .await
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                    {
                        lakeprism_unity::enabled::UnityTableRegistration::Unsupported {
                            reason,
                        } => unsupported.push((source_table, reason)),
                        _ => attached.push(table_name.to_owned()),
                    }
                }
                Ok((attached, unsupported))
            })
        })
    }

    fn register_transcript_segments(
        &self,
        py: Python<'_>,
        segments: Vec<Py<PythonTranscriptSegment>>,
    ) -> PyResult<()> {
        let segments = segments
            .iter()
            .map(|segment| segment.bind(py).borrow().inner.clone())
            .collect::<Vec<_>>();
        self.inner
            .register_transcript_segments(segments)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn register_embedding_records(
        &self,
        py: Python<'_>,
        records: Vec<Py<PythonEmbeddingRecord>>,
    ) -> PyResult<()> {
        let records = records
            .iter()
            .map(|record| record.bind(py).borrow().inner.clone())
            .collect::<Vec<_>>();
        self.inner
            .register_embedding_records(records)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    /// Execute SQL and return rows as dictionaries of string or ``None`` values.
    ///
    /// Arrow's display representation makes the result independent of a Python
    /// Arrow installation while retaining column order in the returned dict.
    fn sql(&self, py: Python<'_>, query: &str) -> PyResult<Vec<Py<PyDict>>> {
        let batches = py.detach(|| {
            self.runtime
                .block_on(self.inner.collect(query))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })?;
        let mut rows = Vec::new();

        for batch in batches {
            let schema = batch.schema();
            let mut names = HashSet::new();
            for field in schema.fields() {
                if !names.insert(field.name()) {
                    return Err(PyValueError::new_err(
                        "SQL result column names must be unique; use AS aliases",
                    ));
                }
            }
            for row_index in 0..batch.num_rows() {
                let row = PyDict::new(py);
                for (column_index, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(column_index);
                    let value = if column.is_null(row_index) {
                        py.None()
                    } else {
                        arrow::util::display::array_value_to_string(column.as_ref(), row_index)
                            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                            .into_pyobject(py)?
                            .into_any()
                            .unbind()
                    };
                    row.set_item(field.name(), value)?;
                }
                rows.push(row.unbind());
            }
        }

        Ok(rows)
    }

    fn execute(&self, py: Python<'_>, query: &str) -> PyResult<Vec<Py<PyDict>>> {
        self.sql(py, query)
    }

    /// Return a lazy DataFusion `EXPLAIN` plan for a SQL statement.
    ///
    /// Like every other `LazyPlan`, this is not evaluated until it is
    /// collected or exported to Arrow.
    fn explain(&self, query: &str) -> PythonLazyPlan {
        self.plan(format!("EXPLAIN {query}"))
    }

    /// List catalog, schema, table, and provider type metadata without
    /// exposing provider credentials.
    fn catalog_tables(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let tables = py.detach(|| {
            self.runtime
                .block_on(self.inner.catalog_tables())
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })?;
        tables
            .into_iter()
            .map(|table| {
                let result = PyDict::new(py);
                result.set_item("catalog", table.catalog_name)?;
                result.set_item("schema", table.schema_name)?;
                result.set_item("name", table.table_name)?;
                result.set_item("type", table.table_type)?;
                result.set_item(
                    "columns",
                    table
                        .schema
                        .fields()
                        .iter()
                        .map(|field| field.name().to_owned())
                        .collect::<Vec<_>>(),
                )?;
                Ok(result.unbind())
            })
            .collect()
    }

    /// Create a lazy local SQL plan. Planning and execution start only when a
    /// terminal method such as ``collect`` or ``to_pyarrow`` is called.
    fn plan(&self, query: String) -> PythonLazyPlan {
        PythonLazyPlan {
            session: Arc::clone(&self.inner),
            runtime: Arc::clone(&self.runtime),
            query,
        }
    }

    /// Return a lazy plan over bounded local PDF pages or DOCX paragraphs.
    fn document_sections(&self, uri: &str) -> PythonLazyPlan {
        self.plan(document_sections_sql(uri))
    }

    /// Return a lazy plan over bounded local DOCX table cells.
    fn document_tables(&self, uri: &str) -> PythonLazyPlan {
        self.plan(document_tables_sql(uri))
    }

    /// Return a lazy plan over local DOCX image metadata and optional bytes.
    #[pyo3(signature = (uri, include_bytes = false))]
    fn document_images(&self, uri: &str, include_bytes: bool) -> PythonLazyPlan {
        self.plan(document_images_sql(uri, include_bytes))
    }

    /// Return a lazy plan over bounded local document search results.
    fn document_search(&self, uri: &str, query: &str, limit: usize) -> PyResult<PythonLazyPlan> {
        if query.is_empty() || limit > 1_024 {
            return Err(PyValueError::new_err(
                "query must be non-empty and limit must not exceed 1024",
            ));
        }
        Ok(self.plan(document_search_sql(uri, query, limit)))
    }

    /// Return a lazy plan over locally decoded video frames. Native decoding
    /// remains feature-gated and does not begin until the plan is consumed.
    #[pyo3(signature = (uri, start_millis, end_millis, every_millis, limit, include_rgb24 = false))]
    #[allow(clippy::too_many_arguments)]
    fn video_frames(
        &self,
        uri: &str,
        start_millis: u64,
        end_millis: u64,
        every_millis: u64,
        limit: usize,
        include_rgb24: bool,
    ) -> PyResult<PythonLazyPlan> {
        validate_decode_request(start_millis, end_millis, every_millis, limit, 32)?;
        Ok(self.plan(video_frames_sql(
            uri,
            start_millis,
            end_millis,
            every_millis,
            limit,
            include_rgb24,
        )))
    }

    /// Return a lazy plan over locally decoded, normalized audio segments.
    #[pyo3(signature = (uri, start_millis, end_millis, segment_millis, limit, include_payload = false))]
    #[allow(clippy::too_many_arguments)]
    fn audio_segments(
        &self,
        uri: &str,
        start_millis: u64,
        end_millis: u64,
        segment_millis: u64,
        limit: usize,
        include_payload: bool,
    ) -> PyResult<PythonLazyPlan> {
        validate_decode_request(start_millis, end_millis, segment_millis, limit, 1_024)?;
        Ok(self.plan(audio_segments_sql(
            uri,
            start_millis,
            end_millis,
            segment_millis,
            limit,
            include_payload,
        )))
    }

    /// Return a lazy semantic ranking plan. `candidate_limit=0` means exact
    /// ranking over every compatible registered embedding; a positive value is
    /// explicitly approximate and appears in `ranking_semantics`.
    #[pyo3(signature = (query, limit, candidate_limit = 0))]
    fn semantic_search(
        &self,
        query: &str,
        limit: usize,
        candidate_limit: usize,
    ) -> PyResult<PythonLazyPlan> {
        if query.is_empty() || limit > 10_000 || candidate_limit > 10_000 {
            return Err(PyValueError::new_err(
                "query must be non-empty and limits must not exceed 10000",
            ));
        }
        Ok(self.plan(format!(
            "SELECT * FROM lakeprism_semantic_search({}, {}, {})",
            sql_literal(query),
            limit,
            candidate_limit
        )))
    }

    #[pyo3(signature = (query, limit, candidate_limit = 0, semantic_weight = 0.5))]
    fn hybrid_search(
        &self,
        query: &str,
        limit: usize,
        candidate_limit: usize,
        semantic_weight: f64,
    ) -> PyResult<PythonLazyPlan> {
        if query.is_empty()
            || limit > 10_000
            || candidate_limit > 10_000
            || !(0.0..=1.0).contains(&semantic_weight)
        {
            return Err(PyValueError::new_err(
                "invalid query, limits, or semantic_weight",
            ));
        }
        Ok(self.plan(format!(
            "SELECT * FROM lakeprism_hybrid_search({}, {}, {}, {})",
            sql_literal(query),
            limit,
            candidate_limit,
            semantic_weight
        )))
    }

    /// Execute SQL and return a PyArrow ``RecordBatchReader``.
    ///
    /// PyArrow is imported only when this method is called. It consumes a
    /// native Arrow C stream, so result batches are not serialized through
    /// IPC or converted to Python rows.
    fn sql_arrow(&self, py: Python<'_>, query: &str) -> PyResult<Py<PyAny>> {
        self.plan(query.to_string()).to_pyarrow(py)
    }

    /// Execute SQL and return an ``arrow_array_stream`` C-data-interface
    /// capsule. Consumers owning Arrow-compatible bindings can import it
    /// without IPC serialization.
    fn sql_arrow_c_stream(&self, py: Python<'_>, query: &str) -> PyResult<Py<PyAny>> {
        self.plan(query.to_string()).to_arrow_c_stream(py)
    }

    /// Execute SQL and return an Arrow IPC stream as ``bytes``.
    ///
    /// This compatibility export necessarily materializes the query. Prefer
    /// ``plan(sql).to_pyarrow()`` or ``sql_arrow_c_stream`` for streaming.
    fn sql_arrow_ipc(&self, py: Python<'_>, query: &str) -> PyResult<Py<PyAny>> {
        let batches = py.detach(|| {
            self.runtime
                .block_on(self.inner.collect(query))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })?;
        let bytes = record_batches_to_arrow_ipc(&batches)?;
        Ok(PyBytes::new(py, &bytes).into_any().unbind())
    }

    fn create_query(&self, deadline_millis: Option<u64>) -> String {
        let deadline = deadline_millis
            .map(|millis| tokio::time::Instant::now() + Duration::from_millis(millis));
        self.inner.create_query(deadline)
    }

    fn cancel_query(&self, query_id: &str) -> bool {
        self.inner.cancel_query(query_id)
    }

    fn query_status(&self, py: Python<'_>, query_id: &str) -> PyResult<Py<PyDict>> {
        query_status_to_python(py, &self.inner, query_id)
    }

    fn execute_query(
        &self,
        py: Python<'_>,
        query_id: &str,
        query: &str,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let batches = py.detach(|| {
            self.runtime
                .block_on(async {
                    let execution = self
                        .inner
                        .execute_registered_query(query, query_id.to_string())
                        .await?;
                    futures::TryStreamExt::try_collect(execution.stream).await
                })
                .map_err(|error: datafusion::error::DataFusionError| {
                    PyRuntimeError::new_err(error.to_string())
                })
        })?;
        batches_to_python_rows(py, batches)
    }
}

/// An immutable lazy SQL plan over the owning local ``MediaSession``.
#[pyclass(name = "LazyPlan")]
struct PythonLazyPlan {
    session: Arc<MediaSession>,
    runtime: Arc<Runtime>,
    query: String,
}

#[pymethods]
impl PythonLazyPlan {
    #[getter]
    fn sql(&self) -> &str {
        &self.query
    }

    fn collect(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let batches = py.detach(|| {
            self.runtime
                .block_on(self.session.collect(&self.query))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })?;
        batches_to_python_rows(py, batches)
    }

    /// Export the plan as a native Arrow C Stream Interface capsule.
    ///
    /// The capsule has Arrow's standard ``arrow_array_stream`` name and
    /// transfers ownership to the first compliant consumer, such as PyArrow.
    fn to_arrow_c_stream(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let stream = py.detach(|| {
            self.runtime
                .block_on(self.session.execute_stream(&self.query))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })?;
        let reader = QueryStreamReader {
            schema: stream.schema(),
            stream,
            runtime: Arc::clone(&self.runtime),
        };
        let ffi_stream = arrow::ffi_stream::FFI_ArrowArrayStream::new(Box::new(reader));
        arrow_stream_capsule(py, ffi_stream)
    }

    /// Import the native C stream into a PyArrow ``RecordBatchReader``.
    fn to_pyarrow(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let pyarrow = PyModule::import(py, "pyarrow")?;
        let capsule = self.to_arrow_c_stream(py)?;
        Ok(pyarrow
            .getattr("RecordBatchReader")?
            .call_method1("_import_from_c_capsule", (capsule,))?
            .unbind())
    }

    fn to_arrow_ipc(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let batches = py.detach(|| {
            self.runtime
                .block_on(self.session.collect(&self.query))
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })?;
        let bytes = record_batches_to_arrow_ipc(&batches)?;
        Ok(PyBytes::new(py, &bytes).into_any().unbind())
    }
}

fn record_batches_to_arrow_ipc(batches: &[arrow::record_batch::RecordBatch]) -> PyResult<Vec<u8>> {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
    let mut output = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut output, &schema)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        }
        writer
            .finish()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    }
    Ok(output)
}

fn batches_to_python_rows(
    py: Python<'_>,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> PyResult<Vec<Py<PyDict>>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let mut names = HashSet::new();
        for field in schema.fields() {
            if !names.insert(field.name()) {
                return Err(PyValueError::new_err(
                    "SQL result column names must be unique; use AS aliases",
                ));
            }
        }
        for row_index in 0..batch.num_rows() {
            let row = PyDict::new(py);
            for (column_index, field) in schema.fields().iter().enumerate() {
                let column = batch.column(column_index);
                let value = if column.is_null(row_index) {
                    py.None()
                } else {
                    arrow::util::display::array_value_to_string(column.as_ref(), row_index)
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                        .into_pyobject(py)?
                        .into_any()
                        .unbind()
                };
                row.set_item(field.name(), value)?;
            }
            rows.push(row.unbind());
        }
    }
    Ok(rows)
}

struct QueryStreamReader {
    schema: arrow::datatypes::SchemaRef,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    runtime: Arc<Runtime>,
}

impl Iterator for QueryStreamReader {
    type Item = arrow::error::Result<arrow::record_batch::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        use futures::TryStreamExt;

        self.runtime
            .block_on(self.stream.try_next())
            .transpose()
            .map(|result| {
                result.map_err(|error| arrow::error::ArrowError::ExternalError(Box::new(error)))
            })
    }
}

impl arrow::record_batch::RecordBatchReader for QueryStreamReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }
}

unsafe extern "C" fn destroy_arrow_stream_capsule(capsule: *mut pyo3::ffi::PyObject) {
    // A compliant Arrow consumer renames a consumed capsule, transferring the
    // stream's release obligation. Unconsumed capsules remain ours to release.
    if unsafe { pyo3::ffi::PyCapsule_IsValid(capsule, c"arrow_array_stream".as_ptr()) } == 0 {
        return;
    }
    let pointer =
        unsafe { pyo3::ffi::PyCapsule_GetPointer(capsule, c"arrow_array_stream".as_ptr()) };
    if pointer.is_null() {
        return;
    }
    let stream =
        unsafe { Box::from_raw(pointer.cast::<arrow::ffi_stream::FFI_ArrowArrayStream>()) };
    drop(stream);
}

fn arrow_stream_capsule(
    py: Python<'_>,
    stream: arrow::ffi_stream::FFI_ArrowArrayStream,
) -> PyResult<Py<PyAny>> {
    let pointer = NonNull::new(Box::into_raw(Box::new(stream)).cast::<c_void>())
        .expect("boxed Arrow stream pointer is non-null");
    let capsule = unsafe {
        PyCapsule::new_with_pointer_and_destructor(
            py,
            pointer,
            c"arrow_array_stream",
            Some(destroy_arrow_stream_capsule),
        )?
    };
    Ok(capsule.into_any().unbind())
}

fn query_status_to_python(
    py: Python<'_>,
    session: &MediaSession,
    query_id: &str,
) -> PyResult<Py<PyDict>> {
    let info = session
        .query(query_id)
        .ok_or_else(|| PyValueError::new_err("query does not exist"))?;
    let result = PyDict::new(py);
    result.set_item("id", info.id)?;
    result.set_item("status", format!("{:?}", info.status).to_ascii_lowercase())?;
    result.set_item("batches", info.metrics.batches)?;
    result.set_item("rows", info.metrics.rows)?;
    result.set_item(
        "elapsed_millis",
        u64::try_from(info.metrics.elapsed.as_millis()).unwrap_or(u64::MAX),
    )?;
    Ok(result.unbind())
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn catalog_error(error: lakeprism_catalog::CatalogError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn validate_local_path_or_file_uri(value: &str) -> PyResult<()> {
    if let Ok(uri) = url::Url::parse(value)
        && uri.scheme() != "file"
    {
        return Err(PyValueError::new_err(
            "only a local path or credential-free file:// URI is supported",
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-rs")]
fn validate_local_file_uri(value: &str) -> PyResult<()> {
    let uri = url::Url::parse(value)
        .map_err(|_| PyValueError::new_err("a credential-free file:// URI is required"))?;
    if uri.scheme() != "file" {
        return Err(PyValueError::new_err(
            "only a credential-free file:// URI is supported",
        ));
    }
    Ok(())
}

fn local_filesystem_path(value: &str) -> PyResult<std::path::PathBuf> {
    match url::Url::parse(value) {
        Ok(uri) => {
            if uri.scheme() != "file" {
                return Err(PyValueError::new_err(
                    "only a local path or credential-free file:// URI is supported",
                ));
            }
            uri.to_file_path()
                .map_err(|_| PyValueError::new_err("file:// URI is not a local filesystem path"))
        }
        Err(_) => Ok(value.into()),
    }
}

#[cfg(feature = "flight")]
fn validate_loopback_flight_endpoint(endpoint: &str) -> PyResult<()> {
    let uri = url::Url::parse(endpoint)
        .map_err(|_| PyValueError::new_err("Flight endpoint must be an HTTP URL"))?;
    if !matches!(uri.scheme(), "http" | "https") {
        return Err(PyValueError::new_err("Flight endpoint must use HTTP"));
    }
    let host = uri
        .host_str()
        .ok_or_else(|| PyValueError::new_err("Flight endpoint must include a host"))?;
    if uri.scheme() == "http" && !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Err(PyValueError::new_err(
            "non-loopback Flight endpoints must use HTTPS",
        ));
    }
    if uri.username() != ""
        || uri.password().is_some()
        || uri.query().is_some()
        || uri.fragment().is_some()
    {
        return Err(PyValueError::new_err(
            "Flight endpoint must not contain credentials, query, or fragment",
        ));
    }
    Ok(())
}

#[cfg(feature = "flight")]
fn flight_authorization(py: Python<'_>, supplier: &Py<PyAny>) -> PyResult<String> {
    let token = supplier
        .bind(py)
        .call0()?
        .extract::<String>()
        .map_err(|_| {
            PyValueError::new_err("Flight auth_supplier must return a bearer token string")
        })?;
    if token.trim().is_empty() || token.contains(['\r', '\n']) {
        return Err(PyValueError::new_err(
            "Flight auth_supplier returned an invalid bearer token",
        ));
    }
    Ok(format!("Bearer {token}"))
}

#[pyfunction]
fn refresh_embedding_index(
    py: Python<'_>,
    snapshot_path: &str,
    manifest_path: &str,
    rows: Vec<Py<PythonDerivedIndexRow>>,
) -> PyResult<Py<PyDict>> {
    let snapshot_path = local_filesystem_path(snapshot_path)?;
    let manifest_path = local_filesystem_path(manifest_path)?;
    let rows = rows
        .iter()
        .map(|row| row.bind(py).borrow().inner.clone())
        .collect::<Vec<_>>();
    let result = py.detach(|| {
        refresh_embedding_index_parquet(&snapshot_path, &manifest_path, &rows)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })?;
    let output = PyDict::new(py);
    output.set_item("emitted_rows", result.emitted_rows)?;
    output.set_item("changed_sources", result.changed_sources)?;
    Ok(output.unbind())
}

/// Write a local Delta table from an Arrow IPC stream.
///
/// This is deliberately IPC-only at the native boundary: notebook callers can
/// pass ``pyarrow.ipc.new_stream`` output without row-by-row conversion, while
/// LakePrism retains Delta's Arrow schema exactly. Remote URLs and credential
/// material are rejected; application-owned Rust integrations cover scoped
/// remote object-store writes.
#[cfg(feature = "delta-rs")]
#[pyfunction]
#[pyo3(signature = (table_uri, ipc_stream, mode = "append", schema_mode = "strict", partition_columns = None, audit_metadata = None, max_commit_retries = None))]
#[allow(clippy::too_many_arguments)]
fn write_delta_ipc(
    py: Python<'_>,
    table_uri: &str,
    ipc_stream: &[u8],
    mode: &str,
    schema_mode: &str,
    partition_columns: Option<Vec<String>>,
    audit_metadata: Option<BTreeMap<String, String>>,
    max_commit_retries: Option<usize>,
) -> PyResult<u64> {
    validate_local_file_uri(table_uri)?;
    let mode = match mode {
        "create" => lakeprism_delta::DeltaWriteMode::Create,
        "append" => lakeprism_delta::DeltaWriteMode::Append,
        "overwrite" => lakeprism_delta::DeltaWriteMode::Overwrite,
        _ => {
            return Err(PyValueError::new_err(
                "mode must be create, append, or overwrite",
            ));
        }
    };
    let schema_mode = match schema_mode {
        "strict" => lakeprism_delta::DeltaSchemaMode::Strict,
        "merge" => lakeprism_delta::DeltaSchemaMode::Merge,
        "overwrite" => lakeprism_delta::DeltaSchemaMode::Overwrite,
        _ => {
            return Err(PyValueError::new_err(
                "schema_mode must be strict, merge, or overwrite",
            ));
        }
    };
    let batches = {
        let reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(ipc_stream), None)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| PyValueError::new_err(error.to_string()))?
    };
    let options = lakeprism_delta::DeltaWriteOptions {
        mode,
        schema_mode,
        partition_columns,
        commit: lakeprism_delta::DeltaCommitProperties {
            audit_metadata: audit_metadata
                .unwrap_or_default()
                .into_iter()
                .map(|(key, value)| (key, serde_json::Value::String(value)))
                .collect(),
            max_commit_retries,
        },
    };
    py.detach(|| {
        let runtime = Runtime::new().map_err(|error| {
            PyRuntimeError::new_err(format!("could not start runtime: {error}"))
        })?;
        runtime
            .block_on(lakeprism_delta::write_delta_table(
                table_uri, batches, options,
            ))
            .map(|result| result.version)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    })
}

fn document_sections_sql(uri: &str) -> String {
    format!(
        "SELECT * FROM lakeprism_document_sections({})",
        sql_literal(uri)
    )
}

fn document_tables_sql(uri: &str) -> String {
    format!(
        "SELECT * FROM lakeprism_document_tables({})",
        sql_literal(uri)
    )
}

fn document_images_sql(uri: &str, include_bytes: bool) -> String {
    format!(
        "SELECT * FROM lakeprism_document_images({}, {include_bytes})",
        sql_literal(uri)
    )
}

fn document_search_sql(uri: &str, query: &str, limit: usize) -> String {
    format!(
        "SELECT * FROM lakeprism_document_search({}, {}, {limit})",
        sql_literal(uri),
        sql_literal(query)
    )
}

fn video_frames_sql(
    uri: &str,
    start_millis: u64,
    end_millis: u64,
    every_millis: u64,
    limit: usize,
    include_rgb24: bool,
) -> String {
    format!(
        "SELECT * FROM lakeprism_video_frames({}, {start_millis}, {end_millis}, \
         {every_millis}, {limit}, {include_rgb24})",
        sql_literal(uri)
    )
}

fn audio_segments_sql(
    uri: &str,
    start_millis: u64,
    end_millis: u64,
    segment_millis: u64,
    limit: usize,
    include_payload: bool,
) -> String {
    format!(
        "SELECT * FROM lakeprism_audio_segments({}, {start_millis}, {end_millis}, \
         {segment_millis}, {limit}, {include_payload})",
        sql_literal(uri)
    )
}

fn validate_decode_request(
    start_millis: u64,
    end_millis: u64,
    interval_millis: u64,
    limit: usize,
    maximum_limit: usize,
) -> PyResult<()> {
    if end_millis < start_millis || interval_millis == 0 || limit == 0 || limit > maximum_limit {
        return Err(PyValueError::new_err(format!(
            "end_millis must be >= start_millis, interval must be non-zero, and limit must be in 1..={maximum_limit}"
        )));
    }
    Ok(())
}
#[pyfunction]
#[pyo3(signature = (timestamps_millis, start_millis = None, end_millis = None, limit = None, include_payload_bytes = false))]
fn plan_video_frames(
    timestamps_millis: Vec<u64>,
    start_millis: Option<u64>,
    end_millis: Option<u64>,
    limit: Option<usize>,
    include_payload_bytes: bool,
) -> PyResult<Vec<u64>> {
    let time_range_millis = match (start_millis, end_millis) {
        (Some(start), Some(end)) if start <= end => Some(start..=end),
        (Some(_), Some(_)) => {
            return Err(PyValueError::new_err(
                "start_millis must be less than or equal to end_millis",
            ));
        }
        (None, None) => None,
        _ => {
            return Err(PyValueError::new_err(
                "start_millis and end_millis must be supplied together",
            ));
        }
    };
    let constraints = MediaConstraints {
        time_range_millis,
        limit_hint: limit,
        projection: MediaProjection {
            include_payload_bytes,
            ..Default::default()
        },
        ..Default::default()
    };
    Ok(plan_frames(timestamps_millis, &constraints).timestamps_millis)
}

#[pymodule]
fn _lakeprism(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PythonMediaRef>()?;
    module.add_class::<PythonTranscriptSegment>()?;
    module.add_class::<PythonEmbeddingRecord>()?;
    module.add_class::<PythonDerivedIndexRow>()?;
    #[cfg(feature = "embedding-subprocess")]
    module.add_class::<PythonEmbeddingSubprocessConfig>()?;
    module.add_class::<PythonLocalCatalog>()?;
    #[cfg(feature = "unity")]
    module.add_class::<PythonUnityQueryContext>()?;
    #[cfg(feature = "unity")]
    module.add_class::<PythonUnityCatalog>()?;
    #[cfg(feature = "flight")]
    module.add_class::<PythonFlightServer>()?;
    #[cfg(feature = "flight")]
    module.add_class::<PythonFlightClient>()?;
    module.add_class::<PythonMediaSession>()?;
    module.add_class::<PythonLazyPlan>()?;
    module.add_function(wrap_pyfunction!(plan_video_frames, module)?)?;
    module.add_function(wrap_pyfunction!(refresh_embedding_index, module)?)?;
    #[cfg(feature = "delta-rs")]
    module.add_function(wrap_pyfunction!(write_delta_ipc, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_capability_plans_use_the_shared_sql_surface() {
        assert_eq!(
            document_sections_sql("file:///a'b.docx"),
            "SELECT * FROM lakeprism_document_sections('file:///a''b.docx')"
        );
        assert_eq!(
            video_frames_sql("file:///clip.mp4", 0, 1_000, 250, 4, false),
            "SELECT * FROM lakeprism_video_frames('file:///clip.mp4', 0, 1000, 250, 4, false)"
        );
        assert_eq!(
            audio_segments_sql("file:///clip.mp4", 0, 1_000, 100, 8, true),
            "SELECT * FROM lakeprism_audio_segments('file:///clip.mp4', 0, 1000, 100, 8, true)"
        );
    }
}
