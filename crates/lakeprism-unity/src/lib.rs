use lakeprism_core::{MediaRef, StorageMode};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileAdapterError {
    #[error(transparent)]
    Core(#[from] lakeprism_core::LakePrismError),
}

/// Credential-free FILE metadata returned by a Databricks-aware provider.
///
/// This is deliberately a descriptor, not a remote reader. Callers must
/// provide a query-scoped file provider to access remote managed or external
/// objects; LakePrism never guesses object-store credentials from this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabricksFile {
    pub uri: String,
    pub media_type: String,
    pub mime_type: Option<String>,
    pub size_bytes: Option<u64>,
    pub etag: Option<String>,
    pub catalog_ref: Option<String>,
    pub storage_mode: StorageMode,
}

impl TryFrom<DatabricksFile> for MediaRef {
    type Error = FileAdapterError;

    fn try_from(file: DatabricksFile) -> Result<Self, Self::Error> {
        let mut media = MediaRef::new(file.uri, file.media_type, file.storage_mode)?;
        media.mime_type = file.mime_type;
        media.size_bytes = file.size_bytes;
        media.etag = file.etag;
        media.catalog_ref = file.catalog_ref;
        Ok(media)
    }
}

/// A governed Unity Catalog Volume path. It is converted into a portable,
/// credential-free `unity-volume://` MediaRef; it is not an object-store URL
/// and cannot be used to infer cloud credentials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnityVolumePath {
    catalog: String,
    schema: String,
    volume: String,
    relative_path: Vec<String>,
}

impl UnityVolumePath {
    pub fn parse(path: &str) -> Result<Self, FileAdapterError> {
        let components = path.strip_prefix("/Volumes/").ok_or_else(|| {
            FileAdapterError::Core(lakeprism_core::LakePrismError::RelativeMediaUri(
                path.to_owned(),
            ))
        })?;
        let parts = components.split('/').collect::<Vec<_>>();
        if parts.len() < 4
            || parts.iter().any(|part| {
                part.is_empty()
                    || *part == "."
                    || *part == ".."
                    || part.contains('?')
                    || part.contains('#')
                    || part.contains('\\')
            })
            || parts[..3].iter().any(|part| {
                !part
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
            })
        {
            return Err(FileAdapterError::Core(
                lakeprism_core::LakePrismError::RelativeMediaUri(path.to_owned()),
            ));
        }
        Ok(Self {
            catalog: parts[0].to_owned(),
            schema: parts[1].to_owned(),
            volume: parts[2].to_owned(),
            relative_path: parts[3..].iter().map(|part| (*part).to_owned()).collect(),
        })
    }

    pub fn governed_path(&self) -> String {
        format!(
            "/Volumes/{}/{}/{}/{}",
            self.catalog,
            self.schema,
            self.volume,
            self.relative_path.join("/")
        )
    }

    pub fn catalog_ref(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.volume)
    }

    pub fn to_media_ref(
        &self,
        media_type: impl Into<String>,
    ) -> Result<MediaRef, FileAdapterError> {
        let mut media = MediaRef::new(
            format!(
                "unity-volume://{}/{}/{}/{}",
                self.catalog,
                self.schema,
                self.volume,
                self.relative_path.join("/")
            ),
            media_type,
            StorageMode::Managed,
        )?;
        media.catalog_ref = Some(self.catalog_ref());
        media
            .metadata
            .insert("unity.volume_path".to_owned(), self.governed_path());
        Ok(media)
    }
}

#[cfg(feature = "unity")]
pub mod enabled {
    use std::collections::BTreeMap;
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use async_trait::async_trait;
    use lakeprism_core::{AccessContext, MediaRef};
    use lakeprism_datafusion::MediaSession;
    use serde::{Deserialize, Serialize};
    use thiserror::Error;
    use url::Url;
    use uuid::Uuid;

    use super::{DatabricksFile, UnityVolumePath};

    const REGISTRY_FILE: &str = "unity-registrations.json";
    const REGISTRY_VERSION: u32 = 1;

    /// A single query's immutable authorization identity. It contains no
    /// bearer material and is the only context accepted by Unity operations.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct UnityQueryContext {
        query_id: String,
        access: AccessContext,
    }

    impl UnityQueryContext {
        pub fn new(query_id: impl Into<String>, access: AccessContext) -> Result<Self, UnityError> {
            let query_id = query_id.into();
            if query_id.trim().is_empty() {
                return Err(UnityError::InvalidQueryId);
            }
            Ok(Self { query_id, access })
        }

        pub fn query_id(&self) -> &str {
            &self.query_id
        }

        pub fn access_context(&self) -> &AccessContext {
            &self.access
        }
    }

    #[derive(Clone, Eq, PartialEq)]
    pub struct VendedCredential {
        bearer_token: String,
    }

    impl VendedCredential {
        pub fn new(bearer_token: impl Into<String>) -> Self {
            Self {
                bearer_token: bearer_token.into(),
            }
        }
    }

    impl std::fmt::Debug for VendedCredential {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("VendedCredential")
                .field("bearer_token", &"[redacted]")
                .finish()
        }
    }

    /// Supplies a fresh, request-local credential. Implementations may refresh
    /// on every call; credentials must not be retained by a catalog or session.
    #[async_trait]
    pub trait CredentialProvider: Send + Sync {
        async fn credential_for(
            &self,
            context: &UnityQueryContext,
        ) -> Result<VendedCredential, UnityError>;
    }

    /// OAuth M2M client configuration for a Databricks service principal.
    ///
    /// `client_secret` is deliberately private and has no serialization or
    /// revealing `Debug` implementation. The scope is constrained to the
    /// Databricks Unity REST audience (`all-apis`).
    #[derive(Clone)]
    pub struct DatabricksM2mConfig {
        workspace_url: Url,
        client_id: String,
        client_secret: String,
    }

    impl DatabricksM2mConfig {
        pub fn new(
            workspace_url: Url,
            client_id: impl Into<String>,
            client_secret: impl Into<String>,
        ) -> Result<Self, UnityError> {
            validate_unity_endpoint(&workspace_url)?;
            let client_id = client_id.into();
            let client_secret = client_secret.into();
            if client_id.trim().is_empty() || client_secret.is_empty() {
                return Err(UnityError::InvalidM2mConfiguration);
            }
            Ok(Self {
                workspace_url,
                client_id,
                client_secret,
            })
        }
    }

    impl std::fmt::Debug for DatabricksM2mConfig {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("DatabricksM2mConfig")
                .field("workspace_url", &self.workspace_url)
                .field("client_id", &self.client_id)
                .field("client_secret", &"[redacted]")
                .finish()
        }
    }

    /// Fresh-token OAuth M2M provider. It does not cache access tokens, which
    /// keeps every Unity REST operation request-scoped.
    #[derive(Clone, Debug)]
    pub struct DatabricksM2mCredentialProvider {
        config: DatabricksM2mConfig,
        client: reqwest::Client,
    }

    impl DatabricksM2mCredentialProvider {
        pub fn new(config: DatabricksM2mConfig) -> Self {
            Self {
                config,
                client: reqwest::Client::new(),
            }
        }
    }

    #[async_trait]
    impl CredentialProvider for DatabricksM2mCredentialProvider {
        async fn credential_for(
            &self,
            _context: &UnityQueryContext,
        ) -> Result<VendedCredential, UnityError> {
            #[derive(Deserialize)]
            struct TokenResponse {
                access_token: String,
                token_type: String,
            }
            let response = self
                .client
                .post(self.config.workspace_url.join("oidc/v1/token")?)
                .form(&[
                    ("grant_type", "client_credentials"),
                    ("client_id", self.config.client_id.as_str()),
                    ("client_secret", self.config.client_secret.as_str()),
                    ("scope", "all-apis"),
                ])
                .send()
                .await?
                .error_for_status()?
                .json::<TokenResponse>()
                .await?;
            if !response.token_type.eq_ignore_ascii_case("bearer")
                || response.access_token.trim().is_empty()
            {
                return Err(UnityError::InvalidOAuthResponse);
            }
            Ok(VendedCredential::new(response.access_token))
        }
    }

    /// Resolves a managed or external FILE using the caller's query context.
    ///
    /// The returned descriptor is portable and credential-free. This trait is
    /// the Rust/Python/Flight boundary: language and RPC layers can provide a
    /// provider, but neither receives a vended credential from LakePrism.
    #[async_trait]
    pub trait DatabricksFileProvider: Send + Sync {
        async fn resolve_file(
            &self,
            file_reference: &str,
            context: &UnityQueryContext,
        ) -> Result<DatabricksFile, UnityError>;
    }

    #[derive(Debug, Error)]
    pub enum UnityError {
        #[error("Unity Catalog table identifier must be a dotted identifier without a slash: {0}")]
        InvalidTableIdentifier(String),
        #[error("Unity query ID must not be empty")]
        InvalidQueryId,
        #[error("Databricks OAuth M2M client ID and secret must both be non-empty")]
        InvalidM2mConfiguration,
        #[error("Databricks OAuth M2M token response is malformed")]
        InvalidOAuthResponse,
        #[error("application credential callback failed")]
        CredentialProvider,
        #[error("Unity registration contains sensitive or credential-bearing metadata: {0}")]
        SensitiveRegistration(String),
        #[error("Unity registration storage location is invalid: {0}")]
        InvalidStorageLocation(String),
        #[error("Unity table cannot be registered locally: {0}")]
        UnsupportedTable(String),
        #[error(
            "remote Databricks FILE access is unsupported without a caller-provided provider: {0}"
        )]
        UnsupportedRemoteFileAccess(String),
        #[error("Unity registration store has an unsupported format version: {0}")]
        UnsupportedRegistryVersion(u32),
        #[error(
            "Unity endpoint must be HTTPS (or loopback HTTP) and contain no credentials, query, or fragment: {0}"
        )]
        InvalidEndpoint(String),
        #[error("Unity temporary credential response is malformed")]
        InvalidTemporaryCredentialResponse,
        #[cfg(feature = "delta-rs")]
        #[error(
            "Unity temporary credentials cannot be translated into a scoped S3 configuration: {0}"
        )]
        InvalidS3CredentialResponse(String),
        #[error(transparent)]
        Http(#[from] reqwest::Error),
        #[error(transparent)]
        Url(#[from] url::ParseError),
        #[error(transparent)]
        Io(#[from] std::io::Error),
        #[error(transparent)]
        Json(#[from] serde_json::Error),
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    pub struct ResolvedUnityTable {
        pub table_id: String,
        pub full_name: String,
        pub table_type: String,
        pub storage_location: Option<String>,
        #[serde(default)]
        pub data_source_format: Option<String>,
        #[serde(default)]
        pub properties: BTreeMap<String, String>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
    #[serde(rename_all = "UPPERCASE")]
    pub enum UnityCredentialOperation {
        Read,
        Write,
    }

    /// Ephemeral temporary credentials returned by Unity. The response payload
    /// is intentionally private, non-serializable, and redacted in Debug.
    /// Consumers must use it immediately within the supplied closure.
    pub struct VendedTemporaryCredentials {
        expires_at: Option<String>,
        payload: serde_json::Value,
    }

    impl VendedTemporaryCredentials {
        pub fn expires_at(&self) -> Option<&str> {
            self.expires_at.as_deref()
        }

        pub fn with_payload<T>(&self, consume: impl FnOnce(&serde_json::Value) -> T) -> T {
            consume(&self.payload)
        }

        /// Translates a Unity AWS temporary-credential response only while it
        /// remains in this request scope. The resulting config owns ephemeral
        /// credentials but is non-serializable and must be passed directly to
        /// a read/write operation, never retained in a catalog or session.
        #[cfg(feature = "delta-rs")]
        pub fn s3_config_for_location(
            &self,
            storage_location: &str,
        ) -> Result<lakeprism_storage::S3CompatibleConfig, UnityError> {
            let location = Url::parse(storage_location).map_err(|_| {
                UnityError::InvalidS3CredentialResponse(
                    "storage location is not an S3 URI".to_owned(),
                )
            })?;
            if location.scheme() != "s3"
                || location.host_str().is_none()
                || !location.username().is_empty()
                || location.password().is_some()
                || location.query().is_some()
                || location.fragment().is_some()
            {
                return Err(UnityError::InvalidS3CredentialResponse(
                    "storage location must be credential-free s3://bucket/path".to_owned(),
                ));
            }
            let string = |names: &[&str]| {
                names.iter().find_map(|name| {
                    self.payload
                        .get(*name)
                        .and_then(serde_json::Value::as_str)
                        .filter(|value| !value.is_empty())
                })
            };
            let access_key =
                string(&["aws_temp_access_key", "access_key_id"]).ok_or_else(|| {
                    UnityError::InvalidS3CredentialResponse(
                        "missing AWS temporary access key".to_owned(),
                    )
                })?;
            let secret_key =
                string(&["aws_temp_secret_key", "secret_access_key"]).ok_or_else(|| {
                    UnityError::InvalidS3CredentialResponse(
                        "missing AWS temporary secret key".to_owned(),
                    )
                })?;
            let session_token =
                string(&["aws_session_token", "session_token"]).ok_or_else(|| {
                    UnityError::InvalidS3CredentialResponse(
                        "missing AWS temporary session token".to_owned(),
                    )
                })?;
            let region = string(&["aws_region", "region"]).unwrap_or("us-west-2");
            let endpoint = string(&["aws_endpoint_url", "endpoint_url"])
                .map(str::to_owned)
                .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
            let credentials = lakeprism_storage::S3Credentials::new(access_key, secret_key)
                .with_session_token(session_token);
            lakeprism_storage::S3CompatibleConfig::new(
                location.host_str().expect("checked S3 bucket"),
                region,
                endpoint,
                credentials,
            )
            .map_err(|error| UnityError::InvalidS3CredentialResponse(error.to_string()))
        }
    }

    impl std::fmt::Debug for VendedTemporaryCredentials {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("VendedTemporaryCredentials")
                .field("expires_at", &self.expires_at)
                .field("payload", &"[redacted]")
                .finish()
        }
    }

    #[derive(Serialize)]
    struct TemporaryTableCredentialRequest<'a> {
        table_id: &'a str,
        operation: UnityCredentialOperation,
    }

    #[derive(Serialize)]
    struct TemporaryPathCredentialRequest<'a> {
        url: &'a str,
        operation: UnityCredentialOperation,
    }

    /// Explicit mapping for a Unity physical table backed by an Iceberg REST
    /// catalog. Unity's generic table response does not standardize a REST
    /// endpoint, so LakePrism maps only these explicit, credential-free
    /// properties and never guesses one from a storage location.
    #[cfg(feature = "iceberg-rest")]
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct UnityIcebergRestMapping {
        pub catalog_name: String,
        pub rest_uri: Url,
        pub warehouse: Option<String>,
        pub prefix: Option<String>,
        pub namespace: Vec<String>,
        pub table_name: String,
    }

    #[cfg(feature = "iceberg-rest")]
    impl UnityIcebergRestMapping {
        pub fn from_resolved(table: &ResolvedUnityTable) -> Result<Self, UnityError> {
            let kind = table.table_type.to_ascii_uppercase();
            let format = table
                .data_source_format
                .as_deref()
                .unwrap_or_default()
                .to_ascii_uppercase();
            if !matches!(kind.as_str(), "MANAGED" | "EXTERNAL") || format != "ICEBERG" {
                return Err(UnityError::UnsupportedTable(format!(
                    "{} is not a physical Unity ICEBERG table",
                    table.full_name
                )));
            }
            let rest_uri = table
                .properties
                .get("iceberg.rest.uri")
                .ok_or_else(|| {
                    UnityError::UnsupportedTable(format!(
                        "{} lacks explicit iceberg.rest.uri metadata",
                        table.full_name
                    ))
                })?
                .parse()
                .map_err(|_| UnityError::InvalidStorageLocation(table.full_name.clone()))?;
            let parts = table
                .properties
                .get("iceberg.table.identifier")
                .map(String::as_str)
                .unwrap_or(&table.full_name)
                .split('.')
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let (table_name, namespace) = parts
                .split_last()
                .ok_or_else(|| UnityError::InvalidTableIdentifier(table.full_name.clone()))?;
            if namespace.is_empty() || parts.iter().any(|part| !valid_table_identifier(part)) {
                return Err(UnityError::InvalidTableIdentifier(table.full_name.clone()));
            }
            Ok(Self {
                catalog_name: table
                    .properties
                    .get("iceberg.rest.catalog")
                    .cloned()
                    .unwrap_or_else(|| "unity-iceberg".to_owned()),
                rest_uri,
                warehouse: table.properties.get("iceberg.rest.warehouse").cloned(),
                prefix: table.properties.get("iceberg.rest.prefix").cloned(),
                namespace: namespace.to_vec(),
                table_name: table_name.to_owned(),
            })
        }
    }

    /// Durable, credential-free registration metadata. It is sufficient to
    /// re-resolve a governed table, but intentionally cannot reconnect to it.
    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    pub struct UnityCatalogRegistration {
        pub endpoint: String,
        pub table_id: String,
        pub full_name: String,
        pub table_type: String,
        pub storage_location: Option<String>,
        pub data_source_format: Option<String>,
    }

    impl UnityCatalogRegistration {
        pub fn from_resolved(
            endpoint: &Url,
            table: &ResolvedUnityTable,
        ) -> Result<Self, UnityError> {
            let registration = Self {
                endpoint: endpoint.as_str().to_owned(),
                table_id: table.table_id.clone(),
                full_name: table.full_name.clone(),
                table_type: table.table_type.clone(),
                storage_location: table.storage_location.clone(),
                data_source_format: table.data_source_format.clone(),
            };
            registration.validate()?;
            Ok(registration)
        }

        fn validate(&self) -> Result<(), UnityError> {
            let endpoint = Url::parse(&self.endpoint)?;
            if !endpoint.username().is_empty()
                || endpoint.password().is_some()
                || endpoint.query().is_some()
                || !valid_table_identifier(&self.full_name)
                || [self.table_id.as_str(), self.table_type.as_str()]
                    .into_iter()
                    .any(looks_sensitive)
                || self.storage_location.as_deref().is_some_and(|value| {
                    looks_sensitive(value)
                        || MediaRef::new(value, "table", lakeprism_core::StorageMode::External)
                            .is_err()
                })
            {
                return Err(UnityError::SensitiveRegistration(self.full_name.clone()));
            }
            Ok(())
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct PersistedRegistry {
        format_version: u32,
        registrations: BTreeMap<String, UnityCatalogRegistration>,
    }

    impl Default for PersistedRegistry {
        fn default() -> Self {
            Self {
                format_version: REGISTRY_VERSION,
                registrations: BTreeMap::new(),
            }
        }
    }

    /// An atomically-written local registry. It deliberately persists only
    /// catalog identity and resolution metadata, never a `CredentialProvider`.
    pub struct UnityCatalogRegistry {
        root: PathBuf,
        registry: PersistedRegistry,
    }

    impl UnityCatalogRegistry {
        pub fn open(root: impl AsRef<Path>) -> Result<Self, UnityError> {
            fs::create_dir_all(root.as_ref())?;
            let root = fs::canonicalize(root)?;
            let registry = load_registry(&root)?;
            Ok(Self { root, registry })
        }

        pub fn register(
            &mut self,
            registration: UnityCatalogRegistration,
        ) -> Result<(), UnityError> {
            registration.validate()?;
            self.registry
                .registrations
                .insert(registration.full_name.clone(), registration);
            persist_registry(&self.root, &self.registry)
        }

        pub fn get(&self, full_name: &str) -> Option<&UnityCatalogRegistration> {
            self.registry.registrations.get(full_name)
        }

        pub fn registrations(&self) -> impl Iterator<Item = &UnityCatalogRegistration> {
            self.registry.registrations.values()
        }
    }

    pub struct UnityCatalogClient<P> {
        base_url: Url,
        credential_provider: P,
        client: reqwest::Client,
    }

    impl<P: CredentialProvider> UnityCatalogClient<P> {
        /// Creates a client only for a TLS endpoint, except loopback HTTP used
        /// by local notebook mocks and protocol tests.
        pub fn new(base_url: Url, credential_provider: P) -> Result<Self, UnityError> {
            validate_unity_endpoint(&base_url)?;
            Ok(Self {
                base_url,
                credential_provider,
                client: reqwest::Client::new(),
            })
        }

        pub fn endpoint(&self) -> &Url {
            &self.base_url
        }

        /// Resolves one table using a newly-vended credential for this request.
        /// No credential is stored in the client, result, or registration.
        pub async fn resolve_table(
            &self,
            table_name: &str,
            context: &UnityQueryContext,
        ) -> Result<ResolvedUnityTable, UnityError> {
            if !valid_table_identifier(table_name) {
                return Err(UnityError::InvalidTableIdentifier(table_name.to_owned()));
            }
            let credential = self.credential_provider.credential_for(context).await?;
            let url = self
                .base_url
                .join(&format!("api/2.1/unity-catalog/tables/{table_name}"))?;
            self.client
                .get(url)
                .bearer_auth(&credential.bearer_token)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
                .map_err(UnityError::from)
        }

        /// Vends one short-lived credential envelope for a physical Unity
        /// table. Its payload is never stored by this client or registry.
        pub async fn vend_temporary_table_credentials(
            &self,
            table_id: &str,
            context: &UnityQueryContext,
        ) -> Result<VendedTemporaryCredentials, UnityError> {
            if table_id.trim().is_empty() {
                return Err(UnityError::InvalidTableIdentifier(table_id.to_owned()));
            }
            self.temporary_credentials(
                "api/2.1/unity-catalog/temporary-table-credentials",
                &TemporaryTableCredentialRequest {
                    table_id,
                    operation: UnityCredentialOperation::Read,
                },
                context,
            )
            .await
        }

        /// Vends a temporary write credential only for the experimental
        /// guarded remote Delta path. It has the same non-persistence
        /// guarantees as the read envelope.
        pub async fn vend_temporary_table_write_credentials(
            &self,
            table_id: &str,
            context: &UnityQueryContext,
        ) -> Result<VendedTemporaryCredentials, UnityError> {
            if table_id.trim().is_empty() {
                return Err(UnityError::InvalidTableIdentifier(table_id.to_owned()));
            }
            self.temporary_credentials(
                "api/2.1/unity-catalog/temporary-table-credentials",
                &TemporaryTableCredentialRequest {
                    table_id,
                    operation: UnityCredentialOperation::Write,
                },
                context,
            )
            .await
        }

        /// Vends one short-lived credential envelope for a governed path.
        /// Signed URLs and embedded credential material are rejected before
        /// any request is issued.
        pub async fn vend_temporary_path_credentials(
            &self,
            path: &UnityVolumePath,
            context: &UnityQueryContext,
        ) -> Result<VendedTemporaryCredentials, UnityError> {
            let url = path.governed_path();
            self.temporary_credentials(
                "api/2.1/unity-catalog/temporary-path-credentials",
                &TemporaryPathCredentialRequest {
                    url: &url,
                    operation: UnityCredentialOperation::Read,
                },
                context,
            )
            .await
        }

        async fn temporary_credentials<T: Serialize + ?Sized>(
            &self,
            endpoint: &str,
            request: &T,
            context: &UnityQueryContext,
        ) -> Result<VendedTemporaryCredentials, UnityError> {
            let credential = self.credential_provider.credential_for(context).await?;
            let payload = self
                .client
                .post(self.base_url.join(endpoint)?)
                .bearer_auth(&credential.bearer_token)
                .json(request)
                .send()
                .await?
                .error_for_status()?
                .json::<serde_json::Value>()
                .await?;
            if !payload.is_object() {
                return Err(UnityError::InvalidTemporaryCredentialResponse);
            }
            let expires_at = payload
                .get("expiration")
                .or_else(|| payload.get("expires_at"))
                .or_else(|| payload.get("expiration_time"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            Ok(VendedTemporaryCredentials {
                expires_at,
                payload,
            })
        }

        /// Resolves a Unity table and registers only a local, credential-free
        /// Parquet location with DataFusion. Remote cloud locations, views,
        /// Delta/Iceberg formats, and unknown formats remain explicitly
        /// unsupported rather than being executed with invented credentials.
        pub async fn resolve_and_register_table(
            &self,
            source_table: &str,
            datafusion_table: &str,
            context: &UnityQueryContext,
            session: &MediaSession,
        ) -> Result<UnityTableRegistration, UnityError> {
            let table = self.resolve_table(source_table, context).await?;
            register_resolved_table(session, datafusion_table, &table).await
        }

        /// Resolves and attaches a managed/external S3 Delta table with
        /// freshly vended Unity credentials. The Delta provider is lazy; this
        /// method does not materialize data or persist temporary credentials.
        #[cfg(feature = "delta-rs")]
        pub async fn resolve_and_register_managed_delta_table(
            &self,
            source_table: &str,
            datafusion_table: &str,
            context: &UnityQueryContext,
            session: &MediaSession,
        ) -> Result<UnityTableRegistration, UnityError> {
            let table = self.resolve_table(source_table, context).await?;
            self.register_managed_delta_table(&table, datafusion_table, None, context, session)
                .await
        }

        /// Experimental remote write path. Callers must explicitly declare
        /// either externally enforced single-writer ownership or a live-
        /// validated conditional-PUT backend. No credential is stored after
        /// this call completes.
        #[cfg(feature = "delta-rs")]
        pub async fn write_managed_delta_table_experimental(
            &self,
            source_table: &str,
            context: &UnityQueryContext,
            batches: Vec<arrow::record_batch::RecordBatch>,
            options: lakeprism_delta::DeltaWriteOptions,
            guard: lakeprism_delta::ExperimentalRemoteWriteGuard,
        ) -> Result<lakeprism_delta::DeltaWriteResult, UnityError> {
            let table = self.resolve_table(source_table, context).await?;
            let location = validated_remote_delta_location(&table)?;
            let temporary = self
                .vend_temporary_table_write_credentials(&table.table_id, context)
                .await?;
            let s3 = temporary.s3_config_for_location(location)?;
            lakeprism_delta::write_s3_delta_table(location, &s3, batches, options, guard)
                .await
                .map_err(|error| UnityError::UnsupportedTable(error.to_string()))
        }

        #[cfg(feature = "delta-rs")]
        async fn register_managed_delta_table(
            &self,
            table: &ResolvedUnityTable,
            datafusion_table: &str,
            schema: Option<(&str, &str)>,
            context: &UnityQueryContext,
            session: &MediaSession,
        ) -> Result<UnityTableRegistration, UnityError> {
            let location = validated_remote_delta_location(table)?;
            let temporary = self
                .vend_temporary_table_credentials(&table.table_id, context)
                .await?;
            let s3 = temporary.s3_config_for_location(location)?;
            if let Some((catalog_name, schema_name)) = schema {
                lakeprism_delta::register_s3_delta_table_in_schema(
                    session,
                    catalog_name,
                    schema_name,
                    datafusion_table,
                    location,
                    &s3,
                )
                .await
                .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
            } else {
                lakeprism_delta::register_s3_delta_table(session, datafusion_table, location, &s3)
                    .await
                    .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
            }
            Ok(UnityTableRegistration::RegisteredManagedS3Delta {
                table_name: datafusion_table.to_owned(),
                source_table: table.full_name.clone(),
                deletion_vectors_verified: lakeprism_delta::read_capabilities().deletion_vectors,
            })
        }

        /// Resolves all physical tables in one Unity schema and attaches them
        /// under `catalog.schema.table` in the local DataFusion session.
        ///
        /// Each table remains a native lazy provider where LakePrism can prove
        /// compatibility; unsupported remote paths are reported, never
        /// replaced with unauthenticated readers.
        pub async fn attach_schema(
            &self,
            catalog_name: &str,
            schema_name: &str,
            context: &UnityQueryContext,
            session: &MediaSession,
        ) -> Result<UnitySchemaRegistration, UnityError> {
            if !valid_table_identifier(catalog_name) || !valid_table_identifier(schema_name) {
                return Err(UnityError::InvalidTableIdentifier(format!(
                    "{catalog_name}.{schema_name}"
                )));
            }
            let credential = self.credential_provider.credential_for(context).await?;
            let url = self.base_url.join(&format!(
                "api/2.1/unity-catalog/tables?catalog_name={catalog_name}&schema_name={schema_name}"
            ))?;
            #[derive(Deserialize)]
            struct TableList {
                #[serde(default)]
                tables: Vec<ResolvedUnityTable>,
            }
            let tables = self
                .client
                .get(url)
                .bearer_auth(credential.bearer_token)
                .send()
                .await?
                .error_for_status()?
                .json::<TableList>()
                .await?
                .tables;
            let mut attached = Vec::new();
            let mut unsupported = Vec::new();
            for table in tables {
                let Some(table_name) = table.full_name.rsplit('.').next() else {
                    unsupported.push(UnityUnsupportedTable {
                        source_table: table.full_name,
                        reason: "Unity table name is invalid".to_owned(),
                    });
                    continue;
                };
                if !valid_table_identifier(table_name) {
                    unsupported.push(UnityUnsupportedTable {
                        source_table: table.full_name,
                        reason: "Unity table name is invalid".to_owned(),
                    });
                    continue;
                }
                #[cfg(feature = "delta-rs")]
                let registration = if is_remote_delta(&table) {
                    self.register_managed_delta_table(
                        &table,
                        table_name,
                        Some((catalog_name, schema_name)),
                        context,
                        session,
                    )
                    .await?
                } else {
                    register_resolved_table_in_schema(
                        session,
                        catalog_name,
                        schema_name,
                        table_name,
                        &table,
                    )
                    .await?
                };
                #[cfg(not(feature = "delta-rs"))]
                let registration = register_resolved_table_in_schema(
                    session,
                    catalog_name,
                    schema_name,
                    table_name,
                    &table,
                )
                .await?;
                match registration {
                    UnityTableRegistration::Unsupported { reason } => {
                        unsupported.push(UnityUnsupportedTable {
                            source_table: table.full_name,
                            reason,
                        });
                    }
                    registration => attached.push(registration),
                }
            }
            Ok(UnitySchemaRegistration {
                catalog_name: catalog_name.to_owned(),
                schema_name: schema_name.to_owned(),
                attached,
                unsupported,
            })
        }

        /// Resolves a Unity ICEBERG table and maps it to an explicitly-declared
        /// Iceberg REST catalog. A freshly vended Unity credential is used only
        /// to construct this request scope; the resulting catalog config and
        /// session registration contain no bearer material.
        #[cfg(feature = "iceberg-rest")]
        pub async fn resolve_and_register_iceberg_rest_table(
            &self,
            source_table: &str,
            datafusion_table: &str,
            context: &UnityQueryContext,
            session: &MediaSession,
            snapshot_id: Option<i64>,
        ) -> Result<UnityTableRegistration, UnityError> {
            let table = self.resolve_table(source_table, context).await?;
            let mapping = UnityIcebergRestMapping::from_resolved(&table)?;
            let credential = self.credential_provider.credential_for(context).await?;
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!(
                    "Bearer {}",
                    credential.bearer_token
                ))
                .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?,
            );
            let client = reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .map_err(UnityError::Http)?;
            let mut properties = std::collections::HashMap::new();
            if let Some(prefix) = mapping.prefix {
                properties.insert("prefix".to_owned(), prefix);
            }
            let config = lakeprism_iceberg::rest::RestCatalogConfig::new(
                mapping.catalog_name,
                mapping.rest_uri,
                mapping.warehouse,
                properties,
            )
            .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
            let catalog = lakeprism_iceberg::rest::ScopedRestCatalog::open_with_client(
                config,
                context.access.clone(),
                client,
            )
            .await
            .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
            catalog
                .register_table(
                    session,
                    datafusion_table,
                    &mapping.namespace,
                    &mapping.table_name,
                    snapshot_id,
                )
                .await
                .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
            Ok(UnityTableRegistration::RegisteredIcebergRest {
                table_name: datafusion_table.to_owned(),
                source_table: table.full_name,
                snapshot_id,
            })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub enum UnityTableRegistration {
        RegisteredLocalParquet {
            table_name: String,
            path: PathBuf,
        },
        #[cfg(feature = "delta-rs")]
        RegisteredLocalDelta {
            table_name: String,
            path: PathBuf,
            deletion_vectors_verified: bool,
        },
        #[cfg(feature = "delta-rs")]
        RegisteredManagedS3Delta {
            table_name: String,
            source_table: String,
            deletion_vectors_verified: bool,
        },
        #[cfg(feature = "iceberg-rest")]
        RegisteredIcebergRest {
            table_name: String,
            source_table: String,
            snapshot_id: Option<i64>,
        },
        Unsupported {
            reason: String,
        },
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct UnityUnsupportedTable {
        pub source_table: String,
        pub reason: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct UnitySchemaRegistration {
        pub catalog_name: String,
        pub schema_name: String,
        pub attached: Vec<UnityTableRegistration>,
        pub unsupported: Vec<UnityUnsupportedTable>,
    }

    #[cfg(feature = "delta-rs")]
    fn is_remote_delta(table: &ResolvedUnityTable) -> bool {
        table
            .data_source_format
            .as_deref()
            .is_some_and(|format| format.eq_ignore_ascii_case("DELTA"))
            && table
                .storage_location
                .as_deref()
                .is_some_and(|location| location.starts_with("s3://"))
    }

    #[cfg(feature = "delta-rs")]
    fn validated_remote_delta_location(table: &ResolvedUnityTable) -> Result<&str, UnityError> {
        if !matches!(
            table.table_type.to_ascii_uppercase().as_str(),
            "MANAGED" | "EXTERNAL"
        ) || !table
            .data_source_format
            .as_deref()
            .is_some_and(|format| format.eq_ignore_ascii_case("DELTA"))
        {
            return Err(UnityError::UnsupportedTable(format!(
                "{} is not a physical Unity Delta table",
                table.full_name
            )));
        }
        let location = table.storage_location.as_deref().ok_or_else(|| {
            UnityError::UnsupportedTable(format!("{} has no storage location", table.full_name))
        })?;
        let url = Url::parse(location)
            .map_err(|_| UnityError::InvalidStorageLocation(location.to_owned()))?;
        if url.scheme() != "s3"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(UnityError::UnsupportedTable(
                "managed Delta currently requires a credential-free s3:// storage location"
                    .to_owned(),
            ));
        }
        Ok(location)
    }

    pub async fn register_resolved_table(
        session: &MediaSession,
        datafusion_table: &str,
        table: &ResolvedUnityTable,
    ) -> Result<UnityTableRegistration, UnityError> {
        let kind = table.table_type.to_ascii_uppercase();
        if !matches!(kind.as_str(), "MANAGED" | "EXTERNAL") {
            return Ok(UnityTableRegistration::Unsupported {
                reason: format!("Unity table type {kind} is not a physical managed/external table"),
            });
        }
        let format = table
            .data_source_format
            .as_deref()
            .unwrap_or_default()
            .to_ascii_uppercase();
        if format != "PARQUET" && format != "DELTA" {
            return Ok(UnityTableRegistration::Unsupported {
                reason: format!("Unity data source format {format:?} is not locally supported"),
            });
        }
        let Some(location) = table.storage_location.as_deref() else {
            return Ok(UnityTableRegistration::Unsupported {
                reason: "Unity table has no storage location".to_owned(),
            });
        };
        let url = Url::parse(location)
            .map_err(|_| UnityError::InvalidStorageLocation(location.to_owned()))?;
        if url.scheme() != "file" {
            return Ok(UnityTableRegistration::Unsupported {
                reason: format!(
                    "remote {} storage requires a caller-provided DataFusion object-store provider",
                    url.scheme()
                ),
            });
        }
        let path = url
            .to_file_path()
            .map_err(|_| UnityError::InvalidStorageLocation(location.to_owned()))?;
        #[cfg(feature = "delta-rs")]
        if format == "DELTA" {
            lakeprism_delta::register_delta_table(session, datafusion_table, url.as_str())
                .await
                .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
            return Ok(UnityTableRegistration::RegisteredLocalDelta {
                table_name: datafusion_table.to_owned(),
                path,
                deletion_vectors_verified: lakeprism_delta::read_capabilities().deletion_vectors,
            });
        }
        if format == "DELTA" {
            return Ok(UnityTableRegistration::Unsupported {
                reason: "Unity Delta requires the lakeprism-unity/delta-rs capability".to_owned(),
            });
        }
        session
            .register_parquet(datafusion_table, path.to_string_lossy().as_ref())
            .await
            .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
        Ok(UnityTableRegistration::RegisteredLocalParquet {
            table_name: datafusion_table.to_owned(),
            path,
        })
    }

    pub async fn register_resolved_table_in_schema(
        session: &MediaSession,
        catalog_name: &str,
        schema_name: &str,
        datafusion_table: &str,
        table: &ResolvedUnityTable,
    ) -> Result<UnityTableRegistration, UnityError> {
        let kind = table.table_type.to_ascii_uppercase();
        if !matches!(kind.as_str(), "MANAGED" | "EXTERNAL") {
            return Ok(UnityTableRegistration::Unsupported {
                reason: format!("Unity table type {kind} is not a physical managed/external table"),
            });
        }
        let Some(location) = table.storage_location.as_deref() else {
            return Ok(UnityTableRegistration::Unsupported {
                reason: "Unity table has no storage location".to_owned(),
            });
        };
        let url = Url::parse(location)
            .map_err(|_| UnityError::InvalidStorageLocation(location.to_owned()))?;
        if url.scheme() != "file" {
            return Ok(UnityTableRegistration::Unsupported {
                reason: format!(
                    "remote {} storage requires an explicit query-scoped storage adapter",
                    url.scheme()
                ),
            });
        }
        let path = url
            .to_file_path()
            .map_err(|_| UnityError::InvalidStorageLocation(location.to_owned()))?;
        let format = table
            .data_source_format
            .as_deref()
            .unwrap_or_default()
            .to_ascii_uppercase();
        match format.as_str() {
            "PARQUET" => {
                session
                    .register_parquet_in_schema(
                        catalog_name,
                        schema_name,
                        datafusion_table,
                        path.to_string_lossy().as_ref(),
                    )
                    .await
                    .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
                Ok(UnityTableRegistration::RegisteredLocalParquet {
                    table_name: datafusion_table.to_owned(),
                    path,
                })
            }
            #[cfg(feature = "delta-rs")]
            "DELTA" => {
                lakeprism_delta::register_delta_table_in_schema(
                    session,
                    catalog_name,
                    schema_name,
                    datafusion_table,
                    url.as_str(),
                )
                .await
                .map_err(|error| UnityError::UnsupportedTable(error.to_string()))?;
                Ok(UnityTableRegistration::RegisteredLocalDelta {
                    table_name: datafusion_table.to_owned(),
                    path,
                    deletion_vectors_verified: lakeprism_delta::read_capabilities()
                        .deletion_vectors,
                })
            }
            _ => Ok(UnityTableRegistration::Unsupported {
                reason: format!("Unity data source format {format:?} is not supported"),
            }),
        }
    }

    /// Adapts FILE metadata from a caller-provided managed/external provider
    /// into the portable representation. It performs no remote reads.
    pub async fn resolve_databricks_file<P: DatabricksFileProvider>(
        provider: &P,
        file_reference: &str,
        context: &UnityQueryContext,
    ) -> Result<MediaRef, UnityError> {
        let file = provider.resolve_file(file_reference, context).await?;
        MediaRef::try_from(file)
            .map_err(|error| UnityError::UnsupportedRemoteFileAccess(error.to_string()))
    }

    fn valid_table_identifier(table_name: &str) -> bool {
        !table_name.is_empty()
            && table_name.split('.').all(|part| {
                !part.is_empty()
                    && part
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '_')
            })
    }

    fn validate_unity_endpoint(endpoint: &Url) -> Result<(), UnityError> {
        let loopback = endpoint
            .host_str()
            .is_some_and(|host| host == "localhost" || host == "::1" || host.starts_with("127."));
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && loopback))
        {
            return Err(UnityError::InvalidEndpoint(endpoint.to_string()));
        }
        Ok(())
    }

    fn looks_sensitive(value: &str) -> bool {
        let normalized = value.to_ascii_lowercase();
        [
            "authorization",
            "credential",
            "password",
            "secret",
            "access_token",
            "api_key",
            "token",
            "bearer ",
        ]
        .iter()
        .any(|needle| normalized.contains(needle))
    }

    fn load_registry(root: &Path) -> Result<PersistedRegistry, UnityError> {
        let path = root.join(REGISTRY_FILE);
        if !path.exists() {
            return Ok(PersistedRegistry::default());
        }
        let registry: PersistedRegistry = serde_json::from_slice(&fs::read(path)?)?;
        if registry.format_version != REGISTRY_VERSION {
            return Err(UnityError::UnsupportedRegistryVersion(
                registry.format_version,
            ));
        }
        for registration in registry.registrations.values() {
            registration.validate()?;
        }
        Ok(registry)
    }

    fn persist_registry(root: &Path, registry: &PersistedRegistry) -> Result<(), UnityError> {
        let destination = root.join(REGISTRY_FILE);
        let temporary = root.join(format!(".unity-{}.tmp", Uuid::new_v4()));
        let payload = serde_json::to_vec_pretty(registry)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        drop(file);
        fs::rename(temporary, destination)?;
        File::open(root)?.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use super::*;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use lakeprism_core::StorageMode;
        use parquet::arrow::ArrowWriter;
        use wiremock::matchers::{
            body_json, body_string_contains, header, header_exists, method, path,
        };
        use wiremock::{Mock, MockServer, ResponseTemplate};

        struct RefreshingCredentialProvider(AtomicUsize);

        #[async_trait]
        impl CredentialProvider for RefreshingCredentialProvider {
            async fn credential_for(
                &self,
                context: &UnityQueryContext,
            ) -> Result<VendedCredential, UnityError> {
                assert_eq!(context.query_id(), "query-1");
                let sequence = self.0.fetch_add(1, Ordering::SeqCst);
                Ok(VendedCredential::new(format!("fresh-token-{sequence}")))
            }
        }

        struct MockFileProvider;

        #[async_trait]
        impl DatabricksFileProvider for MockFileProvider {
            async fn resolve_file(
                &self,
                file_reference: &str,
                context: &UnityQueryContext,
            ) -> Result<DatabricksFile, UnityError> {
                assert_eq!(file_reference, "main.media.clip");
                assert_eq!(context.query_id(), "query-1");
                Ok(DatabricksFile {
                    uri: "file:///media/clip.mp4".to_owned(),
                    media_type: "video".to_owned(),
                    mime_type: Some("video/mp4".to_owned()),
                    size_bytes: Some(12),
                    etag: Some("v1".to_owned()),
                    catalog_ref: Some(file_reference.to_owned()),
                    storage_mode: StorageMode::Managed,
                })
            }
        }

        fn context() -> UnityQueryContext {
            UnityQueryContext::new(
                "query-1",
                AccessContext {
                    principal: "analyst@example.test".to_owned(),
                    catalog_identity: Some("unity-main".to_owned()),
                },
            )
            .unwrap()
        }

        #[tokio::test]
        async fn refreshes_credentials_for_each_query_scoped_request() {
            let server = MockServer::start().await;
            for (name, token) in [
                ("main.media.one", "fresh-token-0"),
                ("main.media.two", "fresh-token-1"),
            ] {
                Mock::given(method("GET"))
                    .and(path(format!("/api/2.1/unity-catalog/tables/{name}")))
                    .and(header("authorization", format!("Bearer {token}")))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "table_id": name,
                        "full_name": name,
                        "table_type": "EXTERNAL",
                        "storage_location": "s3://bucket/table",
                        "data_source_format": "PARQUET"
                    })))
                    .mount(&server)
                    .await;
            }
            let client = UnityCatalogClient::new(
                server.uri().parse().unwrap(),
                RefreshingCredentialProvider(AtomicUsize::new(0)),
            )
            .unwrap();
            assert_eq!(
                client
                    .resolve_table("main.media.one", &context())
                    .await
                    .unwrap()
                    .table_id,
                "main.media.one"
            );
            assert_eq!(
                client
                    .resolve_table("main.media.two", &context())
                    .await
                    .unwrap()
                    .table_id,
                "main.media.two"
            );
        }

        #[tokio::test]
        async fn oauth_m2m_uses_client_credentials_without_exposing_them() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/oidc/v1/token"))
                .and(body_string_contains("grant_type=client_credentials"))
                .and(body_string_contains("scope=all-apis"))
                .and(body_string_contains(
                    "client_id=service-principal-client-id",
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "access-token-not-for-logs",
                    "token_type": "Bearer",
                    "expires_in": 300
                })))
                .mount(&server)
                .await;
            let config = DatabricksM2mConfig::new(
                server.uri().parse().unwrap(),
                "service-principal-client-id",
                "client-secret-not-for-logs",
            )
            .unwrap();
            assert!(!format!("{config:?}").contains("client-secret-not-for-logs"));
            let credential = DatabricksM2mCredentialProvider::new(config)
                .credential_for(&context())
                .await
                .unwrap();
            assert!(!format!("{credential:?}").contains("access-token-not-for-logs"));
        }

        #[tokio::test]
        async fn vends_table_and_path_credentials_only_for_the_query_scope() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
                .and(header_exists("authorization"))
                .and(body_json(serde_json::json!({
                    "table_id": "table-id",
                    "operation": "READ"
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "expiration": "2030-01-01T00:00:00Z",
                    "aws_temp_access_key": "never-persist-this"
                })))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/2.1/unity-catalog/temporary-path-credentials"))
                .and(header_exists("authorization"))
                .and(body_json(serde_json::json!({
                    "url": "/Volumes/main/media/raw/clip.mp4",
                    "operation": "READ"
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "expiration": "2030-01-01T00:00:00Z",
                    "aws_temp_access_key": "never-persist-this"
                })))
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
                .and(header_exists("authorization"))
                .and(body_json(serde_json::json!({
                    "table_id": "table-id",
                    "operation": "WRITE"
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "expiration": "2030-01-01T00:00:00Z",
                    "aws_temp_access_key": "never-persist-this"
                })))
                .mount(&server)
                .await;

            let client = UnityCatalogClient::new(
                server.uri().parse().unwrap(),
                RefreshingCredentialProvider(AtomicUsize::new(0)),
            )
            .unwrap();
            let table = client
                .vend_temporary_table_credentials("table-id", &context())
                .await
                .unwrap();
            let path = client
                .vend_temporary_path_credentials(
                    &UnityVolumePath::parse("/Volumes/main/media/raw/clip.mp4").unwrap(),
                    &context(),
                )
                .await
                .unwrap();
            let write = client
                .vend_temporary_table_write_credentials("table-id", &context())
                .await
                .unwrap();

            assert_eq!(table.expires_at(), Some("2030-01-01T00:00:00Z"));
            assert!(!format!("{table:?}").contains("never-persist-this"));
            assert!(path.with_payload(|value| { value["aws_temp_access_key"].as_str().is_some() }));
            assert_eq!(write.expires_at(), Some("2030-01-01T00:00:00Z"));
        }

        #[cfg(feature = "delta-rs")]
        #[tokio::test]
        async fn translates_vended_aws_credentials_only_at_use_time() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "aws_temp_access_key": "AKIA-not-for-logs",
                    "aws_temp_secret_key": "secret-not-for-logs",
                    "aws_session_token": "session-not-for-logs",
                    "aws_region": "us-east-1"
                })))
                .mount(&server)
                .await;
            let client = UnityCatalogClient::new(
                server.uri().parse().unwrap(),
                RefreshingCredentialProvider(AtomicUsize::new(0)),
            )
            .unwrap();
            let credentials = client
                .vend_temporary_table_credentials("table-id", &context())
                .await
                .unwrap();
            let s3 = credentials
                .s3_config_for_location("s3://lakeprism-test/table")
                .unwrap();
            let options = s3.delta_storage_options();
            assert_eq!(options["AWS_REGION"], "us-east-1");
            assert!(!format!("{credentials:?}").contains("secret-not-for-logs"));
        }

        #[tokio::test]
        async fn attach_schema_reports_remote_tables_without_faking_a_reader() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/2.1/unity-catalog/tables"))
                .and(header_exists("authorization"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "tables": [{
                        "table_id": "remote-id",
                        "full_name": "main.media.remote",
                        "table_type": "MANAGED",
                        "storage_location": "abfss://bucket/table",
                        "data_source_format": "DELTA"
                    }]
                })))
                .mount(&server)
                .await;
            let client = UnityCatalogClient::new(
                server.uri().parse().unwrap(),
                RefreshingCredentialProvider(AtomicUsize::new(0)),
            )
            .unwrap();
            let report = client
                .attach_schema("main", "media", &context(), &MediaSession::new())
                .await
                .unwrap();
            assert!(report.attached.is_empty());
            assert_eq!(report.unsupported.len(), 1);
            assert!(
                report.unsupported[0]
                    .reason
                    .contains("query-scoped storage adapter")
            );
        }

        #[test]
        fn non_loopback_http_endpoint_is_rejected() {
            assert!(matches!(
                UnityCatalogClient::new(
                    "http://workspace.example.test/".parse().unwrap(),
                    RefreshingCredentialProvider(AtomicUsize::new(0)),
                ),
                Err(UnityError::InvalidEndpoint(_))
            ));
        }

        #[tokio::test]
        async fn remote_resolution_is_explicitly_unsupported_for_datafusion() {
            let table = ResolvedUnityTable {
                table_id: "id".to_owned(),
                full_name: "main.media.remote".to_owned(),
                table_type: "EXTERNAL".to_owned(),
                storage_location: Some("s3://bucket/table".to_owned()),
                data_source_format: Some("PARQUET".to_owned()),
                properties: BTreeMap::new(),
            };
            let result = register_resolved_table(&MediaSession::new(), "remote", &table)
                .await
                .unwrap();
            assert!(matches!(result, UnityTableRegistration::Unsupported { .. }));
        }

        #[tokio::test]
        async fn mock_unity_resolution_registers_a_local_governed_parquet_table() {
            let root = PathBuf::from("target")
                .join("lakeprism-unity-tests")
                .join(Uuid::new_v4().to_string());
            fs::create_dir_all(&root).unwrap();
            let parquet_path = root.join("table.parquet");
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![7_i64]))],
            )
            .unwrap();
            let mut writer =
                ArrowWriter::try_new(File::create(&parquet_path).unwrap(), schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            let server = MockServer::start().await;
            let location = Url::from_file_path(fs::canonicalize(&parquet_path).unwrap())
                .unwrap()
                .to_string();
            Mock::given(method("GET"))
                .and(path("/api/2.1/unity-catalog/tables/main.media.local"))
                .and(header("authorization", "Bearer fresh-token-0"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "table_id": "local-id",
                    "full_name": "main.media.local",
                    "table_type": "MANAGED",
                    "storage_location": location,
                    "data_source_format": "PARQUET"
                })))
                .mount(&server)
                .await;
            let client = UnityCatalogClient::new(
                server.uri().parse().unwrap(),
                RefreshingCredentialProvider(AtomicUsize::new(0)),
            )
            .unwrap();
            let session = MediaSession::new();
            assert!(matches!(
                client
                    .resolve_and_register_table(
                        "main.media.local",
                        "local_media",
                        &context(),
                        &session
                    )
                    .await
                    .unwrap(),
                UnityTableRegistration::RegisteredLocalParquet { .. }
            ));
            let output = session
                .collect("SELECT value FROM local_media")
                .await
                .unwrap();
            assert_eq!(
                output[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                7
            );
            fs::remove_dir_all(root).unwrap();
        }

        #[tokio::test]
        async fn file_provider_preserves_managed_access_without_credentials() {
            let media = resolve_databricks_file(&MockFileProvider, "main.media.clip", &context())
                .await
                .unwrap();
            assert_eq!(media.storage_mode, StorageMode::Managed);
            assert_eq!(media.catalog_ref.as_deref(), Some("main.media.clip"));
        }

        #[cfg(feature = "iceberg-rest")]
        #[test]
        fn maps_only_explicit_unity_iceberg_rest_metadata() {
            let mapping = UnityIcebergRestMapping::from_resolved(&ResolvedUnityTable {
                table_id: "iceberg-id".to_owned(),
                full_name: "main.media.frames".to_owned(),
                table_type: "MANAGED".to_owned(),
                storage_location: None,
                data_source_format: Some("ICEBERG".to_owned()),
                properties: BTreeMap::from([
                    (
                        "iceberg.rest.uri".to_owned(),
                        "https://catalog.example.test/".to_owned(),
                    ),
                    ("iceberg.rest.prefix".to_owned(), "unity".to_owned()),
                ]),
            })
            .unwrap();
            assert_eq!(mapping.namespace, vec!["main", "media"]);
            assert_eq!(mapping.table_name, "frames");
            assert_eq!(mapping.prefix.as_deref(), Some("unity"));
        }

        #[cfg(feature = "iceberg-rest")]
        #[test]
        fn refuses_to_guess_a_rest_endpoint_for_unity_iceberg() {
            assert!(matches!(
                UnityIcebergRestMapping::from_resolved(&ResolvedUnityTable {
                    table_id: "iceberg-id".to_owned(),
                    full_name: "main.media.frames".to_owned(),
                    table_type: "MANAGED".to_owned(),
                    storage_location: Some("s3://bucket/frames".to_owned()),
                    data_source_format: Some("ICEBERG".to_owned()),
                    properties: BTreeMap::new(),
                }),
                Err(UnityError::UnsupportedTable(_))
            ));
        }

        #[test]
        fn registration_is_durable_and_rejects_credentials() {
            let root = PathBuf::from("target")
                .join("lakeprism-unity-tests")
                .join(Uuid::new_v4().to_string());
            let endpoint: Url = "https://workspace.example.test/".parse().unwrap();
            let table = ResolvedUnityTable {
                table_id: "id-1".to_owned(),
                full_name: "main.media.local".to_owned(),
                table_type: "MANAGED".to_owned(),
                storage_location: Some("file:///data/media.parquet".to_owned()),
                data_source_format: Some("PARQUET".to_owned()),
                properties: BTreeMap::new(),
            };
            let registration = UnityCatalogRegistration::from_resolved(&endpoint, &table).unwrap();
            let mut registry = UnityCatalogRegistry::open(&root).unwrap();
            registry.register(registration).unwrap();
            drop(registry);
            let registry = UnityCatalogRegistry::open(&root).unwrap();
            assert_eq!(registry.registrations().count(), 1);
            assert!(
                !fs::read_to_string(root.join(REGISTRY_FILE))
                    .unwrap()
                    .contains("fresh-token")
            );
            fs::remove_dir_all(root).unwrap();
        }
    }
}

#[cfg(feature = "unity")]
pub use enabled::*;

#[cfg(test)]
mod file_tests {
    use super::*;

    #[test]
    fn file_adapter_preserves_portable_media_fields() {
        let media = MediaRef::try_from(DatabricksFile {
            uri: "file:///media/video.mp4".to_owned(),
            media_type: "video".to_owned(),
            mime_type: Some("video/mp4".to_owned()),
            size_bytes: Some(1024),
            etag: Some("etag-1".to_owned()),
            catalog_ref: Some("main.media.videos.video".to_owned()),
            storage_mode: StorageMode::Managed,
        })
        .unwrap();

        assert_eq!(
            media.catalog_ref.as_deref(),
            Some("main.media.videos.video")
        );
        assert_eq!(media.storage_mode, StorageMode::Managed);
    }

    #[test]
    fn volume_path_maps_to_a_credential_free_managed_media_ref() {
        let volume = UnityVolumePath::parse("/Volumes/main/media/raw/clip.mp4").unwrap();
        let media = volume.to_media_ref("video").unwrap();
        assert_eq!(media.uri, "unity-volume://main/media/raw/clip.mp4");
        assert_eq!(media.catalog_ref.as_deref(), Some("main.media.raw"));
        assert_eq!(media.storage_mode, StorageMode::Managed);
        assert!(UnityVolumePath::parse("/Volumes/main/media/raw/../clip.mp4").is_err());
    }
}
