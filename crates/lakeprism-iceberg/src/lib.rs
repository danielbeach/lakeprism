#[cfg(feature = "iceberg-rust")]
pub type IcebergAdapterResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(feature = "iceberg-rust")]
pub async fn register_iceberg_table(
    session: &lakeprism_datafusion::MediaSession,
    table_name: &str,
    table: iceberg::table::Table,
    snapshot_id: Option<i64>,
) -> IcebergAdapterResult<()> {
    let provider = match snapshot_id {
        Some(snapshot_id) => {
            iceberg_datafusion::table::IcebergStaticTableProvider::try_new_from_table_snapshot(
                table,
                snapshot_id,
            )
            .await?
        }
        None => {
            iceberg_datafusion::table::IcebergStaticTableProvider::try_new_from_table(table).await?
        }
    };
    session.register_table_provider(table_name, std::sync::Arc::new(provider))?;
    Ok(())
}

/// Apache Iceberg REST Catalog support.
///
/// This module is deliberately opt-in because the Arrow 59-compatible REST
/// implementation is an upstream adapter. It resolves catalog metadata through
/// the standard REST protocol and lets the matching Iceberg DataFusion provider
/// perform scans; it never substitutes a local or in-memory remote scan.
#[cfg(feature = "iceberg-rest")]
pub mod rest {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
    use lakeprism_core::AccessContext;
    use lakeprism_datafusion::MediaSession;
    use reqwest::Url;
    use thiserror::Error;

    /// Credential-free REST catalog configuration.
    ///
    /// `properties` is restricted to non-secret Iceberg REST properties, such
    /// as `prefix`. Authentication belongs in a request-scoped client provider.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RestCatalogConfig {
        pub name: String,
        pub uri: Url,
        pub warehouse: Option<String>,
        pub properties: HashMap<String, String>,
    }

    impl RestCatalogConfig {
        pub fn new(
            name: impl Into<String>,
            uri: Url,
            warehouse: Option<String>,
            properties: HashMap<String, String>,
        ) -> Result<Self, RestCatalogError> {
            let config = Self {
                name: name.into(),
                uri,
                warehouse,
                properties,
            };
            config.validate()?;
            Ok(config)
        }

        fn validate(&self) -> Result<(), RestCatalogError> {
            if self.name.trim().is_empty() {
                return Err(RestCatalogError::InvalidConfig(
                    "catalog name must not be empty".to_owned(),
                ));
            }
            if self.uri.username() != ""
                || self.uri.password().is_some()
                || self.uri.query().is_some()
            {
                return Err(RestCatalogError::SensitiveConfig(
                    "catalog URI contains userinfo or query parameters".to_owned(),
                ));
            }
            if self.warehouse.as_deref().is_some_and(looks_sensitive)
                || self.properties.iter().any(|(key, value)| {
                    looks_sensitive(key)
                        || looks_sensitive(value)
                        || !matches!(key.as_str(), "prefix")
                })
            {
                return Err(RestCatalogError::SensitiveConfig(
                    "only non-secret REST property `prefix` is accepted".to_owned(),
                ));
            }
            Ok(())
        }

        fn catalog_properties(&self) -> HashMap<String, String> {
            let mut properties = self.properties.clone();
            // iceberg-catalog-rest 0.9.x appends `/v1` with string joining.
            // Normalized URLs retain a trailing slash, which would otherwise
            // produce `//v1` and fail against standards-compliant servers.
            properties.insert(
                "uri".to_owned(),
                self.uri.as_str().trim_end_matches('/').to_owned(),
            );
            if let Some(warehouse) = &self.warehouse {
                properties.insert("warehouse".to_owned(), warehouse.clone());
            }
            properties
        }
    }

    /// Creates a fresh HTTP client for exactly one catalog-registration scope.
    ///
    /// Implementations may attach a request-local authorization mechanism, but
    /// must not return its credential to LakePrism or persist it in the config.
    #[async_trait]
    pub trait RestCatalogClientProvider: Send + Sync {
        async fn client_for(
            &self,
            access: &AccessContext,
        ) -> Result<reqwest::Client, RestCatalogError>;
    }

    #[derive(Debug, Error)]
    pub enum RestCatalogError {
        #[error("invalid Iceberg REST catalog configuration: {0}")]
        InvalidConfig(String),
        #[error("Iceberg REST catalog configuration contains credentials: {0}")]
        SensitiveConfig(String),
        #[error("invalid Iceberg identifier: {0}")]
        InvalidIdentifier(String),
        #[error("Iceberg REST catalog error: {0}")]
        Upstream(String),
        #[error(transparent)]
        DataFusion(#[from] datafusion::error::DataFusionError),
    }

    /// A catalog resolved for one caller's access context.
    ///
    /// The REST client is held only by upstream Iceberg catalog state. This
    /// object contains neither credentials nor a serializable credential field.
    #[derive(Clone)]
    pub struct ScopedRestCatalog {
        catalog: Arc<dyn Catalog>,
        config: RestCatalogConfig,
        access: AccessContext,
    }

    impl std::fmt::Debug for ScopedRestCatalog {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("ScopedRestCatalog")
                .field("catalog_name", &self.config.name)
                .field("uri", &self.config.uri)
                .field("access", &self.access)
                .finish_non_exhaustive()
        }
    }

    impl ScopedRestCatalog {
        pub async fn open<P: RestCatalogClientProvider>(
            config: RestCatalogConfig,
            access: AccessContext,
            clients: &P,
        ) -> Result<Self, RestCatalogError> {
            let client = clients.client_for(&access).await?;
            Self::open_with_client(config, access, client).await
        }

        /// Opens a scope using a client constructed by an application boundary.
        /// This permits integrations such as Unity to vend an authorization
        /// header without exporting the bearer token to LakePrism.
        pub async fn open_with_client(
            config: RestCatalogConfig,
            access: AccessContext,
            client: reqwest::Client,
        ) -> Result<Self, RestCatalogError> {
            let catalog = iceberg_catalog_rest::RestCatalogBuilder::default()
                .with_client(client)
                .load(config.name.clone(), config.catalog_properties())
                .await
                .map_err(upstream)?;
            Ok(Self {
                catalog: Arc::new(catalog),
                config,
                access,
            })
        }

        pub fn config(&self) -> &RestCatalogConfig {
            &self.config
        }

        pub fn access_context(&self) -> &AccessContext {
            &self.access
        }

        /// Registers top-level REST namespaces as DataFusion schemas.
        ///
        /// The compatible upstream `IcebergCatalogProvider` flattens a
        /// namespace identifier into a schema name, so nested namespaces are
        /// not safely representable through this catalog bridge. Register a
        /// nested namespace table explicitly with `register_table` instead.
        pub async fn register_catalog(
            &self,
            session: &MediaSession,
            datafusion_catalog: &str,
        ) -> Result<(), RestCatalogError> {
            let namespaces = self.catalog.list_namespaces(None).await.map_err(upstream)?;
            if namespaces
                .iter()
                .any(|namespace| namespace.as_ref().len() != 1)
            {
                return Err(RestCatalogError::InvalidIdentifier(
                    "nested REST namespaces require explicit table registration".to_owned(),
                ));
            }
            let provider =
                iceberg_datafusion::IcebergCatalogProvider::try_new(Arc::clone(&self.catalog))
                    .await
                    .map_err(upstream)?;
            session.register_catalog_provider(datafusion_catalog, Arc::new(provider))?;
            Ok(())
        }

        /// Resolves an Iceberg table through the REST catalog and registers a
        /// snapshot-pinned or current lazy DataFusion provider.
        pub async fn register_table(
            &self,
            session: &MediaSession,
            datafusion_table: &str,
            namespace: &[String],
            table_name: &str,
            snapshot_id: Option<i64>,
        ) -> Result<(), RestCatalogError> {
            if namespace.is_empty()
                || namespace.iter().any(|part| part.is_empty())
                || table_name.is_empty()
            {
                return Err(RestCatalogError::InvalidIdentifier(
                    "namespace and table name must not be empty".to_owned(),
                ));
            }
            let identifier = TableIdent::new(
                NamespaceIdent::from_vec(namespace.to_vec()).map_err(upstream)?,
                table_name.to_owned(),
            );
            let table = self
                .catalog
                .load_table(&identifier)
                .await
                .map_err(upstream)?;
            super::register_iceberg_table(session, datafusion_table, table, snapshot_id)
                .await
                .map_err(|error| RestCatalogError::Upstream(error.to_string()))
        }
    }

    fn upstream(error: iceberg::Error) -> RestCatalogError {
        RestCatalogError::Upstream(error.to_string())
    }

    fn looks_sensitive(value: &str) -> bool {
        let value = value.to_ascii_lowercase();
        [
            "authorization",
            "credential",
            "password",
            "secret",
            "access_token",
            "api_key",
            "token",
            "bearer ",
            "signature",
        ]
        .iter()
        .any(|needle| value.contains(needle))
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Mutex;

        use reqwest::header::{HeaderMap, HeaderValue};
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::*;

        #[derive(Default)]
        struct ContextClientProvider(Mutex<Vec<AccessContext>>);

        #[async_trait]
        impl RestCatalogClientProvider for ContextClientProvider {
            async fn client_for(
                &self,
                access: &AccessContext,
            ) -> Result<reqwest::Client, RestCatalogError> {
                self.0.lock().unwrap().push(access.clone());
                let mut headers = HeaderMap::new();
                headers.insert(
                    "x-lakeprism-principal",
                    HeaderValue::from_str(&access.principal)
                        .map_err(|error| RestCatalogError::InvalidConfig(error.to_string()))?,
                );
                reqwest::Client::builder()
                    .default_headers(headers)
                    .build()
                    .map_err(|error| RestCatalogError::Upstream(error.to_string()))
            }
        }

        #[tokio::test]
        async fn uses_an_access_scoped_client_for_rest_namespace_resolution() {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v1/config"))
                .and(header("x-lakeprism-principal", "analyst@example.test"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "defaults": {},
                    "overrides": {}
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v1/namespaces"))
                .and(header("x-lakeprism-principal", "analyst@example.test"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "namespaces": [["media"]]
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v1/namespaces/media/tables"))
                .and(header("x-lakeprism-principal", "analyst@example.test"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "identifiers": []
                })))
                .mount(&server)
                .await;

            let provider = ContextClientProvider::default();
            let catalog = ScopedRestCatalog::open(
                RestCatalogConfig::new("rest", server.uri().parse().unwrap(), None, HashMap::new())
                    .unwrap(),
                AccessContext {
                    principal: "analyst@example.test".to_owned(),
                    catalog_identity: Some("rest-prod".to_owned()),
                },
                &provider,
            )
            .await
            .unwrap();
            assert_eq!(
                catalog.catalog.list_namespaces(None).await.unwrap(),
                vec![NamespaceIdent::new("media".to_owned())]
            );
            let session = MediaSession::new();
            catalog.register_catalog(&session, "rest").await.unwrap();
            assert!(
                session
                    .schema_names()
                    .iter()
                    .any(|(catalog_name, schema_name)| {
                        catalog_name == "rest" && schema_name == "media"
                    })
            );
            assert_eq!(provider.0.lock().unwrap().len(), 1);
        }

        #[test]
        fn rejects_secret_bearing_rest_config() {
            assert!(matches!(
                RestCatalogConfig::new(
                    "rest",
                    "https://catalog.example.test/?token=nope".parse().unwrap(),
                    None,
                    HashMap::new(),
                ),
                Err(RestCatalogError::SensitiveConfig(_))
            ));
            assert!(matches!(
                RestCatalogConfig::new(
                    "rest",
                    "https://catalog.example.test/".parse().unwrap(),
                    None,
                    HashMap::from([("token".to_owned(), "nope".to_owned())]),
                ),
                Err(RestCatalogError::SensitiveConfig(_))
            ));
        }
    }
}
