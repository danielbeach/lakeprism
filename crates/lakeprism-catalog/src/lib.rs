//! A small, durable, local-only catalog for credential-free media references.
//!
//! The on-disk format is intentionally private and versioned. Mutations take
//! an advisory cross-process lock and replace the catalog atomically, so a
//! restart observes either the old complete catalog or the new complete catalog.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use lakeprism_core::{FeatureLineage, MediaRef};
use lakeprism_datafusion::MediaSession;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const CATALOG_FILE: &str = "catalog.json";
const LOCK_FILE: &str = "catalog.lock";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("table does not exist: {0}")]
    TableNotFound(String),
    #[error("invalid local catalog identifier: {0}")]
    InvalidIdentifier(String),
    #[error("unsupported local catalog DDL: {0}")]
    UnsupportedDdl(String),
    #[error("local catalog has an unsupported format version: {0}")]
    UnsupportedFormat(u32),
    #[error("local catalog is corrupt: {0}")]
    Corrupt(String),
    #[error("local catalog path is unsafe: {0}")]
    UnsafePath(String),
    #[error("catalog metadata must not contain credentials: {0}")]
    CredentialPersistenceDenied(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogDdlResult {
    CreatedTable(String),
    DroppedTable(String),
    Tables(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalTableMetadata {
    pub name: String,
    pub media_count: usize,
    pub lineage: Option<FeatureLineage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CatalogTable {
    media_refs: Vec<MediaRef>,
    lineage: Option<FeatureLineage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedCatalog {
    format_version: u32,
    tables: BTreeMap<String, CatalogTable>,
}

impl Default for PersistedCatalog {
    fn default() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            tables: BTreeMap::new(),
        }
    }
}

/// An in-memory catalog, optionally backed by one safe local directory.
///
/// [`LocalCatalog::open`] enables persistence. [`Default`] remains useful for
/// short-lived embedded callers and intentionally performs no filesystem I/O.
#[derive(Default)]
pub struct LocalCatalog {
    catalog: PersistedCatalog,
    root: Option<PathBuf>,
}

impl LocalCatalog {
    /// Opens a persistent catalog rooted in `root`.
    ///
    /// The supplied directory is created if absent. Catalog filenames are fixed
    /// constants; callers cannot inject an identifier into a filesystem path.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let root = fs::canonicalize(root)?;
        if !root.is_dir() {
            return Err(CatalogError::UnsafePath(root.display().to_string()));
        }
        let catalog = with_exclusive_lock(&root, || load_catalog(&root))?;
        Ok(Self {
            catalog,
            root: Some(root),
        })
    }

    pub fn register_media_table(
        &mut self,
        table_name: impl Into<String>,
        media_refs: Vec<MediaRef>,
    ) -> Result<(), CatalogError> {
        self.register_media_table_with_lineage(table_name, media_refs, None)
    }

    pub fn register_media_table_with_lineage(
        &mut self,
        table_name: impl Into<String>,
        media_refs: Vec<MediaRef>,
        lineage: Option<FeatureLineage>,
    ) -> Result<(), CatalogError> {
        let table_name = validate_identifier(table_name.into())?;
        validate_table_contents(&media_refs, lineage.as_ref())?;
        self.mutate(move |catalog| {
            if catalog.tables.contains_key(&table_name) {
                return Err(CatalogError::TableAlreadyExists(table_name));
            }
            catalog.tables.insert(
                table_name,
                CatalogTable {
                    media_refs,
                    lineage,
                },
            );
            Ok(())
        })
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        let table_name = validate_identifier(table_name.to_owned())?;
        self.mutate(move |catalog| {
            if catalog.tables.remove(&table_name).is_none() {
                return Err(CatalogError::TableNotFound(table_name));
            }
            Ok(())
        })
    }

    /// Executes the deliberately small local catalog DDL surface:
    /// `CREATE MEDIA TABLE name`, `DROP TABLE name`, or `SHOW TABLES`.
    pub fn execute_ddl(&mut self, statement: &str) -> Result<CatalogDdlResult, CatalogError> {
        let normalized = statement.trim().trim_end_matches(';').trim();
        let words = normalized.split_whitespace().collect::<Vec<_>>();
        match words.as_slice() {
            ["CREATE", "MEDIA", "TABLE", name] => {
                self.register_media_table(*name, Vec::new())?;
                Ok(CatalogDdlResult::CreatedTable((*name).to_owned()))
            }
            ["DROP", "TABLE", name] => {
                self.drop_table(name)?;
                Ok(CatalogDdlResult::DroppedTable((*name).to_owned()))
            }
            ["SHOW", "TABLES"] => Ok(CatalogDdlResult::Tables(
                self.table_names().map(str::to_owned).collect(),
            )),
            _ => Err(CatalogError::UnsupportedDdl(normalized.to_owned())),
        }
    }

    pub fn media_table(&self, table_name: &str) -> Result<&[MediaRef], CatalogError> {
        let table_name = validate_identifier(table_name.to_owned())?;
        self.catalog
            .tables
            .get(&table_name)
            .map(|table| table.media_refs.as_slice())
            .ok_or(CatalogError::TableNotFound(table_name))
    }

    pub fn table_metadata(&self, table_name: &str) -> Result<LocalTableMetadata, CatalogError> {
        let table_name = validate_identifier(table_name.to_owned())?;
        let table = self
            .catalog
            .tables
            .get(&table_name)
            .ok_or_else(|| CatalogError::TableNotFound(table_name.clone()))?;
        Ok(LocalTableMetadata {
            name: table_name,
            media_count: table.media_refs.len(),
            lineage: table.lineage.clone(),
        })
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.catalog.tables.keys().map(String::as_str)
    }

    /// Reloads the most recently committed catalog state from disk.
    pub fn refresh(&mut self) -> Result<(), CatalogError> {
        if let Some(root) = &self.root {
            self.catalog = with_exclusive_lock(root, || load_catalog(root))?;
        }
        Ok(())
    }

    pub async fn register_in_session(
        &self,
        session: &MediaSession,
    ) -> datafusion::error::Result<()> {
        for (table_name, table) in &self.catalog.tables {
            session
                .register_media_refs(table_name, &table.media_refs)
                .await?;
        }
        Ok(())
    }

    fn mutate(
        &mut self,
        change: impl FnOnce(&mut PersistedCatalog) -> Result<(), CatalogError>,
    ) -> Result<(), CatalogError> {
        let Some(root) = &self.root else {
            change(&mut self.catalog)?;
            return Ok(());
        };
        let updated = with_exclusive_lock(root, || {
            let mut catalog = load_catalog(root)?;
            change(&mut catalog)?;
            persist_catalog(root, &catalog)?;
            Ok(catalog)
        })?;
        self.catalog = updated;
        Ok(())
    }
}

fn validate_identifier(identifier: String) -> Result<String, CatalogError> {
    let mut chars = identifier.chars();
    let valid = matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_');
    if valid {
        Ok(identifier)
    } else {
        Err(CatalogError::InvalidIdentifier(identifier))
    }
}

fn validate_table_contents(
    media_refs: &[MediaRef],
    lineage: Option<&FeatureLineage>,
) -> Result<(), CatalogError> {
    for media in media_refs {
        MediaRef::new(
            media.uri.clone(),
            media.media_type.clone(),
            media.storage_mode.clone(),
        )
        .map_err(|error| CatalogError::Corrupt(error.to_string()))?;
        if media.catalog_ref.as_deref().is_some_and(looks_sensitive)
            || media
                .metadata
                .iter()
                .any(|(key, value)| looks_sensitive(key) || looks_sensitive(value))
        {
            return Err(CatalogError::CredentialPersistenceDenied(media.uri.clone()));
        }
    }
    if lineage.is_some_and(|lineage| {
        looks_sensitive(&lineage.source.media_uri)
            || looks_sensitive(&lineage.source.source_version)
            || looks_sensitive(&lineage.operator_version)
            || lineage.model.as_deref().is_some_and(looks_sensitive)
            || lineage
                .model_version
                .as_deref()
                .is_some_and(looks_sensitive)
            || lineage
                .parameters
                .iter()
                .any(|(key, value)| looks_sensitive(key) || looks_sensitive(value))
    }) {
        return Err(CatalogError::CredentialPersistenceDenied(
            "lineage".to_owned(),
        ));
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
        "api-key",
        "token",
        "bearer ",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn with_exclusive_lock<T>(
    root: &Path,
    operation: impl FnOnce() -> Result<T, CatalogError>,
) -> Result<T, CatalogError> {
    let lock_path = root.join(LOCK_FILE);
    if lock_path.is_symlink() {
        return Err(CatalogError::UnsafePath(lock_path.display().to_string()));
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    lock.lock_exclusive()?;
    let result = operation();
    FileExt::unlock(&lock)?;
    result
}

fn load_catalog(root: &Path) -> Result<PersistedCatalog, CatalogError> {
    let path = root.join(CATALOG_FILE);
    if path.is_symlink() {
        return Err(CatalogError::UnsafePath(path.display().to_string()));
    }
    if !path.exists() {
        return Ok(PersistedCatalog::default());
    }
    let bytes = fs::read(&path)?;
    let catalog: PersistedCatalog = serde_json::from_slice(&bytes)?;
    if catalog.format_version != FORMAT_VERSION {
        return Err(CatalogError::UnsupportedFormat(catalog.format_version));
    }
    for (name, table) in &catalog.tables {
        validate_identifier(name.clone())?;
        validate_table_contents(&table.media_refs, table.lineage.as_ref())?;
    }
    Ok(catalog)
}

fn persist_catalog(root: &Path, catalog: &PersistedCatalog) -> Result<(), CatalogError> {
    let payload = serde_json::to_vec_pretty(catalog)?;
    let destination = root.join(CATALOG_FILE);
    let temporary = root.join(format!(".catalog-{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&payload)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, &destination)?;
    File::open(root)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakeprism_core::{SourceIdentity, StorageMode};
    use std::sync::{Arc, Barrier};

    fn catalog_root() -> PathBuf {
        let root = PathBuf::from("target")
            .join("lakeprism-catalog-tests")
            .join(Uuid::new_v4().to_string());
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn media(uri: &str) -> MediaRef {
        MediaRef::new(uri, "video", StorageMode::External).unwrap()
    }

    #[tokio::test]
    async fn durable_catalog_recovers_metadata_lineage_and_session_tables() {
        let root = catalog_root();
        let lineage = FeatureLineage {
            source: SourceIdentity {
                media_uri: "file:///media/a.mp4".to_owned(),
                source_version: "v1".to_owned(),
            },
            operator_version: "ingest-v1".to_owned(),
            model: None,
            model_version: None,
            parameters: BTreeMap::new(),
        };
        let mut catalog = LocalCatalog::open(&root).unwrap();
        catalog
            .register_media_table_with_lineage(
                "videos",
                vec![media("file:///media/a.mp4")],
                Some(lineage.clone()),
            )
            .unwrap();
        drop(catalog);

        let catalog = LocalCatalog::open(&root).unwrap();
        assert_eq!(catalog.table_names().collect::<Vec<_>>(), vec!["videos"]);
        assert_eq!(
            catalog.table_metadata("videos").unwrap().lineage,
            Some(lineage)
        );
        let session = MediaSession::new();
        catalog.register_in_session(&session).await.unwrap();
        assert_eq!(
            session.collect("SELECT * FROM videos").await.unwrap()[0].num_rows(),
            1
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ddl_discovery_is_small_and_identifiers_are_safe() {
        let mut catalog = LocalCatalog::default();
        assert_eq!(
            catalog.execute_ddl("CREATE MEDIA TABLE videos;").unwrap(),
            CatalogDdlResult::CreatedTable("videos".to_owned())
        );
        assert_eq!(
            catalog.execute_ddl("SHOW TABLES").unwrap(),
            CatalogDdlResult::Tables(vec!["videos".to_owned()])
        );
        assert!(matches!(
            catalog.execute_ddl("CREATE MEDIA TABLE ../escape"),
            Err(CatalogError::InvalidIdentifier(_))
        ));
    }

    #[test]
    fn concurrent_process_style_mutations_preserve_every_table() {
        let root = catalog_root();
        let barrier = Arc::new(Barrier::new(2));
        let mut threads = Vec::new();
        for index in 0..2 {
            let root = root.clone();
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                let mut catalog = LocalCatalog::open(root).unwrap();
                barrier.wait();
                catalog
                    .register_media_table(format!("video_{index}"), Vec::new())
                    .unwrap();
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let catalog = LocalCatalog::open(&root).unwrap();
        assert_eq!(
            catalog.table_names().collect::<Vec<_>>(),
            vec!["video_0", "video_1"]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_metadata_that_could_persist_a_credential() {
        let mut catalog = LocalCatalog::default();
        let mut media = media("file:///media/a.mp4");
        media.metadata.insert(
            "authorization".to_owned(),
            "Bearer not-persisted".to_owned(),
        );
        assert!(matches!(
            catalog.register_media_table("videos", vec![media]),
            Err(CatalogError::CredentialPersistenceDenied(_))
        ));
    }
}
