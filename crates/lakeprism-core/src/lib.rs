use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LakePrismError {
    #[error("media URI must be absolute: {0}")]
    RelativeMediaUri(String),
    #[error("media URI must not contain credentials or signed-access parameters: {0}")]
    CredentialBearingMediaUri(String),
    #[error("media type must not be empty")]
    EmptyMediaType,
    #[error("requested byte range is invalid")]
    InvalidByteRange,
    #[error("requested byte range ends beyond the source length")]
    ByteRangeExceedsSource,
    #[error("resource byte budget must be greater than zero")]
    ZeroByteBudget,
}

pub type Result<T> = std::result::Result<T, LakePrismError>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StorageMode {
    External,
    Managed,
    Inline,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaRef {
    pub uri: String,
    pub media_type: String,
    pub mime_type: Option<String>,
    pub size_bytes: Option<u64>,
    pub etag: Option<String>,
    pub modified_at_unix_millis: Option<i64>,
    pub catalog_ref: Option<String>,
    pub storage_mode: StorageMode,
    pub metadata: BTreeMap<String, String>,
}

impl MediaRef {
    pub fn new(
        uri: impl Into<String>,
        media_type: impl Into<String>,
        storage_mode: StorageMode,
    ) -> Result<Self> {
        let uri = uri.into();
        validate_media_uri(&uri)?;
        let media_type = media_type.into();
        if media_type.trim().is_empty() {
            return Err(LakePrismError::EmptyMediaType);
        }

        Ok(Self {
            uri,
            media_type,
            mime_type: None,
            size_bytes: None,
            etag: None,
            modified_at_unix_millis: None,
            catalog_ref: None,
            storage_mode,
            metadata: BTreeMap::new(),
        })
    }
}

fn validate_media_uri(uri: &str) -> Result<()> {
    let parsed = Url::parse(uri).map_err(|_| LakePrismError::RelativeMediaUri(uri.to_owned()))?;
    let has_sensitive_query_parameter = parsed.query_pairs().any(|(name, _)| {
        let normalized = name.to_ascii_lowercase();
        matches!(
            normalized.as_str(),
            "signature" | "sig" | "token" | "access_token" | "x-amz-signature" | "x-goog-signature"
        )
    });

    if !parsed.username().is_empty() || parsed.password().is_some() || has_sensitive_query_parameter
    {
        return Err(LakePrismError::CredentialBearingMediaUri(uri.to_owned()));
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AccessContext {
    pub principal: String,
    pub catalog_identity: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceIdentity {
    pub media_uri: String,
    pub source_version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FeatureLineage {
    pub source: SourceIdentity,
    pub operator_version: String,
    pub model: Option<String>,
    pub model_version: Option<String>,
    pub parameters: BTreeMap<String, String>,
}

impl FeatureLineage {
    pub fn is_compatible_with(&self, request: &FeatureLineage) -> bool {
        self.source == request.source
            && self.operator_version == request.operator_version
            && self.model == request.model
            && self.model_version == request.model_version
            && self.parameters == request.parameters
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MediaProjection {
    pub include_payload_bytes: bool,
    pub include_caption: bool,
    pub include_embedding: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MediaConstraints {
    pub time_range_millis: Option<RangeInclusive<u64>>,
    pub page_range: Option<RangeInclusive<u32>>,
    pub streams: Vec<String>,
    pub projection: MediaProjection,
    pub limit_hint: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteRange {
    pub start: u64,
    pub end_exclusive: u64,
}

impl ByteRange {
    pub fn new(start: u64, end_exclusive: u64, source_length: u64) -> Result<Self> {
        if start >= end_exclusive {
            return Err(LakePrismError::InvalidByteRange);
        }
        if end_exclusive > source_length {
            return Err(LakePrismError::ByteRangeExceedsSource);
        }
        Ok(Self {
            start,
            end_exclusive,
        })
    }

    pub fn len(&self) -> u64 {
        self.end_exclusive - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end_exclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_reference_rejects_signed_urls() {
        let result = MediaRef::new(
            "https://example.test/video.mp4?X-Amz-Signature=secret",
            "video",
            StorageMode::External,
        );

        assert!(matches!(
            result,
            Err(LakePrismError::CredentialBearingMediaUri(_))
        ));
    }

    #[test]
    fn feature_lineage_requires_exact_compatibility() {
        let source = SourceIdentity {
            media_uri: "file:///media/video.mp4".to_owned(),
            source_version: "etag-1".to_owned(),
        };
        let lineage = FeatureLineage {
            source: source.clone(),
            operator_version: "frames-v1".to_owned(),
            model: None,
            model_version: None,
            parameters: BTreeMap::new(),
        };
        let changed_source = FeatureLineage {
            source: SourceIdentity {
                source_version: "etag-2".to_owned(),
                ..source
            },
            ..lineage.clone()
        };

        assert!(lineage.is_compatible_with(&lineage));
        assert!(!lineage.is_compatible_with(&changed_source));
    }

    #[test]
    fn byte_range_must_be_non_empty_and_bounded() {
        assert_eq!(ByteRange::new(2, 5, 8).unwrap().len(), 3);
        assert_eq!(
            ByteRange::new(5, 5, 8),
            Err(LakePrismError::InvalidByteRange)
        );
        assert_eq!(
            ByteRange::new(2, 9, 8),
            Err(LakePrismError::ByteRangeExceedsSource)
        );
    }
}
