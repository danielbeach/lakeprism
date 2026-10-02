//! Governed transcript and embedding contracts plus deterministic local indexes.
//!
//! Providers are contracts, not model runtimes. Production model execution is
//! deliberately unavailable in this crate; tests and local development can opt
//! into explicitly named deterministic mock providers.

use std::collections::HashMap;
use std::sync::Arc;

use lakeprism_core::{FeatureLineage, MediaRef};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_TRANSCRIPT_SEGMENTS: usize = 1_024;
pub const MAX_EMBEDDING_DIMENSIONS: usize = 4_096;
pub const MAX_RANKING_CANDIDATES: usize = 10_000;
pub const MAX_OCR_INPUT_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_OCR_RESULTS: usize = 1_024;
pub const MAX_IMAGE_TAGS: usize = 64;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptSegment {
    pub media_id: String,
    pub start_millis: u64,
    pub end_millis: u64,
    pub text: String,
    pub confidence_millis: Option<u16>,
    pub lineage: FeatureLineage,
    pub created_at_unix_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptionRequest {
    pub media: MediaRef,
    pub lineage: FeatureLineage,
    pub start_millis: u64,
    pub end_millis: u64,
    pub max_segments: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EmbeddingRecord {
    /// Stable caller-defined segment/document identifier.
    pub id: String,
    pub text: String,
    pub values: Vec<f32>,
    pub lineage: FeatureLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddingRequest {
    pub text: String,
    pub lineage: FeatureLineage,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RankingSemantics {
    /// Only a bounded candidate subset was scored. Results can omit better rows.
    Approximate,
    /// Every compatible registered candidate was scored and ordered by the
    /// documented score formula.
    Ranked,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RankedResult {
    pub id: String,
    pub text: String,
    pub score: f32,
    pub semantics: RankingSemantics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RankingOptions {
    pub limit: usize,
    /// `None` scores all compatible rows (ranked); `Some(n)` examines at most
    /// n insertion-ordered candidates and is explicitly approximate.
    pub candidate_limit: Option<usize>,
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("a cold transcript extractor returned a segment with incompatible lineage")]
    IncompatibleColdResult,
    #[error("a transcript segment ends before it starts")]
    InvalidTranscriptRange,
    #[error("transcription request range is invalid")]
    InvalidTranscriptionRequest,
    #[error("transcription request exceeds the maximum segment limit")]
    TranscriptLimitExceeded,
    #[error("embedding dimensions must be between 1 and {MAX_EMBEDDING_DIMENSIONS}")]
    InvalidEmbeddingDimensions,
    #[error("embedding rows have incompatible dimensions")]
    IncompatibleEmbeddingDimensions,
    #[error("ranking candidate limit must not exceed {MAX_RANKING_CANDIDATES}")]
    RankingLimitExceeded,
    #[error("embedding index must not exceed {MAX_RANKING_CANDIDATES} rows")]
    EmbeddingIndexCapacityExceeded,
    #[error(
        "actual {capability} execution is unavailable; configure an application-owned provider"
    )]
    ProviderUnavailable { capability: &'static str },
    #[error("application-owned {capability} execution failed: {kind}")]
    ProviderExecution {
        capability: &'static str,
        /// A provider-defined redacted category, never a command line, model
        /// path, media path, model output, or child stderr.
        kind: &'static str,
    },
    #[error("media search request lineage must identify the requested media URI")]
    SearchSourceMismatch,
    #[error("progressive schedule limits must be greater than zero")]
    InvalidScheduleLimits,
    #[error("scheduled work has an empty stable ID")]
    EmptyWorkId,
    #[error("scheduled work has duplicate stable ID {0}")]
    DuplicateWorkId(String),
    #[error("scheduled work {id} exceeds a global limit")]
    WorkExceedsGlobalLimit { id: String },
    #[error("governed media search cannot start because {resource} capacity is exhausted")]
    ResourceExhausted { resource: &'static str },
    #[error("provider input exceeds the {maximum_bytes}-byte limit")]
    ProviderInputTooLarge { maximum_bytes: usize },
    #[error("provider returned more than the {maximum} permitted results")]
    ProviderResultLimitExceeded { maximum: usize },
    #[error("provider returned a result with incompatible lineage")]
    IncompatibleProviderResult,
    #[error("image understanding result contains too many tags")]
    ImageTagLimitExceeded,
}

pub trait TranscriptionProvider: Send + Sync {
    fn transcribe(
        &self,
        request: &TranscriptionRequest,
    ) -> Result<Vec<TranscriptSegment>, IndexError>;
}

/// Explicit non-provider for builds with no linked/local model runtime.
pub struct UnavailableTranscriptionProvider;
impl TranscriptionProvider for UnavailableTranscriptionProvider {
    fn transcribe(&self, _: &TranscriptionRequest) -> Result<Vec<TranscriptSegment>, IndexError> {
        Err(IndexError::ProviderUnavailable {
            capability: "audio transcription",
        })
    }
}

/// Deterministic fixture provider. It never reads audio or claims inference.
#[derive(Default)]
pub struct DeterministicMockTranscriptionProvider {
    rows_by_uri: HashMap<String, Vec<TranscriptSegment>>,
}
impl DeterministicMockTranscriptionProvider {
    pub fn new(rows: impl IntoIterator<Item = TranscriptSegment>) -> Self {
        let mut rows_by_uri: HashMap<String, Vec<TranscriptSegment>> = HashMap::new();
        for row in rows {
            rows_by_uri
                .entry(row.lineage.source.media_uri.clone())
                .or_default()
                .push(row);
        }
        Self { rows_by_uri }
    }
}
impl TranscriptionProvider for DeterministicMockTranscriptionProvider {
    fn transcribe(
        &self,
        request: &TranscriptionRequest,
    ) -> Result<Vec<TranscriptSegment>, IndexError> {
        validate_transcription_request(request)?;
        let rows = self
            .rows_by_uri
            .get(&request.media.uri)
            .into_iter()
            .flatten()
            .filter(|row| row.lineage.is_compatible_with(&request.lineage))
            .filter(|row| {
                row.start_millis >= request.start_millis && row.end_millis <= request.end_millis
            })
            .take(request.max_segments)
            .cloned()
            .collect();
        Ok(rows)
    }
}

pub trait EmbeddingProvider: Send + Sync {
    fn embed(&self, request: &EmbeddingRequest) -> Result<Vec<f32>, IndexError>;
}

pub struct UnavailableEmbeddingProvider;
impl EmbeddingProvider for UnavailableEmbeddingProvider {
    fn embed(&self, _: &EmbeddingRequest) -> Result<Vec<f32>, IndexError> {
        Err(IndexError::ProviderUnavailable {
            capability: "embedding model",
        })
    }
}

/// Deterministic, non-ML hashing fixture for local tests and demos.
#[derive(Clone, Debug)]
pub struct DeterministicMockEmbeddingProvider {
    dimensions: usize,
}
impl DeterministicMockEmbeddingProvider {
    pub fn new(dimensions: usize) -> Result<Self, IndexError> {
        validate_dimensions(dimensions)?;
        Ok(Self { dimensions })
    }
}
impl EmbeddingProvider for DeterministicMockEmbeddingProvider {
    fn embed(&self, request: &EmbeddingRequest) -> Result<Vec<f32>, IndexError> {
        let mut values = vec![0.0; self.dimensions];
        for token in request.text.split_whitespace() {
            let mut hash = 0xcbf29ce484222325_u64;
            for byte in token.to_ascii_lowercase().bytes() {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
            }

            values[(hash as usize) % self.dimensions] += 1.0;
        }
        normalize(&mut values);
        Ok(values)
    }
}

/// Bounded input passed to an application-owned OCR implementation. The
/// caller retains the bytes in memory only for the provider call; providers
/// must not interpret a successful response as evidence of model inference.
#[derive(Clone, Debug)]
pub struct OcrRequest {
    pub bytes: Arc<[u8]>,
    pub media_type: Option<String>,
    pub lineage: FeatureLineage,
    pub max_results: usize,
}

/// Bounded image payload contract shared by metadata and understanding
/// providers. It is an alias because the same source bytes and exact lineage
/// constraints apply, while the provider capability remains distinct.
pub type ImageRequest = OcrRequest;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OcrResult {
    pub ordinal: u32,
    pub text: String,
    pub confidence_millis: Option<u16>,
    pub lineage: FeatureLineage,
}

pub trait OcrProvider: Send + Sync {
    fn recognize(&self, request: &OcrRequest) -> Result<Vec<OcrResult>, IndexError>;
}

/// Safe default used unless an application explicitly installs an OCR engine.
pub struct UnavailableOcrProvider;
impl OcrProvider for UnavailableOcrProvider {
    fn recognize(&self, _: &OcrRequest) -> Result<Vec<OcrResult>, IndexError> {
        Err(IndexError::ProviderUnavailable {
            capability: "document OCR",
        })
    }
}

/// Deterministic fixture OCR provider. It returns only registered fixture
/// rows and never decodes pixels or represents model output.
#[derive(Default)]
pub struct DeterministicMockOcrProvider {
    rows_by_uri: HashMap<String, Vec<OcrResult>>,
}
impl DeterministicMockOcrProvider {
    pub fn new(rows: impl IntoIterator<Item = OcrResult>) -> Self {
        let mut rows_by_uri: HashMap<String, Vec<OcrResult>> = HashMap::new();
        for row in rows {
            rows_by_uri
                .entry(row.lineage.source.media_uri.clone())
                .or_default()
                .push(row);
        }
        Self { rows_by_uri }
    }
}
impl OcrProvider for DeterministicMockOcrProvider {
    fn recognize(&self, request: &OcrRequest) -> Result<Vec<OcrResult>, IndexError> {
        validate_ocr_request(request)?;
        Ok(self
            .rows_by_uri
            .get(&request.lineage.source.media_uri)
            .into_iter()
            .flatten()
            .filter(|row| row.lineage.is_compatible_with(&request.lineage))
            .take(request.max_results)
            .cloned()
            .collect())
    }
}

pub fn run_ocr(
    provider: &dyn OcrProvider,
    request: &OcrRequest,
) -> Result<Vec<OcrResult>, IndexError> {
    validate_ocr_request(request)?;
    let rows = provider.recognize(request)?;
    if rows.len() > request.max_results {
        return Err(IndexError::ProviderResultLimitExceeded {
            maximum: request.max_results,
        });
    }
    if rows
        .iter()
        .any(|row| !row.lineage.is_compatible_with(&request.lineage))
    {
        return Err(IndexError::IncompatibleProviderResult);
    }
    Ok(rows)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageMetadata {
    pub media_type: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub lineage: FeatureLineage,
}

pub trait ImageMetadataProvider: Send + Sync {
    fn inspect(&self, request: &ImageRequest) -> Result<ImageMetadata, IndexError>;
}

pub struct UnavailableImageMetadataProvider;
impl ImageMetadataProvider for UnavailableImageMetadataProvider {
    fn inspect(&self, _: &ImageRequest) -> Result<ImageMetadata, IndexError> {
        Err(IndexError::ProviderUnavailable {
            capability: "image metadata",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageUnderstanding {
    pub caption: Option<String>,
    pub tags: Vec<String>,
    pub lineage: FeatureLineage,
}

pub trait ImageUnderstandingProvider: Send + Sync {
    fn understand(&self, request: &ImageRequest) -> Result<ImageUnderstanding, IndexError>;
}

pub struct UnavailableImageUnderstandingProvider;
impl ImageUnderstandingProvider for UnavailableImageUnderstandingProvider {
    fn understand(&self, _: &ImageRequest) -> Result<ImageUnderstanding, IndexError> {
        Err(IndexError::ProviderUnavailable {
            capability: "image understanding",
        })
    }
}

pub fn run_image_metadata(
    provider: &dyn ImageMetadataProvider,
    request: &ImageRequest,
) -> Result<ImageMetadata, IndexError> {
    validate_ocr_request(request)?;
    let metadata = provider.inspect(request)?;
    if !metadata.lineage.is_compatible_with(&request.lineage) {
        return Err(IndexError::IncompatibleProviderResult);
    }
    Ok(metadata)
}

pub fn run_image_understanding(
    provider: &dyn ImageUnderstandingProvider,
    request: &ImageRequest,
) -> Result<ImageUnderstanding, IndexError> {
    validate_ocr_request(request)?;
    let result = provider.understand(request)?;
    if !result.lineage.is_compatible_with(&request.lineage) {
        return Err(IndexError::IncompatibleProviderResult);
    }
    if result.tags.len() > MAX_IMAGE_TAGS {
        return Err(IndexError::ImageTagLimitExceeded);
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CrossModalKind {
    Transcript,
    Document,
    Image,
    Audio,
    Video,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CrossModalRecord {
    pub id: String,
    pub kind: CrossModalKind,
    pub text: String,
    pub values: Vec<f32>,
    pub lineage: FeatureLineage,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CrossModalResult {
    pub id: String,
    pub kind: CrossModalKind,
    pub text: String,
    pub score: f32,
    pub semantics: RankingSemantics,
}

/// A bounded multimodal index. A query only sees records with an exact source,
/// operator, model, and parameter lineage match; modality never weakens that
/// requirement.
#[derive(Default)]
pub struct CrossModalIndex {
    rows: Vec<CrossModalRecord>,
}
impl CrossModalIndex {
    pub fn insert(&mut self, row: CrossModalRecord) -> Result<(), IndexError> {
        validate_dimensions(row.values.len())?;
        if self.rows.len() == MAX_RANKING_CANDIDATES {
            return Err(IndexError::EmbeddingIndexCapacityExceeded);
        }
        if let Some(existing) = self.rows.first()
            && existing.values.len() != row.values.len()
        {
            return Err(IndexError::IncompatibleEmbeddingDimensions);
        }
        self.rows.push(row);
        Ok(())
    }

    pub fn rank(
        &self,
        query: &[f32],
        lineage: &FeatureLineage,
        options: RankingOptions,
    ) -> Result<Vec<CrossModalResult>, IndexError> {
        validate_dimensions(query.len())?;
        validate_ranking(options)?;
        let semantics = if options.candidate_limit.is_some() {
            RankingSemantics::Approximate
        } else {
            RankingSemantics::Ranked
        };
        let candidates = self
            .rows
            .iter()
            .filter(|row| row.lineage.is_compatible_with(lineage))
            .filter(|row| row.values.len() == query.len());
        let candidates: Box<dyn Iterator<Item = &CrossModalRecord>> = match options.candidate_limit
        {
            Some(limit) => Box::new(candidates.take(limit)),
            None => Box::new(candidates),
        };
        let mut results = candidates
            .map(|row| CrossModalResult {
                id: row.id.clone(),
                kind: row.kind,
                text: row.text.clone(),
                score: cosine(query, &row.values),
                semantics,
            })
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.id.cmp(&right.id))
        });
        results.truncate(options.limit);
        Ok(results)
    }
}

#[derive(Default)]
pub struct TranscriptIndex {
    segments: Vec<TranscriptSegment>,
}
impl TranscriptIndex {
    pub fn insert(&mut self, segment: TranscriptSegment) -> Result<(), IndexError> {
        validate_transcript_range(&segment)?;
        self.segments.push(segment);
        Ok(())
    }

    pub fn has_compatible_lineage(&self, lineage: &FeatureLineage) -> bool {
        self.segments
            .iter()
            .any(|s| s.lineage.is_compatible_with(lineage))
    }
    pub fn search<'a>(
        &'a self,
        query: &'a str,
        lineage: &'a FeatureLineage,
        limit: usize,
    ) -> impl Iterator<Item = &'a TranscriptSegment> + 'a {
        let normalized_query = query.to_lowercase();
        self.segments
            .iter()
            .filter(move |s| {
                s.lineage.is_compatible_with(lineage)
                    && s.text.to_lowercase().contains(&normalized_query)
            })
            .take(limit)
    }
    pub fn all_matching<'a>(
        &'a self,
        normalized_query: &'a str,
        limit: usize,
    ) -> impl Iterator<Item = &'a TranscriptSegment> + 'a {
        self.segments
            .iter()
            .filter(move |s| s.text.to_lowercase().contains(normalized_query))
            .take(limit)
    }
}

/// Declares the bounded work required to make a media feature available.
///
/// This is an application-facing planning contract: it intentionally does not
/// claim that DataFusion can infer or rewrite arbitrary SQL into media work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaWorkEstimate {
    pub stable_id: String,
    pub estimated_cost_units: u64,
    pub max_segments: usize,
}

/// Cost units are application-calibrated, comparable scheduling units rather
/// than elapsed-time promises. They make cold/warm decisions auditable without
/// hiding provider-, hardware-, or cache-dependent latency behind fake values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MediaCostModel {
    pub persisted_index_lookup_units: u64,
    pub cold_transcription_units_per_segment: u64,
}

impl MediaCostModel {
    pub fn estimate_cold(self, segments: usize) -> Option<u64> {
        self.cold_transcription_units_per_segment
            .checked_mul(segments as u64)
    }
}

/// Process-wide bounds for a progressive media-work wave.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgressiveScheduleLimits {
    pub max_work_items: usize,
    pub max_total_cost_units: u64,
    pub max_total_segments: usize,
    pub max_parallelism: usize,
}

/// A deterministic, globally bounded cold-work schedule.
///
/// Work is sorted by the caller-provided stable ID and then grouped into
/// deterministic waves. The schedule never admits work beyond any aggregate
/// bound; callers execute the returned waves using their own governed runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgressiveSchedule {
    work: Vec<MediaWorkEstimate>,
    pub skipped_stable_ids: Vec<String>,
    max_parallelism: usize,
    pub admitted_cost_units: u64,
    pub admitted_segments: usize,
}

impl ProgressiveSchedule {
    pub fn build(
        mut work: Vec<MediaWorkEstimate>,
        limits: ProgressiveScheduleLimits,
    ) -> Result<Self, IndexError> {
        if limits.max_work_items == 0
            || limits.max_total_cost_units == 0
            || limits.max_total_segments == 0
            || limits.max_parallelism == 0
        {
            return Err(IndexError::InvalidScheduleLimits);
        }
        work.sort_by(|left, right| left.stable_id.cmp(&right.stable_id));
        let mut admitted = Vec::with_capacity(work.len().min(limits.max_work_items));
        let mut skipped_stable_ids = Vec::new();
        let mut admitted_cost_units = 0_u64;
        let mut admitted_segments = 0_usize;
        let mut previous: Option<&str> = None;
        for item in &work {
            if item.stable_id.trim().is_empty() {
                return Err(IndexError::EmptyWorkId);
            }
            if previous == Some(item.stable_id.as_str()) {
                return Err(IndexError::DuplicateWorkId(item.stable_id.clone()));
            }
            previous = Some(item.stable_id.as_str());
            let Some(next_cost) = admitted_cost_units.checked_add(item.estimated_cost_units) else {
                skipped_stable_ids.push(item.stable_id.clone());
                continue;
            };
            let Some(next_segments) = admitted_segments.checked_add(item.max_segments) else {
                skipped_stable_ids.push(item.stable_id.clone());
                continue;
            };
            if admitted.len() == limits.max_work_items
                || next_cost > limits.max_total_cost_units
                || next_segments > limits.max_total_segments
            {
                skipped_stable_ids.push(item.stable_id.clone());
                continue;
            }
            admitted_cost_units = next_cost;
            admitted_segments = next_segments;
            admitted.push(item.clone());
        }
        Ok(Self {
            work: admitted,
            skipped_stable_ids,
            max_parallelism: limits.max_parallelism,
            admitted_cost_units,
            admitted_segments,
        })
    }

    pub fn waves(&self) -> impl Iterator<Item = &[MediaWorkEstimate]> {
        self.work.chunks(self.max_parallelism)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaSearchStrategy {
    PersistedIndex,
    ColdExtraction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaSearchCapabilities {
    pub exact_lineage_index: bool,
    pub cold_transcription: bool,
    pub automatic_substitution: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaSearchExplanation {
    pub strategy: MediaSearchStrategy,
    pub capabilities: MediaSearchCapabilities,
    pub estimated_cost_units: u64,
    pub schedule: Option<ProgressiveSchedule>,
    pub limits: ProgressiveScheduleLimits,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaTranscriptSearchRequest {
    pub media: MediaRef,
    pub lineage: FeatureLineage,
    pub query: String,
    pub limit: usize,
    pub max_cold_segments: usize,
    pub cost_model: MediaCostModel,
}

/// Explicit query-path integration point for media-aware transcript lookup.
///
/// Persisted rows are substituted automatically only when their complete
/// lineage exactly matches the request. This is deliberately an API rather
/// than a claimed DataFusion optimizer rule: arbitrary SQL operators cannot be
/// safely rewritten into model execution in DataFusion 55.
pub struct MediaTranscriptSearch {
    persisted: TranscriptIndex,
    provider: Arc<dyn TranscriptionProvider>,
}

impl MediaTranscriptSearch {
    pub fn new(provider: Arc<dyn TranscriptionProvider>) -> Self {
        Self {
            persisted: TranscriptIndex::default(),
            provider,
        }
    }

    pub fn register_persisted(
        &mut self,
        rows: impl IntoIterator<Item = TranscriptSegment>,
    ) -> Result<(), IndexError> {
        for row in rows {
            self.persisted.insert(row)?;
        }
        Ok(())
    }

    pub fn explain(
        &self,
        request: &MediaTranscriptSearchRequest,
        limits: ProgressiveScheduleLimits,
    ) -> Result<MediaSearchExplanation, IndexError> {
        validate_media_search_request(request)?;
        let indexed = self.persisted.has_compatible_lineage(&request.lineage);
        let estimated_cost_units = if indexed {
            request.cost_model.persisted_index_lookup_units
        } else {
            request
                .cost_model
                .estimate_cold(request.max_cold_segments)
                .ok_or_else(|| IndexError::WorkExceedsGlobalLimit {
                    id: request.media.uri.clone(),
                })?
        };
        let schedule = (!indexed)
            .then(|| {
                ProgressiveSchedule::build(
                    vec![MediaWorkEstimate {
                        stable_id: request.media.uri.clone(),
                        estimated_cost_units,
                        max_segments: request.max_cold_segments,
                    }],
                    limits,
                )
            })
            .transpose()?;
        Ok(MediaSearchExplanation {
            strategy: if indexed {
                MediaSearchStrategy::PersistedIndex
            } else {
                MediaSearchStrategy::ColdExtraction
            },
            capabilities: MediaSearchCapabilities {
                exact_lineage_index: indexed,
                cold_transcription: true,
                automatic_substitution: indexed,
            },
            estimated_cost_units,
            schedule,
            limits,
        })
    }

    pub fn search(
        &self,
        request: &MediaTranscriptSearchRequest,
        limits: ProgressiveScheduleLimits,
    ) -> Result<Vec<TranscriptSegment>, IndexError> {
        let explanation = self.explain(request, limits)?;
        if request.limit == 0 {
            return Ok(Vec::new());
        }
        if explanation.strategy == MediaSearchStrategy::PersistedIndex {
            return Ok(self
                .persisted
                .search(&request.query, &request.lineage, request.limit)
                .cloned()
                .collect());
        }
        let scheduled = explanation
            .schedule
            .expect("cold strategy always creates a schedule");
        if scheduled.admitted_segments == 0 {
            return Ok(Vec::new());
        }
        // Iterating waves is intentional: applications may execute each wave
        // through their shared governor, but this one-source adapter retains a
        // single globally bounded request and never expands it.
        let request_segments = scheduled.admitted_segments.min(MAX_TRANSCRIPT_SEGMENTS);
        let mut output = Vec::with_capacity(request.limit);
        for _wave in scheduled.waves() {
            for row in self.provider.transcribe(&TranscriptionRequest {
                media: request.media.clone(),
                lineage: request.lineage.clone(),
                start_millis: 0,
                end_millis: u64::MAX,
                max_segments: request_segments,
            })? {
                if !row.lineage.is_compatible_with(&request.lineage) {
                    return Err(IndexError::IncompatibleColdResult);
                }
                validate_transcript_range(&row)?;
                if row
                    .text
                    .to_lowercase()
                    .contains(&request.query.to_lowercase())
                {
                    output.push(row);
                    if output.len() == request.limit {
                        return Ok(output);
                    }
                }
            }
        }
        Ok(output)
    }
}

fn validate_media_search_request(request: &MediaTranscriptSearchRequest) -> Result<(), IndexError> {
    if request.lineage.source.media_uri != request.media.uri {
        return Err(IndexError::SearchSourceMismatch);
    }
    if request.max_cold_segments == 0 || request.max_cold_segments > MAX_TRANSCRIPT_SEGMENTS {
        return Err(IndexError::TranscriptLimitExceeded);
    }
    Ok(())
}

#[derive(Default)]
pub struct EmbeddingIndex {
    rows: Vec<EmbeddingRecord>,
}
impl EmbeddingIndex {
    pub fn insert(&mut self, row: EmbeddingRecord) -> Result<(), IndexError> {
        validate_dimensions(row.values.len())?;
        if self.rows.len() == MAX_RANKING_CANDIDATES {
            return Err(IndexError::EmbeddingIndexCapacityExceeded);
        }
        if let Some(existing) = self.rows.first()
            && existing.values.len() != row.values.len()
        {
            return Err(IndexError::IncompatibleEmbeddingDimensions);
        }
        self.rows.push(row);
        Ok(())
    }
    pub fn has_compatible_lineage(&self, lineage: &FeatureLineage) -> bool {
        self.rows
            .iter()
            .any(|r| r.lineage.is_compatible_with(lineage))
    }
    pub fn rank(
        &self,
        query: &[f32],
        lineage: &FeatureLineage,
        options: RankingOptions,
    ) -> Result<Vec<RankedResult>, IndexError> {
        validate_dimensions(query.len())?;
        validate_ranking(options)?;
        let semantics = if options.candidate_limit.is_some() {
            RankingSemantics::Approximate
        } else {
            RankingSemantics::Ranked
        };
        let candidates = self
            .rows
            .iter()
            .filter(|r| r.lineage.is_compatible_with(lineage))
            .filter(|r| r.values.len() == query.len());
        let candidates: Box<dyn Iterator<Item = &EmbeddingRecord>> = match options.candidate_limit {
            Some(n) => Box::new(candidates.take(n)),
            None => Box::new(candidates),
        };
        let mut out = candidates
            .map(|r| RankedResult {
                id: r.id.clone(),
                text: r.text.clone(),
                score: cosine(query, &r.values),
                semantics,
            })
            .collect::<Vec<_>>();
        out.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        out.truncate(options.limit);
        Ok(out)
    }
    pub fn rank_hybrid(
        &self,
        query_text: &str,
        query: &[f32],
        lineage: &FeatureLineage,
        options: RankingOptions,
        semantic_weight: f32,
    ) -> Result<Vec<RankedResult>, IndexError> {
        if !(0.0..=1.0).contains(&semantic_weight) {
            return Err(IndexError::InvalidEmbeddingDimensions);
        }
        let mut ranked = self.rank(query, lineage, options)?;
        for row in &mut ranked {
            row.score = semantic_weight * row.score
                + (1.0 - semantic_weight) * keyword_score(query_text, &row.text);
        }
        ranked.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        Ok(ranked)
    }
}

pub enum SearchPlan {
    Indexed,
    Cold,
}
pub fn plan_keyword_search(index: &TranscriptIndex, lineage: &FeatureLineage) -> SearchPlan {
    if index.has_compatible_lineage(lineage) {
        SearchPlan::Indexed
    } else {
        SearchPlan::Cold
    }
}
pub trait TranscriptExtractor: Send + Sync {
    fn extract(
        &self,
        media: &MediaRef,
        lineage: &FeatureLineage,
    ) -> Result<Vec<TranscriptSegment>, IndexError>;
}
pub fn search_cold<E: TranscriptExtractor>(
    extractor: &E,
    media: &[MediaRef],
    query: &str,
    lineage: &FeatureLineage,
    limit: usize,
) -> Result<Vec<TranscriptSegment>, IndexError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut results = Vec::with_capacity(limit);
    for media_ref in media {
        for segment in extractor.extract(media_ref, lineage)? {
            if !segment.lineage.is_compatible_with(lineage) {
                return Err(IndexError::IncompatibleColdResult);
            }
            validate_transcript_range(&segment)?;
            if segment.text.to_lowercase().contains(&query.to_lowercase()) {
                results.push(segment);
                if results.len() == limit {
                    return Ok(results);
                }
            }
        }
    }
    Ok(results)
}
fn validate_transcript_range(s: &TranscriptSegment) -> Result<(), IndexError> {
    if s.end_millis < s.start_millis {
        Err(IndexError::InvalidTranscriptRange)
    } else {
        Ok(())
    }
}
fn validate_transcription_request(r: &TranscriptionRequest) -> Result<(), IndexError> {
    if r.end_millis < r.start_millis {
        return Err(IndexError::InvalidTranscriptionRequest);
    }
    if r.max_segments > MAX_TRANSCRIPT_SEGMENTS {
        return Err(IndexError::TranscriptLimitExceeded);
    }
    Ok(())
}
fn validate_ocr_request(request: &OcrRequest) -> Result<(), IndexError> {
    if request.bytes.len() > MAX_OCR_INPUT_BYTES {
        return Err(IndexError::ProviderInputTooLarge {
            maximum_bytes: MAX_OCR_INPUT_BYTES,
        });
    }
    if request.max_results == 0 || request.max_results > MAX_OCR_RESULTS {
        return Err(IndexError::ProviderResultLimitExceeded {
            maximum: MAX_OCR_RESULTS,
        });
    }
    Ok(())
}
fn validate_dimensions(dimensions: usize) -> Result<(), IndexError> {
    if dimensions == 0 || dimensions > MAX_EMBEDDING_DIMENSIONS {
        Err(IndexError::InvalidEmbeddingDimensions)
    } else {
        Ok(())
    }
}
fn validate_ranking(options: RankingOptions) -> Result<(), IndexError> {
    if options.limit > MAX_RANKING_CANDIDATES
        || options
            .candidate_limit
            .is_some_and(|n| n > MAX_RANKING_CANDIDATES)
    {
        Err(IndexError::RankingLimitExceeded)
    } else {
        Ok(())
    }
}
fn normalize(values: &mut [f32]) {
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm != 0.0 {
        for value in values {
            *value /= norm;
        }
    }
}
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(a, b)| a * b).sum()
}
fn keyword_score(query: &str, text: &str) -> f32 {
    let terms = query
        .split_whitespace()
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>();
    if terms.is_empty() {
        0.0
    } else {
        let lower = text.to_ascii_lowercase();
        terms
            .iter()
            .filter(|t| lower.contains(&t.to_ascii_lowercase()))
            .count() as f32
            / terms.len() as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakeprism_core::{SourceIdentity, StorageMode};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    fn lineage(v: &str) -> FeatureLineage {
        FeatureLineage {
            source: SourceIdentity {
                media_uri: "file:///media/video.mp4".into(),
                source_version: v.into(),
            },
            operator_version: "transcribe-v1".into(),
            model: Some("mock".into()),
            model_version: Some("1".into()),
            parameters: BTreeMap::new(),
        }
    }
    fn segment(text: &str, lineage: FeatureLineage) -> TranscriptSegment {
        TranscriptSegment {
            media_id: "video-1".into(),
            start_millis: 0,
            end_millis: 1000,
            text: text.into(),
            confidence_millis: Some(990),
            lineage,
            created_at_unix_millis: 1,
        }
    }
    #[test]
    fn compatible_index_substitution_is_selected() {
        let l = lineage("e1");
        let mut i = TranscriptIndex::default();
        i.insert(segment("DataFusion supports cancellation", l.clone()))
            .unwrap();
        assert!(matches!(plan_keyword_search(&i, &l), SearchPlan::Indexed));
        assert_eq!(i.search("CANCELLATION", &l, 1).count(), 1)
    }
    #[test]
    fn unavailable_providers_never_fake_inference() {
        let media =
            MediaRef::new("file:///media/video.mp4", "audio", StorageMode::External).unwrap();
        assert!(matches!(
            UnavailableTranscriptionProvider.transcribe(&TranscriptionRequest {
                media,
                lineage: lineage("e1"),
                start_millis: 0,
                end_millis: 1,
                max_segments: 1
            }),
            Err(IndexError::ProviderUnavailable { .. })
        ));
        assert!(matches!(
            UnavailableEmbeddingProvider.embed(&EmbeddingRequest {
                text: "a".into(),
                lineage: lineage("e1")
            }),
            Err(IndexError::ProviderUnavailable { .. })
        ));
        assert!(matches!(
            UnavailableOcrProvider.recognize(&OcrRequest {
                bytes: Arc::from([]),
                media_type: Some("image/png".into()),
                lineage: lineage("e1"),
                max_results: 1,
            }),
            Err(IndexError::ProviderUnavailable { .. })
        ));
        assert!(matches!(
            UnavailableImageMetadataProvider.inspect(&OcrRequest {
                bytes: Arc::from([]),
                media_type: Some("image/png".into()),
                lineage: lineage("e1"),
                max_results: 1,
            }),
            Err(IndexError::ProviderUnavailable { .. })
        ));
    }
    #[test]
    fn cross_modal_search_requires_exact_lineage() {
        let l = lineage("e1");
        let mut index = CrossModalIndex::default();
        index
            .insert(CrossModalRecord {
                id: "caption-1".into(),
                kind: CrossModalKind::Image,
                text: "lake from video frame".into(),
                values: vec![1.0, 0.0],
                lineage: l.clone(),
            })
            .unwrap();
        index
            .insert(CrossModalRecord {
                id: "transcript-1".into(),
                kind: CrossModalKind::Transcript,
                text: "lake discussion".into(),
                values: vec![0.8, 0.2],
                lineage: l.clone(),
            })
            .unwrap();
        assert_eq!(
            index
                .rank(
                    &[1.0, 0.0],
                    &l,
                    RankingOptions {
                        limit: 2,
                        candidate_limit: None,
                    },
                )
                .unwrap()
                .len(),
            2
        );
        assert!(
            index
                .rank(
                    &[1.0, 0.0],
                    &lineage("changed"),
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
    fn exact_lineage_and_explicit_ranking_semantics() {
        let l = lineage("e1");
        let mock = DeterministicMockEmbeddingProvider::new(8).unwrap();
        let mut i = EmbeddingIndex::default();
        for (id, text) in [("a", "lake data"), ("b", "ocean water")] {
            i.insert(EmbeddingRecord {
                id: id.into(),
                text: text.into(),
                values: mock
                    .embed(&EmbeddingRequest {
                        text: text.into(),
                        lineage: l.clone(),
                    })
                    .unwrap(),
                lineage: l.clone(),
            })
            .unwrap();
        }
        let q = mock
            .embed(&EmbeddingRequest {
                text: "lake".into(),
                lineage: l.clone(),
            })
            .unwrap();
        assert_eq!(
            i.rank(
                &q,
                &l,
                RankingOptions {
                    limit: 2,
                    candidate_limit: None
                }
            )
            .unwrap()[0]
                .semantics,
            RankingSemantics::Ranked
        );
        assert_eq!(
            i.rank(
                &q,
                &l,
                RankingOptions {
                    limit: 2,
                    candidate_limit: Some(1)
                }
            )
            .unwrap()[0]
                .semantics,
            RankingSemantics::Approximate
        );
        assert!(
            i.rank(
                &q,
                &lineage("changed"),
                RankingOptions {
                    limit: 1,
                    candidate_limit: None
                }
            )
            .unwrap()
            .is_empty()
        );
    }
    #[test]
    fn mock_transcription_is_bounded() {
        let l = lineage("e1");
        let row = segment("real fixture", l.clone());
        let p = DeterministicMockTranscriptionProvider::new([row]);
        let media =
            MediaRef::new("file:///media/video.mp4", "audio", StorageMode::External).unwrap();
        assert_eq!(
            p.transcribe(&TranscriptionRequest {
                media,
                lineage: l,
                start_millis: 0,
                end_millis: 1000,
                max_segments: 1
            })
            .unwrap()
            .len(),
            1
        );
    }

    fn media_search_request(lineage: FeatureLineage) -> MediaTranscriptSearchRequest {
        MediaTranscriptSearchRequest {
            media: MediaRef::new(
                lineage.source.media_uri.clone(),
                "audio",
                StorageMode::External,
            )
            .unwrap(),
            lineage,
            query: "meeting".into(),
            limit: 2,
            max_cold_segments: 4,
            cost_model: MediaCostModel {
                persisted_index_lookup_units: 1,
                cold_transcription_units_per_segment: 3,
            },
        }
    }

    fn schedule_limits() -> ProgressiveScheduleLimits {
        ProgressiveScheduleLimits {
            max_work_items: 4,
            max_total_cost_units: 100,
            max_total_segments: 16,
            max_parallelism: 2,
        }
    }

    #[test]
    fn progressive_schedule_is_deterministic_and_globally_bounded() {
        let schedule = ProgressiveSchedule::build(
            vec![
                MediaWorkEstimate {
                    stable_id: "z".into(),
                    estimated_cost_units: 4,
                    max_segments: 2,
                },
                MediaWorkEstimate {
                    stable_id: "a".into(),
                    estimated_cost_units: 3,
                    max_segments: 1,
                },
            ],
            schedule_limits(),
        )
        .unwrap();
        assert_eq!(schedule.admitted_cost_units, 7);
        assert_eq!(schedule.admitted_segments, 3);
        assert_eq!(
            schedule
                .waves()
                .flat_map(|wave| wave.iter().map(|work| work.stable_id.as_str()))
                .collect::<Vec<_>>(),
            vec!["a", "z"]
        );
        let bounded = ProgressiveSchedule::build(
            vec![MediaWorkEstimate {
                stable_id: "too-expensive".into(),
                estimated_cost_units: 101,
                max_segments: 1,
            }],
            schedule_limits(),
        )
        .unwrap();
        assert!(bounded.waves().next().is_none());
        assert_eq!(bounded.skipped_stable_ids, ["too-expensive"]);
    }

    #[test]
    fn media_search_substitutes_only_exact_persisted_lineage() {
        let exact = lineage("e1");
        let persisted = segment("meeting action item", exact.clone());
        let service =
            MediaTranscriptSearch::new(Arc::new(DeterministicMockTranscriptionProvider::default()));
        let mut service = service;
        service.register_persisted([persisted]).unwrap();

        let request = media_search_request(exact.clone());
        let warm = service.explain(&request, schedule_limits()).unwrap();
        assert_eq!(warm.strategy, MediaSearchStrategy::PersistedIndex);
        assert!(warm.capabilities.automatic_substitution);
        assert_eq!(warm.estimated_cost_units, 1);
        assert_eq!(
            service.search(&request, schedule_limits()).unwrap().len(),
            1
        );

        let cold_request = media_search_request(lineage("e2"));
        let cold = service.explain(&cold_request, schedule_limits()).unwrap();
        assert_eq!(cold.strategy, MediaSearchStrategy::ColdExtraction);
        assert!(!cold.capabilities.automatic_substitution);
        assert_eq!(cold.estimated_cost_units, 12);
    }

    #[test]
    fn media_search_cold_path_obeys_provider_request_bound() {
        let line = lineage("e1");
        let provider =
            DeterministicMockTranscriptionProvider::new([segment("meeting notes", line.clone())]);
        let service = MediaTranscriptSearch::new(Arc::new(provider));
        let output = service
            .search(&media_search_request(line), schedule_limits())
            .unwrap();
        assert_eq!(output.len(), 1);
    }
}
