use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use lakeprism_core::{FeatureLineage, MediaRef, SourceIdentity, StorageMode};
use lakeprism_index::{
    IndexError, MediaCostModel, MediaTranscriptSearch, MediaTranscriptSearchRequest,
    ProgressiveScheduleLimits, TranscriptSegment, TranscriptionProvider, TranscriptionRequest,
};

struct CountingProvider {
    calls: AtomicUsize,
    row: TranscriptSegment,
}

impl TranscriptionProvider for CountingProvider {
    fn transcribe(&self, _: &TranscriptionRequest) -> Result<Vec<TranscriptSegment>, IndexError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(vec![self.row.clone()])
    }
}

fn main() {
    let lineage = FeatureLineage {
        source: SourceIdentity {
            media_uri: "file:///bench/meeting.wav".into(),
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
        text: "meeting action item".into(),
        confidence_millis: Some(1_000),
        lineage: lineage.clone(),
        created_at_unix_millis: 0,
    };
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        row: row.clone(),
    });
    let service = MediaTranscriptSearch::new(provider.clone());
    let request = MediaTranscriptSearchRequest {
        media: MediaRef::new(&lineage.source.media_uri, "audio", StorageMode::External).unwrap(),
        lineage,
        query: "action".into(),
        limit: 1,
        max_cold_segments: 16,
        cost_model: MediaCostModel {
            persisted_index_lookup_units: 1,
            cold_transcription_units_per_segment: 10,
        },
    };
    let limits = ProgressiveScheduleLimits {
        max_work_items: 1,
        max_total_cost_units: 160,
        max_total_segments: 16,
        max_parallelism: 1,
    };

    let cold_start = Instant::now();
    let cold = service.search(&request, limits).unwrap();
    let cold_elapsed = cold_start.elapsed();
    assert_eq!(cold.len(), 1);
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);

    let mut warm_service = service;
    warm_service.register_persisted([row]).unwrap();
    let warm_start = Instant::now();
    let warm = warm_service.search(&request, limits).unwrap();
    let warm_elapsed = warm_start.elapsed();
    assert_eq!(warm.len(), 1);
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    println!(
        "cold={cold_elapsed:?} warm={warm_elapsed:?} cold_provider_calls=1 warm_provider_calls=0"
    );
    println!(
        "lakeprism_benchmark cold_nanos={} warm_nanos={}",
        cold_elapsed.as_nanos(),
        warm_elapsed.as_nanos()
    );
}
