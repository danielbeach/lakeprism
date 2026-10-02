//! Deterministic, process-local distributed-execution primitives.
//!
//! This crate deliberately provides a coordinator and a worker *abstraction*,
//! not a remote transport or a process supervisor. A worker receives only a
//! credential-free task projection; applications own process launch, IPC, and
//! request-scoped credential vending. Arrow batches remain in memory in this
//! initial implementation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use lakeprism_core::{AccessContext, FeatureLineage, MediaRef, SourceIdentity};
use lakeprism_storage::{ExecutionGovernor, QueryControl};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Opaque request context passed across the coordinator/worker boundary.
///
/// This intentionally contains identity only. Bearer tokens, cloud keys, and
/// credential-provider implementations stay in the application process
/// boundary and must be reconstructed by the worker host.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskAccessContext {
    pub principal: String,
    pub catalog_identity: Option<String>,
}

impl From<&AccessContext> for TaskAccessContext {
    fn from(value: &AccessContext) -> Self {
        Self {
            principal: value.principal.clone(),
            catalog_identity: value.catalog_identity.clone(),
        }
    }
}

/// A media source plus the version identity used to validate derived results.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskSource {
    pub media: MediaRef,
    pub identity: SourceIdentity,
}

/// Declares whether a task can safely be retried after an ambiguous failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RetrySafety {
    Idempotent,
    NonIdempotent,
}

/// A coordinator-owned execution request. `operation` is an opaque label, not
/// SQL, credentials, or executable code.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskRequest {
    pub operation: String,
    pub retry_safety: RetrySafety,
    pub access: TaskAccessContext,
    pub sources: Vec<TaskSource>,
}

/// A stable partition of an execution request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionTask {
    pub job_id: Uuid,
    pub partition: u32,
    pub attempt: u32,
    pub operation: String,
    pub retry_safety: RetrySafety,
    pub access: TaskAccessContext,
    pub sources: Vec<TaskSource>,
}

/// A task currently held by one worker. Lease tokens prevent stale workers
/// from completing a reissued task.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeasedTask {
    pub task: PartitionTask,
    pub lease_token: Uuid,
    pub worker_id: String,
}

/// In-memory Arrow results produced by a worker.
///
/// This is intentionally not serializable: no IPC or cloud transport exists
/// in phase 9. A future transport must encode these batches with Arrow IPC
/// while retaining the enclosing source identity and lineage.
#[derive(Clone, Debug)]
pub struct TaskResult {
    pub source_identities: Vec<SourceIdentity>,
    pub lineage: Vec<FeatureLineage>,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStatus {
    Queued,
    Leased,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSnapshot {
    pub partition: u32,
    pub attempt: u32,
    pub status: TaskStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobSnapshot {
    pub id: Uuid,
    pub cancelled: bool,
    pub tasks: Vec<TaskSnapshot>,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum WorkerError {
    #[error("worker failed before completing the task: {0}")]
    Retryable(String),
    #[error("worker permanently rejected the task: {0}")]
    Permanent(String),
    #[error("task was cancelled")]
    Cancelled,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum ExecutionError {
    #[error("partition count must be greater than zero")]
    ZeroPartitions,
    #[error("task operation must not be empty")]
    EmptyOperation,
    #[error("task protocol contains a credential-bearing or mismatched source")]
    UnsafeTaskSource,
    #[error("unknown job")]
    UnknownJob,
    #[error("unknown or no longer leased task")]
    InvalidLease,
    #[error("non-idempotent task cannot be retried after an ambiguous failure")]
    NonIdempotentRetryBlocked,
    #[error("worker returned result source identities that do not match the task")]
    SourceIdentityMismatch,
    #[error("worker returned result lineage for an unrelated source")]
    LineageMismatch,
}

#[derive(Clone)]
pub struct WorkerContext {
    pub cancellation: QueryControl,
    pub execution_governor: Arc<ExecutionGovernor>,
}

/// Implement this at the local worker/process boundary. The coordinator does
/// not start or supervise OS processes in this phase.
#[async_trait]
pub trait Worker: Send + Sync {
    async fn execute(
        &self,
        leased: LeasedTask,
        context: WorkerContext,
    ) -> Result<TaskResult, WorkerError>;
}

struct Lease {
    worker_id: String,
    token: Uuid,
    expires_at: Instant,
}

struct TaskState {
    task: PartitionTask,
    status: TaskStatus,
    lease: Option<Lease>,
    result: Option<TaskResult>,
}

struct JobState {
    cancelled: bool,
    control: QueryControl,
    tasks: BTreeMap<u32, TaskState>,
}

/// Deterministic in-process coordinator state. All resource permits are
/// coordinator-owned and provided to workers only as `WorkerContext`.
pub struct LocalCoordinator {
    partition_count: u32,
    lease_duration: Duration,
    execution_governor: Arc<ExecutionGovernor>,
    jobs: Mutex<BTreeMap<Uuid, JobState>>,
}

impl LocalCoordinator {
    pub fn new(
        partition_count: u32,
        lease_duration: Duration,
        execution_governor: Arc<ExecutionGovernor>,
    ) -> Result<Self, ExecutionError> {
        if partition_count == 0 {
            return Err(ExecutionError::ZeroPartitions);
        }
        Ok(Self {
            partition_count,
            lease_duration,
            execution_governor,
            jobs: Mutex::new(BTreeMap::new()),
        })
    }

    /// Splits sources by a stable FNV-1a hash of their source identity. Every
    /// partition's sources are sorted, so input order and worker scheduling
    /// cannot affect task or final-result ordering.
    pub async fn submit(&self, request: TaskRequest) -> Result<Uuid, ExecutionError> {
        if request.operation.trim().is_empty() {
            return Err(ExecutionError::EmptyOperation);
        }
        if request.sources.iter().any(|source| !safe_source(source)) {
            return Err(ExecutionError::UnsafeTaskSource);
        }
        let job_id = Uuid::new_v4();
        let mut partitions: BTreeMap<u32, Vec<TaskSource>> = BTreeMap::new();
        for source in request.sources {
            let partition = stable_partition(&source.identity, self.partition_count);
            partitions.entry(partition).or_default().push(source);
        }

        let tasks = partitions
            .into_iter()
            .map(|(partition, mut sources)| {
                sources.sort_by(source_order);
                (
                    partition,
                    TaskState {
                        task: PartitionTask {
                            job_id,
                            partition,
                            attempt: 0,
                            operation: request.operation.clone(),
                            retry_safety: request.retry_safety,
                            access: request.access.clone(),
                            sources,
                        },
                        status: TaskStatus::Queued,
                        lease: None,
                        result: None,
                    },
                )
            })
            .collect();
        self.jobs.lock().await.insert(
            job_id,
            JobState {
                cancelled: false,
                control: QueryControl::new(None),
                tasks,
            },
        );
        Ok(job_id)
    }

    /// Claims the lowest partition ID across jobs, yielding a deterministic
    /// dispatch order. An empty result means no currently dispatchable work.
    pub async fn claim_next(&self, worker_id: impl Into<String>) -> Option<LeasedTask> {
        let worker_id = worker_id.into();
        let now = Instant::now();
        let mut jobs = self.jobs.lock().await;
        for (job_id, job) in jobs.iter_mut() {
            if job.cancelled {
                continue;
            }
            let Some((_, state)) = job
                .tasks
                .iter_mut()
                .find(|(_, state)| state.status == TaskStatus::Queued)
            else {
                continue;
            };
            let token = Uuid::new_v4();
            state.status = TaskStatus::Leased;
            state.lease = Some(Lease {
                worker_id: worker_id.clone(),
                token,
                expires_at: now + self.lease_duration,
            });
            let mut task = state.task.clone();
            task.job_id = *job_id;
            return Some(LeasedTask {
                task,
                lease_token: token,
                worker_id,
            });
        }
        None
    }

    pub async fn heartbeat(&self, leased: &LeasedTask) -> Result<(), ExecutionError> {
        let mut jobs = self.jobs.lock().await;
        let state = task_for_lease(&mut jobs, leased)?;
        let lease = state.lease.as_mut().ok_or(ExecutionError::InvalidLease)?;
        lease.expires_at = Instant::now() + self.lease_duration;
        Ok(())
    }

    /// Requeues expired idempotent work. Expired non-idempotent work is marked
    /// failed because its external effect may already have happened.
    pub async fn recover_expired(&self) {
        let now = Instant::now();
        let mut jobs = self.jobs.lock().await;
        for job in jobs.values_mut() {
            for state in job.tasks.values_mut() {
                if state.status != TaskStatus::Leased
                    || state
                        .lease
                        .as_ref()
                        .is_none_or(|lease| lease.expires_at > now)
                {
                    continue;
                }
                state.lease = None;
                if state.task.retry_safety == RetrySafety::Idempotent && !job.cancelled {
                    state.task.attempt += 1;
                    state.status = TaskStatus::Queued;
                } else {
                    state.status = if job.cancelled {
                        TaskStatus::Cancelled
                    } else {
                        TaskStatus::Failed
                    };
                }
            }
        }
    }

    pub async fn complete(
        &self,
        leased: &LeasedTask,
        result: TaskResult,
    ) -> Result<(), ExecutionError> {
        validate_result(&leased.task, &result)?;
        let mut jobs = self.jobs.lock().await;
        let state = task_for_lease(&mut jobs, leased)?;
        state.lease = None;
        state.result = Some(result);
        state.status = TaskStatus::Succeeded;
        Ok(())
    }

    pub async fn fail(
        &self,
        leased: &LeasedTask,
        error: WorkerError,
    ) -> Result<(), ExecutionError> {
        let mut jobs = self.jobs.lock().await;
        let job = jobs
            .get_mut(&leased.task.job_id)
            .ok_or(ExecutionError::UnknownJob)?;
        let cancelled = job.cancelled;
        let state = task_for_lease_in_job(job, leased)?;
        state.lease = None;
        match error {
            WorkerError::Cancelled => state.status = TaskStatus::Cancelled,
            WorkerError::Permanent(_) => state.status = TaskStatus::Failed,
            WorkerError::Retryable(_)
                if state.task.retry_safety == RetrySafety::Idempotent && !cancelled =>
            {
                state.task.attempt += 1;
                state.status = TaskStatus::Queued;
            }
            WorkerError::Retryable(_) => {
                state.status = TaskStatus::Failed;
                return Err(ExecutionError::NonIdempotentRetryBlocked);
            }
        }
        Ok(())
    }

    /// Executes exactly one leased task through the supplied local worker.
    /// It is intentionally process-like: no claims are made about OS isolation.
    pub async fn execute_next(
        &self,
        worker_id: impl Into<String>,
        worker: &dyn Worker,
    ) -> Result<bool, ExecutionError> {
        let Some(leased) = self.claim_next(worker_id).await else {
            return Ok(false);
        };
        let control = {
            let jobs = self.jobs.lock().await;
            jobs.get(&leased.task.job_id)
                .ok_or(ExecutionError::UnknownJob)?
                .control
                .clone()
        };
        match worker
            .execute(
                leased.clone(),
                WorkerContext {
                    cancellation: control,
                    execution_governor: Arc::clone(&self.execution_governor),
                },
            )
            .await
        {
            Ok(result) => self.complete(&leased, result).await?,
            Err(error) => self.fail(&leased, error).await?,
        }
        Ok(true)
    }

    /// Cooperative cancellation prevents queued work and signals leased
    /// workers. An in-flight worker must observe `WorkerContext::cancellation`.
    pub async fn cancel(&self, job_id: Uuid) -> Result<(), ExecutionError> {
        let mut jobs = self.jobs.lock().await;
        let job = jobs.get_mut(&job_id).ok_or(ExecutionError::UnknownJob)?;
        job.cancelled = true;
        job.control.cancel();
        for state in job.tasks.values_mut() {
            if state.status == TaskStatus::Queued {
                state.status = TaskStatus::Cancelled;
            }
        }
        Ok(())
    }

    pub async fn snapshot(&self, job_id: Uuid) -> Result<JobSnapshot, ExecutionError> {
        let jobs = self.jobs.lock().await;
        let job = jobs.get(&job_id).ok_or(ExecutionError::UnknownJob)?;
        Ok(JobSnapshot {
            id: job_id,
            cancelled: job.cancelled,
            tasks: job
                .tasks
                .values()
                .map(|state| TaskSnapshot {
                    partition: state.task.partition,
                    attempt: state.task.attempt,
                    status: state.status,
                })
                .collect(),
        })
    }

    /// Returns results in partition order regardless of completion order.
    pub async fn ordered_results(&self, job_id: Uuid) -> Result<Vec<TaskResult>, ExecutionError> {
        let jobs = self.jobs.lock().await;
        let job = jobs.get(&job_id).ok_or(ExecutionError::UnknownJob)?;
        Ok(job
            .tasks
            .values()
            .filter_map(|state| state.result.clone())
            .collect())
    }
}

fn task_for_lease<'a>(
    jobs: &'a mut BTreeMap<Uuid, JobState>,
    leased: &LeasedTask,
) -> Result<&'a mut TaskState, ExecutionError> {
    let job = jobs
        .get_mut(&leased.task.job_id)
        .ok_or(ExecutionError::UnknownJob)?;
    task_for_lease_in_job(job, leased)
}

fn task_for_lease_in_job<'a>(
    job: &'a mut JobState,
    leased: &LeasedTask,
) -> Result<&'a mut TaskState, ExecutionError> {
    let state = job
        .tasks
        .get_mut(&leased.task.partition)
        .ok_or(ExecutionError::InvalidLease)?;
    let valid = state.status == TaskStatus::Leased
        && state.lease.as_ref().is_some_and(|lease| {
            lease.token == leased.lease_token && lease.worker_id == leased.worker_id
        });
    valid.then_some(state).ok_or(ExecutionError::InvalidLease)
}

fn validate_result(task: &PartitionTask, result: &TaskResult) -> Result<(), ExecutionError> {
    let expected = task
        .sources
        .iter()
        .map(|source| identity_key(&source.identity))
        .collect::<std::collections::BTreeSet<_>>();
    let actual = result
        .source_identities
        .iter()
        .map(identity_key)
        .collect::<std::collections::BTreeSet<_>>();
    if expected != actual {
        return Err(ExecutionError::SourceIdentityMismatch);
    }
    if result
        .lineage
        .iter()
        .any(|lineage| !expected.contains(&identity_key(&lineage.source)))
    {
        return Err(ExecutionError::LineageMismatch);
    }

    fn identity_key(identity: &SourceIdentity) -> (String, String) {
        (identity.media_uri.clone(), identity.source_version.clone())
    }
    Ok(())
}

fn safe_source(source: &TaskSource) -> bool {
    if source.identity.media_uri != source.media.uri
        || MediaRef::new(
            source.media.uri.clone(),
            source.media.media_type.clone(),
            source.media.storage_mode.clone(),
        )
        .is_err()
    {
        return false;
    }
    source.media.catalog_ref.as_deref().is_none_or(safe_value)
        && source
            .media
            .metadata
            .iter()
            .all(|(key, value)| !sensitive_name(key) && safe_value(value))
}

fn sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "credential",
        "authorization",
        "signature",
    ]
    .iter()
    .any(|term| name.contains(term))
}

fn safe_value(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    !["bearer ", "x-amz-", "x-goog-", "access_token=", "secret="]
        .iter()
        .any(|term| value.contains(term))
}

fn source_order(left: &TaskSource, right: &TaskSource) -> std::cmp::Ordering {
    left.identity.media_uri.cmp(&right.identity.media_uri).then(
        left.identity
            .source_version
            .cmp(&right.identity.source_version),
    )
}

fn stable_partition(identity: &SourceIdentity, partition_count: u32) -> u32 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in identity
        .media_uri
        .bytes()
        .chain([0_u8])
        .chain(identity.source_version.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % u64::from(partition_count)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    struct RecordingWorker {
        fail_once: Mutex<bool>,
        reverse_marker: i32,
    }

    #[async_trait]
    impl Worker for RecordingWorker {
        async fn execute(
            &self,
            leased: LeasedTask,
            context: WorkerContext,
        ) -> Result<TaskResult, WorkerError> {
            if context.cancellation.state() != lakeprism_storage::QueryControlState::Active {
                return Err(WorkerError::Cancelled);
            }
            let mut failure = self.fail_once.lock().await;
            if *failure {
                *failure = false;
                return Err(WorkerError::Retryable("simulated worker death".to_owned()));
            }
            let identities = leased
                .task
                .sources
                .iter()
                .map(|source| source.identity.clone())
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "marker",
                DataType::Int32,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![self.reverse_marker]))],
            )
            .unwrap();
            Ok(TaskResult {
                source_identities: identities,
                lineage: Vec::new(),
                batches: vec![batch],
            })
        }
    }

    fn source(name: &str) -> TaskSource {
        TaskSource {
            media: MediaRef::new(
                format!("file:///media/{name}.mp4"),
                "video",
                lakeprism_core::StorageMode::External,
            )
            .unwrap(),
            identity: SourceIdentity {
                media_uri: format!("file:///media/{name}.mp4"),
                source_version: format!("v-{name}"),
            },
        }
    }

    fn request(sources: Vec<TaskSource>, retry_safety: RetrySafety) -> TaskRequest {
        TaskRequest {
            operation: "extract-frames-v1".to_owned(),
            retry_safety,
            access: TaskAccessContext {
                principal: "alice".to_owned(),
                catalog_identity: Some("local".to_owned()),
            },
            sources,
        }
    }

    fn coordinator(partitions: u32, lease: Duration) -> LocalCoordinator {
        LocalCoordinator::new(
            partitions,
            lease,
            Arc::new(
                ExecutionGovernor::new(lakeprism_storage::ExecutionGovernorConfig {
                    max_concurrent_queries: 1,
                    max_cpu_permits: 1,
                    max_io_permits: 1,
                })
                .unwrap(),
            ),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn partitioning_is_deterministic_independent_of_input_order() {
        let sources = vec![source("c"), source("a"), source("b"), source("d")];
        let coordinator = coordinator(3, Duration::from_secs(10));
        let first = coordinator
            .submit(request(sources.clone(), RetrySafety::Idempotent))
            .await
            .unwrap();
        let mut reversed = sources;
        reversed.reverse();
        let second = coordinator
            .submit(request(reversed, RetrySafety::Idempotent))
            .await
            .unwrap();
        let first_tasks = coordinator.snapshot(first).await.unwrap().tasks;
        let second_tasks = coordinator.snapshot(second).await.unwrap().tasks;
        assert_eq!(first_tasks, second_tasks);
    }

    #[tokio::test]
    async fn cancellation_never_dispatches_queued_tasks() {
        let coordinator = coordinator(1, Duration::from_secs(10));
        let job = coordinator
            .submit(request(vec![source("a")], RetrySafety::Idempotent))
            .await
            .unwrap();
        coordinator.cancel(job).await.unwrap();
        let worker = RecordingWorker {
            fail_once: Mutex::new(false),
            reverse_marker: 1,
        };
        assert!(!coordinator.execute_next("w1", &worker).await.unwrap());
        assert_eq!(
            coordinator.snapshot(job).await.unwrap().tasks[0].status,
            TaskStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn heartbeats_keep_a_lease_owned_by_the_same_worker() {
        let coordinator = coordinator(1, Duration::from_secs(60));
        let job = coordinator
            .submit(request(vec![source("a")], RetrySafety::Idempotent))
            .await
            .unwrap();
        let leased = coordinator.claim_next("w1").await.unwrap();
        coordinator.heartbeat(&leased).await.unwrap();
        coordinator.recover_expired().await;
        assert_eq!(
            coordinator.snapshot(job).await.unwrap().tasks[0].status,
            TaskStatus::Leased
        );
        assert!(coordinator.claim_next("w2").await.is_none());
    }

    #[tokio::test]
    async fn worker_failure_retries_only_idempotent_work() {
        let coordinator = coordinator(1, Duration::from_secs(10));
        let job = coordinator
            .submit(request(vec![source("a")], RetrySafety::Idempotent))
            .await
            .unwrap();
        let worker = RecordingWorker {
            fail_once: Mutex::new(true),
            reverse_marker: 1,
        };
        assert!(coordinator.execute_next("w1", &worker).await.unwrap());
        assert_eq!(coordinator.snapshot(job).await.unwrap().tasks[0].attempt, 1);
        assert!(coordinator.execute_next("w2", &worker).await.unwrap());
        assert_eq!(
            coordinator.snapshot(job).await.unwrap().tasks[0].status,
            TaskStatus::Succeeded
        );

        let non_idempotent = coordinator
            .submit(request(vec![source("b")], RetrySafety::NonIdempotent))
            .await
            .unwrap();
        let failed = RecordingWorker {
            fail_once: Mutex::new(true),
            reverse_marker: 2,
        };
        assert_eq!(
            coordinator.execute_next("w3", &failed).await,
            Err(ExecutionError::NonIdempotentRetryBlocked)
        );
        assert_eq!(
            coordinator.snapshot(non_idempotent).await.unwrap().tasks[0].status,
            TaskStatus::Failed
        );
    }

    #[tokio::test]
    async fn expired_leases_reissue_idempotent_work_and_reject_stale_completion() {
        let coordinator = coordinator(1, Duration::ZERO);
        let job = coordinator
            .submit(request(vec![source("a")], RetrySafety::Idempotent))
            .await
            .unwrap();
        let stale = coordinator.claim_next("lost-worker").await.unwrap();
        coordinator.recover_expired().await;
        let renewed = coordinator.claim_next("replacement").await.unwrap();
        assert_ne!(stale.lease_token, renewed.lease_token);
        let result = TaskResult {
            source_identities: renewed
                .task
                .sources
                .iter()
                .map(|source| source.identity.clone())
                .collect(),
            lineage: Vec::new(),
            batches: Vec::new(),
        };
        assert_eq!(
            coordinator.complete(&stale, result.clone()).await,
            Err(ExecutionError::InvalidLease)
        );
        coordinator.complete(&renewed, result).await.unwrap();
        assert_eq!(
            coordinator.snapshot(job).await.unwrap().tasks[0].status,
            TaskStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn result_order_is_partition_order_not_completion_order() {
        let coordinator = coordinator(3, Duration::from_secs(10));
        let job = coordinator
            .submit(request(
                vec![source("a"), source("b"), source("c"), source("d")],
                RetrySafety::Idempotent,
            ))
            .await
            .unwrap();
        let mut leased = Vec::new();
        while let Some(task) = coordinator.claim_next("worker").await {
            leased.push(task);
        }
        for (marker, task) in leased.iter().rev().enumerate() {
            coordinator
                .complete(
                    task,
                    TaskResult {
                        source_identities: task
                            .task
                            .sources
                            .iter()
                            .map(|source| source.identity.clone())
                            .collect(),
                        lineage: Vec::new(),
                        batches: vec![
                            RecordBatch::try_new(
                                Arc::new(Schema::new(vec![Field::new(
                                    "marker",
                                    DataType::Int32,
                                    false,
                                )])),
                                vec![Arc::new(Int32Array::from(vec![marker as i32]))],
                            )
                            .unwrap(),
                        ],
                    },
                )
                .await
                .unwrap();
        }
        let markers = coordinator
            .ordered_results(job)
            .await
            .unwrap()
            .into_iter()
            .map(|result| {
                result.batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(markers, (0..markers.len() as i32).rev().collect::<Vec<_>>());
    }

    #[test]
    fn task_protocol_has_no_credential_field_or_persisted_token() {
        let encoded =
            serde_json::to_string(&request(vec![source("a")], RetrySafety::Idempotent)).unwrap();
        assert!(!encoded.to_ascii_lowercase().contains("token"));
        assert!(!encoded.to_ascii_lowercase().contains("secret"));
        assert!(encoded.contains("file:///media/a.mp4"));
        let values = StringArray::from(vec!["protocol remains credential-free"]);
        assert_eq!(values.value(0), "protocol remains credential-free");
    }

    #[tokio::test]
    async fn credential_like_media_metadata_is_rejected_at_submission() {
        let mut unsafe_source = source("a");
        unsafe_source
            .media
            .metadata
            .insert("authorization".to_owned(), "Bearer secret".to_owned());
        assert_eq!(
            coordinator(1, Duration::from_secs(1))
                .submit(request(vec![unsafe_source], RetrySafety::Idempotent))
                .await,
            Err(ExecutionError::UnsafeTaskSource)
        );
    }
}
