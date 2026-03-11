use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::Mutex;

use crate::{
    metrics, AllocateTimestampsRequest, AllocateTimestampsResponse, TsoDataPlane, TsoError,
};

#[derive(Clone)]
pub struct TimelineScopedAllocator {
    data_plane: TsoDataPlane,
    timeline_serializers: Arc<DashMap<String, Arc<TimelineSerializer>>>,
    serializer_slots: Arc<AtomicUsize>,
    max_serializers: usize,
}

struct TimelineSerializer {
    serialize: Mutex<()>,
    active_references: AtomicUsize,
}

struct TimelineSerializerLease {
    serializer: Arc<TimelineSerializer>,
}

impl Drop for TimelineSerializerLease {
    fn drop(&mut self) {
        self.serializer
            .active_references
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl TimelineScopedAllocator {
    pub fn new(data_plane: TsoDataPlane) -> Self {
        let max_serializers = data_plane.max_timeline_proxy_lanes();
        Self {
            data_plane,
            timeline_serializers: Arc::new(DashMap::new()),
            serializer_slots: Arc::new(AtomicUsize::new(0)),
            max_serializers,
        }
    }

    pub async fn allocate_timestamps(
        &self,
        request: AllocateTimestampsRequest,
    ) -> Result<AllocateTimestampsResponse, TsoError> {
        let serializer_lease = self.serializer_for(request.timeline_key.as_str())?;
        let timer = metrics::TSO_TIMELINE_PROXY_WAIT.start_timer();
        let _guard = serializer_lease.serializer.serialize.lock().await;
        timer.observe_duration();
        self.data_plane.allocate_timestamps(request).await
    }

    pub async fn allocate_timestamps_with_timeout(
        &self,
        request: AllocateTimestampsRequest,
        timeout_ms: u32,
    ) -> Result<AllocateTimestampsResponse, TimelineProxyError> {
        if timeout_ms == 0 {
            return self
                .allocate_timestamps(request)
                .await
                .map_err(TimelineProxyError::Tso);
        }

        tokio::time::timeout(
            Duration::from_millis(timeout_ms as u64),
            self.allocate_timestamps(request),
        )
        .await
        .map_err(|_| {
            metrics::TSO_TIMELINE_PROXY_TIMEOUT_TOTAL.inc();
            TimelineProxyError::TimedOut
        })?
        .map_err(TimelineProxyError::Tso)
    }

    fn serializer_for(&self, timeline_key: &str) -> Result<TimelineSerializerLease, TsoError> {
        if let Some(existing) = self.timeline_serializers.get(timeline_key) {
            let serializer = existing.clone();
            serializer.active_references.fetch_add(1, Ordering::AcqRel);
            return Ok(TimelineSerializerLease { serializer });
        }

        loop {
            let current = self.serializer_slots.load(Ordering::Acquire);
            if current >= self.max_serializers {
                self.prune_idle_serializers();
                if self.serializer_slots.load(Ordering::Acquire) >= self.max_serializers {
                    metrics::TSO_TIMELINE_PROXY_SATURATED_TOTAL.inc();
                    return Err(TsoError::TimelineIngressSaturated {
                        timeline_key: timeline_key.to_owned(),
                        max_lanes: self.max_serializers,
                    });
                }
                continue;
            }

            if self
                .serializer_slots
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            match self.timeline_serializers.entry(timeline_key.to_owned()) {
                Entry::Occupied(entry) => {
                    let serializer = entry.get().clone();
                    serializer.active_references.fetch_add(1, Ordering::AcqRel);
                    self.serializer_slots.fetch_sub(1, Ordering::AcqRel);
                    return Ok(TimelineSerializerLease { serializer });
                }
                Entry::Vacant(entry) => {
                    let serializer = Arc::new(TimelineSerializer {
                        serialize: Mutex::new(()),
                        active_references: AtomicUsize::new(1),
                    });
                    entry.insert(serializer.clone());
                    metrics::TSO_TIMELINE_PROXY_LANES
                        .set(self.timeline_serializers.len() as i64);
                    return Ok(TimelineSerializerLease { serializer });
                }
            }
        }
    }

    fn prune_idle_serializers(&self) {
        let idle_keys: Vec<String> = self
            .timeline_serializers
            .iter()
            .filter_map(|entry| {
                (entry.value().active_references.load(Ordering::Acquire) == 0)
                    .then(|| entry.key().clone())
            })
            .collect();

        for key in idle_keys {
            if let Entry::Occupied(entry) = self.timeline_serializers.entry(key) {
                if entry.get().active_references.load(Ordering::Acquire) == 0 {
                    entry.remove();
                    self.serializer_slots.fetch_sub(1, Ordering::AcqRel);
                    metrics::TSO_TIMELINE_PROXY_LANES
                        .set(self.timeline_serializers.len() as i64);
                }
            }
        }
    }

    pub fn serializer_count(&self) -> usize {
        self.timeline_serializers.len()
    }

    pub fn max_serializers(&self) -> usize {
        self.max_serializers
    }
}

#[derive(Debug)]
pub enum TimelineProxyError {
    Tso(TsoError),
    TimedOut,
}
