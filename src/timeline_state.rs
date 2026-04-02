use crate::metadata::TimelineRecord;
use crate::runtime::TimelineState;

pub(crate) fn build_timeline_state(
    timeline_record: &TimelineRecord,
    revision: u64,
    recovered_last_issued_tso: Option<u64>,
) -> TimelineState {
    TimelineState {
        route: timeline_record.route.clone(),
        state: timeline_record.state,
        last_issued_tso: recovered_last_issued_tso,
        last_graceful_issued: timeline_record.last_graceful_issued,
        revision,
    }
}
