use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::proto::v1::{
    GetTimelineStatusRequest, GetTimelineStatusResponse, ListTimelineStatusesRequest,
    ListTimelineStatusesResponse, TimelineState as ProtoTimelineState,
};
use crate::{TimelineLifecycleState, TsoControlPlane};

use super::{status_mapping, translation};

const DEFAULT_TIMELINE_STATUS_PAGE_SIZE: usize = 100;
const MAX_TIMELINE_STATUS_PAGE_SIZE: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct TimelineStatusPageToken {
    pub(super) version: u8,
    pub(super) states: Vec<i32>,
    pub(super) owner_worker_endpoint: Option<String>,
    pub(super) last_timeline_key: String,
}

#[derive(Debug)]
struct NormalizedTimelineStatusListRequest {
    states: Vec<TimelineLifecycleState>,
    state_codes: Vec<i32>,
    owner_worker_endpoint: Option<String>,
    page_size: usize,
    start_after_timeline_key: Option<String>,
}

pub(super) async fn get_timeline_status_response(
    control_plane: &TsoControlPlane,
    request: GetTimelineStatusRequest,
    route_cache_ttl_ms: u32,
) -> Result<GetTimelineStatusResponse, Status> {
    let status = control_plane
        .get_timeline_status(&request.timeline_key)
        .await
        .map_err(translation::map_tso_error)?;

    Ok(status_mapping::get_timeline_status_response(
        status,
        route_cache_ttl_ms,
    ))
}

pub(super) async fn list_timeline_statuses_response(
    control_plane: &TsoControlPlane,
    request: ListTimelineStatusesRequest,
    route_cache_ttl_ms: u32,
) -> Result<ListTimelineStatusesResponse, Status> {
    let request = normalize_timeline_status_list_request(request).map_err(|status| *status)?;
    let page = control_plane
        .list_timeline_statuses(
            &request.states,
            request.owner_worker_endpoint.as_deref(),
            request.start_after_timeline_key.as_deref(),
            request.page_size,
        )
        .await
        .map_err(translation::map_tso_error)?;

    let next_page_token = match page.next_start_after {
        Some(last_timeline_key) => encode_timeline_status_page_token(
            request.state_codes,
            request.owner_worker_endpoint,
            last_timeline_key,
        )
        .map_err(|status| *status)?,
        None => String::new(),
    };

    Ok(status_mapping::list_timeline_statuses_response(
        page.statuses,
        next_page_token,
        route_cache_ttl_ms,
    ))
}

fn normalize_timeline_status_list_request(
    request: ListTimelineStatusesRequest,
) -> Result<NormalizedTimelineStatusListRequest, Box<Status>> {
    let mut state_codes = request.states;
    state_codes.sort_unstable();
    state_codes.dedup();

    let mut states = Vec::with_capacity(state_codes.len());
    for state in &state_codes {
        states.push(decode_timeline_state_filter(*state)?);
    }

    let owner_worker_endpoint = request.owner_worker_endpoint;
    if owner_worker_endpoint.as_deref() == Some("") {
        return Err(Box::new(Status::invalid_argument(
            "owner_worker_endpoint must not be empty when present",
        )));
    }

    let page_size = match request.page_size {
        0 => DEFAULT_TIMELINE_STATUS_PAGE_SIZE,
        size => usize::min(size as usize, MAX_TIMELINE_STATUS_PAGE_SIZE),
    };

    let start_after_timeline_key = if request.page_token.is_empty() {
        None
    } else {
        let token: TimelineStatusPageToken =
            serde_json::from_str(&request.page_token).map_err(|_| {
                Box::new(Status::invalid_argument(
                    "page_token is malformed or not issued by this server",
                ))
            })?;
        if token.version != 1
            || token.states != state_codes
            || token.owner_worker_endpoint != owner_worker_endpoint
            || token.last_timeline_key.is_empty()
        {
            return Err(Box::new(Status::invalid_argument(
                "page_token is stale or does not match the current filter shape",
            )));
        }
        Some(token.last_timeline_key)
    };

    Ok(NormalizedTimelineStatusListRequest {
        states,
        state_codes,
        owner_worker_endpoint,
        page_size,
        start_after_timeline_key,
    })
}

fn encode_timeline_status_page_token(
    state_codes: Vec<i32>,
    owner_worker_endpoint: Option<String>,
    last_timeline_key: String,
) -> Result<String, Box<Status>> {
    serde_json::to_string(&TimelineStatusPageToken {
        version: 1,
        states: state_codes,
        owner_worker_endpoint,
        last_timeline_key,
    })
    .map_err(|error| {
        Box::new(Status::internal(format!(
            "failed to encode page token: {error}"
        )))
    })
}

fn decode_timeline_state_filter(state: i32) -> Result<TimelineLifecycleState, Box<Status>> {
    match ProtoTimelineState::try_from(state) {
        Ok(ProtoTimelineState::Creating) => Ok(TimelineLifecycleState::Creating),
        Ok(ProtoTimelineState::Active) => Ok(TimelineLifecycleState::Active),
        Ok(ProtoTimelineState::Draining) => Ok(TimelineLifecycleState::Draining),
        Ok(ProtoTimelineState::Locked) => Ok(TimelineLifecycleState::Locked),
        Ok(ProtoTimelineState::Recovering) => Ok(TimelineLifecycleState::Recovering),
        Ok(ProtoTimelineState::Unspecified) | Err(_) => Err(Box::new(Status::invalid_argument(
            "states must not contain TIMELINE_STATE_UNSPECIFIED",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_list_request_dedups_states_and_applies_default_page_size() {
        let request = normalize_timeline_status_list_request(ListTimelineStatusesRequest {
            states: vec![
                ProtoTimelineState::Recovering as i32,
                ProtoTimelineState::Draining as i32,
                ProtoTimelineState::Recovering as i32,
            ],
            owner_worker_endpoint: Some("worker-a:50051".into()),
            page_size: 0,
            page_token: String::new(),
        })
        .expect("request should normalize");

        assert_eq!(
            request.states,
            vec![
                TimelineLifecycleState::Draining,
                TimelineLifecycleState::Recovering,
            ]
        );
        assert_eq!(
            request.state_codes,
            vec![
                ProtoTimelineState::Draining as i32,
                ProtoTimelineState::Recovering as i32,
            ]
        );
        assert_eq!(request.page_size, DEFAULT_TIMELINE_STATUS_PAGE_SIZE);
        assert_eq!(
            request.owner_worker_endpoint.as_deref(),
            Some("worker-a:50051")
        );
    }

    #[test]
    fn normalize_list_request_rejects_filter_mismatched_page_token() {
        let page_token = serde_json::to_string(&TimelineStatusPageToken {
            version: 1,
            states: vec![ProtoTimelineState::Active as i32],
            owner_worker_endpoint: Some("worker-a:50051".into()),
            last_timeline_key: "timeline-a".into(),
        })
        .expect("page token should encode");

        let error = normalize_timeline_status_list_request(ListTimelineStatusesRequest {
            states: vec![ProtoTimelineState::Active as i32],
            owner_worker_endpoint: Some("worker-b:50051".into()),
            page_size: 1,
            page_token,
        })
        .expect_err("request should be rejected");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "page_token is stale or does not match the current filter shape"
        );
    }
}
