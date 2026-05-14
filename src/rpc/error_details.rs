use prost::Message;
use prost_types::Any;
use tonic::Code;

use crate::proto::v1::ErrorDetail;

const ERROR_DETAIL_TYPE_URL: &str = "type.googleapis.com/chronos.tso.v1.ErrorDetail";

#[derive(Clone, PartialEq, Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

pub(crate) fn encode_error_detail_status(
    code: Code,
    message: impl Into<String>,
    detail: ErrorDetail,
) -> Vec<u8> {
    GoogleRpcStatus {
        code: code as i32,
        message: message.into(),
        details: vec![Any {
            type_url: ERROR_DETAIL_TYPE_URL.into(),
            value: detail.encode_to_vec(),
        }],
    }
    .encode_to_vec()
}

#[doc(hidden)]
pub fn decode_error_detail_from_status_details(details: &[u8]) -> Option<ErrorDetail> {
    if let Ok(status) = GoogleRpcStatus::decode(details) {
        for detail in status.details {
            if is_error_detail_type_url(&detail.type_url) {
                if let Ok(decoded) = ErrorDetail::decode(detail.value.as_slice()) {
                    return Some(decoded);
                }
            }
        }
    }

    ErrorDetail::decode(details).ok()
}

fn is_error_detail_type_url(type_url: &str) -> bool {
    type_url == ERROR_DETAIL_TYPE_URL || type_url.ends_with("/chronos.tso.v1.ErrorDetail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::v1::ErrorCode;

    #[test]
    fn rich_error_status_round_trips_error_detail() {
        let encoded = encode_error_detail_status(
            Code::FailedPrecondition,
            "stale route",
            ErrorDetail {
                code: ErrorCode::RouteVersionMismatch as i32,
                message: "stale route".into(),
                current_epoch: 0,
                current_route_version: 42,
                redirect_endpoint: "worker-b:50051".into(),
                action_blocker: 0,
                next_step: 0,
            },
        );

        let detail = decode_error_detail_from_status_details(&encoded).expect("detail decodes");

        assert_eq!(detail.code, ErrorCode::RouteVersionMismatch as i32);
        assert_eq!(detail.current_route_version, 42);
        assert_eq!(detail.redirect_endpoint, "worker-b:50051");
    }

    #[test]
    fn legacy_raw_error_detail_still_decodes() {
        let legacy = ErrorDetail {
            code: ErrorCode::NotTimelineOwner as i32,
            message: String::new(),
            current_epoch: 0,
            current_route_version: 0,
            redirect_endpoint: "worker-c:50051".into(),
            action_blocker: 0,
            next_step: 0,
        }
        .encode_to_vec();

        let detail = decode_error_detail_from_status_details(&legacy).expect("detail decodes");

        assert_eq!(detail.code, ErrorCode::NotTimelineOwner as i32);
        assert_eq!(detail.redirect_endpoint, "worker-c:50051");
    }
}
