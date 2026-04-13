#include "client.h"

using chronos::tso::v1::AllocateTimestampsRequest;
using chronos::tso::v1::AllocateTimestampsResponse;
using chronos::tso::v1::EnsureTimelineRequest;
using chronos::tso::v1::ErrorCode;
using chronos::tso::v1::ErrorDetail;
using chronos::tso::v1::GetTimelineRouteRequest;
using chronos::tso::v1::ResourceTier;
using chronos::tso::v1::TimelineRoute;
using chronos::tso::v1::TimelineRouteService;
using chronos::tso::v1::TimestampService;

Client::Client(const std::string& addr, const std::string& timeline_key)
    : route_channel_(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())),
      route_stub_(TimelineRouteService::NewStub(route_channel_)),
      timeline_key_(timeline_key) {
  EnsureRoute();
}

std::vector<chronos::tso::v1::TimestampRange> Client::AllocateTimestamps(uint32_t count) {
  auto route = EnsureRoute();
  std::shared_ptr<TimestampService::Stub> tso_stub;
  {
    std::lock_guard<std::mutex> lock(mu_);
    tso_stub = tso_stub_;
  }
  AllocateTimestampsResponse response;
  auto status = AllocateOnce(*tso_stub, route, &response, count);
  if (status.ok()) {
    return {response.ranges().begin(), response.ranges().end()};
  }
  if (!IsStaleRouteError(status)) {
    throw std::runtime_error(status.error_message());
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    route = RefreshRouteLocked();
    tso_stub = tso_stub_;
  }
  status = AllocateOnce(*tso_stub, route, &response, count);
  if (!status.ok()) {
    throw std::runtime_error(status.error_message());
  }
  return {response.ranges().begin(), response.ranges().end()};
}

TimelineRoute Client::EnsureRoute() {
  std::lock_guard<std::mutex> lock(mu_);
  return EnsureRouteLocked();
}

TimelineRoute Client::EnsureRouteLocked() {
  auto it = cache_.find(timeline_key_);
  if (it != cache_.end()) {
    return it->second;
  }
  grpc::ClientContext ctx;
  EnsureTimelineRequest request;
  request.set_timeline_key(timeline_key_);
  request.set_desired_resource_tier(ResourceTier::RESOURCE_TIER_SHARED);
  chronos::tso::v1::EnsureTimelineResponse response;
  auto status = route_stub_->EnsureTimeline(&ctx, request, &response);
  if (!status.ok()) {
    throw std::runtime_error(status.error_message());
  }
  return RefreshRouteLocked();
}

TimelineRoute Client::RefreshRouteLocked() {
  grpc::ClientContext ctx;
  GetTimelineRouteRequest request;
  request.set_timeline_key(timeline_key_);
  chronos::tso::v1::GetTimelineRouteResponse response;
  auto status = route_stub_->GetTimelineRoute(&ctx, request, &response);
  if (!status.ok()) {
    throw std::runtime_error(status.error_message());
  }
  auto route = response.route();
  tso_channel_ = grpc::CreateChannel(route.owner_worker_endpoint(), grpc::InsecureChannelCredentials());
  tso_stub_ = std::shared_ptr<TimestampService::Stub>(TimestampService::NewStub(tso_channel_).release());
  cache_[timeline_key_] = route;
  return route;
}

grpc::Status Client::AllocateOnce(
    TimestampService::Stub& tso_stub,
    const TimelineRoute& route,
    AllocateTimestampsResponse* response,
    uint32_t count) {
  grpc::ClientContext ctx;
  AllocateTimestampsRequest request;
  request.set_timeline_key(route.timeline_key());
  request.set_count(count);
  request.set_expected_epoch(route.epoch());
  request.set_expected_route_version(route.route_version());
  request.set_client_request_id(route.timeline_key() + "-" + std::to_string(request_id_++));
  return tso_stub.AllocateTimestamps(&ctx, request, response);
}

bool Client::IsStaleRouteError(const grpc::Status& status) {
  if (status.error_code() != grpc::StatusCode::FAILED_PRECONDITION) {
    return false;
  }
  ErrorDetail detail;
  if (!detail.ParseFromString(status.error_details())) {
    return false;
  }
  return detail.code() == ErrorCode::ERROR_CODE_NOT_TIMELINE_OWNER ||
         detail.code() == ErrorCode::ERROR_CODE_ROUTE_VERSION_MISMATCH ||
         detail.code() == ErrorCode::ERROR_CODE_EPOCH_MISMATCH;
}
