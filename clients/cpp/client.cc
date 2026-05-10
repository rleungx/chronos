#include "client.h"

#include <grpc/grpc_security_constants.h>

#include <chrono>
#include <random>
#include <sstream>

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

namespace {

std::atomic<uint64_t> g_client_scope_counter{1};

std::string MakeIdempotencyScope() {
  auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  auto counter = g_client_scope_counter.fetch_add(1);
  std::random_device random;
  std::ostringstream out;
  out << std::hex << now << "-" << counter << "-" << random() << "-" << random();
  return out.str();
}

}  // namespace

Client::Client(const std::string& addr, const std::string& timeline_key)
    : Client(addr, timeline_key, TransportConfig{}) {}

Client::Client(
    const std::string& addr,
    const std::string& timeline_key,
    TransportConfig transport_config)
    : Client(addr, timeline_key, std::move(transport_config), false) {}

Client::Client(
    const std::string& addr,
    const std::string& timeline_key,
    TransportConfig transport_config,
    bool idempotency_enabled)
    : route_channel_(nullptr),
      route_stub_(nullptr),
      timeline_key_(timeline_key),
      transport_config_(std::move(transport_config)),
      idempotency_enabled_(idempotency_enabled),
      idempotency_scope_(MakeIdempotencyScope()) {
  route_channel_ = CreateChannel(addr);
  route_stub_ = TimelineRouteService::NewStub(route_channel_);
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
  const auto client_request_id = NextClientRequestId(route.timeline_key());
  auto status = AllocateOnce(*tso_stub, route, &response, count, client_request_id);
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
  status = AllocateOnce(*tso_stub, route, &response, count, client_request_id);
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
  if (!response.has_route()) {
    throw std::runtime_error("chronos returned no route from EnsureTimeline");
  }
  if (response.route().owner_worker_endpoint().empty()) {
    throw std::runtime_error(
        "chronos returned route with empty owner endpoint from EnsureTimeline");
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
  if (!response.has_route()) {
    throw std::runtime_error("chronos returned no route from GetTimelineRoute");
  }
  auto route = response.route();
  if (route.owner_worker_endpoint().empty()) {
    throw std::runtime_error(
        "chronos returned route with empty owner endpoint from GetTimelineRoute");
  }
  tso_channel_ = CreateChannel(route.owner_worker_endpoint());
  tso_stub_ = std::shared_ptr<TimestampService::Stub>(TimestampService::NewStub(tso_channel_).release());
  cache_[timeline_key_] = route;
  return route;
}

grpc::Status Client::AllocateOnce(
    TimestampService::Stub& tso_stub,
    const TimelineRoute& route,
    AllocateTimestampsResponse* response,
    uint32_t count,
    const std::string& client_request_id) {
  grpc::ClientContext ctx;
  AllocateTimestampsRequest request;
  request.set_timeline_key(route.timeline_key());
  request.set_count(count);
  request.set_expected_epoch(route.epoch());
  request.set_expected_route_version(route.route_version());
  request.set_client_request_id(client_request_id);
  return tso_stub.AllocateTimestamps(&ctx, request, response);
}

std::string Client::NextClientRequestId(const std::string& timeline_key) {
  if (!idempotency_enabled_) {
    return "";
  }
  return timeline_key + "-" + idempotency_scope_ + "-" + std::to_string(request_id_++);
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

std::shared_ptr<grpc::ChannelCredentials> Client::CreateChannelCredentials() const {
  if (transport_config_.insecure) {
    return grpc::InsecureChannelCredentials();
  }

  const bool has_private_key = !transport_config_.pem_private_key.empty();
  const bool has_cert_chain = !transport_config_.pem_cert_chain.empty();
  if (has_private_key != has_cert_chain) {
    throw std::invalid_argument(
        "Chronos TLS client certificate and private key must be configured together");
  }

  grpc::SslCredentialsOptions options;
  options.pem_root_certs = transport_config_.pem_root_certs;
  options.pem_private_key = transport_config_.pem_private_key;
  options.pem_cert_chain = transport_config_.pem_cert_chain;
  return grpc::SslCredentials(options);
}

std::shared_ptr<grpc::Channel> Client::CreateChannel(const std::string& endpoint) const {
  grpc::ChannelArguments arguments;
  if (!transport_config_.ssl_target_name_override.empty()) {
    arguments.SetString(
        GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, transport_config_.ssl_target_name_override);
  }
  return grpc::CreateCustomChannel(endpoint, CreateChannelCredentials(), arguments);
}
