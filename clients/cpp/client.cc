#include "client.h"

#include <grpc/grpc_security_constants.h>

#include <chrono>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <utility>

using chronos::tso::v1::AllocateTimestampsRequest;
using chronos::tso::v1::AllocateTimestampsResponse;
using chronos::tso::v1::EnsureTimelineRequest;
using chronos::tso::v1::ErrorCode;
using chronos::tso::v1::ErrorDetail;
using chronos::tso::v1::GetTimelineRouteRequest;
using chronos::tso::v1::TimelineRoute;
using chronos::tso::v1::TimelineRouteService;
using chronos::tso::v1::TimestampService;

namespace {

std::atomic<uint64_t> g_client_scope_counter{1};
constexpr const char* kErrorDetailTypeUrl =
    "type.googleapis.com/chronos.tso.v1.ErrorDetail";

std::string MakeIdempotencyScope() {
  auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  auto counter = g_client_scope_counter.fetch_add(1);
  std::random_device random;
  std::ostringstream out;
  out << std::hex << now << "-" << counter << "-" << random() << "-" << random();
  return out.str();
}

Client::Config ConfigWithTransport(Client::TransportConfig transport_config) {
  Client::Config config;
  config.transport = std::move(transport_config);
  return config;
}

Client::Config ConfigWithTransport(
    Client::TransportConfig transport_config,
    bool idempotency_enabled) {
  auto config = ConfigWithTransport(std::move(transport_config));
  config.idempotency_enabled = idempotency_enabled;
  return config;
}

bool ReadVarint(const std::string& input, size_t* offset, uint64_t* value) {
  uint64_t result = 0;
  for (int shift = 0; shift <= 63; shift += 7) {
    if (*offset >= input.size()) {
      return false;
    }
    const uint8_t byte = static_cast<uint8_t>(input[(*offset)++]);
    result |= static_cast<uint64_t>(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      *value = result;
      return true;
    }
  }
  return false;
}

bool ReadLengthDelimited(
    const std::string& input,
    size_t* offset,
    std::string* value) {
  uint64_t length = 0;
  if (!ReadVarint(input, offset, &length)) {
    return false;
  }
  if (length > input.size() - *offset) {
    return false;
  }
  value->assign(input.data() + *offset, static_cast<size_t>(length));
  *offset += static_cast<size_t>(length);
  return true;
}

bool SkipField(const std::string& input, size_t* offset, uint32_t wire_type) {
  uint64_t varint_value = 0;
  std::string length_delimited_value;
  switch (wire_type) {
    case 0:
      return ReadVarint(input, offset, &varint_value);
    case 1:
      if (input.size() - *offset < 8) {
        return false;
      }
      *offset += 8;
      return true;
    case 2:
      return ReadLengthDelimited(input, offset, &length_delimited_value);
    case 5:
      if (input.size() - *offset < 4) {
        return false;
      }
      *offset += 4;
      return true;
    default:
      return false;
  }
}

bool IsErrorDetailTypeUrl(const std::string& type_url) {
  const std::string suffix = "/chronos.tso.v1.ErrorDetail";
  return type_url == kErrorDetailTypeUrl ||
         (type_url.size() >= suffix.size() &&
          type_url.compare(type_url.size() - suffix.size(), suffix.size(), suffix) == 0);
}

bool ParseErrorDetailAny(const std::string& input, ErrorDetail* detail) {
  size_t offset = 0;
  std::string type_url;
  std::string value;
  while (offset < input.size()) {
    uint64_t tag = 0;
    if (!ReadVarint(input, &offset, &tag)) {
      return false;
    }
    const uint32_t field_number = static_cast<uint32_t>(tag >> 3);
    const uint32_t wire_type = static_cast<uint32_t>(tag & 0x07);
    if (field_number == 1 && wire_type == 2) {
      if (!ReadLengthDelimited(input, &offset, &type_url)) {
        return false;
      }
    } else if (field_number == 2 && wire_type == 2) {
      if (!ReadLengthDelimited(input, &offset, &value)) {
        return false;
      }
    } else if (!SkipField(input, &offset, wire_type)) {
      return false;
    }
  }
  return IsErrorDetailTypeUrl(type_url) && detail->ParseFromString(value);
}

bool ParseRichErrorDetail(const std::string& input, ErrorDetail* detail) {
  size_t offset = 0;
  while (offset < input.size()) {
    uint64_t tag = 0;
    if (!ReadVarint(input, &offset, &tag)) {
      return false;
    }
    const uint32_t field_number = static_cast<uint32_t>(tag >> 3);
    const uint32_t wire_type = static_cast<uint32_t>(tag & 0x07);
    if (field_number == 3 && wire_type == 2) {
      std::string any;
      if (!ReadLengthDelimited(input, &offset, &any)) {
        return false;
      }
      if (ParseErrorDetailAny(any, detail)) {
        return true;
      }
    } else if (!SkipField(input, &offset, wire_type)) {
      return false;
    }
  }
  return false;
}

bool ParseStatusErrorDetail(const std::string& input, ErrorDetail* detail) {
  return ParseRichErrorDetail(input, detail) || detail->ParseFromString(input);
}

}  // namespace

Client::Client(const std::string& addr, const std::string& timeline_key)
    : Client(addr, timeline_key, TransportConfig{}) {}

Client::Client(
    const std::string& addr,
    const std::string& timeline_key,
    TransportConfig transport_config)
    : Client(addr, timeline_key, ConfigWithTransport(std::move(transport_config))) {}

Client::Client(
    const std::string& addr,
    const std::string& timeline_key,
    TransportConfig transport_config,
    bool idempotency_enabled)
    : Client(
          addr,
          timeline_key,
          ConfigWithTransport(std::move(transport_config), idempotency_enabled)) {}

Client::Client(
    const std::string& addr,
    const std::string& timeline_key,
    Config config)
    : route_channel_(nullptr),
      route_stub_(nullptr),
      timeline_key_(timeline_key),
      config_(std::move(config)),
      idempotency_scope_(MakeIdempotencyScope()) {
  route_channel_ = CreateChannel(addr);
  route_stub_ = TimelineRouteService::NewStub(route_channel_);
  EnsureRoute();
}

std::vector<chronos::tso::v1::TimestampRange> Client::AllocateTimestamps(uint32_t count) {
  auto snapshot = EnsureRoute();
  AllocateTimestampsResponse response;
  const auto client_request_id = NextClientRequestId();
  uint32_t stale_retries = 0;

  while (true) {
    auto status = AllocateOnce(
        *snapshot->tso_stub,
        snapshot->route,
        &response,
        count,
        client_request_id);
    if (status.ok()) {
      return {response.ranges().begin(), response.ranges().end()};
    }
    if (!IsStaleRouteError(status) || stale_retries >= config_.stale_route_retry_attempts) {
      throw std::runtime_error(status.error_message());
    }

    ++stale_retries;
    {
      std::lock_guard<std::mutex> lock(mu_);
      snapshot = RefreshRouteIfUnchangedLocked(snapshot);
    }
    SleepBeforeStaleRouteRetry();
    response.Clear();
  }
}

std::shared_ptr<const Client::RouteSnapshot> Client::EnsureRoute() {
  std::lock_guard<std::mutex> lock(mu_);
  return EnsureRouteLocked();
}

std::shared_ptr<const Client::RouteSnapshot> Client::EnsureRouteLocked() {
  if (route_snapshot_ != nullptr) {
    return route_snapshot_;
  }
  grpc::ClientContext ctx;
  ApplyRequestDeadline(&ctx);
  EnsureTimelineRequest request;
  request.set_timeline_key(timeline_key_);
  request.set_desired_resource_tier(config_.desired_resource_tier);
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
  return InstallRouteLocked(response.route());
}

std::shared_ptr<const Client::RouteSnapshot> Client::RefreshRouteIfUnchangedLocked(
    const std::shared_ptr<const RouteSnapshot>& observed) {
  if (route_snapshot_ != nullptr &&
      !SameRouteIdentity(route_snapshot_->route, observed->route)) {
    return route_snapshot_;
  }
  return RefreshRouteLocked();
}

std::shared_ptr<const Client::RouteSnapshot> Client::RefreshRouteLocked() {
  grpc::ClientContext ctx;
  ApplyRequestDeadline(&ctx);
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
  return InstallRouteLocked(route);
}

std::shared_ptr<const Client::RouteSnapshot> Client::InstallRouteLocked(
    const TimelineRoute& route) {
  if (route_snapshot_ != nullptr &&
      route.owner_worker_endpoint() ==
          route_snapshot_->route.owner_worker_endpoint()) {
    auto next = std::make_shared<RouteSnapshot>(RouteSnapshot{
        route,
        route_snapshot_->owner_channel,
        route_snapshot_->tso_stub,
    });
    route_snapshot_ = next;
    return next;
  }

  auto previous_channel = route_snapshot_ == nullptr
      ? nullptr
      : route_snapshot_->owner_channel;
  auto owner_channel = CreateChannel(route.owner_worker_endpoint());
  auto tso_stub = std::shared_ptr<TimestampService::Stub>(
      TimestampService::NewStub(owner_channel).release());
  auto next = std::make_shared<RouteSnapshot>(RouteSnapshot{
      route,
      std::move(owner_channel),
      std::move(tso_stub),
  });
  route_snapshot_ = next;
  RetainStaleOwnerChannelLocked(std::move(previous_channel));
  return next;
}

void Client::SleepBeforeStaleRouteRetry() const {
  if (config_.stale_route_retry_backoff_ms == 0) {
    return;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(config_.stale_route_retry_backoff_ms));
}

grpc::Status Client::AllocateOnce(
    TimestampService::Stub& tso_stub,
    const TimelineRoute& route,
    AllocateTimestampsResponse* response,
    uint32_t count,
    const std::string& client_request_id) {
  grpc::ClientContext ctx;
  ApplyRequestDeadline(&ctx);
  AllocateTimestampsRequest request;
  request.set_timeline_key(route.timeline_key());
  request.set_count(count);
  request.set_expected_epoch(route.epoch());
  request.set_expected_route_version(route.route_version());
  request.set_client_request_id(client_request_id);
  request.set_request_timeout_ms(config_.request_timeout_ms);
  return tso_stub.AllocateTimestamps(&ctx, request, response);
}

void Client::ApplyRequestDeadline(grpc::ClientContext* context) const {
  if (config_.request_timeout_ms == 0) {
    return;
  }
  context->set_deadline(
      std::chrono::system_clock::now() +
      std::chrono::milliseconds(config_.request_timeout_ms));
}

void Client::RetainStaleOwnerChannelLocked(std::shared_ptr<grpc::Channel> previous) {
  if (previous == nullptr) {
    return;
  }
  stale_tso_channels_.push_back(std::move(previous));
  while (stale_tso_channels_.size() > kMaxRetainedStaleOwnerChannels) {
    stale_tso_channels_.pop_front();
  }
}

std::string Client::NextClientRequestId() {
  if (!config_.idempotency_enabled) {
    return "";
  }
  return idempotency_scope_ + "-" + std::to_string(request_id_++);
}

bool Client::SameRouteIdentity(
    const TimelineRoute& left,
    const TimelineRoute& right) const {
  return left.timeline_key() == right.timeline_key() &&
         left.generator_id() == right.generator_id() &&
         left.owner_worker_endpoint() == right.owner_worker_endpoint() &&
         left.epoch() == right.epoch() &&
         left.route_version() == right.route_version() &&
         left.resource_tier() == right.resource_tier();
}

bool Client::IsStaleRouteError(const grpc::Status& status) {
  if (status.error_code() != grpc::StatusCode::FAILED_PRECONDITION) {
    return false;
  }
  ErrorDetail detail;
  if (!ParseStatusErrorDetail(status.error_details(), &detail)) {
    return false;
  }
  return detail.code() == ErrorCode::ERROR_CODE_NOT_TIMELINE_OWNER ||
         detail.code() == ErrorCode::ERROR_CODE_ROUTE_VERSION_MISMATCH ||
         detail.code() == ErrorCode::ERROR_CODE_EPOCH_MISMATCH;
}

std::shared_ptr<grpc::ChannelCredentials> Client::CreateChannelCredentials() const {
  if (config_.transport.insecure) {
    return grpc::InsecureChannelCredentials();
  }

  const bool has_private_key = !config_.transport.pem_private_key.empty();
  const bool has_cert_chain = !config_.transport.pem_cert_chain.empty();
  if (has_private_key != has_cert_chain) {
    throw std::invalid_argument(
        "Chronos TLS client certificate and private key must be configured together");
  }

  grpc::SslCredentialsOptions options;
  options.pem_root_certs = config_.transport.pem_root_certs;
  options.pem_private_key = config_.transport.pem_private_key;
  options.pem_cert_chain = config_.transport.pem_cert_chain;
  return grpc::SslCredentials(options);
}

std::shared_ptr<grpc::Channel> Client::CreateChannel(const std::string& endpoint) const {
  grpc::ChannelArguments arguments;
  if (!config_.transport.ssl_target_name_override.empty()) {
    arguments.SetString(
        GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, config_.transport.ssl_target_name_override);
  }
  return grpc::CreateCustomChannel(endpoint, CreateChannelCredentials(), arguments);
}
