#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "client.h"

using chronos::tso::v1::AllocateTimestampsRequest;
using chronos::tso::v1::AllocateTimestampsResponse;
using chronos::tso::v1::EnsureTimelineRequest;
using chronos::tso::v1::EnsureTimelineResponse;
using chronos::tso::v1::ErrorCode;
using chronos::tso::v1::ErrorDetail;
using chronos::tso::v1::GetTimelineRouteRequest;
using chronos::tso::v1::GetTimelineRouteResponse;
using chronos::tso::v1::ResourceTier;
using chronos::tso::v1::TimelineRoute;
using chronos::tso::v1::TimelineRouteService;
using chronos::tso::v1::TimestampRange;
using chronos::tso::v1::TimestampService;

namespace {

Client::TransportConfig InsecureTransport() {
  Client::TransportConfig config;
  config.insecure = true;
  return config;
}

struct RouteServiceImpl final : TimelineRouteService::Service {
  explicit RouteServiceImpl(std::shared_ptr<TimelineRoute> route) : route(std::move(route)) {}

  grpc::Status EnsureTimeline(
      grpc::ServerContext*,
      const EnsureTimelineRequest* request,
      EnsureTimelineResponse* response) override {
    route->set_timeline_key(request->timeline_key());
    last_desired_resource_tier = request->desired_resource_tier();
    response->mutable_route()->CopyFrom(*route);
    return grpc::Status::OK;
  }

  grpc::Status GetTimelineRoute(
      grpc::ServerContext*,
      const GetTimelineRouteRequest* request,
      GetTimelineRouteResponse* response) override {
    route->set_timeline_key(request->timeline_key());
    response->mutable_route()->CopyFrom(*route);
    return grpc::Status::OK;
  }

  std::shared_ptr<TimelineRoute> route;
  ResourceTier last_desired_resource_tier = ResourceTier::RESOURCE_TIER_UNSPECIFIED;
};

struct TimestampServiceImpl final : TimestampService::Service {
  explicit TimestampServiceImpl(std::shared_ptr<TimelineRoute> route, bool stale_once)
      : route(std::move(route)), stale_once(stale_once) {}

  grpc::Status AllocateTimestamps(
      grpc::ServerContext*,
      const AllocateTimestampsRequest* request,
      AllocateTimestampsResponse* response) override {
    ++allocate_calls;
    last_request_timeout_ms = request->request_timeout_ms();
    {
      std::lock_guard<std::mutex> lock(request_ids_mu);
      request_ids.push_back(request->client_request_id());
    }

    if (stale_once.exchange(false)) {
      route->set_route_version(route->route_version() + 1);
      ErrorDetail detail;
      detail.set_code(ErrorCode::ERROR_CODE_ROUTE_VERSION_MISMATCH);
      detail.set_current_route_version(route->route_version());
      detail.set_redirect_endpoint(route->owner_worker_endpoint());
      return grpc::Status(
          grpc::StatusCode::FAILED_PRECONDITION,
          "stale route",
          detail.SerializeAsString());
    }

    if (request->expected_epoch() != route->epoch() ||
        request->expected_route_version() != route->route_version()) {
      ErrorDetail detail;
      detail.set_code(ErrorCode::ERROR_CODE_ROUTE_VERSION_MISMATCH);
      detail.set_current_route_version(route->route_version());
      detail.set_redirect_endpoint(route->owner_worker_endpoint());
      return grpc::Status(
          grpc::StatusCode::FAILED_PRECONDITION,
          "stale route",
          detail.SerializeAsString());
    }

    response->set_timeline_key(request->timeline_key());
    response->set_generator_id(route->generator_id());
    response->set_epoch(route->epoch());
    response->set_route_version(route->route_version());
    auto* range = response->add_ranges();
    range->set_start_tso(100);
    range->set_end_tso(100 + request->count() - 1);
    return grpc::Status::OK;
  }

  std::shared_ptr<TimelineRoute> route;
  std::atomic<bool> stale_once;
  std::atomic<int> allocate_calls{0};
  std::atomic<uint32_t> last_request_timeout_ms{0};
  std::mutex request_ids_mu;
  std::vector<std::string> request_ids;
};

struct MissingEnsureRouteServiceImpl final : TimelineRouteService::Service {
  grpc::Status EnsureTimeline(
      grpc::ServerContext*,
      const EnsureTimelineRequest*,
      EnsureTimelineResponse*) override {
    return grpc::Status::OK;
  }

  grpc::Status GetTimelineRoute(
      grpc::ServerContext*,
      const GetTimelineRouteRequest*,
      GetTimelineRouteResponse*) override {
    return grpc::Status::OK;
  }
};

struct MissingGetRouteServiceImpl final : TimelineRouteService::Service {
  grpc::Status EnsureTimeline(
      grpc::ServerContext*,
      const EnsureTimelineRequest* request,
      EnsureTimelineResponse* response) override {
    auto* route = response->mutable_route();
    route->set_timeline_key(request->timeline_key());
    route->set_generator_id(7);
    route->set_owner_worker_endpoint("127.0.0.1:1");
    route->set_epoch(3);
    route->set_route_version(11);
    route->set_resource_tier(ResourceTier::RESOURCE_TIER_SHARED);
    return grpc::Status::OK;
  }

  grpc::Status GetTimelineRoute(
      grpc::ServerContext*,
      const GetTimelineRouteRequest*,
      GetTimelineRouteResponse*) override {
    return grpc::Status::OK;
  }
};

struct RunningServer {
  std::unique_ptr<grpc::Server> server;
  int port;
};

template <typename Service>
RunningServer StartServer(Service* service) {
  grpc::ServerBuilder builder;
  int selected_port = 0;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &selected_port);
  builder.RegisterService(service);
  return {builder.BuildAndStart(), selected_port};
}

TimelineRoute MakeRoute(const std::string& owner_endpoint) {
  TimelineRoute route;
  route.set_timeline_key("orders.primary");
  route.set_generator_id(7);
  route.set_owner_worker_endpoint(owner_endpoint);
  route.set_epoch(3);
  route.set_route_version(11);
  route.set_resource_tier(ResourceTier::RESOURCE_TIER_SHARED);
  return route;
}

void TestAllocateAgainstOwnerEndpoint() {
  auto route = std::make_shared<TimelineRoute>(MakeRoute("unused"));
  auto owner_service = TimestampServiceImpl(route, false);
  auto owner_server = StartServer(&owner_service);
  route->set_owner_worker_endpoint("127.0.0.1:" + std::to_string(owner_server.port));

  auto route_service = RouteServiceImpl(route);
  auto route_server = StartServer(&route_service);

  Client client(
      "127.0.0.1:" + std::to_string(route_server.port),
      "orders.primary",
      InsecureTransport());
  auto ranges = client.AllocateTimestamps(1);
  if (ranges.size() != 1 || ranges.front().start_tso() != 100) {
    throw std::runtime_error("allocate against owner endpoint failed");
  }
  if (owner_service.allocate_calls.load() != 1) {
    throw std::runtime_error("owner endpoint was not used for allocation");
  }
  {
    std::lock_guard<std::mutex> lock(owner_service.request_ids_mu);
    if (owner_service.request_ids.size() != 1 || !owner_service.request_ids[0].empty()) {
      throw std::runtime_error("default allocation should omit client request id");
    }
  }
}

void TestRefreshesStaleRouteAndRetries() {
  auto route = std::make_shared<TimelineRoute>(MakeRoute("unused"));
  auto owner_service = TimestampServiceImpl(route, true);
  auto owner_server = StartServer(&owner_service);
  route->set_owner_worker_endpoint("127.0.0.1:" + std::to_string(owner_server.port));

  auto route_service = RouteServiceImpl(route);
  auto route_server = StartServer(&route_service);

  Client client(
      "127.0.0.1:" + std::to_string(route_server.port),
      "orders.primary",
      InsecureTransport(),
      true);
  auto ranges = client.AllocateTimestamps(1);
  if (ranges.size() != 1 || ranges.front().start_tso() != 100) {
    throw std::runtime_error("stale route retry failed");
  }
  if (owner_service.allocate_calls.load() != 2) {
    throw std::runtime_error("expected one stale attempt and one retry");
  }
  {
    std::lock_guard<std::mutex> lock(owner_service.request_ids_mu);
    if (owner_service.request_ids.size() != 2 ||
        owner_service.request_ids[0].empty() ||
        owner_service.request_ids[0] != owner_service.request_ids[1]) {
      throw std::runtime_error("retry did not reuse logical request id");
    }
  }
}

void TestClientConfigFlowsIntoRequests() {
  auto route = std::make_shared<TimelineRoute>(MakeRoute("unused"));
  auto owner_service = TimestampServiceImpl(route, false);
  auto owner_server = StartServer(&owner_service);
  route->set_owner_worker_endpoint("127.0.0.1:" + std::to_string(owner_server.port));

  auto route_service = RouteServiceImpl(route);
  auto route_server = StartServer(&route_service);

  Client::Config config;
  config.transport = InsecureTransport();
  config.desired_resource_tier = ResourceTier::RESOURCE_TIER_WARM;
  config.request_timeout_ms = 1500;
  config.stale_route_retry_attempts = 2;
  config.stale_route_retry_backoff_ms = 0;
  config.idempotency_enabled = true;

  Client client(
      "127.0.0.1:" + std::to_string(route_server.port),
      "orders.primary",
      std::move(config));
  auto ranges = client.AllocateTimestamps(1);
  if (ranges.size() != 1 || ranges.front().start_tso() != 100) {
    throw std::runtime_error("configured client allocation failed");
  }
  if (route_service.last_desired_resource_tier != ResourceTier::RESOURCE_TIER_WARM) {
    throw std::runtime_error("configured desired resource tier was not sent");
  }
  if (owner_service.last_request_timeout_ms.load() != 1500) {
    throw std::runtime_error("configured request timeout was not sent");
  }
  {
    std::lock_guard<std::mutex> lock(owner_service.request_ids_mu);
    if (owner_service.request_ids.size() != 1 || owner_service.request_ids[0].empty()) {
      throw std::runtime_error("configured idempotency did not send request id");
    }
  }
}

void TestIdempotentClientsUseDistinctRequestIds() {
  auto route = std::make_shared<TimelineRoute>(MakeRoute("unused"));
  auto owner_service = TimestampServiceImpl(route, false);
  auto owner_server = StartServer(&owner_service);
  route->set_owner_worker_endpoint("127.0.0.1:" + std::to_string(owner_server.port));

  auto route_service = RouteServiceImpl(route);
  auto route_server = StartServer(&route_service);

  Client first(
      "127.0.0.1:" + std::to_string(route_server.port),
      "orders.primary",
      InsecureTransport(),
      true);
  Client second(
      "127.0.0.1:" + std::to_string(route_server.port),
      "orders.primary",
      InsecureTransport(),
      true);
  auto first_ranges = first.AllocateTimestamps(1);
  auto second_ranges = second.AllocateTimestamps(1);
  if (first_ranges.size() != 1 || second_ranges.size() != 1) {
    throw std::runtime_error("idempotent client allocation failed");
  }
  {
    std::lock_guard<std::mutex> lock(owner_service.request_ids_mu);
    if (owner_service.request_ids.size() != 2 ||
        owner_service.request_ids[0].empty() ||
        owner_service.request_ids[1].empty() ||
        owner_service.request_ids[0] == owner_service.request_ids[1]) {
      throw std::runtime_error("independent clients reused a request id");
    }
  }
}

void TestRejectsPartialClientIdentity() {
  try {
    Client::TransportConfig config;
    config.pem_cert_chain = "cert";
    Client client(
        "127.0.0.1:1",
        "orders.primary",
        config);
    throw std::runtime_error("expected partial client identity to be rejected");
  } catch (const std::invalid_argument&) {
  }
}

void TestRejectsMissingEnsureRoute() {
  auto service = MissingEnsureRouteServiceImpl();
  auto server = StartServer(&service);

  try {
    Client client(
        "127.0.0.1:" + std::to_string(server.port),
        "orders.primary",
        InsecureTransport());
    throw std::runtime_error("expected missing ensure route to be rejected");
  } catch (const std::runtime_error& ex) {
    if (std::string(ex.what()).find("no route from EnsureTimeline") == std::string::npos) {
      throw;
    }
  }
}

void TestRejectsMissingRefreshedRoute() {
  auto service = MissingGetRouteServiceImpl();
  auto server = StartServer(&service);

  try {
    Client client(
        "127.0.0.1:" + std::to_string(server.port),
        "orders.primary",
        InsecureTransport());
    throw std::runtime_error("expected missing refreshed route to be rejected");
  } catch (const std::runtime_error& ex) {
    if (std::string(ex.what()).find("no route from GetTimelineRoute") == std::string::npos) {
      throw;
    }
  }
}

}  // namespace

int main() {
  try {
    TestAllocateAgainstOwnerEndpoint();
    TestRefreshesStaleRouteAndRetries();
    TestClientConfigFlowsIntoRequests();
    TestIdempotentClientsUseDistinctRequestIds();
    TestRejectsPartialClientIdentity();
    TestRejectsMissingEnsureRoute();
    TestRejectsMissingRefreshedRoute();
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }
}
