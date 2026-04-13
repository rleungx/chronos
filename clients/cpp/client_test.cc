#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>

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

struct RouteServiceImpl final : TimelineRouteService::Service {
  explicit RouteServiceImpl(std::shared_ptr<TimelineRoute> route) : route(std::move(route)) {}

  grpc::Status EnsureTimeline(
      grpc::ServerContext*,
      const EnsureTimelineRequest* request,
      EnsureTimelineResponse* response) override {
    route->set_timeline_key(request->timeline_key());
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
};

struct TimestampServiceImpl final : TimestampService::Service {
  explicit TimestampServiceImpl(std::shared_ptr<TimelineRoute> route, bool stale_once)
      : route(std::move(route)), stale_once(stale_once) {}

  grpc::Status AllocateTimestamps(
      grpc::ServerContext*,
      const AllocateTimestampsRequest* request,
      AllocateTimestampsResponse* response) override {
    ++allocate_calls;

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

  Client client("127.0.0.1:" + std::to_string(route_server.port), "orders.primary");
  auto ranges = client.AllocateTimestamps(1);
  if (ranges.size() != 1 || ranges.front().start_tso() != 100) {
    throw std::runtime_error("allocate against owner endpoint failed");
  }
  if (owner_service.allocate_calls.load() != 1) {
    throw std::runtime_error("owner endpoint was not used for allocation");
  }
}

void TestRefreshesStaleRouteAndRetries() {
  auto route = std::make_shared<TimelineRoute>(MakeRoute("unused"));
  auto owner_service = TimestampServiceImpl(route, true);
  auto owner_server = StartServer(&owner_service);
  route->set_owner_worker_endpoint("127.0.0.1:" + std::to_string(owner_server.port));

  auto route_service = RouteServiceImpl(route);
  auto route_server = StartServer(&route_service);

  Client client("127.0.0.1:" + std::to_string(route_server.port), "orders.primary");
  auto ranges = client.AllocateTimestamps(1);
  if (ranges.size() != 1 || ranges.front().start_tso() != 100) {
    throw std::runtime_error("stale route retry failed");
  }
  if (owner_service.allocate_calls.load() != 2) {
    throw std::runtime_error("expected one stale attempt and one retry");
  }
}

}  // namespace

int main() {
  try {
    TestAllocateAgainstOwnerEndpoint();
    TestRefreshesStaleRouteAndRetries();
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }
}
