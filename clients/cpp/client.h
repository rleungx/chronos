#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "tso.grpc.pb.h"

class Client {
 public:
  Client(const std::string& addr, const std::string& timeline_key);
  std::vector<chronos::tso::v1::TimestampRange> AllocateTimestamps(uint32_t count);

 private:
  chronos::tso::v1::TimelineRoute EnsureRoute();
  chronos::tso::v1::TimelineRoute EnsureRouteLocked();
  chronos::tso::v1::TimelineRoute RefreshRouteLocked();
  grpc::Status AllocateOnce(
      const chronos::tso::v1::TimelineRoute& route,
      chronos::tso::v1::AllocateTimestampsResponse* response,
      uint32_t count);
  bool IsStaleRouteError(const grpc::Status& status);

  std::shared_ptr<grpc::Channel> route_channel_;
  std::shared_ptr<grpc::Channel> tso_channel_;
  std::unique_ptr<chronos::tso::v1::TimelineRouteService::Stub> route_stub_;
  std::unique_ptr<chronos::tso::v1::TimestampService::Stub> tso_stub_;
  std::unordered_map<std::string, chronos::tso::v1::TimelineRoute> cache_;
  std::string timeline_key_;
  std::mutex mu_;
  std::atomic<uint64_t> request_id_{1};
};
