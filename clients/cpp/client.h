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
  struct TransportConfig {
    bool insecure = false;
    std::string pem_root_certs;
    std::string pem_private_key;
    std::string pem_cert_chain;
    std::string ssl_target_name_override;
  };

  Client(const std::string& addr, const std::string& timeline_key);
  Client(
      const std::string& addr,
      const std::string& timeline_key,
      TransportConfig transport_config);
  Client(
      const std::string& addr,
      const std::string& timeline_key,
      TransportConfig transport_config,
      bool idempotency_enabled);
  std::vector<chronos::tso::v1::TimestampRange> AllocateTimestamps(uint32_t count);

 private:
  chronos::tso::v1::TimelineRoute EnsureRoute();
  chronos::tso::v1::TimelineRoute EnsureRouteLocked();
  chronos::tso::v1::TimelineRoute RefreshRouteLocked();
  grpc::Status AllocateOnce(
      chronos::tso::v1::TimestampService::Stub& tso_stub,
      const chronos::tso::v1::TimelineRoute& route,
      chronos::tso::v1::AllocateTimestampsResponse* response,
      uint32_t count,
      const std::string& client_request_id);
  bool IsStaleRouteError(const grpc::Status& status);
  std::string NextClientRequestId(const std::string& timeline_key);
  std::shared_ptr<grpc::ChannelCredentials> CreateChannelCredentials() const;
  std::shared_ptr<grpc::Channel> CreateChannel(const std::string& endpoint) const;

  std::shared_ptr<grpc::Channel> route_channel_;
  std::shared_ptr<grpc::Channel> tso_channel_;
  std::unique_ptr<chronos::tso::v1::TimelineRouteService::Stub> route_stub_;
  std::shared_ptr<chronos::tso::v1::TimestampService::Stub> tso_stub_;
  std::unordered_map<std::string, chronos::tso::v1::TimelineRoute> cache_;
  std::string timeline_key_;
  TransportConfig transport_config_;
  bool idempotency_enabled_;
  std::string idempotency_scope_;
  std::mutex mu_;
  std::atomic<uint64_t> request_id_{1};
};
