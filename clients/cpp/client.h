#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
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

  struct Config {
    chronos::tso::v1::ResourceTier desired_resource_tier =
        chronos::tso::v1::RESOURCE_TIER_SHARED;
    uint32_t request_timeout_ms = 0;
    uint32_t stale_route_retry_attempts = 3;
    uint64_t stale_route_retry_backoff_ms = 5;
    bool idempotency_enabled = false;
    TransportConfig transport;
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
  Client(
      const std::string& addr,
      const std::string& timeline_key,
      Config config);
  std::vector<chronos::tso::v1::TimestampRange> AllocateTimestamps(uint32_t count);

 private:
  chronos::tso::v1::TimelineRoute EnsureRoute();
  chronos::tso::v1::TimelineRoute EnsureRouteLocked();
  chronos::tso::v1::TimelineRoute RefreshRouteIfUnchangedLocked(
      const chronos::tso::v1::TimelineRoute& observed_route);
  chronos::tso::v1::TimelineRoute RefreshRouteLocked();
  chronos::tso::v1::TimelineRoute InstallRouteLocked(
      const chronos::tso::v1::TimelineRoute& route);
  void SleepBeforeStaleRouteRetry() const;
  grpc::Status AllocateOnce(
      chronos::tso::v1::TimestampService::Stub& tso_stub,
      const chronos::tso::v1::TimelineRoute& route,
      chronos::tso::v1::AllocateTimestampsResponse* response,
      uint32_t count,
      const std::string& client_request_id);
  bool IsStaleRouteError(const grpc::Status& status);
  bool SameRouteIdentity(
      const chronos::tso::v1::TimelineRoute& left,
      const chronos::tso::v1::TimelineRoute& right) const;
  std::string NextClientRequestId(const std::string& timeline_key);
  std::shared_ptr<grpc::ChannelCredentials> CreateChannelCredentials() const;
  std::shared_ptr<grpc::Channel> CreateChannel(const std::string& endpoint) const;

  std::shared_ptr<grpc::Channel> route_channel_;
  std::shared_ptr<grpc::Channel> tso_channel_;
  std::unique_ptr<chronos::tso::v1::TimelineRouteService::Stub> route_stub_;
  std::shared_ptr<chronos::tso::v1::TimestampService::Stub> tso_stub_;
  chronos::tso::v1::TimelineRoute route_;
  bool has_route_ = false;
  std::string timeline_key_;
  Config config_;
  std::string idempotency_scope_;
  std::mutex mu_;
  std::atomic<uint64_t> request_id_{1};
};
