#include "client.h"

#include <iostream>

int main() {
  Client::Config config;
  config.transport.insecure = true;

  Client client("127.0.0.1:50051", "orders.primary", config);
  const auto ranges = client.AllocateTimestamps(1);

  std::cout << "tso=" << ranges.front().start_tso() << std::endl;
  return 0;
}
