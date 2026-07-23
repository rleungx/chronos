#include <chronos/client.h>

int main() {
  Client::Config config;
  config.transport.insecure = true;
  return config.request_timeout_ms == 0 ? 1 : 0;
}
