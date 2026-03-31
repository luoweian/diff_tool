#pragma once
// Stub header for local build — replaced by real DatabusClient in production
#include <memory>
#include <string>

class DatabusClient {
public:
    explicit DatabusClient(const std::string& /*topic*/) {}
    int send(const char* /*payload*/, const std::string& /*request_id*/) { return 0; }
};
