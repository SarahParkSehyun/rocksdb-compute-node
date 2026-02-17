// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
//
// Standalone eRPC BloomCheck client test binary

#include "tools/bloomcheck_erpc_client_lib.h"

#include <cstdio>
#include <cstdlib>
#include <string>

int main(int argc, char** argv) {
  std::string local_uri = "10.1.1.2:31850";
  std::string server_uri = "10.1.1.1:31850";
  int numa_node = 1;
  uint8_t rpc_id = 0;
  uint8_t server_rpc_id = 0;
  int iters = 1;
  int warmup = 0;
  int print_each = 1;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.find("--local_uri=") == 0) {
      local_uri = arg.substr(12);
    } else if (arg.find("--server_uri=") == 0) {
      server_uri = arg.substr(13);
    } else if (arg.find("--numa_node=") == 0) {
      numa_node = std::atoi(arg.substr(12).c_str());
    } else if (arg.find("--rpc_id=") == 0) {
      // In thread-local mode, this works as base rpc_id.
      rpc_id = static_cast<uint8_t>(std::atoi(arg.substr(9).c_str()));
    } else if (arg.find("--server_rpc_id=") == 0) {
      server_rpc_id = static_cast<uint8_t>(std::atoi(arg.substr(16).c_str()));
    } else if (arg.find("--iters=") == 0) {
      iters = std::atoi(arg.substr(8).c_str());
    } else if (arg.find("--warmup=") == 0) {
      warmup = std::atoi(arg.substr(9).c_str());
    } else if (arg.find("--print_each=") == 0) {
      print_each = std::atoi(arg.substr(13).c_str());
    } else if (arg == "--help" || arg == "-h") {
      fprintf(stdout,
              "Usage: %s [options]\n"
              "Options:\n"
              "  --local_uri=URI      Client local URI (default: 10.1.1.2:31850)\n"
              "  --server_uri=URI     Server URI (default: 10.1.1.1:31850)\n"
              "  --numa_node=N        NUMA node (default: 1)\n"
              "  --rpc_id=N           Base rpc_id for thread-local allocation (default: 0)\n"
              "  --server_rpc_id=N    Server RPC ID (default: 0)\n"
              "  --iters=N            Number of iterations (default: 1)\n"
              "  --warmup=N           Warmup iterations (default: 0)\n"
              "  --print_each=N       Print each result (default: 1)\n"
              "\n"
              "Timeout envs:\n"
              "  BLOOMCHECK_ERPC_CONNECT_TIMEOUT_MS (default: 2000)\n"
              "  BLOOMCHECK_ERPC_RPC_TIMEOUT_MS     (default: 100)\n",
              argv[0]);
      return 0;
    }
  }

  ROCKSDB_NAMESPACE::ErpcBloomCheckClient client(local_uri, server_uri, numa_node, rpc_id,
                                                  server_rpc_id);

  if (!client.Init()) {
    fprintf(stderr, "[main] Failed to initialize client\n");
    return 1;
  }

  for (int i = 0; i < warmup; ++i) {
    uint64_t dummy_ts = 0;
    uint64_t dummy_rtt = 0;
    client.Noop(&dummy_ts, &dummy_rtt);
  }

  for (int i = 0; i < iters; ++i) {
    uint64_t server_ts_ns = 0;
    uint64_t rtt_ns = 0;
    bool ok = client.Noop(&server_ts_ns, &rtt_ns);
    if (print_each) {
      fprintf(stdout, "[NOOP] iter=%d ok=%d rtt_ns=%llu server_ts_ns=%llu\n", i + 1,
              ok ? 1 : 0, static_cast<unsigned long long>(rtt_ns),
              static_cast<unsigned long long>(server_ts_ns));
    }
  }

  return 0;
}
