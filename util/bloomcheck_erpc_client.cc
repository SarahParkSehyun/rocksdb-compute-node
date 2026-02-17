// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "util/bloomcheck_erpc_client.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <time.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tools/bloomcheck.pb.h"

// eRPC includes
#if __has_include("erpc/transport_impl/infiniband/ib_transport.h")
#include "erpc/transport_impl/infiniband/ib_transport.h"
#include "erpc/nexus.h"
#include "erpc/rpc.h"
#elif __has_include("transport_impl/infiniband/ib_transport.h")
#include "transport_impl/infiniband/ib_transport.h"
#include "nexus.h"
#include "rpc.h"
#else
#error "Cannot find eRPC headers. Check ERPC_ROOT include path."
#endif

namespace ROCKSDB_NAMESPACE {
namespace {

uint64_t NowNs() {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0;
  }
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
         static_cast<uint64_t>(ts.tv_nsec);
}

int ParseEnvMs(const char* name, int default_value) {
  const char* val = std::getenv(name);
  if (val == nullptr || val[0] == '\0') {
    return default_value;
  }
  char* end = nullptr;
  long parsed = std::strtol(val, &end, 10);
  if (end == val || parsed <= 0 || parsed > 600000) {
    return default_value;
  }
  return static_cast<int>(parsed);
}

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void*) {
  // Client-side Noop path does not need session event hooks.
}

struct ProcessContext;
struct ThreadContext;

struct InflightNoopCall {
  ThreadContext* tc = nullptr;
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
  uint64_t send_ns = 0;
  uint64_t rtt_ns = 0;
  uint64_t server_ts_ns = 0;
  bool parse_ok = false;
  std::atomic<bool> done{false};
};

struct ProcessContext {
  uint64_t client_id = 0;
  std::unique_ptr<erpc::Nexus> nexus;
  std::string local_uri;
  std::string server_uri;
  int numa_node = 0;
  uint8_t server_rpc_id = 0;
  uint8_t base_rpc_id = 0;
  std::atomic<uint16_t> next_rpc_id{0};
  int connect_timeout_ms = 2000;
  int rpc_timeout_ms = 100;
  bool initialized = false;
};

struct ThreadContext {
  explicit ThreadContext(std::shared_ptr<ProcessContext> process_ctx)
      : pc(std::move(process_ctx)) {}

  std::shared_ptr<ProcessContext> pc;
  uint8_t rpc_id = 0;
  std::unique_ptr<erpc::Rpc<erpc::CTransport>> rpc;
  int session_num = -1;
  bool connected = false;
  std::vector<InflightNoopCall*> timed_out_calls;

  ~ThreadContext() { ReapTimedOutCalls(); }

  void ReapTimedOutCalls() {
    for (size_t i = 0; i < timed_out_calls.size();) {
      InflightNoopCall* call = timed_out_calls[i];
      if (call->done.load(std::memory_order_acquire)) {
        rpc->free_msg_buffer(call->req_msgbuf);
        rpc->free_msg_buffer(call->resp_msgbuf);
        delete call;
        timed_out_calls[i] = timed_out_calls.back();
        timed_out_calls.pop_back();
      } else {
        ++i;
      }
    }
  }

  bool InitThread() {
    if (connected) {
      return true;
    }

    const uint16_t raw_id = pc->next_rpc_id.fetch_add(1, std::memory_order_relaxed);
    const uint16_t rpc_id_range = static_cast<uint16_t>(erpc::kMaxRpcId) + 1;
    const uint16_t assigned =
        static_cast<uint16_t>((static_cast<uint16_t>(pc->base_rpc_id) + raw_id) %
                              rpc_id_range);
    rpc_id = static_cast<uint8_t>(assigned);

    rpc = std::make_unique<erpc::Rpc<erpc::CTransport>>(pc->nexus.get(), this, rpc_id,
                                                         sm_handler);
    if (!rpc) {
      fprintf(stderr, "[ErpcBloomCheckClient] Failed to create per-thread Rpc\n");
      return false;
    }

    session_num = rpc->create_session(pc->server_uri, pc->server_rpc_id);
    if (session_num < 0) {
      fprintf(stderr, "[ErpcBloomCheckClient] Failed to create session (rpc_id=%u)\n",
              static_cast<unsigned>(rpc_id));
      return false;
    }

    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(pc->connect_timeout_ms) * 1000000ULL;
    while (!rpc->is_connected(session_num)) {
      rpc->run_event_loop_once();
      if (NowNs() > deadline_ns) {
        fprintf(stderr,
                "[ErpcBloomCheckClient] Session connect timeout (%d ms, rpc_id=%u)\n",
                pc->connect_timeout_ms, static_cast<unsigned>(rpc_id));
        return false;
      }
    }

    connected = true;
    return true;
  }
};

thread_local std::unordered_map<uint64_t, std::unique_ptr<ThreadContext>>
    tls_thread_ctx_map;
std::atomic<uint64_t> g_client_id_seq{1};

void noop_cont_func(void* context, void* tag) {
  (void)context;
  auto* call = static_cast<InflightNoopCall*>(tag);

  const uint64_t recv_ns = NowNs();
  call->rtt_ns = (recv_ns > call->send_ns) ? (recv_ns - call->send_ns) : 0;

  if (call->resp_msgbuf.get_data_size() > 0) {
    bloomcheck::NoopResponse resp;
    call->parse_ok = resp.ParseFromArray(
        call->resp_msgbuf.buf_, static_cast<int>(call->resp_msgbuf.get_data_size()));
    if (call->parse_ok && resp.has_server_ts_ns()) {
      call->server_ts_ns = resp.server_ts_ns();
    }
  } else {
    call->parse_ok = false;
  }

  call->done.store(true, std::memory_order_release);
}

}  // namespace

struct ErpcBloomCheckClientImpl {
  explicit ErpcBloomCheckClientImpl(const std::string& local_uri,
                                    const std::string& server_uri, int numa_node,
                                    uint8_t base_rpc_id, uint8_t server_rpc_id) {
    pc = std::make_shared<ProcessContext>();
    pc->client_id = g_client_id_seq.fetch_add(1, std::memory_order_relaxed);
    pc->local_uri = local_uri;
    pc->server_uri = server_uri;
    pc->numa_node = numa_node;
    pc->base_rpc_id = base_rpc_id;
    pc->server_rpc_id = server_rpc_id;
    pc->connect_timeout_ms = ParseEnvMs("BLOOMCHECK_ERPC_CONNECT_TIMEOUT_MS", 2000);
    pc->rpc_timeout_ms = ParseEnvMs("BLOOMCHECK_ERPC_RPC_TIMEOUT_MS", 100);
  }

  bool Init() {
    std::lock_guard<std::mutex> lock(init_mu);
    if (pc->initialized) {
      return true;
    }

    try {
      pc->nexus = std::make_unique<erpc::Nexus>(pc->local_uri, pc->numa_node, 0);
      pc->initialized = true;
      fprintf(stderr,
              "[ErpcBloomCheckClient] ProcessContext initialized: local=%s server=%s "
              "base_rpc_id=%u server_rpc_id=%u connect_timeout_ms=%d rpc_timeout_ms=%d\n",
              pc->local_uri.c_str(), pc->server_uri.c_str(),
              static_cast<unsigned>(pc->base_rpc_id),
              static_cast<unsigned>(pc->server_rpc_id), pc->connect_timeout_ms,
              pc->rpc_timeout_ms);
      return true;
    } catch (const std::exception& e) {
      fprintf(stderr, "[ErpcBloomCheckClient] Nexus init failed: %s\n", e.what());
      return false;
    }
  }

  ThreadContext* GetOrCreateThreadContext() {
    auto it = tls_thread_ctx_map.find(pc->client_id);
    if (it != tls_thread_ctx_map.end()) {
      return it->second.get();
    }

    auto tc = std::make_unique<ThreadContext>(pc);
    ThreadContext* tc_ptr = tc.get();
    tls_thread_ctx_map.emplace(pc->client_id, std::move(tc));
    return tc_ptr;
  }

  std::shared_ptr<ProcessContext> pc;
  std::mutex init_mu;
};

ErpcBloomCheckClient::ErpcBloomCheckClient(const std::string& local_uri,
                                           const std::string& server_uri, int numa_node,
                                           uint8_t rpc_id, uint8_t server_rpc_id)
    : impl_(std::make_unique<ErpcBloomCheckClientImpl>(local_uri, server_uri, numa_node,
                                                        rpc_id, server_rpc_id)) {}

ErpcBloomCheckClient::~ErpcBloomCheckClient() {
  if (impl_ != nullptr && impl_->pc != nullptr) {
    tls_thread_ctx_map.erase(impl_->pc->client_id);
  }
}

bool ErpcBloomCheckClient::Init() { return impl_->Init(); }

bool ErpcBloomCheckClient::IsConnected() const {
  if (!impl_->pc->initialized) {
    return false;
  }
  auto it = tls_thread_ctx_map.find(impl_->pc->client_id);
  return (it != tls_thread_ctx_map.end()) && it->second && it->second->connected;
}

bool ErpcBloomCheckClient::Noop(uint64_t* server_ts_ns, uint64_t* rtt_ns) {
  if (!impl_->Init()) {
    return false;
  }

  ThreadContext* tc = impl_->GetOrCreateThreadContext();
  if (tc == nullptr || !tc->InitThread()) {
    return false;
  }

  tc->ReapTimedOutCalls();

  bloomcheck::NoopRequest req;
  std::string payload;
  req.SerializeToString(&payload);

  auto* call = new InflightNoopCall();
  call->tc = tc;
  call->req_msgbuf = tc->rpc->alloc_msg_buffer_or_die(256);
  call->resp_msgbuf = tc->rpc->alloc_msg_buffer_or_die(256);

  if (!payload.empty()) {
    tc->rpc->resize_msg_buffer(&call->req_msgbuf, payload.size());
    memcpy(call->req_msgbuf.buf_, payload.data(), payload.size());
  } else {
    tc->rpc->resize_msg_buffer(&call->req_msgbuf, 0);
  }

  call->send_ns = NowNs();
  tc->rpc->enqueue_request(tc->session_num, kReqTypeNoop, &call->req_msgbuf,
                           &call->resp_msgbuf, noop_cont_func, call);

  const uint64_t deadline_ns =
      call->send_ns + static_cast<uint64_t>(impl_->pc->rpc_timeout_ms) * 1000000ULL;
  bool timed_out = false;
  while (!call->done.load(std::memory_order_acquire)) {
    tc->rpc->run_event_loop_once();
    if (NowNs() > deadline_ns) {
      timed_out = true;
      break;
    }
  }

  if (timed_out) {
    tc->timed_out_calls.push_back(call);
    fprintf(stderr, "[ErpcBloomCheckClient] Noop timeout (%d ms, rpc_id=%u)\n",
            impl_->pc->rpc_timeout_ms, static_cast<unsigned>(tc->rpc_id));
    return false;
  }

  if (server_ts_ns != nullptr) {
    *server_ts_ns = call->server_ts_ns;
  }
  if (rtt_ns != nullptr) {
    *rtt_ns = call->rtt_ns;
  }

  tc->rpc->free_msg_buffer(call->req_msgbuf);
  tc->rpc->free_msg_buffer(call->resp_msgbuf);
  const bool ok = call->parse_ok;
  delete call;
  return ok;
}

}  // namespace ROCKSDB_NAMESPACE


