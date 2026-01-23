// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#pragma once

#include <grpcpp/grpcpp.h>
#include <string>
#include <vector>
#include "bloomcheck.grpc.pb.h"

namespace ROCKSDB_NAMESPACE {

// BloomCheckServiceClient provides a C++ client interface for the BloomCheck gRPC service
class BloomCheckServiceClient {
 public:
  explicit BloomCheckServiceClient(const std::string& server_address)
      : channel_(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())),
        stub_(bloomcheck::BloomCheck::NewStub(channel_)) {}

  // Check if a key may exist in the given SST file (by file_number)
  // Returns true if key may exist (or on error for safety), false if definitely not
  bool CheckKey(const std::string& user_key, uint64_t sst_file_number) {
    bloomcheck::BatchRequest request;
    bloomcheck::KeyRequest* key_req = request.add_reqs();
    key_req->set_user_key(user_key);
    key_req->add_sst_file_numbers(sst_file_number);

    bloomcheck::BatchResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->Check(&context, request, &response);

    // On error, return true (safe default to prevent false negatives)
    if (!status.ok()) {
      return true;
    }

    if (response.resps_size() > 0) {
      const auto& key_resp = response.resps(0);
      // If any SST file_number is in may_match, return true
      return key_resp.may_match_sst_file_numbers_size() > 0;
    }

    return true;  // Safe default
  }

  // Check multiple keys against multiple SST files
  bool CheckBatch(const bloomcheck::BatchRequest& request,
                  bloomcheck::BatchResponse* response) {
    grpc::ClientContext context;
    grpc::Status status = stub_->Check(&context, request, response);
    return status.ok();
  }

  // Noop RPC for latency measurement (sync unary)
  bool Noop() {
    bloomcheck::NoopRequest request;
    bloomcheck::NoopResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->Noop(&context, request, &response);
    return status.ok();
  }

 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<bloomcheck::BloomCheck::Stub> stub_;
};

}  // namespace ROCKSDB_NAMESPACE

