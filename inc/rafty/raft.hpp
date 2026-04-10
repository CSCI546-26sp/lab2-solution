#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"

// it will pick up correct header
// when you generate the grpc proto files
#include "raft.grpc.pb.h"

using namespace toolings;

namespace rafty {
// Remove: student impl
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

class Raft : public raftpb::RaftService::Service {
public:
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();
  grpc::Status AppendEntries(::grpc::ServerContext* context, const ::raftpb::AppendEntriesRequest* request, ::raftpb::AppendEntriesResponse* response) override ;
  grpc::Status RequestVote(::grpc::ServerContext* context, const ::raftpb::RequestVoteRequest* request, ::raftpb::RequestVoteResponse* response) override ;
  // WARN: do not modify the signature
  // TODO: implement `run`, `propose` and `get_state`
  void run(); /* lab 1 */
  ProposalResult propose(const std::string &data); /* lab 1 */
  State get_state() const; /* lab 2 */

  // lab3: sycn propose
  ProposalResult propose_sync(const std::string &data);

  // WARN: do not modify the signature
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();

  // extra functions
  std::condition_variable election_cv_;
  
  
  bool heartbeat_received_ = false;
  void run_election_timer();
  void reset_election_timer();
  void start_election();
  void send_heartbeat_to(uint64_t peer_id);
  void send_heartbeats();

  //election
  uint64_t current_term_ = 0;
  int64_t voted_for_ = -1;
  bool is_leader_ = false;
  std::vector<raftpb::Entry> log_entries_;
  uint64_t commit_index_ = 0;
  uint64_t last_applied_ = 0;


  //leader
  std::unordered_map<uint64_t, uint64_t> next_index_;
  std::unordered_map<uint64_t, uint64_t> match_index_;
private:
  // WARN: do not modify `create_context` and `apply`.

  // invoke `create_context` when creating context for rpc call.
  // args: the id of which raft instance the RPC will go to.
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
  void apply(const ApplyResult &result);

protected:
  // WARN: do not modify `mtx` and `logger`.
  mutable std::mutex mtx_;
  std::unique_ptr<rafty::utils::logger> logger;

private:
  // WARN: do not modify the declaration of
  // `id`, `listening_addr`, `peer_addrs`,
  // `dead`, `ready_queue`, `peers_`, and `server_`.
  uint64_t id_;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;

  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;

  std::thread election_timer_thread_;
};
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
