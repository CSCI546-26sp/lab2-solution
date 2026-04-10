#include "common/utils/rand_gen.hpp"
#include <mutex>
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty
{
  using grpc::ServerBuilder;
  using grpc::ServerContext;
  using grpc::experimental::ClientInterceptorFactoryInterface;
  using grpc::experimental::CreateCustomChannelWithInterceptors;

  Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
      : logger(utils::logger::get_logger(config.id)), id_(config.id), listening_addr(config.addr),
        peer_addrs(config.peer_addrs), dead(false), ready_queue(ready)
  // TODO: add more field if desired
  {
    // TODO: finish it
  }
  Raft::~Raft()
  {
    this->stop_server();
    if (election_timer_thread_.joinable())
    {
      election_timer_thread_.join();
    }
  }

  void Raft::run()
  {
    // TODO: kick off the raft instance
    // Note: this function should be non-blocking

    // lab 1
    election_timer_thread_ = std::thread(&Raft::run_election_timer, this);
  }
  grpc::Status Raft::AppendEntries(::grpc::ServerContext *context, const ::raftpb::AppendEntriesRequest *request, ::raftpb::AppendEntriesResponse *response)
  {
    std::lock_guard<std::mutex> lock(mtx_);
    // Implement the logic for handling AppendEntries RPC here
    //Reply false if term < currentTerm
    if (request->term() > current_term_)
    {
      current_term_ = request->term();
      voted_for_ = -1;
      is_leader_ = false;
      logger->info("Leader staled, updated to term {}", current_term_);
    }

    if (request->term() < current_term_)
    {
      response->set_term(current_term_);
      response->set_success(false);
      return grpc::Status::OK;
    }
    reset_election_timer();
    //Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
    if (request->prev_log_index() > 0)
    {
      if (log_entries_.size() < static_cast<size_t>(request->prev_log_index()) ||
          log_entries_[request->prev_log_index() - 1].term() != request->prev_log_term())
      {
        response->set_term(current_term_);
        response->set_success(false);
        return grpc::Status::OK;
      }
    }
    //If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    logger->info("AppendEntries: received {} entries from {}, my log size={}", request->entries_size(), request->leader_id(), log_entries_.size());
    for (const auto& entry: request->entries())
    {
      size_t idx = entry.index()-1;
      if (idx < log_entries_.size()){
        if (log_entries_[idx].term() != entry.term()){
          logger->info("Truncating log at index {} (my term={}, leader term={})", idx, log_entries_[idx].term(), entry.term());
          log_entries_.resize(idx);
          log_entries_.push_back(entry);
        }
      }else{
        log_entries_.push_back(entry);
      }
    }
    //If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if (request->leader_commit() > commit_index_)
    {
      commit_index_ = std::min(request->leader_commit(), static_cast<uint64_t>(log_entries_.size()));
      for (size_t i = last_applied_; i < commit_index_; ++i)
      {
        last_applied_ += 1;
        apply(ApplyResult{true, log_entries_[i].command(), log_entries_[i].index()});
      }
      logger->info("Candidate updated commit_index to {}", commit_index_);
    }

    response->set_term(current_term_);
    response->set_success(true);


    return grpc::Status::OK;
  }
  grpc::Status Raft::RequestVote(::grpc::ServerContext *context, const ::raftpb::RequestVoteRequest *request, ::raftpb::RequestVoteResponse *response)
  {
    // Implement the logic for handling RequestVote RPC here
    std::lock_guard<std::mutex> lock(mtx_);
    if (request->term() < current_term_)
    {
      response->set_term(current_term_);
      response->set_vote_granted(false);
    }
    else
    {
      if (request->term() > current_term_)
      {
        current_term_ = request->term();
        voted_for_ = -1;
        is_leader_ = false;
      }
      response->set_term(current_term_);
      uint64_t my_last_index = log_entries_.empty() ? 0 : log_entries_.back().index();
      uint64_t my_last_term = log_entries_.empty() ? 0 : log_entries_.back().term();
      bool log_ok = (request->last_log_term() > my_last_term) ||
              (request->last_log_term() == my_last_term && 
               request->last_log_index() >= my_last_index);
      if ((voted_for_ == -1 || voted_for_ == request->candidate_id()) && log_ok)
      {
        logger->info("Voting for candidate {}", request->candidate_id());
        voted_for_ = request->candidate_id();
        response->set_vote_granted(true);
        reset_election_timer();
      }
      else
      {
        response->set_vote_granted(false);
      }
    }
    return grpc::Status::OK;
  }
  void Raft::run_election_timer()
  {
    while (!is_dead())
    {
      bool am_leader = false;
      {
        std::lock_guard<std::mutex> lock(mtx_);
        am_leader = is_leader_;
      }

      if (am_leader)
      {
        // Leader: send heartbeats periodically
        send_heartbeats();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
      else
      {
        // Follower/Candidate: wait for heartbeat or timeout
        bool got_heartbeat = false;
        {
          std::unique_lock<std::mutex> lock(mtx_);
          int timeout = 150 + utils::RandGen::get_instance().intn(150);
          got_heartbeat = election_cv_.wait_for(
              lock, std::chrono::milliseconds(timeout),
              [this]
              { return this->heartbeat_received_ || this->is_dead(); });
          if (is_dead())
          {
            break;
          }
          heartbeat_received_ = false;
        }
        if (!got_heartbeat)
        {
          // start election
          logger->info("Election timeout, starting election");
          uint64_t election_term;
          {
            std::lock_guard<std::mutex> lock(mtx_);
            voted_for_ = id_;
            current_term_ += 1;
            election_term = current_term_;
          }

          int votes_granted = 1; // vote for self
          for (const auto &peer : peer_addrs)
          {
            raftpb::RequestVoteRequest request;
            request.set_candidate_id(id_);
            request.set_term(election_term);
            request.set_last_log_index(log_entries_.empty() ? 0 : log_entries_.back().index());
            request.set_last_log_term(log_entries_.empty() ? 0 : log_entries_.back().term());
            auto context = create_context(peer.first);
            raftpb::RequestVoteResponse response;
            grpc::Status status = peers_[peer.first]->RequestVote(context.get(), request, &response);
            if (status.ok())
            {
              std::lock_guard<std::mutex> lock(mtx_);
              logger->info("Received vote from {}: {}", peer.first, response.vote_granted());
              if (response.vote_granted())
              {
                votes_granted += 1;
              }
            }
            else
            {
              logger->error("Failed to request vote from {}: {}", peer.first, status.error_message());
            }
          }
          {
            std::lock_guard<std::mutex> lock(mtx_);
            if (votes_granted > (peer_addrs.size() + 1) / 2)
            {
              logger->info("Becomes leader for term {}", election_term);
              // Become leader
              is_leader_ = true;

              // Initialize next_index_ and match_index_
              for (const auto &peer : peer_addrs)
              {
                next_index_[peer.first] = log_entries_.size() + 1;
                match_index_[peer.first] = 0;
              }
            }
            else
            {
              logger->info("Failed to become leader for term {}, got {} votes", election_term, votes_granted);
            }
          }
        }
      }
    }
  }
  void Raft::reset_election_timer()
  {
    {
      heartbeat_received_ = true;
    }
    election_cv_.notify_all();
  }
  void Raft::send_heartbeat_to(uint64_t peer_id)
  {
    raftpb::AppendEntriesRequest request;
    raftpb::AppendEntriesResponse response;
    std::unique_ptr<grpc::ClientContext> context;

    {
      std::lock_guard<std::mutex> lock(mtx_);
      // Check if still leader before sending (might have stepped down after previous heartbeat)
      if (!is_leader_) {
        return;
      }
      request.set_leader_id(id_);
      request.set_term(current_term_);
      auto next_index = next_index_[peer_id];
      auto prev_index = next_index - 1;
      request.set_prev_log_index(prev_index);
      if(prev_index == 0){
        request.set_prev_log_term(0);
      } else {
        request.set_prev_log_term(log_entries_[prev_index - 1].term());
      }
      for (size_t i = next_index - 1; i < log_entries_.size(); ++i)
      {
        auto entry = request.add_entries();
        *entry = log_entries_[i];
      }
      request.set_leader_commit(commit_index_);
      context = create_context(peer_id);
    }
    grpc::Status status = peers_[peer_id]->AppendEntries(context.get(), request, &response);

    {
      std::lock_guard<std::mutex> lock(mtx_);
      
      if (!status.ok())
      {
        logger->error("Failed to send heartbeat to {}: {}", peer_id, status.error_message());
      }
      else
      {
        if (!response.success()){
          if (response.term() > current_term_)
          {
            current_term_ = response.term();
            voted_for_ = -1;
            is_leader_ = false;
            logger->info("Stepping down as leader, term updated to {}", current_term_);
            return;
          } 
          logger->info("AppendEntries rejected by {}, decrementing next_index", peer_id);
          next_index_[peer_id] = (next_index_[peer_id] > 1) ? next_index_[peer_id] - 1 : 1;
          return;
        }

        logger->info("AppendEntries accepted by {}", peer_id);

        next_index_[peer_id] = log_entries_.size() + 1;
        match_index_[peer_id] = log_entries_.size();

        // Find the highest index where we have majority replication
        // Only commit entries from current term (Raft §5.4.2)
        for (size_t n = log_entries_.size(); n > commit_index_; --n)
        {
          // Only commit entries from current term
          if (log_entries_[n - 1].term() != current_term_)
          {
            continue;
          }

          size_t match_count = 1; // count self
          for (const auto &peer : peer_addrs)
          {
            if (match_index_[peer.first] >= n)
            {
              match_count += 1;
            }
          }

          if (match_count > (peer_addrs.size() + 1) / 2)
          {
            commit_index_ = n;
            logger->info("Leader updated commit_index to {}", commit_index_);
            for (size_t i = last_applied_; i < commit_index_; ++i){
              last_applied_ += 1;
              apply(ApplyResult{true, log_entries_[i].command(), log_entries_[i].index()});
            }
            break;
          }
        }
        // logger->info("Heartbeat sent to {}", peer_id);
      }
    }
  }

  void Raft::send_heartbeats()
  {
    
    { 
      std::lock_guard<std::mutex> lock(mtx_);
      if (is_leader_ == false)
      {
        return;
      }
    }
    for (const auto &peer : peer_addrs)
    {
      send_heartbeat_to(peer.first);
    }
  }
  State Raft::get_state() const
  {
    // TODO: lab 1
    std::lock_guard<std::mutex> lock(mtx_);
    State state;
    state.term = current_term_;
    state.is_leader = is_leader_;
    return state;
  }

  ProposalResult Raft::propose(const std::string &data)
  {
    // TODO: lab 2
    std::lock_guard<std::mutex> lock(mtx_);
    if (!is_leader_)
    {
      return ProposalResult{0, current_term_, false };
    }
    // Append the new entry to the log
    raftpb::Entry new_entry;
    new_entry.set_term(current_term_);
    new_entry.set_index(log_entries_.size() + 1);
    new_entry.set_command(data);
    log_entries_.push_back(new_entry);

    return ProposalResult{new_entry.index(), current_term_, true };

  }

  ProposalResult Raft::propose_sync(const std::string &data)
  {
    // TODO: lab 3
  }

  // TODO: add more functions if desired.

} // namespace rafty
