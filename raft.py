# implementation of the raft algorithm from the paper:
# https://raft.github.io/raft.pdf

class Server:

    def __init__(self):

        # Variables used by all machines:

        # latest term server has seen
        self.current_term = 0
        # candidate_id that received vote in current term
        self.voted_for = None
        # log entries; each entry contains a command and the term when
        # the entry was received by the leader
        # form of log entry: [term number, command]
        self.log = []
        # index of highest log entry known to be committed
        # (log entries are committed when they are known to be replicated on 
        #  a majority of the servers)
        self.commit_index = 0
        # index of highest log entry that has been received
        self.last_applied = 0
        
        # Variables used when the machine is a Leader:
        # (all re-initialized after each election)
        
        # contains the indexes of the next log entry to be sent to each server
        self.next_index = []
        # contains the indexes of the highest log entry know to be replicated
        # on each server
        self.match_index = []


    def process_append_entries_rpc(self, rpc):
        
        # case when we receive rpc from an old leader
        if rpc.term < self.current_term: return self.current_term, False
        # case when there is a gap between the logs we have and the logs
        # we are receiving
        curr_num_entries = len(self.log)
        if curr_num_entries < rpc.prev_log_idx or \
           self.log[prev_log_term][0] != rpc.prev_log_term:
            return rpc.term, False
      
        # which index of rpc.entries we should start at when appending to our
        # current log (will not be 0 if we have previously received some of the same        # entries)
        start_idx = 0
        # if the current log already contains entries after prev_log_idx
        # we check that the common entries are the same, otherwise we remove
        # our incorrect entry and all following entries
        if curr_num_entries > rpc.prev_log_idx:
            num_common_elems = curr_num_entries - prev_log_idx - 1
            for i in range(0, num_common_elems):
                log_idx = prev_log_idx+i+1
                # if the term number of the corresponding entries not the same
                # remove incorrect entry and all following entries
                if self.log[log_idx][0] != rpc.entries[i][0]:
                    self.log = self.log[:log_idx]
                    start_idx = i
                    break
                start_idx = i

        # add all new entries to our log
        self.log.append(rpc.entries[start_idx:])

        # ensure our highest committed index is either our leaders committed
        # index or the highest index in our log
        if rpc.leader_commit > self.commit_index:
            self.commit_index = min(rpc.leader_commit, len(self.log)-1)
        
        return rpc.term, True

    def process_request_vote_rpc(self, rpc):
        if rpc.candidate_term < self.current_term: return False
        # if we have not yet issued a vote, or if this candidates logs
        # are at least as up to date as ours, vote for this candidate
        if self.voted_for == None or rpc.last_log_idx >= (len(self.log)-1):
            self.voted_for = rpc.candidate_id
            return True
        return False



    def __str__(self):
        return "\nServer:\n" \
               "Term: {}\n" \
               "Voted for: {}\n" \
               "Last 5 log entries: {}\n" \
               "Highest log entry known committed: {}\n" \
               "Highest log entry received: {}\n" \
               "Next log indexes for each Server: {}\n" \
               "Highest log indexes replicated on each Server: {}\n" \
                .format(self.current_term, self.voted_for, self.log,
                        self.commit_index, self.last_applied,
                        self.next_index, self.match_index)


class AppendEntries:
    
    # implementation of an AppendEntries remote procedure call (RPC)
    def __init__(self, term, leader_id, prev_log_idx,
            prev_log_term, entries, leader_commit):
        # term of the leader who is sending
        self.term = term
        # ID of leader who is sending; included so followers can redirect clients
        self.leader_id = leader_id
        # index of log entry immediately preceding new ones
        self.prev_log_idx = prev_log_idx
        # term of log entry immediately preceding new ones
        self.prev_log_term = prev_log_term
        # list of log entries being sent (empty for heartbeat)
        self.entries = entries
        # the index of the highest log entry known (to the leader) to be committed
        self.leader_commit = leader_commit
    
    def __str__(self):
        return "\nAppendEntries RPC:\n" \
               "Term: {}\n" \
                "Leader ID:{}\n" \
                "Previous Log Idx: {}\n" \
                "Previous Log Term: {}\n" \
                "Entries: {}\n" \
                "Highest Commit known to Leader: {}" \
                .format(self.term, self.leader_id, self.prev_log_idx,
                        self.prev_log_term, self.entries, self.leader_commit)

class RequestVote:

    # implementation of a RequestVote remote procedure call (RPC)
    def __init__(self, candidate_term, candidate_id, last_log_idx, last_log_term):
        self.candidate_term = candidate_term
        self.candidate_id = candidate_id
        # index of candidate's last log entry
        self.last_log_idx = last_log_idx
        # term of candidate's last log entry
        self.last_log_term = last_log_term

    def __str__(self):
        return "\nRequestVote RPC:\n" \
               "Candidate Term: {}\n" \
               "Candidate ID: {}\n" \
               "Last Log Index: {}\n" \
               "Last Log Term: {}" \
               .format(self.candidate_term, self.candidate_id,
                        self.last_log_idx, self.last_log_term)

if __name__ == '__main__':
    server = Server();
    sample_ae_rpc = AppendEntries(1, 1, 1, 1, [], 1);
    print(sample_ae_rpc)
    print(server)
    server.process_append_entries_rpc(sample_ae_rpc)
    sample_rv_rpc = RequestVote(1, 1, 1, 1)
    print(sample_rv_rpc)
    server.process_request_vote_rpc(sample_rv_rpc)
    print(server)
