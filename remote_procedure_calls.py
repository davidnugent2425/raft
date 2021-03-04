
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

