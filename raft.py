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
        self.log = []
        # index of highest log entry known to be committed
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

class AppendEntries:
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
        output = "AppendEntries RPC:\n" \
                 "Term: {}\n" \
                 "Leader ID:{}\n" \
                 "Previous Log Idx: {}\n" \
                 "Previous Log Term: {}\n" \
                 "Entries: {}\n" \
                 "Highest Commit known to Leader: {}" \
                 .format(self.term, self.leader_id, self.prev_log_idx,
                          self.prev_log_term, self.entries, self.leader_commit)
        return output


if __name__ == '__main__':
    server = Server();
    append_entries = AppendEntries(1, 1, 1, 1, [], 1);
    print(append_entries)
