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


if __name__ == '__main__':
    server = Server();
