import asyncio

LEADER = 1
CANDIDATE = 2
FOLLOWER = 3

class Server:

    def __init__(self, server_id):

        # Variables used by all machines:
        
        self.server_id = server_id
        # leader (1), candidate (2) or follower (3)
        self.status = 0
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
        self._timeout = 0
        # task for handling timer
        self._timer_task = None
        
        # Variables used when the machine is a Leader:
        # (all re-initialized after each election)
        
        # contains the indexes of the next log entry to be sent to each server
        self.next_index = []
        # contains the indexes of the highest log entry know to be replicated
        # on each server
        self.match_index = []

        self._reset_timer()


    def _reset_timer(self):
        if self._timer_task is not None: self._timer_task.cancel()
        self._timeout = 2
        self._timer_task = asyncio.ensure_future(self._timer_job())


    async def _timer_job(self):
        await asyncio.sleep(self._timeout)
        # if we have not voted for another candidate, convert into a candidate
        if self.voted_for == None:
            self._convert_to_candidate()
        else: self._reset_timer()


    def _convert_to_candidate(self):
        self.current_term += 1
        self.voted_for = self.server_id
        self.status = CANDIDATE


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
        
        # if we have not applied all of the commands we know to be committed,
        # apply them now
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            self.execute(self.log[self.last_applied][1])
        
        if rpc.term > self.current_term: self._convert_to_follower(rpc.term)

        return rpc.term, True

    
    def _convert_to_follower(self, new_term):
        self.current_term = new_term
        self.status = FOLLOW


    def execute(self, command):
        # for now just print the command from the logs
        print(command)


    def process_request_vote_rpc(self, rpc):
        if rpc.candidate_term < self.current_term:
            return self.current_term, False
        
        # if we have not yet issued a vote, or if this candidates logs
        # are at least as up to date as ours, vote for this candidate
        if self.voted_for == None or rpc.last_log_idx >= (len(self.log)-1):
            self.voted_for = rpc.candidate_id
            return rpc.candidate_term, True

        if rpc.term > self.current_term: self._convert_to_follower(rpc.term)
        
        return rpc.candidate_term, False



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
