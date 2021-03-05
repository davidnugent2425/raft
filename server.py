import asyncio
import socket
from random import random
import pickle

LEADER = 1
CANDIDATE = 2
FOLLOWER = 3

APPEND_ENTRIES = 1
REQUEST_VOTE = 2
VOTE = 3

class Server:

    def __init__(self, server_id, total_num_servers):

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

        # Variables used when the machine is a Candidate:
        self.votes_received = 0
        self.total_num_servers = total_num_servers

        # start timer which will be used for follower and candidate timeouts
        self._reset_timer()

        self.loop = asyncio.get_event_loop()

        # set up socket for this server
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('127.0.0.1', 50000+self.server_id))
        self.sock.listen()

        # process receiving connections from other servers
        #asyncio.ensure_future(self.receive_connection())
        #print("receiving connections")
        self.loop.create_task(self.receive_connection())
        
        # connections by socket to other servers in the network
        self.connections = {}
        for i in range(self.total_num_servers):
            if i == self.server_id: continue
            asyncio.ensure_future(self.establish_connection(i))

    
    async def establish_connection(self, server_index):
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.connect(('127.0.0.1', 50000+server_index))
        print("Server {} connected to Server {}" \
                .format(self.server_id, server_index))
        self.connections[server_index] = connection
            

    def _reset_timer(self):
        print("Server {} is resetting timer".format(self.server_id))
        if self._timer_task is not None: self._timer_task.cancel()
        # timeout is between 1.5-2.5 seconds
        self._timeout = 5 + random()
        # if we are the leader, we send out heartbeats when channel is idle
        # so our timeout must be earlier than the followers
        if self.status == LEADER: self._timeout = random() * 0.5
        self._timer_task = asyncio.ensure_future(self._timer_job())


    async def _timer_job(self):
        await asyncio.sleep(self._timeout)
        if self.status == LEADER:
            self.send_heartbeats()
        # if we have not voted for another candidate, convert to candidate,
        # if we are already a candidate, restart candidacy
        elif self.voted_for == None or self.status == CANDIDATE:
            self._convert_to_candidate()
        else: self._reset_timer()


    def _convert_to_candidate(self):
        # block not executed if election restarted after timeout
        if self.status != CANDIDATE:
            self.current_term += 1
            self.voted_for = self.server_id
            self.status = CANDIDATE
        print("Server {} is a Candidate with term {}" \
                .format(self.server_id, self.current_term))
        self._reset_timer()
        self.send_request_votes()


    async def receive_msgs(self, server, addr):
        while True:
            message = await self.loop.sock_recv(server, 1024)
            rpc_dict = pickle.loads(message)
            if rpc_dict["type"] == REQUEST_VOTE:
                print("Request Vote from {} received by {}" \
                        .format(rpc_dict["candidate_id"], self.server_id))
                term, voted = self.process_request_vote_rpc(rpc_dict)
                self.send_vote_msg(rpc_dict["candidate_id"], term, voted)
            elif rpc_dict["type"] == VOTE:
                self.process_receive_vote_response(rpc_dict)
            elif rpc_dict["type"] == APPEND_ENTRIES:
                term, success = self.process_append_entries_rpc(rpc_dict)
                if success: self._reset_timer()

    async def receive_connection(self):
        while True:
            #try:
            server, addr = await self.loop.sock_accept(self.sock)
            self.loop.create_task(self.receive_msgs(server, addr))
            #except:
            #    print("what")
            
    
    def send_vote_msg(self, candidate_id, term, voted):
        vote_dict = {"type": VOTE,
                     "from": self.server_id,
                     "voted": voted,
                     "term": term}
        data = pickle.dumps(vote_dict)
        self.connections[candidate_id].send(data)

    def send_request_votes(self):
        log_term = 0 if len(self.log) == 0 else self.log[self.last_applied][0]
        rpc_dict = {"type": REQUEST_VOTE,
                    "candidate_term": self.current_term,
                    "candidate_id": self.server_id,
                    "last_log_idx": self.last_applied,
                    "last_log_term": log_term}
        data = pickle.dumps(rpc_dict)
        for connection in self.connections.values():
            connection.send(data)


    def process_receive_vote_response(self, response):
        print("Vote received by {} from {}" \
                .format(self.server_id, response["from"]))
        if self.status != CANDIDATE: return
        # if the voter is at a higher term than us, become a follower
        if response["term"] > self.current_term:
            self._convert_to_follower(response["term"])
        # if the voter votes for us
        elif response["voted"] == True: self.votes_received += 1
        # if we've received votes from majority of servers: become leader
        if self.votes_received > self.total_num_servers // 2:
            self._convert_to_leader()

    def _convert_to_leader(self):
        print("Server {} is now the Leader".format(self.server_id))
        self.status = LEADER
        self.send_heartbeats()


    def process_received_command(self, cmd):
        if self.status != LEADER:
            return self.forward_received_command(cmd)
        new_log = [self.current_term, cmd]
        self.log.append(new_log)
        self.distribute_append_entries_rpcs(new_log)
        self.execute(cmd)
        return True
    
    def distribute_append_entries_rpcs(self, new_log):
        #TODO send relevant amount of logs to each server
        last_log_index = len(self.log)-1
        for i in range(total_num_servers):
            if i == self.server_id: continue
            logs_to_send = self.log[-1]
            dest_serv_next_index = self.next_index[i]
            if last_log_index >= dest_serv_next_index:
                logs_to_send = self.log[dest_serv_next_index:]
            success = send_append_entries_rpc(i, logs_to_send)
            if success:
                self.next_index[i] = last_log_index + 1
        #TODO if all AppendEntries successful, update match_index
        #TODO if AppendEntries fails because of log inconsistency:
        #     decrement next_index and retry
        #TODO if majority of match_index >= N, and N is from current term,
        #     commit_index = N
        return True

    def send_append_entries_rpc(self, dest_serv_id, logs_to_send):
        #send an AppendEntries RPC to a server
        prev_log_idx = self.next_index[dest_serv_id]-1
        rpc_dict = {"type": APPEND_ENTRIES,
                    "term": self.current_term,
                    "leader_id": self.server_id,
                    "prev_log_idx": prev_log_idx,
                    "prev_log_term": self.log[prev_log_idx],
                    "entries": logs_to_send,
                    "leader_commit": self.commit_index}
        data = pickle.dumps(rpc_dict)
        self.connections[i].send(data)
        return None
    
    def forward_received_command(self, cmd):
        #TODO forward received command to the leader for distribution
        return None


    def send_heartbeats(self):
        # send empty AppendEntries RPC to each server to avoid timeouts
        # during idle times
        rpc_dict = {"type": APPEND_ENTRIES,
                    "term": self.current_term,
                    "leader_id": self.server_id,
                    "entries": [],
                    "leader_commit": self.commit_index}
        data = pickle.dumps(rpc_dict)
        for connection in self.connections.values():
            connection.send(data)
        self._reset_timer()

    def process_append_entries_rpc(self, rpc):

        # case when we receive rpc from an old leader
        if rpc["term"] < self.current_term: return self.current_term, False
        # when we receive rpc from a new leader
        elif rpc["term"] > self.current_term:
            self._convert_to_follower(rpc["term"])

        # case when it is just a heartbeat message
        if len(rpc["entries"]) == 0: 
            print("Server {} received heartbeat from Leader".format(self.server_id))
            return self.current_term, True

        # case when there is a gap between the logs we have and the logs
        # we are receiving
        curr_num_entries = len(self.log)
        if curr_num_entries < rpc["prev_log_idx"] or \
           self.log[rpc["prev_log_idx"]][0] != rpc["prev_log_term"]:
            return rpc["term"], False
      
        # which index of rpc.entries we should start at when appending to our
        # current log (will not be 0 if we have previously received some of the same        # entries)
        start_idx = 0
        # if the current log already contains entries after prev_log_idx
        # we check that the common entries are the same, otherwise we remove
        # our incorrect entry and all following entries
        if curr_num_entries > rpc["prev_log_idx"]:
            num_common_elems = curr_num_entries - rpc["prev_log_idx"] - 1
            for i in range(0, num_common_elems):
                log_idx = rpc["prev_log_idx"]+i+1
                # if the term number of the corresponding entries not the same
                # remove incorrect entry and all following entries
                if self.log[log_idx][0] != rpc["entries"][i][0]:
                    self.log = self.log[:log_idx]
                    start_idx = i
                    break
                start_idx = i

        # add all new entries to our log
        self.log.append(rpc["entries"][start_idx:])

        # ensure our highest committed index is either our leaders committed
        # index or the highest index in our log
        if rpc["leader_commit"] > self.commit_index:
            self.commit_index = min(rpc["leader_commit"], len(self.log)-1)
        
        # if we have not applied all of the commands we know to be committed,
        # apply them now
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            self.execute(self.log[self.last_applied][1])
        
        return rpc["term"], True

    
    def _convert_to_follower(self, new_term):
        self.current_term = new_term
        self.status = FOLLOWER
        self.voted_for = None


    def execute(self, command):
        # for now just print the command from the logs
        print(command)


    def process_request_vote_rpc(self, rpc):
        if rpc["candidate_term"] < self.current_term:
            return self.current_term, False
        
        # if we have not yet issued a vote, or if this candidates logs
        # are at least as up to date as ours, vote for this candidate
        if self.voted_for == None and rpc["last_log_idx"] >= (len(self.log)-1):
            self.voted_for = rpc["candidate_id"]
            return rpc["candidate_term"], True
        
        return rpc["candidate_term"], False



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

