import asyncio
import socket
from random import random
import pickle
import tools
import numpy as np

LEADER = 1
CANDIDATE = 2
FOLLOWER = 3

APPEND_ENTRIES = 1
REQUEST_VOTE = 2
VOTE = 3
APPEND_ENTRIES_RESPONSE = 4
FORWARDED_CMD = 5

BASE_PORT_NUM = 50000
#TODOself.follower_timeout = 5


labels = {LEADER: "Leader",
          CANDIDATE: "Candidate",
          FOLLOWER: "Follower"}

class Server:

    def __init__(self, server_id, total_num_servers):

        # Variables used by all machines:
        
        self.server_id = server_id
        # leader (1), candidate (2) or follower (3)
        self.status = FOLLOWER
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
        self.commit_index = -1
        # index of highest log entry that has been received
        self.last_applied = -1
        self._timeout = 0
        # scale the timeout of followers positively with the size of the network
        self.follower_timeout = 2 + 0.06*total_num_servers
        # task for handling timer
        self._timer_task = None
        # used for testing system
        self.dead = False
        self.unreachable = []
        
        # Variables used when the machine is a Leader:
        # (all re-initialized after each election)
        
        # contains the indexes of the next log entry to be sent to each server
        self.next_index = [0]*total_num_servers
        # contains the indexes of the highest log entry know to be replicated
        # on each server
        self.match_index = [-1]*total_num_servers

        # Variables used when the machine is a Candidate:
        self.votes_received = 0
        self.total_num_servers = total_num_servers

        # start timer which will be used for follower and candidate timeouts
        self._reset_timer()

        self.loop = asyncio.get_event_loop()

        # set up socket for this server
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind(('127.0.0.1', BASE_PORT_NUM+self.server_id))
            self.sock.listen()
        except:
            print("Server {} unavailable".format(self.server_id))

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
        try:
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.connect(('127.0.0.1', BASE_PORT_NUM+server_index))
            #TODO verbose
            #print("{} {} connected to Server {}" \
            #        .format(labels[self.status], self.server_id, server_index))
            self.connections[server_index] = connection
        except: print("Server {} unable to connect to Server {}" \
                        .format(self.server_id, server_index))
            

    def _reset_timer(self):
        if self.dead: return
        #TODO logging
        #print("{} {} is resetting timer" \
        #        .format(labels[self.status], self.server_id))
        if self._timer_task is not None: self._timer_task.cancel()
        # timeout is between self.follower_timeout and FOLLOWER_TIMEOUT+2
        self._timeout = self.follower_timeout + (random()*2)
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
        while not self.dead:
            message = await self.loop.sock_recv(server, 1024)
            if message == b'': continue # skip if empty
            # if it can be parsed by pickle, it's an internal message
            if(tools.is_pickle_stream(message)):
                rpc_dict = pickle.loads(message)
                if rpc_dict["type"] == REQUEST_VOTE:
                    #TODO verbose
                    #print("Request Vote from {} received by {}" \
                    #        .format(rpc_dict["candidate_id"], self.server_id))
                    term, voted = self.process_request_vote_rpc(rpc_dict)
                    self.send_vote_msg(rpc_dict["candidate_id"], term, voted)
                elif rpc_dict["type"] == VOTE:
                    self.process_receive_vote_response(rpc_dict)
                elif rpc_dict["type"] == APPEND_ENTRIES:
                    term, success = self.process_append_entries_rpc(rpc_dict)
                    if success: self._reset_timer()
                    self.send_append_entries_response(term, success, rpc_dict)
                elif rpc_dict["type"] == APPEND_ENTRIES_RESPONSE:
                    self.process_append_entries_response(rpc_dict)
                elif rpc_dict["type"] == FORWARDED_CMD:
                    if self.status == LEADER:
                        self.process_received_command(rpc_dict["cmd"])
                else: print("unexpected internal message")
            else:
                # if it's not an internal message, we will treat it as a command
                # for our logs
                cmd = message.decode('utf-8')
                if cmd == 'KILL': 
                    self.dead = True
                    continue
                print("{} {} received command: {}" \
                        .format(labels[self.status], self.server_id, cmd))
                if self.status == LEADER:
                    self.process_received_command(cmd)
                else: self.forward_received_command(cmd)


    async def receive_connection(self):
        while True:
            server, addr = await self.loop.sock_accept(self.sock)
            self.loop.create_task(self.receive_msgs(server, addr))


    def process_append_entries_response(self, response_dict):
        responder_idx = response_dict["responder_id"]
        if response_dict["term"] > self.current_term:
            print("{} {} received message from {} that other leader \
                    has higher term {}" \
                    .format(labels[self.status], self.server_id,
                        responder_idx, response_dict["term"]))
            self._convert_to_follower(response_dict["term"])
        elif response_dict["was_heartbeat"]:
            #TODO logging
            #print("{} {} received heartbeat response from {}" \
            #        .format(labels[self.status], self.server_id, responder_idx))
            return
        elif response_dict["success"] == False:
            print("{} {} received mismatched logs failure from {}" \
                    .format(labels[self.status], self.server_id, responder_idx))
            # if the failure is due to mismatched logs, send more (older) logs
            self.next_index[responder_idx] -= 1
            self.send_append_entries_rpc(responder_id,
                    self.log[self.next_index[responder_id]:])
        else:
            print("{} {} received successful log response from {}" \
                    .format(labels[self.status], self.server_id, responder_idx))
            # if the entries were successfuly appended set the next 
            # expected log for the responder to be our next expected log
            self.next_index[responder_idx] = len(self.log)
            new_match_idx = len(self.log)-1
            self.match_index[responder_idx] = new_match_idx
            self.check_if_new_commit_index(new_match_idx)            


    def check_if_new_commit_index(self, new_match_idx):
        if new_match_idx <= self.commit_index: return
        # if there is a majority of match_index[i] >= new_match_idx, and
        # log[new_match_idx].term == current_term, set commit_idx = new_match_idx
        if self.log[new_match_idx][0] == self.current_term:
            temp = np.array(self.match_index)
            # to find the number of match_indexes with at leat the value of 
            # new_match_idx:
            num_greater_equal = sum(((temp-new_match_idx)>=0).astype(int))
            # if this is a majority
            if num_greater_equal > self.num_available_servers() // 2:
                self.commit_index = new_match_idx
                print("New commit_index of {} {} is {}" \
                        .format(labels[self.status], self.server_id, new_match_idx))
            
    
    def send_vote_msg(self, candidate_id, term, voted):
        vote_dict = {"type": VOTE,
                     "from": self.server_id,
                     "voted": voted,
                     "term": term}
        data = pickle.dumps(vote_dict)
        #TODO verbose
        #if voted:
        #    print("Vote from {} {} sent to {}" \
        #        .format(labels[self.status], self.server_id, candidate_id))
        self.send_data(candidate_id, data)


    def send_request_votes(self):
        log_term = 0 if len(self.log) == 0 else self.log[self.last_applied][0]
        rpc_dict = {"type": REQUEST_VOTE,
                    "candidate_term": self.current_term,
                    "candidate_id": self.server_id,
                    "last_log_idx": self.last_applied,
                    "last_log_term": log_term}
        data = pickle.dumps(rpc_dict)
        for i in range(self.total_num_servers):
            if i == self.server_id: continue
            self.send_data(i, data)


    def send_data(self, dest_server_num, data):
        try:
            if dest_server_num in self.unreachable: return
            self.connections[dest_server_num].send(data)
            #TODO maybe allow servers to re-enter network
            #if dest_server_num in self.unreachable:
            #    self.unreachable.remove(dest_server_num)
        except:
            print("{} {} unable to reach {}" \
                    .format(labels[self.status], self.server_id, dest_server_num))
            if dest_server_num not in self.unreachable:
                self.unreachable.append(dest_server_num)


    def num_available_servers(self):
        return self.total_num_servers - len(self.unreachable)


    def process_receive_vote_response(self, response):
        if self.status != CANDIDATE: return
        # if the voter is at a higher term than us, become a follower
        if response["term"] > self.current_term:
            self._convert_to_follower(response["term"])
        # if the voter votes for us
        elif response["voted"] == True:
            self.votes_received += 1
            print("Vote received by {} {} from {}" \
                    .format(labels[self.status], self.server_id, response["from"]))
        # if we've received votes from majority of servers: become leader
        #TODO verbose
        #print("Num votes for {} is {}".format(self.server_id, self.votes_received))
        if self.votes_received > self.num_available_servers() // 2:
            self._convert_to_leader()


    def _convert_to_leader(self):
        print("Server {} is now the Leader".format(self.server_id))
        self.status = LEADER
        # initialise the required next index for each server to be our
        # required next index
        self.next_index = [len(self.log)] * self.total_num_servers
        self.send_heartbeats()


    def process_received_command(self, cmd):
        new_log = [self.current_term, cmd]
        self.log.append(new_log)
        self.next_index[self.server_id] += 1
        self.match_index[self.server_id] = len(self.log)-1
        self.distribute_append_entries_rpcs(new_log)
        self.execute(cmd)
        self.last_applied = self.match_index[self.server_id]
        return True


    def distribute_append_entries_rpcs(self, new_log):
        last_log_index = len(self.log)-1
        all_successful = True
        for i in range(self.total_num_servers):
            if i == self.server_id: continue
            logs_to_send = self.log[-1]
            dest_serv_next_index = self.next_index[i]
            if last_log_index >= dest_serv_next_index:
                logs_to_send = self.log[dest_serv_next_index:]
            self.send_append_entries_rpc(i, logs_to_send)
        return True


    def send_append_entries_rpc(self, dest_serv_id, logs_to_send):
        #send an AppendEntries RPC to a server
        prev_log_idx = self.next_index[dest_serv_id]-1
        rpc_dict = {"type": APPEND_ENTRIES,
                    "term": self.current_term,
                    "leader_id": self.server_id,
                    "prev_log_idx": prev_log_idx,
                    "prev_log_term": self.log[prev_log_idx][0],
                    "entries": logs_to_send,
                    "leader_commit": self.commit_index}
        data = pickle.dumps(rpc_dict)
        self.send_data(dest_serv_id, data)
    

    def forward_received_command(self, cmd):
        print("{} {} is forwarding command" \
                .format(labels[self.status], self.server_id))
        forwarded_dict = {"type": FORWARDED_CMD,
                          "cmd": cmd}
        data = pickle.dumps(forwarded_dict)
        for i in range(self.total_num_servers):
            if i == self.server_id: continue
            self.send_data(i, data)


    def send_heartbeats(self):
        # send empty AppendEntries RPC to each server to avoid timeouts
        # during idle times
        rpc_dict = {"type": APPEND_ENTRIES,
                    "term": self.current_term,
                    "leader_id": self.server_id,
                    "entries": [],
                    "leader_commit": self.commit_index}
        data = pickle.dumps(rpc_dict)
        for i in range(self.total_num_servers):
            if i == self.server_id: continue
            self.send_data(i, data)
        self._reset_timer()


    def process_append_entries_rpc(self, rpc):

        # case when we receive rpc from an old leader
        if rpc["term"] < self.current_term: return self.current_term, False
        # when we receive rpc from a new leader
        elif rpc["term"] > self.current_term:
            self._convert_to_follower(rpc["term"])

        # case when it is just a heartbeat message
        if len(rpc["entries"]) == 0: 
            #TODO logging
            #print("{} {} received heartbeat from {}" \
            #        .format(labels[self.status], self.server_id, rpc["leader_id"]))
            self.execute_committed_commands(rpc)
            return self.current_term, True

        # case when there is a gap between the logs we have and the logs
        # we are receiving
        curr_num_entries = len(self.log)
        if rpc["prev_log_idx"] >= 0 and \
           (curr_num_entries-1 < rpc["prev_log_idx"] or \
           self.log[rpc["prev_log_idx"]][0] != rpc["prev_log_term"]):
            print("Gap in logs for {} {}" \
                    .format(labels[self.status], self.server_id))
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

        print("{} {} received new logs: {} from Leader {}"\
                .format(labels[self.status], self.server_id, 
                    rpc["entries"][start_idx:], rpc["leader_id"]))
        
        # add all new entries to our log
        self.log += rpc["entries"][start_idx:]

        self.execute_committed_commands(rpc)
        
        return rpc["term"], True


    def execute_committed_commands(self, rpc):
        # ensure our highest committed index is either our leaders committed
        # index or the highest index in our log
        if rpc["leader_commit"] > self.commit_index:
            self.commit_index = min(rpc["leader_commit"], len(self.log)-1)
        
        # if we have not applied all of the commands we know to be committed,
        # apply them now
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            print("{} {} current log: {}". \
                    format(labels[self.status], self.server_id, self.log))
            self.execute(self.log[self.last_applied][1])


    def send_append_entries_response(self, term, success, rpc_dict):
        response = {"type": APPEND_ENTRIES_RESPONSE,
                    "term": term,
                    "was_heartbeat": len(rpc_dict["entries"])==0,
                    "responder_id": self.server_id,
                    "success": success}
        data = pickle.dumps(response)
        self.send_data(rpc_dict["leader_id"], data)

    
    def _convert_to_follower(self, new_term):
        self.current_term = new_term
        self.status = FOLLOWER
        self.voted_for = None


    def execute(self, command):
        # for now just print the command from the logs
        print("{} {} Executing command: {}" \
                .format(labels[self.status], self.server_id, command))


    def process_request_vote_rpc(self, rpc):
        if rpc["candidate_term"] < self.current_term:
            return self.current_term, False
        
        # if we have not yet issued a vote, and if this candidates logs
        # are at least as up to date as ours, vote for this candidate
        if self.voted_for == None and rpc["last_log_idx"] >= (len(self.log)-1):

            self.voted_for = rpc["candidate_id"]
            return rpc["candidate_term"], True
        
        return rpc["candidate_term"], False


    def __str__(self):
        return "\n{}: {}\n" \
               "Term: {}\n" \
               "Voted for: {}\n" \
               "Last 5 log entries: {}\n" \
               "Highest log entry known committed: {}\n" \
               "Highest log entry received: {}\n" \
               "Next log indexes for each Server: {}\n" \
               "Highest log indexes replicated on each Server: {}\n" \
                .format(labels[self.status], self.server_id,
                        self.current_term, self.voted_for, self.log,
                        self.commit_index, self.last_applied,
                        self.next_index, self.match_index)

