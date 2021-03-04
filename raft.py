# implementation of the raft algorithm from the paper:
# https://raft.github.io/raft.pdf

import asyncio
from server import Server
from remote_procedure_calls import AppendEntries, RequestVote

async def main():
    server = Server(1);
    sample_ae_rpc = AppendEntries(1, 1, 1, 1, [], 1);
    print(sample_ae_rpc)
    print(server)
    server.process_append_entries_rpc(sample_ae_rpc)
    sample_rv_rpc = RequestVote(1, 1, 1, 1)
    print(sample_rv_rpc)
    server.process_request_vote_rpc(sample_rv_rpc)
    print(server)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
