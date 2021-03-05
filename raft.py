# implementation of the raft algorithm from the paper:
# https://raft.github.io/raft.pdf

import asyncio
from server import Server
#from remote_procedure_calls import AppendEntries, RequestVote

async def main():
    servers = []
    num_servers = 5
    for i in range(num_servers):
        servers.append(Server(i, num_servers))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
