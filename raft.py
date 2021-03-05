# implementation of the raft algorithm from the paper:
# https://raft.github.io/raft.pdf

import asyncio
from server import Server
from remote_procedure_calls import AppendEntries, RequestVote

async def main():
    servers = []
    for i in range(3):
        servers.append(Server(i, 3))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
