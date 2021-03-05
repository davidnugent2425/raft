# implementation of the raft algorithm from the paper:
# https://raft.github.io/raft.pdf

import asyncio
from server import Server

async def main():
    servers = []
    num_servers = None
    while True:
        num_servers = input("\nEnter the numbers of Servers " \
                            "you want in the network (min. 3):\n")
        if num_servers.isalnum() and int(num_servers) > 2: 
            num_servers = int(num_servers)
            break

    for i in range(num_servers):
        try:
            servers.append(Server(i, num_servers))
        except:
            print("Server {} unavailable".format(i))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
