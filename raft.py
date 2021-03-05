# implementation of the raft algorithm from the paper:
# https://raft.github.io/raft.pdf

import asyncio
from server import Server

# simple script for taking user input and running the Server class on multiple ports

async def main():
    servers = []
    num_servers = None
    verbose = False
    while True:
        user_input = input("\nEnter the numbers of Servers " \
                            "you want in the network (min. 3):\n" \
                            "Add tag -v for verbose output\n")
        user_input_split = user_input.split(' ')
        # check if user input -v for verbose
        if len(user_input_split) > 1:
            num_servers = user_input_split[0]
            if user_input_split[1] == "-v": verbose = True
        else: num_servers = user_input
        try:
            if int(num_servers) > 2: 
                num_servers = int(num_servers)
                # if number valid, start building network
                break
        except:
            pass
        # if number invalid, turn verbose back off just incase
        verbose = False

    for i in range(num_servers):
        servers.append(Server(i, num_servers, verbose))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
