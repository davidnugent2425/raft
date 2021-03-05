import socket
from server import BASE_PORT_NUM

def write():
    user_input = input('\nEnter your next command:\n')
    input_parts = user_input.split(' ')
    command = input_parts[0]
    server_id = int(input_parts[1])
    print("Server ID: {}".format(server_id))
    dest_port = server_id + BASE_PORT_NUM
    if command == 'cmd':
        cmd_to_send = user_input[user_input.find('\"')+1:-1]
    elif command == 'KILL':
        cmd_to_send = 'KILL'
    else:
        print("Invalid command.")
        return
    print("Command:", cmd_to_send)
    print("Dest Port: {}".format(dest_port))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', dest_port))
        sock.send(cmd_to_send.encode('utf-8'))
        sock.close()
    except:
        print("Unable to send to port {}".format(dest_port))

if __name__ == '__main__':
    while(True):
        write()
