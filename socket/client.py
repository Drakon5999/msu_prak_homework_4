# создание сокета, клиент

import socket
import sys
import json
sys.path.append("..")
from libs import tools
from libs.constants import *

COMMANDS = {
    b'': 1
}

file_name = sys.argv[1]
command = sys.argv[2]

with open(file_name, "r") as f:
    twits_json = f.read()

bytes_to_send = bytes(twits_json, encoding=ENCODING)
with socket.create_connection(("127.0.0.1", SERVER_PORT)) as sock:
    sock.sendall(bytes(command, encoding=ENCODING))
    sock.sendall(int.to_bytes(len(bytes_to_send), INT_SIZE, BYTE_ORDER))
    sock.sendall(bytes_to_send)

    parser = tools.DataParser(COMMANDS)
    while True:
        data = sock.recv(1024)
        if not data:
            break
        parser.parse_data(data)

    if not parser.is_data_ready:
        print("fail")
    else:
        print(json.dumps(json.loads(parser.data_to_process.decode(ENCODING)), indent=4, sort_keys=True))
