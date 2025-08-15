#
# This is a simple client I wrote to test the server. It connects to the
# server, sends a command, and prints the response.
#

import socket
import json

HOST, PORT = "localhost", 9999
def send_command(command):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        sock.sendall(command.encode('utf-8'))
        received = str(sock.recv(1024), "utf-8")
        return received.strip()

print("Putting a value...")
print(send_command("PUT key1 value_one"))
print("Reading a value...")
print(send_command("READ key1"))
