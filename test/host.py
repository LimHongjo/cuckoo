import socket
import threading

from bsonhandler import BsonHandler

def readProtocol(client):
    buf = ""

    while '\x0A' not in buf:
        buf += client.recv(1)
    buf = buf.strip()

    return buf


def handleClient(client):
    # Parse protocol and Set handler
    protocol = readProtocol(client)
    if " " in protocol:
        command, version = protocol.split()
        version = int(version)
    else:
        command, version = protocol, None

    print("# [" + str(version) + "] New client(" + str(version) + ") connected")

    if command == "BSON":
        print("# [" + str(version) + "] Check C:\\socket" + str(version) + ".txt file")
        handler = BsonHandler(client, version)
    elif command == "FILE":
        # handler = FileUpload(self, version)
        print("# [" + str(version) + "] FILE command not yet implemented")
        client.close()
        return
    elif command == "LOG":
        # handler = LogHandler(self, version)
        print("# [" + str(version) + "] LOG command not yet implemented")
        client.close()
        return

    handler.handle()


if __name__ == "__main__":
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 2042))
    server.listen(5)
    print("# Listening...")
    while True:
        client, _ = server.accept()
        t = threading.Thread(target=handleClient, args=(client,))
        t.start()