import os.path
import sys
import socket
import threading
import json

BUFSIZE = 1024 * 1024

class Folders():
    @staticmethod
    def create(root=".", folders=None):
        """Creates a directory or multiple directories.
        @param root: root path.
        @param folders: folders list to be created.
        @raise CuckooOperationalError: if fails to create folder.
        If folders is None, we try to create the folder provided by `root`.
        """
        if isinstance(root, (tuple, list)):
            root = os.path.join(*root)

        if folders is None:
            folders = [""]
        elif isinstance(folders, basestring):
            folders = folders,

        for folder in folders:
            folder_path = os.path.join(root, folder)
            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)

class FileUpload():
    lock = threading.Lock()

    def __init__(self, handler):
        self.handler = handler
        self.upload_max_size = 134217728
        self.storagepath = "C:\\file"
        self.fd = None

        self.filelog = os.path.join(self.storagepath, "files.json")

    def __iter__(self):
        # Read until newline for file path, e.g.,
        # shots/0001.jpg or files/9498687557/libcurl-4.dll.bin

        dump_path = self.handler.read_newline().replace("\\", "/")

        if self.handler.version >= 2:
            filepath = self.handler.read_newline()
            pids = map(int, self.handler.read_newline().split())
        else:
            filepath, pids = None, []

        print("# [" + str(self.handler.version) + "] File upload request for " + dump_path)

        dir_part, filename = os.path.split(dump_path)

        if "./" in dump_path or not dir_part or dump_path.startswith("/"):
            print("! [" + str(self.handler.version) + "] FileUpload failure, banned path: " + dump_path)
            self.handler.disconnect()

        try:
            Folders.create(self.storagepath, dir_part)
        except OSError:
            print("! [" + str(self.handler.version) + "] Unable to create folder " + dir_part)
            return

        file_path = os.path.join(self.storagepath, dump_path)

        if not file_path.startswith(self.storagepath):
            print("! [" + str(self.handler.version) + "] FileUpload failure, path sanitization failed.")
            self.handler.disconnect()

        #if os.path.exists(file_path):
        #    print("! [" + str(self.handler.version) + "] Analyzer tried to overwrite an existing file, closing connection.")
        #    return

        self.fd = open(file_path, "wb")
        chunk = self.handler.read_any()
        while chunk:
            self.fd.write(chunk)

            if self.fd.tell() >= self.upload_max_size:
                print("! [" + str(self.handler.version) + "] Uploaded file length larger than upload_max_size, stopping upload.")
                self.fd.write("... (truncated)")
                break

            chunk = self.handler.read_any()

        self.lock.acquire()

        with open(self.filelog, "a+b") as f:
            f.write("%s\n" % json.dumps({
                "path": dump_path,
                "filepath": filepath,
                "pids": pids,
            }))

        self.lock.release()

        print("# [" + str(self.handler.version) + "] Uploaded file length: " + str(self.fd.tell()))
        return
        yield

    def close(self):
        if self.fd:
            self.fd.close()


class FileHandler():
    """Result handler.

    This handler speaks our analysis log network protocol.
    """

    def __init__(self, client, version):
        self.client = client
        self.version = version

    def handle(self):
        # Initialize the protocol handler class for this connection.
        self.parser = FileUpload(self)

        for event in self.parser:
            pass

    def read(self, length):
        buf = ""

        try:
            while len(buf) < length:
                tmp = self.client.recv(length - len(buf))
                if not tmp:
                    self.disconnect()
                    break
                buf += tmp
        except socket.error:
            self.disconnect()

        return buf

    def read_any(self):
        buf = ""

        try:
            buf = self.client.recv(BUFSIZE)
            if not buf:
                self.disconnect()
        except socket.error:
            self.disconnect()

        return buf

    def read_newline(self):
        buf = ""

        while "\n" not in buf:
            buf += self.read(1)
        buf = buf.strip()

        return buf

    def disconnect(self):
        print("# [" + str(self.version) + "] Disconnected")
        if self.parser.fd:
            self.parser.fd.close()
        self.client.close()
        sys.exit()
