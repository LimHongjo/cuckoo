import bson
import datetime
import hashlib
import os.path
import struct
import ntpath
import sys
import socket

# The BSON module provided by pymongo works through its "BSON" class.
if hasattr(bson, "BSON"):
    bson_decode = lambda d: bson.BSON(d).decode()
# The BSON module provided by "pip install bson" works through the
# "loads" function (just like pickle etc.)
elif hasattr(bson, "loads"):
    bson_decode = lambda d: bson.loads(d)

# 20 Mb max message length.
MAX_MESSAGE_LENGTH = 2000 * 1024 * 1024

def pointer_converter_32bit(v):
    return "0x%08x" % (v % 2 ** 32)

def pointer_converter_64bit(v):
    return "0x%016x" % (v % 2 ** 64)

def default_converter_32bit(v):
    if isinstance(v, (int, long)) and v < 0:
        return v % 2 ** 32

    # Try to avoid various unicode issues through usage of latin-1 encoding.
    if isinstance(v, str):
        return v.decode("latin-1")
    return v

def default_converter_64bit(v):
    # Don't convert signed 64-bit integers into unsigned 64-bit integers as
    # MongoDB doesn't support 64-bit unsigned integers (and ElasticSearch
    # probably doesn't either).
    # if isinstance(v, (int, long)) and v < 0:
    # return v % 2**64

    # Try to avoid various unicode issues through usage of latin-1 encoding.
    if isinstance(v, str):
        return v.decode("latin-1")
    return v

def get_filename_from_path(path):
    """Cross-platform filename extraction from path.
    @param path: file path.
    @return: filename.
    """
    dirpath, filename = ntpath.split(path)
    return filename if filename else ntpath.basename(dirpath)

class BsonParser():
    """Receives and interprets .bson logs from the monitor.

    The monitor provides us with "info" messages that explain how the function
    arguments will come through later on. This class remembers these info
    mappings and then transforms the api call messages accordingly.

    Other message types typically get passed through after renaming the
    keys slightly.
    """
    converters_32bit = {
        None: default_converter_32bit,
        "p": pointer_converter_32bit,
        "x": pointer_converter_32bit,
    }

    converters_64bit = {
        None: default_converter_64bit,
        "p": pointer_converter_64bit,
        "x": pointer_converter_32bit,
    }

    def __init__(self, handler):
        self.fd = handler
        self.infomap = {}
        self.flags_value = {}
        self.flags_bitmask = {}
        self.pid = None
        self.is_64bit = False
        self.buffer_sha1 = None

    def resolve_flags(self, apiname, argdict, flags):
        # Resolve 1:1 values.
        for argument, values in self.flags_value[apiname].items():
            if isinstance(argdict[argument], str):
                value = int(argdict[argument], 16)
            else:
                value = argdict[argument]

            if value in values:
                flags[argument] = values[value]

        # Resolve bitmasks.
        for argument, values in self.flags_bitmask[apiname].items():
            if argument in flags:
                continue

            flags[argument] = []

            if isinstance(argdict[argument], str):
                value = int(argdict[argument], 16)
            else:
                value = argdict[argument]

            for key, flag in values:
                # TODO Have the monitor provide actual bitmasks as well.
                if (value & key) == key:
                    flags[argument].append(flag)

            flags[argument] = "|".join(flags[argument])

    def determine_unserializers(self, arginfo):
        """Determines which unserializers (or converters) have to be used in
        order to parse the various arguments for this function call. Keeps in
        mind whether the current bson is 32-bit or 64-bit."""
        argnames, converters = [], []

        for argument in arginfo:
            if isinstance(argument, (tuple, list)):
                argument, argtype = argument
            else:
                argtype = None

            if self.is_64bit:
                converter = self.converters_64bit[argtype]
            else:
                converter = self.converters_32bit[argtype]

            argnames.append(argument)
            converters.append(converter)

        return argnames, converters

    def __iter__(self):
        self.fd.seek(0)

        while True:
            data = self.fd.read(4)
            if not data:
                #print("over!")
                return

            if len(data) != 4:
                print("! [" + str(self.fd.version) + "] BsonParser lacking data.")
                return

            blen = struct.unpack("I", data)[0]
            if blen > MAX_MESSAGE_LENGTH:
                print("! [" + str(self.fd.version) + "] BSON message larger than MAX_MESSAGE_LENGTH, stopping handler.")
                return

            data += self.fd.read(blen - 4)
            if len(data) < blen:
                print("! [" + str(self.fd.version) + "] BsonParser lacking data.")
                return

            try:
                dec = bson_decode(data)
            except Exception as e:
                print("! [" + str(self.fd.version) + "] BsonParser decoding problem " + str(e) + " on data[:50] " + str(repr(data[:50])))
                return

            mtype = dec.get("type", "none")
            index = dec.get("I", -1)

            # debug bson object
            self.fd.writeFile("bson : " + str(dec))
            self.fd.writeFile("\n")

            if mtype == "info":
                # API call index info message, explaining the argument names, etc.
                name = dec.get("name", "NONAME")
                arginfo = dec.get("args", [])
                category = dec.get("category")

                argnames, converters = self.determine_unserializers(arginfo)
                self.infomap[index] = name, arginfo, argnames, converters, category

                if dec.get("flags_value"):
                    self.flags_value[name] = {}
                    for arg, values in dec["flags_value"].items():
                        self.flags_value[name][arg] = dict(values)

                if dec.get("flags_bitmask"):
                    self.flags_bitmask[name] = {}
                    for arg, values in dec["flags_bitmask"].items():
                        self.flags_bitmask[name][arg] = values
                continue

            # Handle dumped buffers.
            if mtype == "buffer":
                buf = dec.get("buffer")
                sha1 = dec.get("checksum")
                self.buffer_sha1 = hashlib.sha1(buf).hexdigest()

                # Why do we pass along a sha1 checksum again?
                if sha1 != self.buffer_sha1:
                    print("! [" + str(self.fd.version) + "] Incorrect sha1 passed along for a buffer.")

                filepath = os.path.join(
                    "C:\\", "buffer"+str(self.fd.version), self.buffer_sha1
                )
                with open(filepath, "wb") as f:
                    f.write(buf)

                continue

            tid = dec.get("T", 0)
            time = dec.get("t", 0)

            parsed = {
                "type": mtype,
                "tid": tid,
                "time": time,
            }

            if mtype == "debug":
                parsed["message"] = dec.get("msg", "")
                print("# [" + str(self.fd.version) + "] Debug message from monitor: " + str(parsed["message"]))
            else:
                # Regular api call from monitor
                if index not in self.infomap:
                    print("! [" + str(self.fd.version) + "] Got API with unknown index - monitor needs to explain first: " + str(dec))
                    continue

                apiname, arginfo, argnames, converters, category = self.infomap[index]
                args = dec.get("args", [])

                if len(args) != len(argnames):
                    print("! [" + str(self.fd.version) + "] Inconsistent arg count (compared to arg names) on " + str(dec) + " : " + str(
                        argnames) + " names " + str(apiname))
                    continue

                argdict = {}
                for idx, value in enumerate(args):
                    argdict[argnames[idx]] = converters[idx](value)

                # Special new process message from the monitor.
                if apiname == "__process__":
                    parsed["type"] = "process"

                    if "TimeLow" in argdict:
                        timelow = argdict["TimeLow"]
                        timehigh = argdict["TimeHigh"]

                        parsed["pid"] = pid = argdict["ProcessIdentifier"]
                        parsed["ppid"] = argdict["ParentProcessIdentifier"]
                        modulepath = argdict["ModulePath"]

                    elif "time_low" in argdict:
                        timelow = argdict["time_low"]
                        timehigh = argdict["time_high"]

                        if "pid" in argdict:
                            parsed["pid"] = pid = argdict["pid"]
                            parsed["ppid"] = argdict["ppid"]
                        else:
                            parsed["pid"] = pid = argdict["process_identifier"]
                            parsed["ppid"] = argdict["parent_process_identifier"]

                        modulepath = argdict["module_path"]

                    else:
                        raise Exception(
                            "I don't recognize the bson log contents."
                        )

                    # FILETIME is 100-nanoseconds from 1601 :/
                    vmtimeunix = (timelow + (timehigh << 32))
                    vmtimeunix = vmtimeunix / 10000000.0 - 11644473600
                    vmtime = datetime.datetime.fromtimestamp(vmtimeunix)
                    parsed["first_seen"] = vmtime

                    procname = get_filename_from_path(modulepath)
                    parsed["process_path"] = modulepath
                    parsed["process_name"] = procname
                    parsed["command_line"] = argdict.get("command_line")

                    # Is this a 64-bit process?
                    if argdict.get("is_64bit"):
                        self.is_64bit = True

                    # Is this process being "tracked"?
                    parsed["track"] = bool(argdict.get("track", 1))
                    parsed["modules"] = argdict.get("modules", {})

                    self.pid = pid

                elif apiname == "__thread__":
                    parsed["pid"] = pid = argdict["ProcessIdentifier"]

                # elif apiname == "__anomaly__":
                    # tid = argdict["ThreadIdentifier"]
                    # subcategory = argdict["Subcategory"]
                    # msg = argdict["Message"]
                    # self.handler.log_anomaly(subcategory, tid, msg)
                    # return True
                elif apiname == "__action__":
                    parsed["type"] = "action"
                    parsed["action"] = argdict["action"]
                else:
                    parsed["type"] = "apicall"
                    parsed["pid"] = self.pid
                    parsed["api"] = apiname
                    parsed["category"] = category
                    parsed["status"] = argdict.pop("is_success", 1)
                    parsed["return_value"] = argdict.pop("retval", 0)
                    parsed["arguments"] = argdict
                    parsed["flags"] = {}

                    parsed["stacktrace"] = dec.get("s", [])
                    parsed["uniqhash"] = dec.get("h", 0)

                    if "e" in dec and "E" in dec:
                        parsed["last_error"] = dec["e"]
                        parsed["nt_status"] = dec["E"]

                    if apiname in self.flags_value:
                        self.resolve_flags(apiname, argdict, parsed["flags"])

                    if self.buffer_sha1:
                        parsed["buffer"] = self.buffer_sha1
                        self.buffer_sha1 = None

            yield parsed


class BsonHandler():
    """Result handler.

    This handler speaks our analysis log network protocol.
    """

    def __init__(self, client, version):
        self.client = client
        self.version = version
        self.file = open("C:\\socket" + str(version) + ".txt", "w")

    def handle(self):
        # Initialize the protocol handler class for this connection.
        self.parser = BsonParser(self)

        for event in self.parser:
            pass

    def seek(self, pos):
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

    def writeFile(self, str):
        self.file.write(str)
        self.file.flush()

    def disconnect(self):
        print("# [" + str(self.version) + "] Disconnected")
        self.client.close()
        self.file.close()
        sys.exit()

