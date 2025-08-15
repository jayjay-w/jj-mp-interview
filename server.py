#
# This is the server part of our key-value store. It's the core of the
# whole thing—it handles the actual storage and retrieval of your data.
# It listens on a network port, waits for requests, and processes them.
#

import os
import struct
import json
import socketserver
import threading
import time
from collections import OrderedDict

# --- Configuration ---
HOST, PORT = "localhost", 9999

DATA_FILE = "store.dat"
COMPACTION_THRESHOLD = 0.5  # If half the file is old, overwritten data, I'll clean it up.

# --- The communication protocol ---
# I'm using a simple text-based format for the network. It's easy to read.
# Put: "PUT <key> <value>\n"
# Read: "READ <key>\n"
# Delete: "DELETE <key>\n"
# BatchPut: "BATCHPUT <num_items>\n<key1> <value1>\n<key2> <value2>\n..."
# ReadKeyRange: "READRANGE <start_key> <end_key>\n"
# Every response will start with "OK" or "ERROR", so you always know what's going on.

class KeyValueStore:
    """
    This is my persistent Key/Value store. The big idea here is to be
    super fast for writes and not lose data if something goes wrong.
    It's like a notebook where I only ever add new entries. To find the
    most recent entry for a key, I use an index that I keep in memory.
    """
    def __init__(self):
        self.data_file = DATA_FILE
        self.lock = threading.Lock() # I need this lock because multiple clients will be accessing me at once.
        self.keys = OrderedDict()  # This is my in-memory index: key -> (file_position, entry_size, timestamp)
        self.data_size = 0  # Total size of all the data in the file
        self.deleted_size = 0  # Size of entries that have been overwritten or deleted (the garbage)
        self._load_from_disk()

    def _load_from_disk(self):
        """
        When I first start up, I have to rebuild my index. I do this by
        reading through the entire data file, entry by entry, and making
        sure my in-memory index points to the very last value I saw for each key.
        This is my crash recovery plan!
        """
        print("Rebuilding my index from the data file...")
        self.keys.clear()
        self.data_size = 0
        self.deleted_size = 0
        
        if not os.path.exists(self.data_file):
            print("No data file found. I'll create one when you give me some data to save.")
            return

        with open(self.data_file, 'rb') as f:
            while True:
                pos = f.tell() # Remember where I am in the file
                # Every entry starts with a fixed-size header:
                # timestamp (8 bytes), key_size (4 bytes), value_size (4 bytes)
                header = f.read(16)
                if not header:
                    break
                
                timestamp, key_size, value_size = struct.unpack("!QII", header)
                key = f.read(key_size).decode('utf-8')
                value = f.read(value_size) # I don't need to save the value in memory, just its location
                
                entry_size = 16 + key_size + value_size
                if key in self.keys:
                    old_pos, old_size, _ = self.keys[key]
                    self.deleted_size += old_size # If I see a key again, its old data is now garbage
                
                self.keys[key] = (pos, entry_size, timestamp)
                self.data_size += entry_size
        print(f"Index rebuilt. I found {len(self.keys)} keys. My data file is {self.data_size} bytes big.")

    def _compact(self):
        """
        This function is my cleanup crew. It's time to get rid of all the old,
        overwritten data. I'll create a brand new file and just copy over
        the most recent entry for each key. This reclaims a lot of space.
        """
        print("Starting a compaction process to clean up the data file...")
        new_file = f"{self.data_file}.tmp"
        with open(self.data_file, 'rb') as old_f, open(new_file, 'wb') as new_f:
            for key, (pos, size, timestamp) in self.keys.items():
                old_f.seek(pos)
                entry_data = old_f.read(size)
                
                new_pos = new_f.tell()
                new_f.write(entry_data)
                
                # After moving the data, I have to update my in-memory index to the new location.
                self.keys[key] = (new_pos, size, timestamp)

        os.replace(new_file, self.data_file)
        self.deleted_size = 0
        self.data_size = os.path.getsize(self.data_file)
        print("Compaction finished. The file is much smaller now.")

    def put(self, key, value):
        """Adds or updates a key-value pair. It's just a quick append to the end of the file."""
        with self.lock:
            # First, check if it's time to do some house-cleaning
            if self.data_size > 0 and self.deleted_size / self.data_size > COMPACTION_THRESHOLD:
                self._compact()
                
            value_bytes = value.encode('utf-8')
            key_bytes = key.encode('utf-8')
            
            timestamp = int(time.time())
            key_size = len(key_bytes)
            value_size = len(value_bytes)
            
            # Pack the header into a nice, tidy byte string
            header = struct.pack("!QII", timestamp, key_size, value_size)
            
            with open(self.data_file, 'ab') as f:
                pos = f.tell() # This is the position where the new entry will start
                f.write(header)
                f.write(key_bytes)
                f.write(value_bytes)
                
            entry_size = 16 + key_size + value_size
            
            # Now, update my in-memory index.
            if key in self.keys:
                # If this key existed before, its old entry is now just garbage.
                old_pos, old_size, _ = self.keys[key]
                self.deleted_size += old_size
            
            self.keys[key] = (pos, entry_size, timestamp)
            self.data_size += entry_size
            return True

    def read(self, key):
        """Reads the value for a given key. It's a two-step process: find the location in memory, then jump to it in the file."""
        with self.lock:
            if key not in self.keys:
                return None
            
            pos, entry_size, _ = self.keys[key]
            
            with open(self.data_file, 'rb') as f:
                f.seek(pos) # Jump straight to the right spot
                header = f.read(16)
                timestamp, key_size, value_size = struct.unpack("!QII", header)
                # Skip the key data and go right to the value
                f.seek(pos + 16 + key_size)
                value = f.read(value_size).decode('utf-8')
            
            # I use a special "DELETED" value to mark things for removal.
            if value == "DELETED":
                return None
            return value

    def read_key_range(self, start_key, end_key):
        """
        I'll find all the keys and their latest values within a given alphabetical range.
        This isn't the most efficient thing because I have to sort all the keys first.
        """
        result = []
        with self.lock:
            # First, get a list of keys and their file positions while holding the lock
            keys_to_read = []
            sorted_keys = sorted(self.keys.keys())
            
            for key in sorted_keys:
                if start_key <= key <= end_key:
                    # Get the file position and size.
                    keys_to_read.append((key, self.keys[key][0], self.keys[key][1]))

        # Now, release the lock and read the values from the file
        with open(self.data_file, 'rb') as f:
            for key, pos, size in keys_to_read:
                f.seek(pos)
                header = f.read(16)
                timestamp, key_size, value_size = struct.unpack("!QII", header)
                f.seek(pos + 16 + key_size)
                value = f.read(value_size).decode('utf-8')
                if value != "DELETED":
                    result.append((key, value))
        return result

    def batch_put(self, items):
        """
        Adds a bunch of key-value pairs at once. This is way faster than calling `put`
        over and over because I can just do one big file append.
        """
        with self.lock:
            if self.data_size > 0 and self.deleted_size / self.data_size > COMPACTION_THRESHOLD:
                self._compact()
            
            with open(self.data_file, 'ab') as f:
                for key, value in items.items():
                    value_bytes = value.encode('utf-8')
                    key_bytes = key.encode('utf-8')
                    
                    timestamp = int(time.time())
                    key_size = len(key_bytes)
                    value_size = len(value_bytes)
                    
                    header = struct.pack("!QII", timestamp, key_size, value_size)
                    pos = f.tell()
                    
                    f.write(header)
                    f.write(key_bytes)
                    f.write(value_bytes)
                    
                    entry_size = 16 + key_size + value_size
                    if key in self.keys:
                        old_pos, old_size, _ = self.keys[key]
                        self.deleted_size += old_size
                    
                    self.keys[key] = (pos, entry_size, timestamp)
                    self.data_size += entry_size
            return True

    def delete(self, key):
        """
        I delete a key by writing a special "tombstone" entry. It's basically a `put`
        with a value of "DELETED". The old data will be cleaned up during the next compaction run.
        """
        with self.lock:
            # Check if the key exists before trying to delete it
            if key not in self.keys:
                return False

            value_bytes = "DELETED".encode('utf-8')
            key_bytes = key.encode('utf-8')

            timestamp = int(time.time())
            key_size = len(key_bytes)
            value_size = len(value_bytes)

            header = struct.pack("!QII", timestamp, key_size, value_size)
            
            with open(self.data_file, 'ab') as f:
                pos = f.tell()
                f.write(header)
                f.write(key_bytes)
                f.write(value_bytes)

            entry_size = 16 + key_size + value_size
            
            # Now, update my in-memory index.
            if key in self.keys:
                old_pos, old_size, _ = self.keys[key]
                self.deleted_size += old_size
            
            self.keys[key] = (pos, entry_size, timestamp)
            self.data_size += entry_size
            
            # Since we just added a tombstone, we may need to compact
            if self.data_size > 0 and self.deleted_size / self.data_size > COMPACTION_THRESHOLD:
                self._compact()
            
            return True


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # A new thread is created for each client, so I can handle multiple clients at once.
        data = self.request.recv(1024).decode('utf-8').strip()
        
        # Cleanly stop the server for testing
        if data == "SHUTDOWN":
            self.server.shutdown()
            self.request.sendall(b"OK\n")
            return

        command, *args = data.split(' ', 1)
        
        store = self.server.store
        response = ""

        try:
            print(f"Received command: {command} with args: {args}")
            if command == "PUT":
                key, value = args[0].split(' ', 1)
                store.put(key, value)
                response = f"OK\n"
            elif command == "READ":
                key = args[0]
                value = store.read(key)
                response = f"OK {value}\n" if value is not None else "OK NULL\n"
            elif command == "DELETE":
                key = args[0]
                if store.delete(key):
                    response = "OK\n"
                else:
                    response = "ERROR Key not found\n"
            elif command == "READRANGE":
                start_key, end_key = args[0].split(' ', 1)
                results = store.read_key_range(start_key, end_key)
                response = "OK " + json.dumps(results) + "\n"
            elif command == "BATCHPUT":
                # This part is a placeholder. For a real system, I'd need to handle
                # multi-line input more carefully.
                pass
            else:
                response = "ERROR Invalid command\n"
        except Exception as e:
            response = f"ERROR {e}\n"
        finally:
            self.request.sendall(response.encode('utf-8'))

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def __init__(self, server_address, RequestHandlerClass, store):
        super().__init__(server_address, RequestHandlerClass)
        self.store = store
        self.daemon_threads = True

if __name__ == '__main__':
    # This block ensures that the server runs only when this script is executed directly.
    # It will not run when the file is imported as a module in another script, like server_tests.py.
    print("Starting the server...")
    kv_store = KeyValueStore()
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler, kv_store)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Server shut down.")
    finally:
        server.server_close()
