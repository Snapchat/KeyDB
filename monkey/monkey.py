import random
import time
import socket
import asyncore
import threading
import argparse
from pprint import pprint

# Globals
ops = {}
numclients = 0
numkeys = 0
runtime = 0
clients = []

def _buildResp(*args):
    result = "*" + str(len(args)) + "\r\n"
    for v in args:
        result = result + "$" + str(len(v)) + "\r\n"
        result = result + v + "\r\n"
    return result.encode('utf-8')

class Client(asyncore.dispatcher):
    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))
        self.buf = b''
        self.inbuf = b''
        self.callbacks = list()
        self.client_id = 0
        self.get_client_id()

    def handle_connect(self):
        pass

    def handle_read(self):
        self.inbuf += self.recv(8192)
        self.parse_response()

    def handle_write(self):
        sent = self.send(self.buf)
        self.buf = self.buf[sent:]

    def handle_close(self):
        self.close()

    def writable(self):
        return len(self.buf) > 0

    def parse_array(self, startpos):
        assert(self.inbuf[startpos] == ord('*'))
        endrange = self.inbuf[startpos+1:].find(ord('\r')) + 1 + startpos
        assert(endrange > 0)
        numargs = int(self.inbuf[startpos+1:endrange])
        if numargs == -1: # Nil array, used in some returns
            startpos = endrange + 2
            return startpos, []
        assert(numargs > 0)
        args = list()
        startpos = endrange + 2 # plus 1 gets us to the '\n' and the next gets us to the start char

        while len(args) < numargs:
            # We're parsing entries of the form "$N\r\nnnnnnn\r\n"
            if startpos >= len(self.inbuf):
                return # Not the full response
            if self.inbuf[startpos] == ord('*'):
                startpos, arr = self.parse_array(startpos)
                args.append(arr)
            else:
                assert(self.inbuf[startpos] == ord('$'))
                startpos = startpos + 1
                endrange = self.inbuf[startpos:].find(b'\r')
                if endrange < 0:
                    return
                endrange += startpos
                assert(endrange <= len(self.inbuf))
                length = int(self.inbuf[startpos:endrange])
                if length < 0:
                    return
                startpos = endrange + 2
                assert((startpos + length) <= len(self.inbuf))
                assert(self.inbuf[startpos+length] == ord('\r'))
                assert(self.inbuf[startpos+length+1] == ord('\n'))
                args.append(self.inbuf[startpos:(startpos+length)])
                startpos += length + 2
        assert(len(args) == numargs)
        return startpos, args

    def parse_response(self):
        if len(self.inbuf) == 0:
            return
        
        while len(self.inbuf) > 0:
            if self.inbuf[0] == ord('+') or self.inbuf[0] == ord('-') or self.inbuf[0] == ord(':'):
                # This is a single line response
                endpos = self.inbuf.find(b'\n')
                if endpos < 0:
                    return  # incomplete response
                self.callbacks[0](self, self.inbuf[0:endpos-1])
                self.callbacks.pop(0)
                self.inbuf = self.inbuf[endpos+1:]

            elif self.inbuf[0] == ord('*'):
                #RESP response
                try:
                    startpos, args = self.parse_array(0)
                except:
                    return # Not all data here yet
                self.callbacks[0](self, args)
                self.callbacks.pop(0)
                self.inbuf = self.inbuf[startpos:]
            else:
                print("ERROR: Unknown response:")
                pprint(self.inbuf)
                assert(False)


    def default_result_handler(self, result):
        pprint(result)

    # Public Methods
    def set(self, key, val, callback = default_result_handler):
        self.buf += _buildResp("set", key, val)
        self.callbacks.append(callback)

    def lpush(self, key, val, callback = default_result_handler):
        self.buf += _buildResp("lpush", key, val)
        self.callbacks.append(callback)

    def blpop(self, *keys, timeout=0, callback=default_result_handler):
        self.buf += _buildResp("blpop", *keys, str(timeout))
        self.callbacks.append(callback)

    def delete(self, key, callback = default_result_handler):
        self.buf += _buildResp("del", key)
        self.callbacks.append(callback)

    def unblock(self, client_id, callback=default_result_handler):
        self.buf += _buildResp("client", "unblock", str(client_id))
        self.callbacks.append(callback)

    def scan(self, iter, match=None, count=None, callback = default_result_handler):
        args = ["scan", str(iter)]
        if match != None:
            args.append("MATCH")
            args.append(match)
        if count != None:
            args.append("COUNT")
            args.append(str(count))
        self.buf += _buildResp(*args)
        self.callbacks.append(callback)

    def get_client_id(self):
        self.buf += _buildResp("client", "id")
        self.callbacks.append(self.store_client_id)

    def store_client_id(self, c, resp):
        assert(resp[0] == ord(':'))
        self.client_id = int(resp[1:])
        assert(self.client_id == c.client_id)

    def get(self, key, callback = None):
       return 

def getrandomkey():
    return str(random.randrange(0, numkeys))

def handle_lpush_response(c, resp=None):
    global ops
    if resp != None:
        ops['lpush'] += 1
        assert(resp[0] == ord(':'))
    c.lpush("list_" + getrandomkey(), 'bardsklfjkldsjfdlsjflksdfjklsdjflksd kldsjflksd jlkdsjf lksdjklds jrfklsdjfklsdjfkl', handle_lpush_response)

def handle_blpop_response(c, resp=None):
    global ops
    if resp != None:
        ops['blpop'] += 1
    c.blpop("list_" + getrandomkey(), callback=handle_blpop_response)

def handle_set_response(c, resp=None):
    global ops
    if resp != None:
        ops['set'] += 1
        assert(resp[0] == ord('+'))
    c.set("str_" + getrandomkey(), 'bardsklfjkldsjfdlsjflksdfjklsdjflksd kldsjflksd jlkdsjf lksdjklds jrfklsdjfklsdjfkl', handle_set_response)

def handle_del_response(c, resp=None):
    global ops
    if resp != None:
        ops['del'] += 1
    c.delete("list_" + getrandomkey(), handle_del_response)

def scan_callback(c, resp=None):
    global ops
    nextstart = int(resp[0])
    c.scan(nextstart, count=500, callback=scan_callback)
    ops['scan'] += 1

def clear_ops():
    global ops
    ops = {'lpush': 0, 'blpop': 0, 'del': 0, 'scan': 0, 'set': 0, 'get': 0}

def stats_thread():
    global ops
    global runtime
    i = 0
    while i < runtime or not runtime:
        time.sleep(1)
        print("Ops per second: " + str({k:v for (k,v) in ops.items() if v}))
        clear_ops()
        i += 1
    asyncore.close_all()

def flush_db_sync():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect(('127.0.0.1', 6379))
    server.send(_buildResp("flushdb"))
    resp = server.recv(8192)
    assert(resp[:3] == "+OK".encode('utf-8'))

def init_blocking():
    global clients
    if numkeys > 100 * numclients:
        print("WARNING: High ratio of keys to clients. Most lpushes will not be popped and unblocking will take a long time!")
    for i in range(numclients):
        clients.append(Client('127.0.0.1', 6379))
        if i % 2:
            handle_blpop_response(clients[-1])
        else:
            handle_lpush_response(clients[-1])

def init_lpush():
    global clients
    for i in range(numclients):
        clients.append(Client('127.0.0.1', 6379))
        for i in range (10):
            handle_lpush_response(clients[-1])
        #handle_set_response(clients[-1], None)

    scan_client = Client('127.0.0.1', 6379)
    scan_client.scan(0, count=500, callback=scan_callback)

    del_client = Client('127.0.0.1', 6379)
    handle_del_response(del_client)

def main(test, flush):
    clear_ops()

    if flush:
        flush_db_sync()

    try:
        globals()[f"init_{test}"]()
    except KeyError:
        print(f"Test \"{test}\" not found. Exiting...")
        exit()
    except ConnectionRefusedError:
        print("Could not connect to server. Is it running?")
        print("Exiting...")
        exit()

    threading.Thread(target=stats_thread).start()
    asyncore.loop()
    print("Done.")

parser = argparse.ArgumentParser(description="Test use cases for KeyDB.")
parser.add_argument('test', choices=[x[5:] for x in filter(lambda name: name.startswith("init_"), globals().keys())], help="which test to run")
parser.add_argument('-c', '--clients', type=int, default=50, help="number of running clients to use")
parser.add_argument('-k', '--keys', type=int, default=100000, help="number of keys to choose from for random tests")
parser.add_argument('-t', '--runtime', type=int, default=0, help="how long to run the test for (default: 0 for infinite)")
parser.add_argument('-f', '--flush', action="store_true", help="flush the db before running the test")

if __name__ == "__main__":
    try:
        args = parser.parse_args()
    except:
        exit()
    numclients = args.clients
    numkeys = args.keys
    runtime = args.runtime
    main(args.test, args.flush)
