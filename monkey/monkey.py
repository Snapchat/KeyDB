import keydb
import random
import sched, time
import socket
import asyncore
import threading
import sys
from pprint import pprint

# Parameters
numclients = 50
#numkeys = 1000000
numkeys  = 100000

# Globals
ops=0
s = sched.scheduler(time.time, time.sleep)
g_exit = False

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

    def get(self, key, callback = None):
       return 


def getrandomkey():
    return str(random.randrange(0, numkeys))

def handle_lpush_response(c, resp):
    global ops
    if resp != None:
        ops = ops + 1
        assert(resp[0] == ord(':'))
    c.lpush("list_" + getrandomkey(), 'bardsklfjkldsjfdlsjflksdfjklsdjflksd kldsjflksd jlkdsjf lksdjklds jrfklsdjfklsdjfkl', handle_lpush_response)

def handle_set_response(c, resp):
    global ops
    if resp != None:
        ops = ops + 1
        assert(resp[0] == ord('+'))
    c.set("str_" + getrandomkey(), 'bardsklfjkldsjfdlsjflksdfjklsdjflksd kldsjflksd jlkdsjf lksdjklds jrfklsdjfklsdjfkl', handle_set_response)

def scan_callback(c, resp):
    global ops
    nextstart = int(resp[0])
    c.scan(nextstart, count=500, callback=scan_callback)
    ops = ops+1

def stats_thread():
    global ops
    global g_exit
    while not g_exit:
        time.sleep(1)
        print("Ops per second: " + str(ops))
        ops = 0

def main():
    global g_exit
    clients = []

    for i in range(numclients):
        clients.append(Client('127.0.0.1', 6379))
        for i in range (10):
            handle_lpush_response(clients[-1], None)
        #handle_set_response(clients[-1], None)

    scan_client = Client('127.0.0.1', 6379)
    scan_client.scan(0, count=500, callback=scan_callback)

    threading.Thread(target=stats_thread).start()
    asyncore.loop()
    g_exit = True
    sys.exit(0)
    print("DONE")

if __name__ == "__main__":
    main()
