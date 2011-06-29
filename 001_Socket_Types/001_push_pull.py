#!/usr/bin/env python2

# PUSH and PULL sockets work together to load balance messages going one way.
# Multiple PULL sockets connected to a PUSH each receive messages from the PUSH.
# ZeroMQ automatically load balances the messages between all pull sockets.
#
# We're going to build a simple load balanced message system that looks like this:
#
#                         push_sock
#                         /       \
#                  pull_sock1   pull_sock2
#
# Each socket will get its own thread, so you'll see them run simultanously

import threading
import time
import zmq


context = zmq.Context()

class PullSocket(threading.Thread):

    def __init__(self, num):
        threading.Thread.__init__(self)
        self.num = num
        self.kill_recieved = False

    def run(self):
        pull_sock = context.socket(zmq.PULL)
        time.sleep(3)
        print "Pull " + str(self.num) + " connecting"
        pull_sock.connect("tcp://127.0.0.1:2200")
        message = pull_sock.recv()
        while message and not self.kill_recieved:
            print "Pull" + str(self.num) + ": I recieved a message '" + message + "'"
            message = pull_sock.recv()

class PushSocket(threading.Thread):

    """ Here we define the Push socket. """
    def __init__(self):
        threading.Thread.__init__(self)
        self.kill_recieved = False

    def run(self):
        push_sock = context.socket(zmq.PUSH)
        push_sock.bind("tcp://127.0.0.1:2200")
        for i in range (0, 7):
            message = str(i + 1) + " Potato"
            print "Sending " + message
            message = push_sock.send(message)
            time.sleep(1)
    
threads = []

thread_push = PushSocket()
thread_push.start()
threads.append(thread_push)

thread_pull = PullSocket(0)
thread_pull.start()
threads.append(thread_pull)

thread_pull_2 = PullSocket(1)
thread_pull_2.start()
threads.append(thread_pull_2)

thread_push.join()
thread_pull.join()
thread_pull_2.join()

context.term()
