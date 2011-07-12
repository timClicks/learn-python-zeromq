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

class PullSocket(threading.Thread):
    """ Definition of the pull socket.

        It recieves the messages from the push socket and prints them.
    """
    def __init__(self, num):
        """ Initialize the pull socket. """
        threading.Thread.__init__(self)
        self.num = num
        self.deamon=True                          # Deamon = True -> The thread finishes when the main process does

    def run(self):
        """ Listen the 2200 port and start the loop receiving and printing. """
        pull_sock = context.socket(zmq.PULL)
        time.sleep(2)
        print "Pull " + str(self.num) + " connecting"
        pull_sock.connect("ipc://127.0.0.1:2200")
        message = pull_sock.recv()
        while message:
            print "Pull" + str(self.num) + ": I recieved a message '" + message + "'"
            message = pull_sock.recv()

class PushSocket(threading.Thread):
    """ Definition of the push socket. 
    
        It sends the message 'N Potato' as a load balancer
        to all the Pull sockets.
    """

    def __init__(self):
        """ Initializer. """
        threading.Thread.__init__(self)
        self.deamon = True                          # Deamon = True -> The thread finishes when the main process does

    def run(self):
        """ Bind the 2200 port and send the messages. """
        push_sock = context.socket(zmq.PUSH)
        push_sock.bind("ipc://127.0.0.1:2200")
        for i in range (0, 7):
            message = str(i + 1) + " Potato"
            print "Sending " + message
            message = push_sock.send(message)
            time.sleep(1)                           # Without this sleep, all the messages go to Pull0. That means that
                                                    # zeromq assign the pull target switching by time, not by message.

if __name__ == "__main__":                          # Start the logic
    
    context = zmq.Context()
    try:                                           
        start_time = time.clock()
        # init the push socket thread
        thread_push = PushSocket()
        thread_push.start()

        # init the two pull socket threads
        for i in [0,1]:
            thread_pull = PullSocket(i)
            thread_pull.start()

        time.sleep(25)                              # Wait enough time to let the push-pull proceses end
    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"

    context.term()                                      # End the ZeroMQ context before to leave
