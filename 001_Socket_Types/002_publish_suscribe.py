#!/usr/bin/env python2
import threading
import time
import zmq

# PUB and SUB sockets work together to broadcast messages out to many clients
# A single PUB socket can talk to multiple SUB sockets at the same time.
# Messages only go one way, from the PUB to the SUB.
# 
# SUB sockets can filter the messages they receive, checking the prefix of the message
# for an exact sequence of bytes, discarding messages that don't start with this prefix.
#
# One important thing to note about PUB sockets, is that when created with 'bind'
# that is when listening for incoming SUB connections, they don't queue messages unless
# there's a connected SUB socket, their messages are effectively black holed.
#
# So, don't plan on all PUB messages making their way anywhere unless there's a connected
# SUB socket. When created with 'connect' (which is perhaps atypical for a pub/sub topology),
# queing of messages does takes place.
#
# We're also going to learn about multipart messages in this example
#
# We're going to build a simple pub/sub message system that looks like this:
#
#                          pub_sock
#                         /       \
#                  sub_sock1     sub_sock2
#
# Each socket will get its own thread, so you'll see them run simultanously

class PubSocket(threading.Thread):
    """ Definition of the pub socket.

        It published messages in unidirectional way.
    """
    def __init__(self):
        """ Initialize the pull socket. """
        threading.Thread.__init__(self)
        self.deamon=True                          # Deamon = True -> The thread finishes when the main process does

    def run(self):
        """ Listen the 2200 port and start the loop receiving and printing. """
        pub_sock = context.socket(zmq.PUB)
        pub_sock.connect("tcp://127.0.0.1:2200")
        while True:
            # ZeroMQ messages can be broken up into multiple parts.
            # Messages are guaranteed to either come with all parts or not at all,
            # so don't worry about only receiving a partial message.
            
            # We're going to send the topic in a separate part, it's what the SUB socket
            # will use to decide if it wants to receive this message.
            # You don't need to use two parts for a pub/sub socket, but if you're using
            # topics its a good idea as matching terminates after the first part.
            print "P: Sending our first message, about the Time Machine"
            pub_sock.send('Important', zmq.SNDMORE)     #Topic
            pub_sock.send('Find Time Machine')          #Body

            print "P: Sending our second message, about Brawndo"
            pub_sock.send('Useless', zmq.SNDMORE)   #Topic
            pub_sock.send('Drink Brawndo')               #Body

            time.sleep(1)

class SubSocket(threading.Thread):

    def __init__(self, num, subs_message):
        threading.Thread.__init__(self)
        self.num = num
        self.subs_message = subs_message
        self.deamon = True

    def run(self):
        sub_sock = context.socket(zmq.SUB)
        time.sleep(1)
        print "S" + str(self.num) + ": Subscribing to messages with topic " + self.subs_message
        # sub_sock.setsockopt(zmq.SUBSCRIBE, self.subs_message) 
        sub_sock.setsockopt(zmq.SUBSCRIBE, self.subs_message) 
        time.sleep(1)
        sub_sock.connect("tcp://127.0.0.1:2200")
        time.sleep(1)
        for i in range (5):                 # wait for five messages
              topic    = sub_sock.recv()
              if sub_sock.more_parts:
                    body     = sub_sock.recv()
      
              print "S" + str(self.num) + ": I received a message! The topic was " + topic
              print "S" + str(self.num) + ": The body of the message was " + body
            
if __name__ == "__main__":                          # Start the logic
    
    context = zmq.Context()

    try:                                           
        start_time = time.clock()
        # init the push socket thread
        thread_pub = PubSocket()
        thread_pub.start()

        # init the two pull socket threads
        for i in range(3):
            if (i % 2 == 0):
                thread_sub = SubSocket(i, "Important")
            else:
                thread_sub = SubSocket(i, "Useless")
            thread_sub.start()

        time.sleep(25)

    except (KeyboardInterrupt, SystemExit):
        print "Received keyboard interrupt, system exiting"

    context.term()
