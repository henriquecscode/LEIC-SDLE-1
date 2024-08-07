import zmq
from os.path import exists
import os
import re
from message_parser import server_get_message
from message import AckUnsubscribe, Put, Get, Subscribe, Unsubscribe, AckSubscribe, AckPut, AckGet, NackGet
from topic import Topic
import pandas as pd


class Server:
    topics = {}
    server_path = "./src/data/server/"

    def __init__(self, rep_port):
        isExist = os.path.exists(self.server_path)
        if(isExist):
            self.load()
        else:
            try: 
                current_directory = os.getcwd()
                final_directory=os.path.join(current_directory, self.server_path)

                os.makedirs(final_directory) 
            except OSError as error: 
                print(error)  
        self.start_sockets(rep_port) # First we create the sockets.

    def load(self):
        server_files = os.listdir(self.server_path)
        print('FILES IN STABLE STORAGE:', server_files)
        data_subscribers = [file for file in server_files if file.startswith("subscribers_")]
        data_messages = [file for file in server_files if file.startswith("messages_")]
        server_files_data = list(zip(data_subscribers, data_messages))
        print('SERVER DATA:', server_files_data)
        for file_pair in server_files_data:
            print(file_pair)
            result = re.search('(subscribers_|messages_)(.*).csv', file_pair[0])
            topic_name = result.group(2)
            # s = 'asdf=5;iwantthis123jasd'
            # result = re.search('asdf=5;(.*)123jasd', s)
            print('')
            print('TOPIC NAME:', topic_name)
            topic = Topic(topic_name, server_path=self.server_path, save=False)
            self.topics[topic_name] = topic

        print('')
        print('TOPICS:', self.topics)
        
    def save(self):
        for topic in self.topics.values():
            topic.save()


    def start_sockets(self, rep_port):
        context = zmq.Context() # Standard for ZMQ.

        self.rep_port = rep_port
        self.rep_port = context.socket(zmq.REP)
        self.rep_port.bind(f"tcp://*:{rep_port}")

    def run(self):
        while True:
            self.listen() # This is basically polling - we're constantly listening on the port for new messages.

    def listen(self):
        packet = self.rep_port.recv() # We get the message.

        message = server_get_message(packet) # We parse the message, so we can use it for something.

        # Should multithread the function invocation
        if isinstance(message, Put):
            self.put(message)
        elif isinstance(message, Get):
            self.get(message)
        elif isinstance(message, Subscribe):
            self.subscribe(message)
        elif isinstance(message, Unsubscribe):
            self.unsubscribe(message)
        else:
            return

    def put(self, message: Put):
        id = message.client_id
        topic = message.topic_id

        if topic not in self.topics.keys(): #If put has a topic that doesn't exist, it creates a topic
            self.topics[topic] = Topic(topic, server_path=self.server_path)            
            print('CREATED TOPIC', self.topics)

        # We save the latest message ID we got.
        new_message = self.topics[topic].put(message) 
        print('ADDED MESSAGE TO TOPIC', self.topics)

        
        # ACK_PUT
        ack_put = AckPut(id, topic, new_message)
        self.rep_port.send(ack_put())
        print('SERVER SENT ACK_PUT TO CLIENT', id)

        pass

    def get(self, message: Get):
        # Doesn't need to check if client is subscribed
        # to the topic - that's done in client.py.
        
        id = message.client_id
        topic = message.topic_id
        wanted_message = message.message

        if topic not in self.topics.keys():
            return 

        wanted_msg = self.topics[topic].get(id, wanted_message)
        print('GOT MESSAGE FROM TOPIC', topic)

        # ACK_GET
        if (wanted_msg != 'already_delivered'):
            ack_get = AckGet(id, topic, wanted_message, wanted_msg)
            self.rep_port.send(ack_get())
        else: #NACK_GET
            nack_get = NackGet(id, topic, wanted_message)
            self.rep_port.send(nack_get())
        

    def subscribe(self, message: Subscribe):
        id = message.client_id # This is the unpackaging of the message.
        topic = message.topic_id

        if topic not in self.topics:
            # If we got a message from a topic we haven't
            # gotten a message from before, we add a new topic
            # instance to the server's list of topics.
            print('NEW TOPIC CREATED:', topic)
            self.topics[topic] = Topic(topic, server_path=self.server_path) 

        print('TOPICS UPDATE:', self.topics)

        # We save the latest message ID we got.
        latest_message_id = self.topics[topic].subscribe(id) 
        print('LATEST MESSAGE ID:', latest_message_id)
        
        # and create and ACK_SUBSCRIBE message to send back through the reply socket.
        ack_subscribe = AckSubscribe(topic, latest_message_id)
        self.rep_port.send(ack_subscribe())
        print('SERVER SENT ACK_SUBSCRIBE TO CLIENT', id)

        pass

    def unsubscribe(self, message: Unsubscribe):
        id = message.client_id
        topic = message.topic_id

        if not topic in self.topics:
            return
            
        latest_message_id = self.topics[topic].unsubscribe(id)
        print('LATEST MESSAGE', latest_message_id)
        ack_unsubscribe = AckUnsubscribe(topic, latest_message_id)

        if self.topics[topic].check_topic_deletion():
            del self.topics[topic]
        else:
            print('NOTHING :)')

        self.rep_port.send(ack_unsubscribe())
        print('SERVER SENT ACK_UNSUBSCRIBE TO CLIENT', id)
