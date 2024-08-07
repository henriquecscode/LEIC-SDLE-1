import zmq
from os.path import exists
import os
from message import Subscribe, Unsubscribe, Put, Get, AckGet, NackGet
from message_parser import client_get_message
import pandas as pd

class Client:
    REQUEST_TIMEOUT = 2500
    REQUEST_RETRIES = 3

    def __init__(self, id, req_port):
        self.id = id
        self.topics = {}
        self.check_folder_exists()
        self.load()
        self.start_sockets(req_port)

    def check_folder_exists(self):
        isExist = os.path.exists('./src/data/client/')
        if(not isExist):
            try: 
                current_directory = os.getcwd()
                final_directory=os.path.join(current_directory, './src/data/client/')

                os.makedirs(final_directory) 
            except OSError as error: 
                print(error) 

    def load(self):
        filename = f"./src/data/client/{self.id}.csv"

        if (not exists(filename) or os.stat(filename).st_size == 0):
            print("PATH", filename, "DOESN'T POINT TO AN EXISTING")
            return

        df = pd.read_csv(filename)
        for i, row in df.iterrows():
            topic_id = row['topic_id']
            topic_message_id = row['topic_message_id']
            self.topics[topic_id] = topic_message_id

        print(self.topics)

    def save(self):
        df = pd.DataFrame(self.topics.items(), columns=['topic_id', 'topic_message_id'])
        df.to_csv(f"./src/data/client/{self.id}.csv")


    def start_sockets(self, req_port):
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.req_port = req_port
        self.create_req_socket()

    def create_req_socket(self):
        self.req_socket = self.context.socket(zmq.REQ)
        req_string = "tcp://localhost:"+self.req_port
        self.req_socket.connect(req_string)
        self.poller.register(self.req_socket, zmq.POLLIN)

    def put(self, topic_id, message):
        
        message = Put(self.id, topic_id, message)
        # self.req_socket.send(message())
        print(message)
        #Lazy pirate pattern 
        try:
          message = self.send(message())
        except Exception as e:
           print(e)
           return


    def get(self, topic_id):
        if topic_id not in self.topics.keys():
            print('CLIENT', self.id, 'IS NOT SUBSCRIBED TO TOPIC', topic_id)
            return

        topic_message_id = self.topics[topic_id]
        
        message = Get(self.id, topic_id, topic_message_id)
        # self.req_socket.send(message())
        print(message())
        # Lazy pirate pattern 
        try:
          message = self.send(message())
        except Exception as e:
           print(e)
           return

        if isinstance(message, AckGet):
            self.topics[topic_id] = int(self.topics[topic_id]) + 1
            self.save()
            print("Message:",message.message)
            pass
        elif isinstance(message, NackGet):
            print('MOST RECENT MESSAGE IN TOPIC', topic_id, ' HAS ALREADY BEEN DELIVERED OR IS INEXISTANT')
            pass
        else:
            print('SOMETHING STUPID HAPPENED.')
            pass


    def subscribe(self, topic_id):

        if topic_id in self.topics:
            print(f"CLIENT {self.id} IS ALREADY SUBSCRIBED TO TOPIC {topic_id} WITH MESSAGE NUMBER {self.topics[topic_id]}")
            return
        message = Subscribe(self.id, topic_id)
        # self.req_socket.send(message())
        # packet = self.req_socket.recv()
        # Lazy pirate pattern 
        try:
          message = self.send(message())
        except Exception as e:
           print(e)
           return
           
        # ack_put_message = client_get_message(packet)
        ack_put_message = message
        topic_id = ack_put_message.topic_id
        topic_message_id = ack_put_message.topic_message_id
        self.topics[topic_id] = topic_message_id     
        self.save()
        print(f"CLIENT {self.id} SUBSCRIBED TO TOPIC {topic_id}. MOST RECENT MESSAGE ID IS {topic_message_id}\n")

    def unsubscribe(self, topic_id):

        if topic_id in self.topics.keys(): #before or after ack?
            del self.topics[topic_id]

        message = Unsubscribe(self.id, topic_id)
        # self.req_socket.send(message())
        # message = self.req_socket.recv()
        print(message)
        # Lazy pirate pattern 
        try:
          message = self.send(message())
        except Exception as e:
           print(e)
           return
        self.save()

    def send(self, message):
        self.req_socket.send(message)
        retries_left = self.REQUEST_RETRIES
        while True:
            if (self.req_socket.poll(self.REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
                packet = self.req_socket.recv()
                message = client_get_message(packet)
                if message is None:
                    print(f"SERVER SENT MALFORMED MESSAGE: {packet}")
                    continue
                
                return message

            retries_left -= 1
            print("NO RESPONSE FROM SERVER")
            # Socket is confused. Close and remove it.
            self.req_socket.setsockopt(zmq.LINGER, 0)
            self.req_socket.close()

            
            print("RECONNECTING TO SERVER…")
            # Create new connection
            self.create_req_socket()

            if retries_left == 0:
                print("SERVER APPEARS TO BE OFF, ABANDONING OPERATION")
                return
            else:    
                self.req_socket.send(message)

    # https://stackoverflow.com/a/64030200

    # def retry(func):
    #     def wrapper(self, *args, **kwargs):
    #         client.send(request)

    #         retries_left = REQUEST_RETRIES
    #         while True:
    #             if (client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
    #                 reply = client.recv()
    #                 if int(reply) == sequence:
    #                     logging.info("Server replied OK (%s)", reply)
    #                     retries_left = REQUEST_RETRIES
    #                     break
    #                 else:
    #                     logging.error("Malformed reply from server: %s", reply)
    #                     continue

    #             retries_left -= 1
    #             logging.warning("No response from server")
    #             # Socket is confused. Close and remove it.
    #             client.setsockopt(zmq.LINGER, 0)
    #             client.close()
    #             if retries_left == 0:
    #                 logging.error("Server seems to be offline, abandoning")
    #                 sys.exit()

    #             logging.info("Reconnecting to server…")
    #             # Create new connection
    #             client = context.socket(zmq.REQ)
    #             client.connect(SERVER_ENDPOINT)
    #             logging.info("Resending (%s)", request)
    #             client.send(request)
    #     return wrapper

    # https://stackoverflow.com/a/64030200