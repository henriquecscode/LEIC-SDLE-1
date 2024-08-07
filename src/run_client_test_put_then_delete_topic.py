import time
from client import Client
import os
from dotenv import load_dotenv

load_dotenv()

rep_port = os.getenv('SERVER_REP_PORT')

client = Client(6, rep_port)
client.subscribe('my_amazing_topic')
time.sleep(3)
for i in range(5):
    client.put('my_amazing_topic', f'yo, i\'m testing put {i}')
    time.sleep(1.5)


client.unsubscribe('my_amazing_topic')
