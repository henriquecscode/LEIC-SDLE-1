import time
from client import Client
import os
from dotenv import load_dotenv

load_dotenv()

rep_port = os.getenv('SERVER_REP_PORT')

client = Client(3, rep_port)
client.subscribe('animals')
client.put('animals', 'yo, i\'m testing put')


client = Client(6, pub_port, rep_port)
client.subscribe('animals')
