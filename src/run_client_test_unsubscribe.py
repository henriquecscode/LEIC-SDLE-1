import time
from client import Client
import os
from dotenv import load_dotenv
import time

load_dotenv()

rep_port = os.getenv('SERVER_REP_PORT')

client = Client(2, rep_port)
client.subscribe('animals')
time.sleep(2)
client.unsubscribe('animals')
