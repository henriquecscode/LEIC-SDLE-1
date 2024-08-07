import time
from client import Client
import os
from dotenv import load_dotenv

load_dotenv()

rep_port = os.getenv('SERVER_REP_PORT')

client = Client(1, rep_port)
client.get('animals')
