import time
from client import Client
import os
from dotenv import load_dotenv

load_dotenv()

rep_port = os.getenv('SERVER_REP_PORT')

clients = []
for i in range(5):
    clients.append(Client(i, rep_port))

for i in range(5):
    clients[i].subscribe('animals')
    clients[i].put('animals', 'eu adoro animais ' + str(i))
    time.sleep(2)
