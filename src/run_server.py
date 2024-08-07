from server import Server
import os
from dotenv import load_dotenv

load_dotenv()

rep_port = os.getenv('SERVER_REP_PORT')

server = Server(rep_port)
print('server created')

server.run()
# while True:
#     read_input = input()