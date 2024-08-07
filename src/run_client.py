from client import Client
import os
from dotenv import load_dotenv

load_dotenv()

rep_port = os.getenv('SERVER_REP_PORT')
#id = 1
#client = Client(id, pub_port, rep_port)

#client.subscribe('animals')
#client.put('animals', 'eu adoro animais')

#id = 2
#client_2 = Client(id, pub_port, rep_port)

#client_2.subscribe('animals')
#client.put('animals', 'eu odeio animais')

#client_2.get('animals')

# client.unsubscribe(client.id, 'animals')

# client_2.put('animals', 'i hate animals')

#client_2.get('animals') 

##### Client interface #####
print("Client id: ")
id = int(input())
client = Client(id, rep_port)
while True:
    print('')
    print("Topic:")
    topic = input()
    print("Commands: SUBSCRIBE, UNSUBSCRIBE, PUT, GET")
    command = input().upper()
    if command == "SUBSCRIBE":
        client.subscribe(topic)
    elif command == "UNSUBSCRIBE":
        client.unsubscribe(topic)
    elif command == "GET":
        client.get(topic)
    elif command == "PUT":
        print("Message: ")
        message = input()
        client.put(topic, message)