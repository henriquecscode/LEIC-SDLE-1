from message import Put, Get, Subscribe, Unsubscribe, AckPut, AckGet, NackGet, AckSubscribe, AckUnsubscribe
import string 

server_message_types = [Put, Get, Subscribe, Unsubscribe]
client_message_types = [AckPut, AckGet, NackGet, AckSubscribe, AckUnsubscribe]

def service_get_message(packet, message_types):
    packet_message = packet.decode()
    parts = packet_message.split(";")
    header = parts[0]

    for message_type in message_types:
        if message_type.header == header:
            return message_type.get(parts)

    return None

def server_get_message(packet):
    # Receives all possible messages from the Server
    return service_get_message(packet, server_message_types)

def client_get_message(packet):
    # Receives all possible messages from the Client
    return service_get_message(packet, client_message_types)