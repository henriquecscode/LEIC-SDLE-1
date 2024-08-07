from message import Put, Get, Subscribe, Unsubscribe, ClientRecoverCrash

message_types = [Put, Get, Subscribe, Unsubscribe, ClientRecoverCrash]

def get_message(packet):
    # Receives all possible messages from the Server
    parts = packet.split(";")
    header = parts[0]

    for message_type in message_types:
        if message_type.header == header:
            return {header: header,
                    parts: message_type.get(parts)}
