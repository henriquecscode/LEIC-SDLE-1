class Message:
    header = ""
    
    def __init__(self):
        pass

class TopicMessage(Message):
    header = ""

    def __init__(self, client_id, topic_id, message):
        self.client_id = client_id
        self.topic_id = topic_id
        self.message = message

    def __call__(self):
        message = f"{self.header};{self.client_id};{self.topic_id};{self.message}"
        return str.encode(message)

    @staticmethod
    def get(topic_message_parts):
        tmp = topic_message_parts
        return (tmp[1], tmp[2], ';'.join(tmp[3:]))


class TopicInfo(Message):
    header = ""

    def __init__(self, client_id, topic_id):
        self.client_id = client_id
        self.topic_id = topic_id

    def __call__(self):
        message = f"{self.header};{self.client_id};{self.topic_id}"
        return str.encode(message)

class Put(TopicMessage):
    header = "put"
    
    def __init__(self, client_id, topic_id, message):
        super().__init__(client_id, topic_id, message)

    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return Put(tmp[1], tmp[2], ';'.join(tmp[3:])) 

class AckPut(TopicMessage):
    header = "ack_put"
    
    def __init__(self, client_id, topic_id, message):
        super().__init__(client_id, topic_id, message)

    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return AckPut(tmp[1], tmp[2], ';'.join(tmp[3:])) 

class Get(TopicMessage):
    header = "get"
    
    def __init__(self, client_id, topic_id, topic_message_id):
        super().__init__(client_id, topic_id, topic_message_id)
        
    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return Get(tmp[1], tmp[2], tmp[3]) 
      
class AckGet(Message):
    header = "ack_get"
    
    def __init__(self, client_id, topic_id, message_id, message):
        self.client_id = client_id
        self.topic_id = topic_id
        self.message_id = message_id
        self.message = message
     
    def __call__(self):
        message=f"{self.header};{self.client_id};{self.topic_id};{self.message_id};{self.message}"
        return str.encode(message)

    @staticmethod
    def get(topic_message_parts):
        tmp = topic_message_parts
        return AckGet(tmp[1], tmp[2], tmp[3], ';'.join(tmp[4:]))

class NackGet(TopicMessage):
    # When this message is sent, it means that a get
    # can't be fulfilled because the requested message
    # was already delivered.

    header = "nack_get"
    
    def __init__(self, client_id, topic_id, topic_message_id):
        super().__init__(client_id, topic_id, topic_message_id)
        
    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return NackGet(tmp[1], tmp[2], tmp[3]) 


class Subscribe(TopicInfo):
    header = "subscribe"
    
    def __init__(self, client_id, topic_id):
        super().__init__(client_id, topic_id)

    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return Subscribe(tmp[1], tmp[2])
        # Shouldn't it be tmp[1] and tmp[2]? Because the header has already been parsed in message_parser.py, I think? - Less.

class AckSubscribe(Message):
    header = "ack_subscribe"
    
    def __init__(self, topic_id, topic_message_id):
        self.topic_id = topic_id
        self.topic_message_id = topic_message_id

    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return AckSubscribe(tmp[1], ';'.join(tmp[2:])) 

    def __call__(self):
        message = f"{self.header};{self.topic_id};{self.topic_message_id}"
        return str.encode(message)

class Unsubscribe(TopicInfo):
    header = "unsubscribe"
    
    def __init__(self, client_id, topic_id):
        super().__init__(client_id, topic_id)
        
    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return Unsubscribe(tmp[1], tmp[2]) 
    
class AckUnsubscribe(TopicInfo):
    header = "ack_unsubscribe"

    def __init__(self, client_id, topic_id):
        super().__init__(client_id, topic_id)

    @staticmethod
    def get(message_parts):
        tmp = message_parts
        return AckUnsubscribe(tmp[1], tmp[2]) 
