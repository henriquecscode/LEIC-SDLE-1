import pandas as pd
import os

class ClientStatus:
    def __init__(self, id, message_id):
        self.id = id
        self.message_id = message_id

    def inc(self):
        self.message_id += 1

    def set(self, id):
        self.message_id = id


class Topic:

    def __init__(self, id, message_id=0, server_path="", save=True):
        self.id = id
        self.message_id = message_id
        self.path = server_path

        self.notDeliveredMessages = []
        self.subscribers = {}
        if save:
            self.save_all()
        else:
            self.load()

    def save_all(self):
        self.save_subscribers()
        self.save_messages()

    def save_subscribers(self):
        serialized_subscribers = [
            [subscriber.id, subscriber.message_id] for subscriber in self.subscribers.values()
        ]
        df = pd.DataFrame(
            serialized_subscribers,
            columns=["subscriber_id", "subscriber_last_message_id"],
        )
        df.to_csv(f"{self.path}subscribers_{self.id}.csv")
        print('SUBSCRIBERS FOR TOPIC', self.id, 'SAVED IN STABLE STORAGE\n')

    def save_messages(self):
        df = pd.DataFrame(
            self.notDeliveredMessages, columns=["topic_message_id", "topic_message"]
        )
        df.to_csv(f"{self.path}messages_{self.id}.csv")
        print('MESSAGES FOR TOPIC', self.id, 'SAVED IN STABLE STORAGE\n')

    def load(self):
        messages_path = self.path + "messages_" + self.id + ".csv"
        subscribers_path = self.path + "subscribers_" + self.id + ".csv"
        if not os.stat(messages_path).st_size == 0:
            df_messages = pd.read_csv(messages_path)
            df_messages.sort_values("topic_message_id", ascending=True)
            for _, row in df_messages.iterrows():
                topic_message_id = row["topic_message_id"]
                topic_message = row["topic_message"]
                self.notDeliveredMessages.append([topic_message_id, topic_message])
                self.message_id = df_messages["topic_message_id"].max() + 1

        if not os.stat(subscribers_path).st_size == 0:
            df_subscribers = pd.read_csv(subscribers_path)

            for _, row in df_subscribers.iterrows():
                subscriber_id = row["subscriber_id"]
                subscriber_last_message_id = row["subscriber_last_message_id"]
                self.subscribers[subscriber_id] = ClientStatus(
                    subscriber_id, subscriber_last_message_id
                )
                
        print('LOADING FROM STABLE STORAGE\n')

        print('SUBSCRIPTIONS')
        for key,val in self.subscribers.items():
            print(key, val.message_id)

        print('')

        print('MESSAGES')
        for val in self.notDeliveredMessages:
            print(val[0], val[1])

        print('')
        

    def subscribe(self, client_id):
        print('SUBSCRIBE in TOPIC', self.id, 'for CLIENT', client_id)
        client_id = int(client_id)
        if client_id not in self.subscribers:
            print('LATEST MESSAGE ID', self.message_id)
            self.subscribers[client_id] = ClientStatus(client_id, self.message_id)
            self.save_subscribers()

        return self.message_id

    def unsubscribe(self, client_id):
        client_id = int(client_id)
        print('UNSUBSCRIBE in TOPIC', self.id, 'for CLIENT', client_id)
        if client_id not in self.subscribers:
            return

        del self.subscribers[client_id]
        self.save_subscribers()

        
        if self.check_topic_deletion():
            self.notDeliveredMessages = []
            self.save_messages()
            return
            
        return

    def put(self, message):
        if message not in self.notDeliveredMessages:
            self.notDeliveredMessages.append([self.message_id, message.message])
            self.save_messages()
            self.message_id += 1
        return message

    def get(self, id, message_id):
        id = int(id)
        message_id = int(message_id)
        notDeliveredMessages_ids = [
            message[1]
            for message in self.notDeliveredMessages
            if message[0] == message_id
        ]

        if len(notDeliveredMessages_ids) == 0:
            return "already_delivered"

        if len(notDeliveredMessages_ids) != 1:
            print("Error: more than one message with id", message_id, ". Getting first")

        message = notDeliveredMessages_ids[0]
        for key, val in self.subscribers.items():
            print(key, val.id, val.message_id)

        self.subscribers[id].set(message_id)
        self.save_subscribers()
        self.check_message_deletion()

        return message

    def check_topic_deletion(self):
        return not bool(self.subscribers)
        
    def check_message_deletion(self):
            
        min_client_message_id = min(
            [client.message_id for client in self.subscribers.values()]
        )
        changed = False
        while True:
            if len(self.notDeliveredMessages) == 0:
                break
            oldest_message_id = self.notDeliveredMessages[0][0]
            if min_client_message_id - oldest_message_id >= 1:
                self.notDeliveredMessages.pop(0)
                changed = True
            else:
                break

        if changed:
            self.save_messages()
