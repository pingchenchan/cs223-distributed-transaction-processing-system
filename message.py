from enum import Enum, auto
import time
import json
import websockets
# Message class
# 1. User message: from command line, execute transaction
# 2. Detailed message: for detailed message

class MessageType(Enum):
    USER = auto()
    HOP = auto()
    FORWARD = auto()
    BACKWARD = auto()
class TransactionType(Enum):
    T1 = auto()
    T2 = auto()
    T3 = auto()
    T4 = auto()
    T5 = auto()
    T6 = auto()
    T7 = auto()

def message_to_json(message):
    if  message.message_type == MessageType.USER:
        return json.dumps({
            'message_type': message.message_type.name,
            'transaction_type': message.transaction_type.name,
            'data': message.data
        })
    elif message.message_type == MessageType.FORWARD:
        return json.dumps({
            'message_type': message.message_type.name,
            'transaction_type': message.transaction_type.name,
            'transaction_id' : message.transaction_id,  # The ID of the transaction
            'hop_id' : message.hop_id  ,
            'origin_server' : message.origin_server , 
            'target_server' : message.target_server ,
            'data' : message.data
        })
    elif message.message_type == MessageType.BACKWARD:
        return json.dumps({
            'message_type': message.message_type.name,
            'transaction_type': message.transaction_type.name,
            'transaction_id' : message.transaction_id,  # The ID of the transaction
            'hop_id' : message.hop_id  ,
            'result' : message.result  , # The result of the operation (True or False)
            'origin_server' : message.origin_server , # The identifier of the origin server
            'target_server' : message.target_server  # The identifier of the target server
        })


class WebSocketClient:
    @staticmethod
    async def send_message(message, uri):
        message_json = message_to_json(message)
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(message_json)
                return await websocket.recv()
        except websockets.ConnectionClosedError as e:
            print(f"Connection closed: {e}")
        except Exception as e:
            print(f"Error sending message: {e}")

class Message:
    def __lt__(self, other):
        # This is critical for the priority queue to work properly
        # higher priority will be popped first
        # if the priority is the same, the older message will be popped first
        if self.priority != other.priority:
            return self.priority > other.priority 
        return self.created_at < other.created_at
    def __init__(self, message_type, transaction_type):
        self.created_at = time.time()  # The time when the message was created
        self.priority = 0  # The priority of the message
        # The type of the message (e.g., 'User', 'Forward', 'Backward')
        if isinstance(message_type, MessageType):
            self.message_type = message_type
        else:
            raise ValueError(f"Invalid message type: {message_type}")

        # The type of transaction (e.g., 'T1' to 'T7')
        if isinstance(transaction_type, TransactionType):
            self.transaction_type = transaction_type
        else:
            raise ValueError(f"Invalid transaction type: {transaction_type}")


    def __str__(self):
        return f"Message({self.message_type}, {self.transaction_type})"

    def __repr__(self):
        return self.__str__()


class UserMessage(Message):
    def __init__(self, message_type, transaction_type, data):
        super().__init__(message_type, transaction_type)
        self.data = data  # Additional data for the UserMessage

    def __str__(self):
        return f"UserMessage({self.message_type}, {self.transaction_type}, {self.data})"

class HopMessage(Message):
    def __init__(self, message_type, transaction_type, hop):
        super().__init__(message_type, transaction_type)
        self.hop = hop  

    def __str__(self):
        return f"HopMessage({self.message_type}, {self.transaction_type}, {self.hop})"

class ForwardMessage(Message):
    def __init__(self, message_type, transaction_type, target_server, transaction_id, hop_id, origin_server,data):
        super().__init__(message_type, transaction_type)
        self.transaction_id = transaction_id  
        self.hop_id = hop_id  
        self.origin_server = origin_server  
        self.target_server = target_server 
        self.data = data

    def __str__(self):
        return f"ForwardMessage({self.message_type}, {self.transaction_id}, {self.hop_id})"


class BackwardMessage(Message):
    def __init__(self, message_type, transaction_type, transaction_id,hop_id, result, origin_server, target_server):
        super().__init__(message_type, transaction_type)
        self.transaction_id = transaction_id  # The ID of the transaction
        self.hop_id = hop_id
        self.result = result  # The result of the operation (True or False)
        self.origin_server = origin_server  # The identifier of the origin server
        self.target_server = target_server  # The identifier of the target server
        


    def __str__(self):
        return f"BackwardMessage({self.message_type}, {self.transaction_id},{self.hop_id} ,{self.result})"

async def send_message(message, uri):
    reply = await WebSocketClient.send_message(message, uri)
    return reply