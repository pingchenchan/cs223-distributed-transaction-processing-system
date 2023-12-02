from enum import Enum, auto
import time
import json
import websockets
import asyncio
from datetime import datetime

# Message class
# 1. User message: from command line, execute transaction
# 2. Detailed message: for detailed message
class PRIORITY(Enum):
    HOP_1 = 10
    HOP_2 = 0
    HOP_BACKWARD = 5
    HOP_FORWARD = 5


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

class TimeTracker:
    def __init__(self):
        self.events = []  # [(start_time, end_time), ...]

    def __str__(self):
        return str(self.events)

    def start_event(self, timestamp=None):
        if not timestamp:
            timestamp = datetime.now()
        self.events.append((timestamp, None))

    def end_event(self, timestamp=None):
        if not timestamp:
            timestamp = datetime.now()
        if self.events and self.events[-1][1] is None:
            start_time, _ = self.events[-1]
            self.events[-1] = (start_time, timestamp)

    def get_total_time(self): 
        return sum((end_time - start_time).total_seconds() * 1000000  # microseconds
                   for start_time, end_time in self.events if start_time and end_time)

    def get_event_count(self):
        return len(self.events)

    def concat(self, other): # concat two time tracker
        if isinstance(other, TimeTracker):
            self.events.extend(other.events)
        else:
            raise TypeError("The 'other' object must be an instance of TimeTracker")
    
    def load_from_data(self, data): # load from json data
        """Load events from a list of dictionaries."""
        for item in data:
            # If data is [], this loop does not execute
            start_time = datetime.strptime(item['start_time'], "%Y-%m-%d %H:%M:%S.%f") if item['start_time'] else None
            end_time = datetime.strptime(item['end_time'], "%Y-%m-%d %H:%M:%S.%f") if item['end_time'] else None
            if start_time and end_time:
                self.events.append((start_time, end_time))

def message_to_json(message):
    if  message.message_type == MessageType.USER:
        return json.dumps({
            'message_type': message.message_type.name,
            'transaction_type': message.transaction_type.name,
            'data': message.data
        })



    # print("Type of queue_tracker:", type(message.queue_tracker), message.queue_tracker)
    queue_tracker_data = []

    if isinstance(message.queue_tracker, TimeTracker):
        queue_tracker_data = [
            {
                'start_time': start_time.strftime("%Y-%m-%d %H:%M:%S.%f") if start_time else None,
                'end_time': end_time.strftime("%Y-%m-%d %H:%M:%S.%f") if end_time else None
            } for start_time, end_time in message.queue_tracker.events
        ]


    if message.message_type == MessageType.FORWARD:
        return json.dumps({
            'message_type': message.message_type.name,
            'transaction_type': message.transaction_type.name,
            'transaction_id' : message.transaction_id,  # The ID of the transaction
            'hop_id' : message.hop_id  ,
            'origin_server' : message.origin_server , 
            'target_server' : message.target_server ,
            'data' : message.data,
            'websocket_receive_time' : message.websocket_receive_time.strftime("%Y-%m-%d %H:%M:%S.%f") if message.websocket_receive_time else None,
            'queue_tracker' : queue_tracker_data, # [(start_queue_trackertep, exit_queue_timestep), ...]

        })
    elif message.message_type == MessageType.BACKWARD:
        return json.dumps({
            'message_type': message.message_type.name,
            'transaction_type': message.transaction_type.name,
            'transaction_id' : message.transaction_id,  # The ID of the transaction
            'hop_id' : message.hop_id  ,
            'result' : message.result  , # The result of the operation (True or False)
            'origin_server' : message.origin_server , # The identifier of the origin server
            'target_server' : message.target_server , # The identifier of the target server
            'websocket_receive_time': message.websocket_receive_time.strftime("%Y-%m-%d %H:%M:%S.%f") if message.websocket_receive_time else None,
            'websocket_send_time': message.websocket_send_time.strftime("%Y-%m-%d %H:%M:%S.%f") if message.websocket_send_time else None,
            'queue_tracker': queue_tracker_data,
            'server_exe': message.server_exe,
            #TODO  which server exe this hop info
        })


def convert_to_datetime(date_string, format="%Y-%m-%d %H:%M:%S.%f"):
    # Convert a string to a datetime object
    if date_string:
        return datetime.strptime(date_string, format)
    return None

class Message:
    def __lt__(self, other):
        # This is critical for the priority queue to work properly
        # higher priority will be popped first
        # if the priority is the same, the older message will be popped first
        if self.priority != other.priority:
            return self.priority > other.priority 
        return self.created_at < other.created_at
    def __init__(self, message_type, transaction_type, priority, queue_tracker=None ):
        self.created_at = time.time()  # The time when the message was created
        self.priority = priority  # The default priority of the message

        ''' ONLY FOR FORWARD MESSAGE & BACKWARD MESSAGE'''
        ''' HOP MESSAGE USE self.HOP TO RECORD THIS INFO'''
        # Check if queue_tracker is either None or an instance of TimeTracker
        if queue_tracker is not None and not isinstance(queue_tracker, TimeTracker):
            raise TypeError("queue_tracker must be an instance of TimeTracker")
        self.queue_tracker = queue_tracker if queue_tracker is not None else TimeTracker()

        
        
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
    
    # Methods specific to queue wait time
    def start_queue_wait(self,timestamp=None):
        self.queue_tracker.start_event(timestamp)

    def end_queue_wait(self,timestamp=None):
        self.queue_tracker.end_event(timestamp)

        # Aggregate data access methods
    def get_total_queue_wait_time(self):
        return self.queue_tracker.get_total_time()

    def get_queue_wait_count(self):
        return self.queue_tracker.get_event_count()

    def __str__(self):
        return f"Message({self.message_type}, {self.transaction_type})"

    def __repr__(self):
        return self.__str__()


class UserMessage(Message):
    def __init__(self, message_type, transaction_type, data):
        super().__init__(message_type, transaction_type, priority=None)
        self.data = data  # Additional data for the UserMessage

    def __str__(self):
        return f"UserMessage({self.message_type}, {self.transaction_type}, {self.data})"

class HopMessage(Message): #for priority queue
    def __init__(self, message_type, transaction_type, hop, priority):
        super().__init__(message_type, transaction_type, priority)
        self.hop = hop  

    def __str__(self):
        return f"HopMessage({self.message_type}, {self.transaction_type}, {self.hop})"

class ForwardMessage(Message):
    def __init__(self, message_type, transaction_type, target_server, transaction_id, hop_id, origin_server,data, websocket_receive_time=None,websocket_send_time=None,queue_tracker=None):
        super().__init__(message_type, transaction_type, priority=PRIORITY.HOP_FORWARD.value)
        self.transaction_id = transaction_id  
        self.hop_id = hop_id  
        self.origin_server = origin_server  
        self.target_server = target_server 
        self.data = data

        '''POV of target server'''
        self.websocket_receive_time = websocket_receive_time 





    def __str__(self):
        return f"ForwardMessage({self.message_type}, {self.transaction_id}, {self.hop_id})"


class BackwardMessage(Message):
    def __init__(self, message_type, transaction_type, transaction_id,hop_id, result, origin_server, target_server,websocket_receive_time=None,websocket_send_time=None,queue_tracker=None,server_exe=None ):
        super().__init__(message_type, transaction_type, priority=PRIORITY.HOP_BACKWARD.value ,queue_tracker=queue_tracker)
        self.transaction_id = transaction_id  # The ID of the transaction
        self.hop_id = hop_id
        self.result = result  # The result of the operation (True or False)
        self.origin_server = origin_server  # The identifier of the origin server
        self.target_server = target_server  # The identifier of the target server
        '''POV of target server'''
        self.websocket_receive_time = websocket_receive_time 
        self.websocket_send_time = websocket_send_time 
        self.server_exe = None #TODO  which server exe this hop info




    def __str__(self):
        return f"BackwardMessage({self.message_type}, {self.transaction_id},{self.hop_id} ,{self.result})"

async def send_message(message, uri):
    reply = await WebSocketClient.send_message(message, uri)
    return reply

class WebSocketClient:
    # restrict the number of concurrent connections
    _semaphore = asyncio.Semaphore(1000)  
    @staticmethod
    async def send_message(message, uri):
        message_json = message_to_json(message)
        async with WebSocketClient._semaphore:  # use semaphore to restrict the number of concurrent connections
            try:
                async with websockets.connect(uri) as websocket:
                    await websocket.send(message_json)
                    
                    return await websocket.recv()
            except websockets.ConnectionClosedError as e:
                print(f"Connection closed: {e}")
            except Exception as e:
                print(f"Error sending message: {e}")


class WebSocketClientForBatchMessage:
    _semaphore = asyncio.Semaphore(1000) 
    @staticmethod
    async def send_messages(messages, uri):
        async with WebSocketClient._semaphore:
            try:
                async with websockets.connect(uri) as websocket:
                    for message in messages:
                        await websocket.send(message)
                        reply = await websocket.recv()
            except websockets.ConnectionClosedError as e:
                print(f"Connection closed: {e}")
            except Exception as e:
                print(f"Error sending messages: {e}")