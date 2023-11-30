from enum import Enum, auto
# Message class
# 1. User message: from command line, execute transaction
# 2. Detailed message: for detailed message
class MessageType(Enum):
    USER = auto()
    FORWARD = auto()
    BACKWARD = auto()
class TransactioType(Enum):
    T1 = auto()
    T2 = auto()
    T3 = auto()
    T4 = auto()
    T5 = auto()
    T6 = auto()
    T7 = auto()

class Message:
    def __init__(self, message_type, transaction_type, target_server):
        # The type of the message (e.g., 'User', 'Forward', 'Backward')
        if isinstance(message_type, MessageType):
            self.message_type = message_type
        else:
            raise ValueError(f"Invalid message type: {message_type}")

        # The type of transaction (e.g., 'T1' to 'T7')
        if isinstance(transaction_type, TransactioType):
            self.transaction_type = transaction_type
        else:
            raise ValueError(f"Invalid transaction type: {transaction_type}")

        # The identifier of the target server
        self.target_server = target_server

    def __str__(self):
        return f"Message({self.message_type}, {self.transaction_type}, {self.target_server})"

    def __repr__(self):
        return self.__str__()


class UserMessage(Message):
    def __init__(self, message_type, transaction_type, target_server, data):
        super().__init__(message_type, transaction_type, target_server)
        self.data = data  # Additional data for the UserMessage

    def __str__(self):
        return f"UserMessage({self.message_type}, {self.transaction_type}, {self.data})"


class ForwardMessage(Message):
    def __init__(self, message_type, transaction_type, target_server, transaction_id, hop_id, action, table_name, data, origin_server):
        super().__init__(message_type, transaction_type, target_server)
        self.transaction_id = transaction_id  # The ID of the transaction
        self.hop_id = hop_id  # The ID of the hop
        self.action = action  # The action to be performed
        self.table_name = table_name  # The name of the table involved
        self.data = data  # Additional data for the ForwardMessage
        self.origin_server = origin_server  # The identifier of the origin server

    def __str__(self):
        return f"ForwardMessage({self.message_type}, {self.transaction_id}, {self.hop_id}, {self.action}, {self.table_name}, {self.data})"


class BackwardMessage(Message):
    def __init__(self, message_type, transaction_type, target_server, transaction_id, result, forward_message_id, origin_server):
        super().__init__(message_type, transaction_type, target_server)
        self.transaction_id = transaction_id  # The ID of the transaction
        self.result = result  # The result of the operation (True or False)
        self.forward_message_id = forward_message_id  # The ID of the corresponding forward message
        self.origin_server = origin_server  # The identifier of the origin server

    def __str__(self):
        return f"BackwardMessage({self.message_type}, {self.transaction_id}, {self.result})"
