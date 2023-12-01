from datetime import datetime
import sqlite3
from sqlite import *
import uuid
import message


class Hop:
    VALID_STATUSES = {'Active', 'Completed', 'Failed'} 
    def __init__(self,transaction_id, hop_id, data):
        self.transaction_id = transaction_id 
        self.hop_id = hop_id
        self.status = 'Active'
        self.end_time = None
        self.data = data
#TODO  which server exe this hop info


    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value in self.VALID_STATUSES:
            self._status = value
        else:
            raise ValueError(f"Status must be one of {self.VALID_STATUSES}, not '{value}'")


class Transaction:
    def __init__(self, transaction_id, transaction_type, total_hops):
        self.transaction_id = transaction_id
        self.transaction_type = transaction_type
        self.status = 'Active'
        self.start_time = datetime.now()
        self.end_time = None
        self.hops = {} #{1: Hop, 2: Hop, ...}
        self.total_hops = total_hops
        self.last_completed_hop = 0



class HistoryTable:
    _instance = None 
    _is_initialized = False 

    def __new__(cls): 
        if cls._instance is None: # Singleton pattern
            cls._instance = super(HistoryTable, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._is_initialized:
            self.transactions = {}
            self._is_initialized = True


    def add_transaction(self, transaction_type, total_hops)-> str:
        transaction_id = str(uuid.uuid4())  # Generate a random transaction ID
        self.transactions[transaction_id] = Transaction(transaction_id, transaction_type, total_hops)
        # self.generate_corresponding_hop(transaction_id, data)
        
        return transaction_id

    def generate_corresponding_hop(self, transaction_id, data)-> Hop:
        transaction_type = self.transactions.get(transaction_id).transaction_type
        if transaction_type in [transaction_type.T1, transaction_type.T2, transaction_type.T5, transaction_type.T6, transaction_type.T7]:
            self.transactions[transaction_id].hops[1] = Hop(transaction_id=transaction_id, hop_id=1, data=data) #hop_id starts from 1
        if transaction_type in [transaction_type.T3, transaction_type.T4]:
            self.transactions[transaction_id].hops[1] = Hop(transaction_id=transaction_id, hop_id=1, data=data)
            self.transactions[transaction_id].hops[2] = Hop(transaction_id=transaction_id, hop_id=2, data=data)
        return self.transactions[transaction_id].hops



    def complete_transaction_hop(self,hop):
        transaction = self.transactions.get(hop.transaction_id)
        if hop and hop.status == 'Active':
            hop.status = 'Completed'
            hop.end_time = datetime.now()
            transaction.last_completed_hop = hop.hop_id
        else:
            print(f"Unable to complete hop: {hop_id} not found or already completed.")

    def find_next_hop(self, transaction_id):
        transaction = self.transactions.get(transaction_id)
        if transaction:
            for hop_id in range(transaction.last_completed_hop + 1, transaction.total_hops + 1):
                hop = transaction.hops.get(hop_id)
                if hop and hop.status == 'Active':
                    return hop
            return None
        else:
            print(f"Transaction {transaction_id} not found.")



    def complete_transaction(self, transaction_id):
        transaction = self.transactions.get(transaction_id)
        if transaction:
            transaction.status = 'Completed'
            transaction.end_time = datetime.now()
            for hop in transaction.hops:
                if transaction.hops[hop].status != 'Completed':
                    hop.status = 'Completed'
                    hop.end_time = datetime.now()
            return True
  
        else:
            print(f"Transaction {transaction_id} not found.")
            return False


# Usage
# history_table = HistoryTable()
# history_table.add_transaction('TX123', 'Insert', 2)
# history_table.add_hop('TX123', 1, 'Insert', 'Cameras', {'camera_id': 4, 'model_name': 'Nikon D5600', 'resolution': '24.2MP', 'lens_type': 'AF-P DX', 'price': 600.00})
# history_table.complete_transaction('TX123')
