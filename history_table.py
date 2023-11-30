from datetime import datetime
import sqlite3
from sqlite import *



class Hop:
    VALID_STATUSES = {'Active', 'Completed', 'Failed'} 

    def __init__(self, hop_id, action, table_name, data):
        self.hop_id = hop_id
        self.action = action
        self.table_name = table_name
        self.status = 'Active'
        self.end_time = None
        self.data = data

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
        self.hops = {}
        self.total_hops = total_hops
        self.last_completed_hop = 0



class HistoryTable:
    def __init__(self):
        self.transactions = {}

    def add_transaction(self, transaction_id, transaction_type, total_hops):
        self.transactions[transaction_id] = Transaction(transaction_id, transaction_type, total_hops)

    def add_hop(self, transaction_id, hop_id, action, table_name, data):
        transaction = self.transactions.get(transaction_id)
        if transaction:
            transaction.hops[hop_id] = Hop(hop_id, action, table_name, data)
        else:
            print(f"Transaction {transaction_id} not found.")


    def complete_transaction_hop(self, transaction_id, hop_id):
        transaction = self.transactions.get(transaction_id)
        if transaction:
            hop = transaction.hops.get(hop_id)
            if hop and hop.status == 'Active':
                hop.status = 'Completed'
                hop.end_time = datetime.now()
                transaction.last_completed_hop = hop_id
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
        else:
            print(f"Transaction {transaction_id} not found.")


# Usage
# history_table = HistoryTable()
# history_table.add_transaction('TX123', 'Insert', 2)
# history_table.add_hop('TX123', 1, 'Insert', 'Cameras', {'camera_id': 4, 'model_name': 'Nikon D5600', 'resolution': '24.2MP', 'lens_type': 'AF-P DX', 'price': 600.00})
# history_table.complete_transaction('TX123')
