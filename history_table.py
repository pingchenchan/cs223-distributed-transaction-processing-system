from datetime import datetime
import sqlite3
from sqlite import *
import uuid
from message import *
from collections import defaultdict


class Hop:
    VALID_STATUSES = {'Active', 'Completed', 'Failed'} 
    def __init__(self,transaction_id, hop_id, data):
        self.transaction_id = transaction_id 
        self.hop_id = hop_id
        self.status = 'Active'
        self.start_time = datetime.now()
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
            if transaction.last_completed_hop == transaction.total_hops:
                transaction.status = 'Completed'
                transaction.end_time = datetime.now()
                return True
            else:
                return False
        elif hop and hop.status == 'Completed':
            pass
            # print('Hop has already been completed.', 'Hop ID:', hop.hop_id,'transaction type:', transaction.transaction_type)
        else:
            print(f"Unable to complete hop: {hop.hop_id} not found or status id fail. Hop status is : {hop.status }")

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


  

    


    def write_transaction_log(self, each_transaction=True):
        """Write the transaction log to a file."""
        
        total_latency = 0
        total_transactions = 0
        total_first_hop_latency_by_type = defaultdict(int)
        total_second_hop_latency_by_type = defaultdict(int)
        first_hop_count_by_type = defaultdict(int) 
        second_hop_count_by_type = defaultdict(int)

        total_latency_by_type = defaultdict(float)  # Save the total latency for each type of transaction
        count_by_type = defaultdict(int)  # Save the count for each type of transaction

        with open('transaction_log.txt', 'w') as f:
            if each_transaction:
                f.write("Transaction Log Summary\n")
                f.write("======================\n\n")

            for transaction_id, transaction in self.transactions.items():
                transaction_latency = "Not finished"
                if transaction.end_time:
                    transaction_latency = (transaction.end_time - transaction.start_time).total_seconds() * 1000000  # in microseconds
                    total_latency += transaction_latency
                    total_transactions += 1
                    total_latency_by_type[transaction.transaction_type] += transaction_latency
                    count_by_type[transaction.transaction_type] += 1
                if each_transaction:
                    f.write(f"Transaction ID: {transaction_id}\n")
                    # f.write(f"    Start Time: {transaction.start_time.strftime('%H:%M:%S.%f')}, End Time: {transaction.end_time.strftime('%H:%M:%S.%f') if transaction.end_time else 'Not finished'}, Latency (us): {transaction_latency}\n")
                    f.write(f"    Transaction Type: {transaction.transaction_type.name}, Latency (us): {transaction_latency}\n")
                for hop in transaction.hops.values():
                    hop_latency = "Not finished"
                    if hop.end_time:
                        hop_latency = (hop.end_time - hop.start_time).total_seconds() * 1000000
                        # if hop.hop_id in [1, 2] and transaction.transaction_type in [TransactionType.T3, TransactionType.T4]:
                        if hop.hop_id == 1:
                            total_first_hop_latency_by_type[transaction.transaction_type] += hop_latency
                            first_hop_count_by_type[transaction.transaction_type] += 1
                        elif hop.hop_id == 2:
                            total_second_hop_latency_by_type[transaction.transaction_type] += hop_latency
                            second_hop_count_by_type[transaction.transaction_type] += 1
                    # f.write(f"    Hop ID: {hop.hop_id}, Start Time: {hop.start_time.strftime('%H:%M:%S.%f')}, End Time: {hop.end_time.strftime('%H:%M:%S.%f') if hop.end_time else 'Not finished'}, Latency (us): {hop_latency}\n")
                    # f.write(f"    Hop ID: {hop.hop_id}, Latency (us): {hop_latency}\n")

            # Calculate and write average latencies
            f.write("\nOverall Summary\n")
            f.write("---------------\n")
            avg_transaction_latency = total_latency / total_transactions if total_transactions > 0 else 0
            f.write(f"Average Transaction Latency: {avg_transaction_latency:.2f} microseconds\n\n")
            
            f.write("Average Latency by Transaction Type\n")
            f.write("-----------------------------------\n")
            for transaction_type in TransactionType:
                avg_latency = (total_latency_by_type[transaction_type] / count_by_type[transaction_type]) if count_by_type[transaction_type] > 0 else 0
                f.write(f"{transaction_type.name}: {avg_latency:7.2f} microseconds\n")

            f.write("\nAverage Latency by Hop for Each Transaction Type\n")
            f.write("------------------------------------------------\n")
            for transaction_type in TransactionType:
                avg_latency = (total_latency_by_type[transaction_type] / count_by_type[transaction_type]) if count_by_type[transaction_type] > 0 else 0
                avg_first_hop_latency = (total_first_hop_latency_by_type[transaction_type] / first_hop_count_by_type[transaction_type]) if first_hop_count_by_type[transaction_type] > 0 else 0
                avg_second_hop_latency = (total_second_hop_latency_by_type[transaction_type] / second_hop_count_by_type[transaction_type]) if second_hop_count_by_type[transaction_type] > 0 else 0
                f.write(f"{transaction_type.name} - Average TTransaction Latency: {avg_latency:7.2f} microseconds, First Hop: {avg_first_hop_latency:7.2f} microseconds, Second Hop: {avg_second_hop_latency:7.2f} microseconds\n")

            f.write("\n")