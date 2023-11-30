from datetime import datetime
import sqlite3
from sqlite import *
from history_table import *

conn = sqlite3.connect('shop.db')
cursor = conn.cursor()

def execute_hop(transaction_id, hop_id):
    transaction = HistoryTable.transactions.get(transaction_id)
    if transaction:
        hop = transaction.hops.get(hop_id)
        if hop and hop.status == 'Active':
            hop.status = 'Executing'
            hop.start_time = datetime.now()
            if transaction_id == 3:
                if hop_id == 1:
                    result = transaction_3_hop1(cursor, hop.data['customer_id'], hop.data['quantity'])
                elif hop_id == 2:
                    result = transaction_3_hop2(cursor, hop.data['customer_id'], hop.data['quantity'])
            elif transaction_id == 4:
                if hop_id == 1:
                    result = transaction_4_hop1(cursor, hop.data['camera_id'])
                elif hop_id == 2:
                    result = transaction_4_hop2(cursor, hop.data['camera_id'], hop.data['quantity'])
            elif transaction_id == 5:
                result = transaction_5(cursor, hop.data['customer_id'])
            elif transaction_id == 6:
                result = transaction_6(cursor, hop.data['camera_id'])
            elif transaction_id == 7:
                result = transaction_7(cursor, hop.data['order_id'])
            if result:
                HistoryTable.complete_transaction_hop(transaction_id, hop_id)
                if hop_id == transaction.total_hops:
                    HistoryTable.complete_transaction(transaction_id)
                return True
        else:
            print(f"Unable to execute hop: {hop_id} not found or already executed.")
    else:
        print(f"Transaction {transaction_id} not found.")
    return False
