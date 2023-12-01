from message import *
import json
import random
import asyncio
import websockets
import time
from message import *




def generate_transaction_data(transaction_type):
    if transaction_type == TransactionType.T1:
        # create a Transaction 1 data
        name = f"Name_{random.randint(1, 100)}"
        email = f"email{random.randint(1, 100)}@example.com"
        address = f"Address_{random.randint(1, 100)}"
        return {'name': name, 'email': email, 'address': address}
    elif transaction_type == TransactionType.T2:
        # create a Transaction 2 data
        model_name = f"Model_{random.randint(1, 100)}"
        resolution = random.choice(["1080p", "4K", "720p"])
        lens_type = random.choice(["wide", "telephoto", "standard"])
        price = random.uniform(100, 1000)
        return {'model_name': model_name, 'resolution': resolution, 'lens_type': lens_type, 'price': price}
    elif transaction_type == TransactionType.T3 or transaction_type == TransactionType.T4:
        customer_id = random.randint(1, 3)
        camera_id = random.randint(1, 3)
        quantity = random.randint(1, 10)
        return {'customer_id': customer_id, 'camera_id': camera_id, 'quantity': quantity}   
    elif transaction_type == TransactionType.T5:
        customer_id = random.randint(1, 3)
        return {'customer_id': customer_id}
    elif transaction_type == TransactionType.T6:
        camera_id = random.randint(1, 3)
        return {'camera_id': camera_id}
    elif transaction_type == TransactionType.T7:
        order_id = random.randint(1, 3)
        return {'order_id': order_id}

async def send_testing_message(uri):
    for i in range(200):
        # sleep_for = random.uniform(0.3, 1.0)
        # await asyncio.sleep(sleep_for)

        # randomly choose a transaction type
        transaction_type = random.choice([TransactionType.T1, TransactionType.T2,  TransactionType.T5, TransactionType.T6, TransactionType.T7])
        # transaction_type = random.choice([TransactionType.T3, TransactionType.T4])
        
        ## create a dummy data for the transaction
        data = generate_transaction_data(transaction_type)
        # create a UserMessage
        message = UserMessage(MessageType.USER, transaction_type, data)

        # transform the message to JSON
        reply = await WebSocketClient.send_message(message, uri)
        print(f"=> The client info: idx-{i} msg has {reply}")
       
