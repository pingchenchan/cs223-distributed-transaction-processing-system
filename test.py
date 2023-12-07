from message import *
import json
import random
import asyncio
import websockets
import time
from message import *
from history_table import *




def generate_transaction_data(transaction_type):
    if transaction_type == TransactionType.T1:
        # create a Transaction 1 data
        name = f"Name_{random.randint(1, 100)}"
        email = f"email{random.randint(1, 100)}@example.com"
        address = f"Address_{random.randint(1, 100)}"
        return {'name': name, 'email': email, 'address': address} 
    #NOTE: In T1, customer_id is autogenerated in the database beacuse it is a insert statement rather than a select statement
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
async def countdown_timer(seconds):
    for remaining in range(seconds, 0, -1):
        print(f"Remaining time: {remaining} seconds wait.", end="\r", flush=True)
        await asyncio.sleep(1)

async def send_testing_message(uri, current_server_type):
    messages = [] 
    start_time = time.time()
    loops=300
    for i in range(loops):
        # sleep_for = random.uniform(0.0001, 0.001)
        # await asyncio.sleep(sleep_for)

        '''Choose transaction types, feel free to change it '''
        # transaction_type = random.choice([TransactionType.T1, TransactionType.T2,  TransactionType.T5, TransactionType.T6, TransactionType.T7])
        transaction_type = random.choice([TransactionType.T1, TransactionType.T2, TransactionType.T3, TransactionType.T4, TransactionType.T5, TransactionType.T6, TransactionType.T7])
        # transaction_type = random.choice([TransactionType.T3, TransactionType.T4])
        # transaction_type = random.choice([ TransactionType.T3,TransactionType.T4, TransactionType.T7])
        

        data = generate_transaction_data(transaction_type)
        message = UserMessage(MessageType.USER, transaction_type, data)



        '''method 1: send batch messages by using WebSocketClientForBatchMessage()'''
        message_json = message_to_json(message)
        messages.append(message_json)


        '''method 2: send message one by one with await WebSocketClient.send_message(message, uri)'''
        # reply = await WebSocketClient.send_message(message, uri)
        # print(f"=> The client info: idx-{i} msg has {reply}")

        '''method 3: send batch messages by using WebSocketClient()'''
        # asyncio.create_task(WebSocketClient.send_message(message, uri)) 
    
    '''Following code is for method 1:'''
    '''For other methods, please comment out the following line'''
    reply = await WebSocketClientForBatchMessage.send_messages(messages, uri) 



    '''calculate the time for processing messages'''
    print(f"=> The {current_server_type} client info: idx-{i} msg has receeived")
    end_time = time.time()  # 
    elapsed_time = end_time - start_time  # 
    # print(f"Reply received in {elapsed_time:.2f} seconds: {reply}")
    # reply = await WebSocketClientForBatchMessage.send_messages(messages, uri) 

    '''write transaction log to history table'''
    history_table = HistoryTable()
    sleep_time = max(0.01*loops,1)

    '''
    Timer for executing the lots of concurrent transactions and than write transaction log,
    otherwise, the logs will have many uncompleted transactions. 
    Sometimes, the displayed number flashes because there are three counters counting down at the same time.
    If still have many uncompleted transactions, please increase the sleep_time.
    '''
    await asyncio.gather(
    countdown_timer(int(sleep_time))
)
    await history_table.write_transaction_log(each_transaction=False)
    
    print(f"wrote {current_server_type} transaction log ")

 
       
