from message import *
import json
import random
import asyncio
import websockets
import time
from history_table import *
from test import countdown_timer
random.seed(114514)
TX_LIST = [TransactionType.T1, TransactionType.T2, TransactionType.T3, TransactionType.T4, TransactionType.T5, TransactionType.T6, TransactionType.T7]


def generate_transaction_data(transaction_type, range=1, server=1):
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
    elif transaction_type == TransactionType.T3:
        if random.random() > 0.9:
            server = 1 - server
        # customer_id = random.randint(1, range) * 2
        customer_id = random.randint(1, 1) * 2
        if server == 1:
            customer_id = customer_id - 1
        camera_id = 1
        quantity = random.randint(1, 10)
        return {'customer_id': customer_id, 'camera_id': camera_id, 'quantity': quantity}   
    elif transaction_type == TransactionType.T4:
        customer_id = 1
        camera_id = random.randint(1, range)
        quantity = random.randint(1, 10)
        return {'customer_id': customer_id, 'camera_id': camera_id, 'quantity': quantity}   
    elif transaction_type == TransactionType.T5:
        if random.random() > 0.9:
            server = 1 - server
        # customer_id = random.randint(1, range) * 2
        customer_id = random.randint(1, 1) * 2
        if server == 1:
            customer_id = customer_id - 1
        return {'customer_id': customer_id}
    elif transaction_type == TransactionType.T6:
        camera_id = random.randint(1, range)
        return {'camera_id': camera_id}
    elif transaction_type == TransactionType.T7:
        customer_id = random.randint(1, 3)
        order_id = random.randint(1, 3)
        return {'order_id': order_id, 'customer_id': customer_id}

async def send_testing_message(uri, server, loops=500):
    # 0-stock 1-customerA 2-customerB
    added_lines = 1
    added_orders = 1
    messages = [] 
    start_time = time.time()
    if server == 0:
        data = generate_transaction_data(TransactionType.T2)
        message = UserMessage(MessageType.USER, TransactionType.T2, data)

        '''method 1: send batch messages by using WebSocketClientForBatchMessage()'''
        message_json = message_to_json(message)
        messages.append(message_json)
        for i in range(loops):
            sleep_for = random.uniform(0.0001, 0.001)
            await asyncio.sleep(sleep_for)
            '''Choose transaction types, feel free to change it '''
            transaction_type = random.choices(TX_LIST, weights=(0, 5, 0, 10, 10, 10, 10))[0]
            # print(f"server-{server} transaction_type={transaction_type}")
            data = generate_transaction_data(transaction_type, range=added_lines)
            message = UserMessage(MessageType.USER, transaction_type, data)

            '''method 1: send batch messages by using WebSocketClientForBatchMessage()'''
            message_json = message_to_json(message)
            messages.append(message_json)

            if transaction_type == TransactionType.T1 or transaction_type == TransactionType.T2:
                added_lines = added_lines + 1
            if transaction_type == TransactionType.T3 or transaction_type == TransactionType.T4:
                added_orders = added_orders + 1
    elif server == 1 or server == 2:
        data = generate_transaction_data(TransactionType.T1)
        message = UserMessage(MessageType.USER, TransactionType.T1, data)

        '''method 1: send batch messages by using WebSocketClientForBatchMessage()'''
        message_json = message_to_json(message)
        messages.append(message_json)
        for i in range(loops):
            sleep_for = random.uniform(0.0001, 0.001)
            await asyncio.sleep(sleep_for)
            '''Choose transaction types, feel free to change it '''
            transaction_type = random.choices(TX_LIST, weights=(5, 0, 10, 0, 5, 10, 5))[0]
            # print(f"server-{server} transaction_type={transaction_type}")

            data = generate_transaction_data(transaction_type, range=added_lines, server=server)
            message = UserMessage(MessageType.USER, transaction_type, data)

            '''method 1: send batch messages by using WebSocketClientForBatchMessage()'''
            message_json = message_to_json(message)
            messages.append(message_json)

            if transaction_type == TransactionType.T1 or transaction_type == TransactionType.T2:
                added_lines = added_lines + 1
            if transaction_type == TransactionType.T3 or transaction_type == TransactionType.T4:
                added_orders = added_orders + 1
    
    '''Following code is for method 1:'''
    '''For other methods, please comment out the following line'''
    reply = await WebSocketClientForBatchMessage.send_messages(messages, uri) 


    '''calculate the time for processing messages'''
    end_time = time.time()  # 
    elapsed_time = end_time - start_time  # 
    print(f"Reply received in server-{ server}, elapsed_time ={elapsed_time:.2f} seconds, reply= {reply}")


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
    history_table = HistoryTable()
    await history_table.write_transaction_log(each_transaction=False,filename='test_realworld_sim')
    print(f"wrote transaction log ")

    # g = 0
    # for i in history_table.transactions:
    #     g =i
    # print(history_table.transactions[i].hops[1].queue_tracker)
