import asyncio
import websockets
import random
import json
from history_table import *
from message import UserMessage, HopMessage, ForwardMessage, BackwardMessage, MessageType, TransactionType
from test import *



history_table = HistoryTable()
# Priority queue for handling messages
priority_queue = asyncio.PriorityQueue()
# Maximum number of retries for failed messages
MAX_RETRIES = 3
 # Dictionary to keep track of connected clients
connected_clients = {}  
calculate_total_hops = lambda transaction_type: 1 if transaction_type in [TransactionType.T1, TransactionType.T2, TransactionType.T5, TransactionType.T6, TransactionType.T7] else 2


async def listener_handler(websocket, path):
    client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    connected_clients[client_id] = websocket
    try:
        async for message in websocket:
            # print(f"Received from {client_id}: '{message}'")
            try:
                message_data = json.loads(message)
                message_type = message_data['message_type']
                transaction_type = TransactionType[message_data['transaction_type']]
                total_hops = calculate_total_hops(transaction_type)
                transaction_id = history_table.add_transaction(transaction_type, total_hops)
                hops = history_table.generate_corresponding_hop(transaction_id, message_data['data'])
                for hop_id in range(1, total_hops+1): #hop index starts from 1
                    hop_message = HopMessage(MessageType.HOP, transaction_type, hops[hop_id])
                    await priority_queue.put((1, client_id, hop_message ))  # 1 is the priority
                
            except json.JSONDecodeError:
                print("Error parsing JSON")
            
    finally:
        del connected_clients[client_id]


async def thread_handler(db):
    while True:
        # Wait until an item is available, asynchronously
        _, client_id , message  = await priority_queue.get()
        transaction_type = message.transaction_type
        message_type = message.message_type
        try:
            # print(f"priority_queue Processing item: '{message}'")
            if message_type == MessageType.BACKWARD:
                success = message.get('success', False)
                retry_count = message.get('retry_count', 0)

                if not success and retry_count < MAX_RETRIES:
                    print("Resending message.")
                    message['retry_count'] = retry_count + 1
                    await priority_queue.put((1, message))  # Requeue with the same priority
                else:
                    print("Handling successful backward message.")
                    
                    # Process completed hop and update the history table
                    # Additional logic for processing the message goes here

            elif message_type == MessageType.FORWARD:
                print("Processing forward message.")
                # Process the current hop
                # Send a backward message with completion or failure information

            elif message_type == MessageType.USER :
                print("Processing user message.")
                # Handle new or in-progress transactions
                # Send a forward message to the next hop
                
                print("Processing transaction.")
                # Transaction processing logic goes here
    
            elif message_type == MessageType.HOP:
                execute_hop(db, message.data.transaction_id, message.data.hop_id)
                print("Processing hop message.")
                # Execute the hop
                # Send a forward message to the next hop

            if client_id in connected_clients:
                await connected_clients[client_id].send(f"Handling successful backward message and priority_queue.qsize is {priority_queue.qsize()}" )
        finally:
            priority_queue.task_done()
        

def execute_hop(db, transaction_id, hop_id):
    transaction = history_table.transactions.get(transaction_id)
    transaction_type = transaction.transaction_type
    if transaction:
        hop = transaction.hops.get(hop_id)
        if hop and hop.status == 'Active':
            hop.start_time = datetime.now()
            
            if transaction_type == TransactionType.T1:
                result = transaction_1(db, hop.data['name'], hop.data['email'], hop.data['address'])
                row_count = get_row_count(db, 'Customers')
                print(f"Number of rows in Customers: {row_count}")
            elif transaction_type== TransactionType.T2:
                result = transaction_2(db, hop.data['model_name'], hop.data['resolution'], hop.data['lens_type'], hop.data['price'])
                row_count = get_row_count(db, 'Cameras')
                print(f"Number of rows in Cameras: {row_count}")
            
            elif transaction_type == TransactionType.T3:
                if hop_id == 1:
                    result = transaction_3_hop1(db, hop.data['customer_id'])
                elif hop_id == 2:
                    result = transaction_3_hop2(db, hop.data['customer_id'], hop.data['quantity'])
            elif transaction_type == TransactionType.T4:
                if hop_id == 1:
                    result = transaction_4_hop1(db, hop.data['camera_id'])
                elif hop_id == 2:
                    result = transaction_4_hop2(db, hop.data['camera_id'], hop.data['quantity'])
            elif transaction_type == TransactionType.T5:
                result = transaction_5(db, hop.data['customer_id'])
            elif transaction_type == TransactionType.T6:
                result = transaction_6(db, hop.data['camera_id'])
            elif transaction_type == TransactionType.T7:
                result = transaction_7(db, hop.data['order_id'])
            if result:
                print('transaction_type: ', transaction_type, 'hop_id: ', hop_id, 'result: ', result)
                history_table.complete_transaction_hop(hop)
                if hop_id == transaction.total_hops:
                    history_table.complete_transaction(transaction_id)
                return True
        else:
            print(f"Unable to execute hop: {hop_id} not found or already executed.")
    else:
        print(f"Transaction {transaction_id} not found.")
    return False



async def main():
    build_db() #if db does not exist, create it
    db = Database('shop.db')
    # Start the server, handler and client

    server_task = websockets.serve(listener_handler, 'localhost', 8898)
    handler_task = asyncio.create_task(thread_handler(db))
    client_task = asyncio.create_task(send_testing_message('ws://localhost:8898'))


    #concurently run the server, handler and client
    await asyncio.gather(server_task, handler_task, client_task)

    

# Run the main function to start the program
asyncio.run(main())


