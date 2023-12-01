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
 # Dictionary to keep track of connected clients, {client_id: websocket}
connected_clients = {} 
calculate_total_hops = lambda transaction_type: 1 if transaction_type in [TransactionType.T1, TransactionType.T2, TransactionType.T5, TransactionType.T6, TransactionType.T7] else 2

SERVER_PORT = 8898
ORDER_SERVER_PORT = 8898


#TODO add first hop/transaction start & fininsh time to txt file
#TODO record edges of each transaction, total transmition 
#TODO hop waiting time in queue

#TODO mute print
#TODO hop1 priority = 10, hop2 priority = 0, backward priority = 5

#TODO priority mechanism
async def listener_handler(websocket, path):
    client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    connected_clients[client_id] = websocket
    try:
        async for message in websocket:
            # print(f"Received from {client_id}: '{message}'")
            try:
                message_data = json.loads(message)
                message_type = MessageType[message_data['message_type']]
                if message_type == MessageType.USER:
                    transaction_type = TransactionType[message_data['transaction_type']]
                    total_hops = calculate_total_hops(transaction_type)
                    transaction_id = history_table.add_transaction(transaction_type, total_hops)
                    hops = history_table.generate_corresponding_hop(transaction_id, message_data['data'])
                    #TODO only put first hop into priority queue, 
                    # after first hop is completed, put second hop into priority queue
                    # for hop_id in range(1, total_hops+1): #hop index starts from 1
                    hop_message = HopMessage(MessageType.HOP, transaction_type, hops[1])
                    await priority_queue.put(( client_id, hop_message ))
                    await connected_clients[client_id].send(f"successful sent to server" )




                # TODO if first hop failed, reinsert the first hop into the priority queue
                elif message_type == MessageType.FORWARD:
                    transaction_type = TransactionType[message_data['transaction_type']]
                    transaction_id = message_data['transaction_id']
                    hop_id = message_data['hop_id']
                    origin_server = message_data['origin_server']
                    target_server = message_data['target_server']
                    data = message_data['data']
                    hop_message = ForwardMessage(message_type=MessageType.FORWARD, transaction_type=transaction_type, target_server=target_server, transaction_id=transaction_id, hop_id=hop_id, origin_server=origin_server, data=data)
                    print('=> The forward info: server successful receive forward message')
                    # await connected_clients[client_id].send('successful receive forward process message)
                    # The client doesn't use await so it doesn't need to send messages back to the client.
                    await priority_queue.put(( client_id, hop_message ))  # 1 is the priority
                
                elif message_type == MessageType.BACKWARD:
                    transaction_type = TransactionType[message_data['transaction_type']]
                    transaction_id = message_data['transaction_id']
                    hop_id = message_data['hop_id']
                    origin_server = message_data['origin_server']
                    target_server = message_data['target_server']
                    result = message_data['result']
                    hop_message = BackwardMessage(message_type=MessageType.BACKWARD, transaction_type=transaction_type, transaction_id=transaction_id, hop_id=hop_id,origin_server=origin_server, target_server=target_server, result=result)
                    await priority_queue.put(( client_id, hop_message ))
                print(f"priority_queue.qsize is {priority_queue.qsize()}")

            except json.JSONDecodeError:
                print("Error parsing JSON")
            
    finally:
        del connected_clients[client_id]

def insert_nexthop_to_priority_queue(transaction_id, hop_id):
    transaction = history_table.transactions.get(transaction_id)
    if transaction:
        hop = transaction.hops.get(hop_id)
        if hop and hop.status == 'Active':
            hop_message = HopMessage(MessageType.HOP, transaction.transaction_type, hop)
            priority_queue.put((1, hop_message))
        else:
            print(f"Unable to insert hop: {hop_id} not found or already completed.")
    else:
        print(f"Transaction {transaction_id} not found.")

#TODO locking mechanism
def Acquire_locks(transaction_type):
    pass



async def thread_handler(db):
    while True:
        # Wait until an item is available, asynchronously
        client_id , message  = await priority_queue.get()
        transaction_type = message.transaction_type
        message_type = message.message_type
        try:
            # print('message_type',message_type,'message', message)
            if message_type == MessageType.BACKWARD: 
                
                success = message.result #bool
                print(f"=> Backward Processing info: {message.transaction_type}-hop{message.hop_id }, success = {success }.")
                
                #TODO complete_hop -> if is last hop, complete the transaction
                if success: #if is secend hop, complete the transaction
                    transaction_completed = history_table.complete_transaction(message.transaction_id)
                else:
                    pass


                print(f"=> Transaction {message.transaction_type} completed: {transaction_completed}")
                
                #TODO push secend hop into priority queue


                #TODO                 
                # retry_count = message.retry_count
                # if not success and retry_count < MAX_RETRIES:
                #     print("Resending message.")
                #     message['retry_count'] = retry_count + 1
                #     await priority_queue.put((1, message))  # Requeue with the same priority
                # else:
                #     print("Handling successful backward message.")
                    
                # Process completed hop and update the history table
                # Additional logic for processing the message goes here

            elif message_type == MessageType.FORWARD: 
                print(f"=> Forward Processing info: {message.transaction_type}-hop{message.hop_id }message.")

                # Process the current hop
                result = await execute_hop(db, message.transaction_id, message.hop_id)

                
                # Send a backward message with completion or failure information
                backward_message = BackwardMessage(message_type=MessageType.BACKWARD, transaction_type=transaction_type, transaction_id=message.transaction_id, hop_id=message.hop_id,origin_server=message.target_server, target_server=message.origin_server, result=result)
                
                #creat a task to async send message to order server, in order to not block the main thread
                asyncio.create_task(send_message(backward_message, f"ws://localhost:{message.origin_server}")) 
                



                

            elif message_type == MessageType.USER :
                pass
                # Can delete this part, every user message will be transformed to hop message 
                # and put into priority queue by listener_handler function

    
            elif message_type == MessageType.HOP: 
                #TODO  Locking logic
                #Forwards the message to the orders server
                if transaction_type in [ TransactionType.T3, TransactionType.T4] and message.hop.hop_id ==2:
                    forward_message = ForwardMessage( message_type=MessageType.FORWARD, transaction_type=transaction_type, target_server=ORDER_SERVER_PORT, transaction_id=message.hop.transaction_id, hop_id=message.hop.hop_id, origin_server=SERVER_PORT, data=message.hop.data)
                    
                    #creat a task to async send message to order server, in order to not block the main thread
                    asyncio.create_task(send_message(forward_message, f"ws://localhost:{ORDER_SERVER_PORT}")) 
                    

                else:
                    #p rocess the current hop
                    await execute_hop(db, message.hop.transaction_id, message.hop.hop_id)
                    
                    print(f"=> HOP Processing info: {message.transaction_type}-hop{message.hop.hop_id }")

        finally:
            priority_queue.task_done()
        

async def execute_hop(db, transaction_id, hop_id):
    #TODO if failed(LOCK) reinsert the hop into priority queue, priority +1
    transaction = history_table.transactions.get(transaction_id)
    transaction_type = transaction.transaction_type
    if transaction:
        hop = transaction.hops.get(hop_id)
        if hop and hop.status == 'Active':
            hop.start_time = datetime.now()
            
            if transaction_type == TransactionType.T1:
                result = transaction_1(db, hop.data['name'], hop.data['email'], hop.data['address'])
                row_count = get_row_count(db, 'Customers')
                # print(f"Number of rows in Customers: {row_count}")
            elif transaction_type== TransactionType.T2:
                result = transaction_2(db, hop.data['model_name'], hop.data['resolution'], hop.data['lens_type'], hop.data['price'])
                row_count = get_row_count(db, 'Cameras')
                # print(f"Number of rows in Cameras: {row_count}")
            
            elif transaction_type == TransactionType.T3:
                if hop_id == 1:
                    result = transaction_3_hop1(db, hop.data['customer_id'])
                    # print('|| transaction_3_hop1 result: ', result)
                elif hop_id == 2:
                    result = transaction_3_hop2(db, hop.data['customer_id'], hop.data['quantity'])
                    # print('|| transaction_3_hop2 result: ', result)
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
                # print('transaction_type: ', transaction_type, 'hop_id: ', hop_id, 'result: ', result)
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

    server_task = websockets.serve(listener_handler, 'localhost', SERVER_PORT )
    handler_task = asyncio.create_task(thread_handler(db))
    client_task = asyncio.create_task(send_testing_message(f"ws://localhost:{SERVER_PORT}"))


    #concurently run the server, handler and client
    await asyncio.gather(server_task, handler_task, client_task)

    

# Run the main function to start the program
asyncio.run(main())


