import asyncio
import websockets
import random
import json
from history_table import *
from message import *
from test import *
from lock_manager import *


# The two-phase locking protocol variant for concurrency control
# where Transaction 3-4 can proceed only if Transaction 7 is not in progress, and vice versa.
lock_manager = LockManager()

history_table = HistoryTable()
# Priority queue for handling messages
priority_queue = asyncio.PriorityQueue()
# Maximum number of retries for failed messages
MAX_RETRIES = 3
# Dictionary to keep track of connected clients, {client_id: websocket}
connected_clients = {}
calculate_total_hops = (
    lambda transaction_type: 1
    if transaction_type
    in [
        TransactionType.T1,
        TransactionType.T2,
        TransactionType.T5,
        TransactionType.T6,
        TransactionType.T7,
    ]
    else 2
)


SERVER_PORT = 8898
ORDER_SERVER_PORT = 8898
CURRENT_SERVER_PORT = 8898


# Total numbers/time of transmition and numbers/time of waiting in quene => Check hop.queue_tracker, hop.websocket_tracker
# Hop waiting time in queue ,transmission time, total time => Analysis function is write_transaction_log in history_table.py

# TODO mute print
# PRIORITY enum in message.py, first hop: 10, second hop: 0, backward and forward : 5


async def listener_handler(websocket, path):
    client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    connected_clients[client_id] = websocket
    try:
        async for message in websocket:
            try:
                message_data = json.loads(message)
                await connected_clients[client_id].send(f"successful sent to server")
                message_type = MessageType[message_data["message_type"]]

                """   Message Type: USER   """
                if message_type == MessageType.USER:
                    transaction_type = TransactionType[message_data["transaction_type"]]
                    total_hops = calculate_total_hops(transaction_type)
                    transaction_id = history_table.add_transaction(
                        transaction_type, total_hops
                    )
                    hops = history_table.generate_corresponding_hop(
                        transaction_id, message_data["data"]
                    )
                    # only put first hop into priority queue,
                    # after first hop is completed, put second hop into priority queue
                    # for hop_id in range(1, total_hops+1): #hop index starts from 1
                    hop_message = HopMessage(
                        MessageType.HOP, transaction_type, hops[1], PRIORITY.HOP_1.value
                    )

                    await priority_queue.put((client_id, hop_message))
                    hops[1].start_queue_wait()
                    # await connected_clients[client_id].send(f"successful sent to server" )

                """   Message Type: FORWARD   """
                if message_type == MessageType.FORWARD:
                    transaction_type = TransactionType[message_data["transaction_type"]]
                    transaction_id = message_data["transaction_id"]
                    hop_id = message_data["hop_id"]
                    origin_server = message_data["origin_server"]
                    target_server = message_data["target_server"]
                    data = message_data["data"]
                    hop_message = ForwardMessage(
                        message_type=MessageType.FORWARD,
                        transaction_type=transaction_type,
                        target_server=target_server,
                        transaction_id=transaction_id,
                        hop_id=hop_id,
                        origin_server=origin_server,
                        data=data,
                    )
                    hop_message.websocket_receive_time = datetime.now()
                    # print('forward_message received info', hop_message.websocket_receive_time)

                    # print('=> The forward info: server successful receive forward message')
                    await priority_queue.put(
                        (client_id, hop_message)
                    )  # 1 is the priority
                    hop_message.start_queue_wait()

                """   Message Type: BACKWARD   """
                if message_type == MessageType.BACKWARD:
                    transaction_type = TransactionType[message_data["transaction_type"]]
                    transaction_id = message_data["transaction_id"]
                    hop_id = message_data["hop_id"]
                    origin_server = message_data["origin_server"]
                    target_server = message_data["target_server"]
                    result = message_data["result"]
                    hop_executor_server = message_data["hop_executor_server"]

                    # print('backward_message received info', message_data)

                    # get hop info and update the websocket_tracker timestamp
                    hop = history_table.transactions.get(transaction_id).hops.get(
                        hop_id
                    )
                    hop.end_websocket_transmission(
                        convert_to_datetime(message_data["websocket_receive_time"])
                    )
                    hop.start_websocket_transmission(
                        convert_to_datetime(message_data["websocket_send_time"])
                    )
                    hop.end_websocket_transmission()

                    backward_queue_tracker = TimeTracker()
                    backward_queue_tracker.load_from_data(message_data["queue_tracker"])
                    hop.queue_tracker.concat(backward_queue_tracker)

                    # print('concated hop.queue_tracker info', hop.queue_tracker)

                    hop_message = BackwardMessage(
                        message_type=MessageType.BACKWARD,
                        transaction_type=transaction_type,
                        transaction_id=transaction_id,
                        hop_id=hop_id,
                        origin_server=origin_server,
                        target_server=target_server,
                        result=result,
                        hop_executor_server=hop_executor_server,
                    )
                    await priority_queue.put((client_id, hop_message))
                    hop_message.start_queue_wait()
                # print (f"priority_queue.qsize is {priority_queue.qsize()}")

            except json.JSONDecodeError:
                print("Error parsing JSON")

    finally:
        del connected_clients[client_id]


async def insert_nexthop_to_priority_queue(transaction_id, hop_id, client_id):
    transaction = history_table.transactions.get(transaction_id)
    if transaction:
        hop_2 = transaction.hops.get(hop_id)
        assert hop_id == 2
        if hop_2 and hop_2.status == "Active":
            hop_message = HopMessage(
                MessageType.HOP,
                transaction.transaction_type,
                hop_2,
                PRIORITY.HOP_2.value,
            )
            await priority_queue.put((client_id, hop_message))
            hop_2.start_queue_wait()
        else:
            print(f"Unable to insert hop: {hop_id} not found or already completed.")
    else:
        print(f"Transaction {transaction_id} not found.")


# TODO locking mechanism
def Acquire_locks(transaction_type):
    pass


async def thread_handler(db):
    # sleep_for = random.uniform(2, 3)
    # await asyncio.sleep(1)
    while True:
        # Wait until an item is available, asynchronously

        client_id, message = await priority_queue.get()

        transaction_type = message.transaction_type
        message_type = message.message_type
        try:
            # print('message_type',message_type,'message', message)
            """Message Type: BACKWARD"""
            if message_type == MessageType.BACKWARD:
                message.end_queue_wait()
                success = message.result  # bool
                # print(f"=> Backward Processing info: {message.transaction_type}-hop{message.hop_id }, success = {success }.")
                transaction = history_table.transactions.get(message.transaction_id)
                hop = transaction.hops.get(message.hop_id)
                # Process completed hop and update the history table
                if success:
                    history_table.complete_transaction_hop(
                        hop, message.hop_executor_server
                    )
                else :
                    forward_message = ForwardMessage(
                        message_type=MessageType.FORWARD,
                        transaction_type=transaction_type,
                        target_server=ORDER_SERVER_PORT,
                        transaction_id=message.transaction_id,
                        hop_id=message.hop_id,
                        origin_server=SERVER_PORT,
                        data=hop.data,
                    )
                    # get the current hop info and update the websocket_tracker timestamp
                    hop.start_websocket_transmission()
                    # creat a task to async send message to order server, in order to not block the main thread
                    asyncio.create_task(
                        send_message(
                            forward_message, f"ws://localhost:{ORDER_SERVER_PORT}"
                        )
                    )


                # If secend hop is not completed, put second hop into priority queue
                if message.hop_id == 1:
                    transaction.hops[2].start_processing()
                    await insert_nexthop_to_priority_queue(
                        message.transaction_id, 2, client_id
                    )

                # Additional logic for processing the message goes here
            """   Message Type: FORWARD   """
            if message_type == MessageType.FORWARD:
                message.end_queue_wait()
                # print(f"=> Forward Processing info: {message.transaction_type}-hop{message.hop_id }message.")
                # print('forward_message received info 2', message.websocket_receive_time)

                websocket_receive_time = message.websocket_receive_time

                # execute_hop() includes Locking mechanism 
                result = await execute_hop(db, message.transaction_id, message.hop_id)

                
            
                # Send a backward message with completion or failure information
                backward_message = BackwardMessage(
                    message_type=MessageType.BACKWARD,
                    transaction_type=transaction_type,
                    transaction_id=message.transaction_id,
                    hop_id=message.hop_id,
                    origin_server=message.target_server,
                    target_server=message.origin_server,
                    result=result,
                    websocket_receive_time=websocket_receive_time,
                    websocket_send_time=datetime.now(),
                    queue_tracker=message.queue_tracker,
                    hop_executor_server=CURRENT_SERVER_PORT,
                )
                # creat a task to async send message to order server, in order to not block the main thread
                asyncio.create_task(
                    send_message(
                        backward_message, f"ws://localhost:{message.origin_server}"
                    )
                )
            """   Message Type: USER   """
            if message_type == MessageType.USER:
                pass
                # Can delete this part, every user message will be transformed to hop message
                # and put into priority queue by listener_handler function

            """   Message Type: HOP   """
            if message_type == MessageType.HOP:
                message.hop.end_queue_wait()
                # TODO  Locking logic
                # Forwards the message to the orders server
                # If first hop failed, reinsert the first hop into the priority queue, priority +1
                if transaction_type in [TransactionType.T3, TransactionType.T4]:
                    if message.hop.hop_id == 1:
                        result = await execute_hop(
                            db, message.hop.transaction_id, message.hop.hop_id
                        )
                        if result:
                            # complete the first hop
                            history_table.complete_transaction_hop(
                                message.hop, CURRENT_SERVER_PORT
                            )

                            # set 2nd hop start time and put into priority queue
                            history_table.transactions.get(
                                message.hop.transaction_id
                            ).hops[2].start_processing()
                            await insert_nexthop_to_priority_queue(
                                message.hop.transaction_id, 2, client_id
                            )
                        else:
                            history_table.transactions.get(
                                message.hop.transaction_id
                            ).hops[1].status = "Failed"   
                            message.priority += 1
                            await priority_queue.put((client_id, message))

                    if message.hop.hop_id == 2:

                        # send forward message to order server
                        forward_message = ForwardMessage(
                            message_type=MessageType.FORWARD,
                            transaction_type=transaction_type,
                            target_server=ORDER_SERVER_PORT,
                            transaction_id=message.hop.transaction_id,
                            hop_id=message.hop.hop_id,
                            origin_server=SERVER_PORT,
                            data=message.hop.data,
                        )
                        # update this hop's websocket_tracker timestamp
                        history_table.transactions.get(
                            message.hop.transaction_id
                        ).hops.get(message.hop.hop_id).start_websocket_transmission()

                        # creat a task to async send message to order server, in order to not block the main thread
                        asyncio.create_task(
                            send_message(
                                forward_message, f"ws://localhost:{ORDER_SERVER_PORT}"
                            )
                        )

                else: # transaction_type in [TransactionType.T1, TransactionType.T2, TransactionType.T5, TransactionType.T6, TransactionType.T7]:
                    # p rocess the current hop
                    
                    result = await execute_hop(
                        db, message.hop.transaction_id, message.hop.hop_id
                    )
                    if result:
                        history_table.complete_transaction_hop(
                            message.hop, CURRENT_SERVER_PORT
                        )
                    else:
                        history_table.transactions.get(message.hop.transaction_id).hops.get(message.hop.hop_id).status = "Failed"
                        message.priority += 1
                        await priority_queue.put((client_id, message))

                    # print(f"=> HOP Processing info: {message.transaction_type}-hop{message.hop.hop_id }")

        finally:
            priority_queue.task_done()
            # if priority_queue.qsize()>=1:
            #     print(f"handler priority_queue.qsize is {priority_queue.qsize()}")


async def execute_hop(db, transaction_id, hop_id):
    # TODO add logic handling server which own different table
    transaction = history_table.transactions.get(transaction_id)
    
    if not transaction:
        print(f"Transaction {transaction_id} not found.")
        return False
    
    transaction_type = transaction.transaction_type
    hop = transaction.hops.get(hop_id)

    if not hop : 
        print(f"Unable to execute hop: {hop_id} not found")
        return False
    elif  hop.status == "Completed":
        return True


    lock_acquired = await lock_manager.acquire_all_locks(transaction_type)
    if not lock_acquired:
        print(f"Transaction {transaction_type} failed to acquire necessary locks")
        return False

    try:
        # print(f"Lock success! Transaction {transaction_type} is executing")
        # Process the current hop
        if transaction_type == TransactionType.T1:
            result = transaction_1(
                db, hop.data["name"], hop.data["email"], hop.data["address"]
            )
            # row_count = get_row_count(db, "Customers")
        elif transaction_type == TransactionType.T2:
            result = transaction_2(
                db,
                hop.data["model_name"],
                hop.data["resolution"],
                hop.data["lens_type"],
                hop.data["price"],
            )
            # row_count = get_row_count(db, "Cameras")
        elif transaction_type == TransactionType.T3:
            if hop_id == 1:
                result = transaction_3_hop1(db, hop.data["customer_id"])
            elif hop_id == 2:
                result = transaction_3_hop2(
                    db, hop.data["customer_id"], hop.data["quantity"]
                )
        elif transaction_type == TransactionType.T4:
            if hop_id == 1:
                result = transaction_4_hop1(db, hop.data["camera_id"])
            elif hop_id == 2:
                result = transaction_4_hop2(
                    db, hop.data["camera_id"], hop.data["quantity"]
                )
        elif transaction_type == TransactionType.T5:
            result = transaction_5(db, hop.data["customer_id"])
        elif transaction_type == TransactionType.T6:
            result = transaction_6(db, hop.data["camera_id"])
        elif transaction_type == TransactionType.T7:
            result = transaction_7(db, hop.data["order_id"])
    finally:
        lock_manager.release_locks(transaction_type)
        if not result:
            print(f"Lock released! Transaction {transaction_type} is failed")
            return False
        # print(f"Lock released! Transaction {transaction_type} is completed")
        # TODO if transaction completed, send backward message to origin client
        return True



async def main():
    build_db()  # if db does not exist, create it
    db = Database("shop.db")
    # Start the server, handler and client

    server_task = websockets.serve(listener_handler, "localhost", SERVER_PORT)
    handler_task = asyncio.create_task(thread_handler(db))
    client_task = asyncio.create_task(
        send_testing_message(f"ws://localhost:{SERVER_PORT}")
    )

    # concurently run the server, handler and client
    await asyncio.gather(server_task, handler_task, client_task)


# Run the main function to start the program
asyncio.run(main())
