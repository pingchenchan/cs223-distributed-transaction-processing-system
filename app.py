import asyncio
import websockets
import random
import json
from history_table import *
from message import *
from test_realworld_sim import *
from lock_manager import *
import argparse

# The two-phase locking protocol variant for concurrency control
# where Transaction 3-4 can proceed only if Transaction 7 is not in progress, and vice versa.
lock_manager = LockManager()

history_table = HistoryTable()
# Priority queue for handling messages
priority_queue = asyncio.PriorityQueue()

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


ORDER_SERVER_PORT = None
CUSTOMER_1_SERVER_PORT = None
CUSTOMER_2_SERVER_PORT = None

SERVER_PORT = None
CURRENT_SERVER_TYPE = None


# Total numbers/time of transmition and numbers/time of waiting in quene => Check hop.queue_tracker, hop.websocket_tracker
# Hop waiting time in queue ,transmission time, total time => Analysis function is write_transaction_log in history_table.py

# TODO mute print
# PRIORITY enum in message.py, first hop: 10, second hop: 0, backward and forward : 5


async def listener_handler(websocket):
    client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    connected_clients[client_id] = websocket
    try:
        async for message in websocket:
            try:
                message_data = json.loads(message)
                await connected_clients[client_id].send('')
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
        print(f"123 Transaction {transaction_id} not found.")


# check if this hop can be done in this server; if not, send forward message to order server
def check_hop(transaction_type, hop_id, customer_id=None):
    global CURRENT_SERVER_PORT, ORDER_SERVER_PORT, CUSTOMER_1_SERVER_PORT, CUSTOMER_2_SERVER_PORT, CURRENT_SERVER_TYPE
    # Define transaction types and hops that must be executed on the order server
    order_server_hops = {
        (TransactionType.T2, 1),
        (TransactionType.T3, 2),
        (TransactionType.T4, 1),
        (TransactionType.T4, 2),
        (TransactionType.T6, 1),
        (TransactionType.T7, 1),
    }

    # Define transaction types and hops that must be executed on the customer servers
    customer_server_hops = {
        (TransactionType.T1, 1),
        (TransactionType.T3, 1),
        (TransactionType.T5, 1),
    }

    # Check for hops that must be executed on the order server
    if (transaction_type, hop_id) in order_server_hops:
        return CURRENT_SERVER_PORT == ORDER_SERVER_PORT, ORDER_SERVER_PORT

    # Check for hops that must be executed on the customer servers
    elif (transaction_type, hop_id) in customer_server_hops:
        try:
            if transaction_type == TransactionType.T1:
                if CURRENT_SERVER_PORT in [CUSTOMER_1_SERVER_PORT, CUSTOMER_2_SERVER_PORT]:
                    return True, CURRENT_SERVER_PORT
                else:
                    return False, CUSTOMER_1_SERVER_PORT

            target_server_port = (
                CUSTOMER_2_SERVER_PORT if customer_id % 2 == 0 else CUSTOMER_1_SERVER_PORT
            )
            return (
                CURRENT_SERVER_PORT in [CUSTOMER_1_SERVER_PORT, CUSTOMER_2_SERVER_PORT],
                target_server_port,
            )
        except Exception as e:
            print(f"Invalid hop: {transaction_type}-{hop_id}, customer_id: {customer_id},current_server_port: {CURRENT_SERVER_TYPE}, *** Error checking hop: {e}))")

    # Invalid hop case
    print(
        f"Invalid hop:  {transaction_type}-{hop_id}, customer_id: {customer_id},current_server_port: {CURRENT_SERVER_TYPE}"
    )
    return False, None


async def thread_handler(db) :
    global CURRENT_SERVER_TYPE, CURRENT_SERVER_PORT
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
                # print(f"=> received Backward Processing info: {message.transaction_type}-hop{message.hop_id }, success = {success }.")
                transaction = history_table.transactions.get(message.transaction_id)
                hop = transaction.hops.get(message.hop_id)
                # Process completed hop and update the history table
                if success:
                    history_table.complete_transaction_hop(
                        hop, message.hop_executor_server
                    )
                    # print(f"=> completed Backward Processing info:transaction {transaction.status}-hop {hop.status}, hop executor server: {message.hop_executor_server}, hop_id: {message.hop_id}")
                    # print(f" Start Time: {transaction.start_time}, End Time: {transaction.end_time}, Status: {transaction.status}")
                    # print(f" Hop {hop.hop_id} Start Time: {hop.start_time}, End Time: {hop.end_time}, Status: {hop.status}, ")
                else:
                    # If failed, resend forward message to origin server
                    forward_message = ForwardMessage(
                        message_type=MessageType.FORWARD,
                        transaction_type=transaction_type,
                        target_server=ORDER_SERVER_PORT,
                        transaction_id=message.transaction_id,
                        hop_id=message.hop_id,
                        origin_server=CURRENT_SERVER_PORT,
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
                if message.hop_id == 1 and transaction_type in [transaction_type.T3, transaction_type.T4]:
                    transaction.hops[2].start_processing()
                    # chech if this hop can be done in this server; if not, send forward message to order server
                    is_current_server, target_server = check_hop(
                        transaction_type, 2, hop.data["customer_id"]
                    )
                    if is_current_server: # if this hop can be done in this server
                        await insert_nexthop_to_priority_queue(
                            message.transaction_id, 2, client_id
                        )
                    else: # if this hop cannot be done in this server, send forward message to target server
                        forward_message = ForwardMessage(
                            message_type=MessageType.FORWARD,
                            transaction_type=transaction_type,
                            target_server=message.origin_server,
                            transaction_id=message.transaction_id,
                            hop_id=2,
                            origin_server= CURRENT_SERVER_PORT,
                            data=hop.data,
                        )
                        # get the current hop info and update the websocket_tracker timestamp
                        transaction.hops[2].start_websocket_transmission()
                        # creat a task to async send message to order server, in order to not block the main thread
                        asyncio.create_task(
                            send_message(
                                forward_message, f"ws://localhost:{target_server}"
                            )
                        )

                # Additional logic for processing the message goes here
            """   Message Type: FORWARD   """
            if message_type == MessageType.FORWARD:
                message.end_queue_wait()
                # print(f"=> Forward Processing info: {message.transaction_type}-hop{message.hop_id }message.")
                # print('forward_message received info 2', message.websocket_receive_time)

                websocket_receive_time = message.websocket_receive_time

                # execute_hop() includes Locking mechanism
                # '''for testing'''
                # if message.transaction_id not in history_table.transactions:
                #     print(f"000 Transaction {message.transaction_id} not found.")
                #     return 

                # ''' '''
                result = await execute_hop(db, message.transaction_id, message.hop_id, message.transaction_type, message.data)

                # Send a backward message with completion or failure information
                backward_message = BackwardMessage(
                    message_type=MessageType.BACKWARD,
                    transaction_type=transaction_type,
                    transaction_id=message.transaction_id,
                    hop_id=message.hop_id,
                    origin_server=CURRENT_SERVER_PORT,
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
                # print(f"=> send backward_message  Processing info: {message.transaction_type}-hop{message.hop_id }message.{message.origin_server}")
            """   Message Type: USER   """
            if message_type == MessageType.USER:
                pass
                # Can delete this part, every user message will be transformed to hop message
                # and put into priority queue by listener_handler function

            """   Message Type: HOP   """
            # 假设 server_ports 是一个包含服务器端口信息的字典
            server_ports = {
                'current_server': CURRENT_SERVER_PORT,
                'order_server': ORDER_SERVER_PORT,
                'customer_1_server': CUSTOMER_1_SERVER_PORT,
                'customer_2_server': CUSTOMER_2_SERVER_PORT
            }

            """ Message Type: HOP """
            if message_type == MessageType.HOP:
                message.hop.end_queue_wait()

                # print('message.hop.data.get("customer_id")', message.hop.data.get("customer_id"),message.hop.data)
                # use check_hop() to check if this hop can be done in this server; if not, send forward message to order server
                can_execute_here, target_server_port = check_hop(
                    transaction_type,
                    message.hop.hop_id,
                    message.hop.data.get("customer_id"),  
                )

                # if this hop can be done in this server
                if can_execute_here:
                    # '''for testing'''
                    # if message.hop.transaction_id not in history_table.transactions:
                    #     print(f"000 Transaction {message.hop.transaction_id} not found.")
                    #     return 

                    # ''' '''
                    result = await execute_hop(
                        db, message.hop.transaction_id, message.hop.hop_id
                    )

                    if result:
                        history_table.complete_transaction_hop(
                            message.hop, server_ports['current_server']
                        )

                        # if first hop is completed, put second hop into priority queue
                        if message.hop.hop_id == 1 and transaction_type in [TransactionType.T3, TransactionType.T4]:
                            history_table.transactions.get(
                                message.hop.transaction_id
                            ).hops[2].start_processing()
                            await insert_nexthop_to_priority_queue(
                                message.hop.transaction_id, 2, client_id
                            )
                    else:
                        # if failed, reinsert the hop into the priority queue, priority +1
                        history_table.transactions.get(
                            message.hop.transaction_id
                        ).hops.get(message.hop.hop_id).status = "Failed"
                        message.priority += 1
                        await priority_queue.put((client_id, message))
                else:
                    # if this hop cannot be done in this server, send forward message to target server
                    if target_server_port:
                        forward_message = ForwardMessage(
                            message_type=MessageType.FORWARD,
                            transaction_type=transaction_type,
                            target_server=target_server_port,
                            transaction_id=message.hop.transaction_id,
                            hop_id=message.hop.hop_id,
                            origin_server=server_ports['current_server'],
                            data=message.hop.data,
                        )
                        history_table.transactions.get(
                            message.hop.transaction_id
                        ).hops.get(message.hop.hop_id).start_websocket_transmission()

                        asyncio.create_task(
                            send_message(
                                forward_message, f"ws://localhost:{target_server_port}"
                            )
                        )

        finally:
            priority_queue.task_done()

async def execute_hop(db, transaction_id, hop_id, transaction_type=None, data=None): # last two parameters are for ForwardMessage
    transaction = history_table.transactions.get(transaction_id)
    isForwardMessage = False
    if not transaction: # is ForwardMessage, only need to execute hop
        isForwardMessage = True
    else: # is HopMessage, need to check if hop is completed
        transaction_type = transaction.transaction_type
        hop = transaction.hops.get(hop_id)

        if not hop:
            print(f"Unable to execute hop: {hop_id} not found")
            return False
        elif hop.status == "Completed":
            return True

    lock_acquired = await lock_manager.acquire_all_locks(transaction_type)
    if not lock_acquired:
        print(f"Transaction {transaction_type} failed to acquire necessary locks")
        return False
    
    parameters = {
        "name": data.get("name") if isForwardMessage else hop.data.get("name"),
        "email": data.get("email") if isForwardMessage else hop.data.get("email"),
        "address": data.get("address") if isForwardMessage else hop.data.get("address"),
        "model_name": data.get("model_name") if isForwardMessage else hop.data.get("model_name"),
        "resolution": data.get("resolution") if isForwardMessage else hop.data.get("resolution"),
        "lens_type": data.get("lens_type") if isForwardMessage else hop.data.get("lens_type"),
        "price": data.get("price") if isForwardMessage else hop.data.get("price"),
        "customer_id": data.get("customer_id") if isForwardMessage else hop.data.get("customer_id"),
        "camera_id": data.get("camera_id") if isForwardMessage else hop.data.get("camera_id"),
        "quantity": data.get("quantity") if isForwardMessage else hop.data.get("quantity"),
        "order_id": data.get("order_id") if isForwardMessage else hop.data.get("order_id"),
    }

    try:
        # print(f"Lock success! Transaction {transaction_type} is executing")
        # Process the current hop
        result = False
        if transaction_type == TransactionType.T1:
            result = transaction_1(
                db, parameters["name"], parameters["email"], parameters["address"]
            )
            # print("SQL info: T1 successed, customer_count", get_row_count(db, "Customers"))
            # row_count = get_row_count(db, "Customers")
        elif transaction_type == TransactionType.T2:
            result = transaction_2(
                db,
                parameters["model_name"],
                parameters["resolution"],
                parameters["lens_type"],
                parameters["price"],
            )
            if not result:
                print("SQL info: T2 failed")
            # row_count = get_row_count(db, "Cameras")
        elif transaction_type == TransactionType.T3:
            if hop_id == 1:
                result = transaction_3_hop1(db, parameters["customer_id"])
            elif hop_id == 2:
                result = transaction_3_hop2(
                    db, parameters["customer_id"], parameters["quantity"]
                )
        elif transaction_type == TransactionType.T4:
            if hop_id == 1:
                result = transaction_4_hop1(db, parameters["camera_id"])
            elif hop_id == 2:
                result = transaction_4_hop2(
                    db, parameters["camera_id"], parameters["quantity"]
                )
        elif transaction_type == TransactionType.T5:
            result = transaction_5(db, parameters["customer_id"])
        elif transaction_type == TransactionType.T6:
            result = transaction_6(db, parameters["camera_id"])
        elif transaction_type == TransactionType.T7:
            result = transaction_7(db, parameters["order_id"])
    finally:
        lock_manager.release_locks(transaction_type)
        if not result:
            print(f"Lock released! Transaction {transaction_type} is failed")
            return False
        # print(f"Lock released! Transaction {transaction_type} is completed")
        # TODO if transaction completed, send backward message to origin client
        return True


async def main(port, server_type):
    with open("config.json", "r") as config_file:
        config = json.load(config_file)

    global CURRENT_SERVER_PORT, CURRENT_SERVER_TYPE, ORDER_SERVER_PORT, CUSTOMER_1_SERVER_PORT, CUSTOMER_2_SERVER_PORT
    ORDER_SERVER_PORT = config["order_server"].get("port")
    CUSTOMER_1_SERVER_PORT = config["client1_server"].get("port")
    CUSTOMER_2_SERVER_PORT = config["client2_server"].get("port")
    
    server_map= {ORDER_SERVER_PORT:0, CUSTOMER_1_SERVER_PORT:1, CUSTOMER_2_SERVER_PORT:2}

    print(f"Starting {server_type} server on port {port}")
    
    CURRENT_SERVER_PORT, CURRENT_SERVER_TYPE = port, server_type
    history_table.current_server_type = CURRENT_SERVER_TYPE
    await build_db(CURRENT_SERVER_TYPE)  # if db does not exist, create it
    db = Database(f"./database/{CURRENT_SERVER_TYPE}.db")
    # transaction_1(db, 'name', 'email', 'address')
    # transaction_1(db, 'name', 'email', 'address')
    # transaction_1(db, 'name', 'email', 'address')
    # transaction_2(db, 'name', 'email', 'address', 1)
    # transaction_2(db, 'name', 'email', 'address', 1)
    # transaction_2(db, 'name', 'email', 'address', 1)

    assert CURRENT_SERVER_PORT != None and CURRENT_SERVER_TYPE != None

    server_task = websockets.serve(listener_handler, "localhost", CURRENT_SERVER_PORT)
    handler_task = asyncio.create_task(
        thread_handler(db)
    )
    client_task = asyncio.create_task(
        send_testing_message(f"ws://localhost:{CURRENT_SERVER_PORT}", server_map[CURRENT_SERVER_PORT])
    )

    # concurently run the server, handler and client
    await asyncio.gather(server_task, handler_task, client_task)


# Run the main function to start the program
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the server application.")
    parser.add_argument("port", type=int, help="Port number to run the server on.")
    parser.add_argument(
        "server_type", choices=["w", "c1", "c2"], help="Type of the server [w, c1, c2]."
    )

    args = parser.parse_args()
    asyncio.run(main(args.port, args.server_type))
