import asyncio
import websockets
import random
import json
from history_table import *

# Priority queue for handling messages
priority_queue = asyncio.PriorityQueue()
# Maximum number of retries for failed messages
MAX_RETRIES = 3
 # Dictionary to keep track of connected clients
connected_clients = {}  


async def listener_handler(websocket, path):
    client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
    connected_clients[client_id] = websocket
    try:
        async for message in websocket:
            print(f"Received from {client_id}: '{message}'")
            await priority_queue.put((1, {'client_id': client_id, 'message': message}))
    finally:
        del connected_clients[client_id]


async def thread_handler():
    while True:
        # Wait until an item is available, asynchronously
        _, item = await priority_queue.get()
        message = json.loads(item['message'])
        client_id = item['client_id']

        message_type = message.get('type')
        print(f"priority_queue Processing item: '{message}'")
        if message_type == 'backward':
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

        elif message_type == 'forward':
            print("Processing forward message.")
            # Process the current hop
            # Send a backward message with completion or failure information

        else:  # Handle new or in-progress transactions
            
            print("Processing transaction.")
            # Transaction processing logic goes here
            if client_id in connected_clients:
                    response = json.dumps({'status': 'processed', 'data': "Handling successful backward message."})
                    print("Sent to client.")
                    await connected_clients[client_id].send(response)


        priority_queue.task_done()

async def send_receive_message(uri):
    for i in range(20):
        sleep_for = random.uniform(1.0, 3.0)
        await asyncio.sleep(sleep_for)
        
        message = json.dumps({
            'type': 'message',  # or 'backward', 'forward' based on your requirements
            'data': f'This is message {i}',
            'success': random.choice([True, False]),  # Example field
            'transaction_id': f'transaction_{i}',  # Example field
        })

        async with websockets.connect(uri) as websocket:
            await websocket.send(message)
            print(f"Sent: '{i}'")
            reply = await websocket.recv()
            print(f"The reply is: '{reply}'")

async def main():
    build_db() #if db does not exist, create it

    server_task = websockets.serve(listener_handler, 'localhost', 8898)
    handler_task = asyncio.create_task(thread_handler())
    client_task = asyncio.create_task(send_receive_message('ws://localhost:8898'))


    #concurently run the server, handler and client
    await asyncio.gather(server_task, handler_task, client_task)
    

# Run the main function to start the program
asyncio.run(main())


