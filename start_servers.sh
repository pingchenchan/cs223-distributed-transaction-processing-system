#!/bin/bash

mkdir -p ./database
mkdir -p ./log

# read config.json to get port numbers
ORDER_SERVER_PORT=$(jq '.order_server.port' config.json)
CLIENT1_SERVER_PORT=$(jq '.client1_server.port' config.json)
CLIENT2_SERVER_PORT=$(jq '.client2_server.port' config.json)

# store port numbers in an array
PORTS=($ORDER_SERVER_PORT $CLIENT1_SERVER_PORT $CLIENT2_SERVER_PORT)

# make sure no process is running on the ports
for PORT in "${PORTS[@]}"
do
    PID=$(lsof -ti tcp:"$PORT")
    if [ ! -z "$PID" ]; then
        echo "Killing process on port $PORT"
        kill -9 "$PID"
    fi
done

# run the servers
python3 app.py $ORDER_SERVER_PORT "w" &
python3 app.py $CLIENT1_SERVER_PORT "c1" &
python3 app.py $CLIENT2_SERVER_PORT "c2" &
