# CompSci223-project
### Introduction
This project is an implementation of a distributed transaction processing system using the techniques used in Yang Zhang et al. “Transaction chains: achieving serializability with low latency in geo-distributed storage systems”. The system is designed to ensure low transactional latency and fault tolerance, and to implement effective data partitioning strategies across geographic locations to improve data integrity and accessibility. This project utilize of transaction chopping and the analysis of SC-Cycles (Serializability and Commutativity Cycles)

### install library
```shell
pip install -r requirements.txt
brew install jq #for .sh file read config.json
```

### run server
```shell
./start_servers.sh
```

### CONFIG
- Please check config.json.


### Error handle
- If you encounter the following error in a Unix-like environment::

```
OSError: [Errno 48] error while attempting to bind on address ('127.0.0.1', 8898): address already in use
```
You can resolve this issue by following these steps:
```
lsof -i :8898
kill <PID>
```
