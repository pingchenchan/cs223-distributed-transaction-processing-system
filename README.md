# CompSci223-project

### install library


```shell
pip install -r requirements.txt
brew install jq #for .sh file read config.json
```

### run server
```shell
./start_servers.sh
```

### IDEA
- Study how to choose the best priority for each hop type.

### CONFIG
- Please check config.json.



### Question


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
