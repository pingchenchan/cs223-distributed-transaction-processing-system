# CompSci223-project

### install library
```shell
pip install -r requirements.txt
```

### run server
```shell
python3 app.py
```



### TODO


### Questiom
- After sending forward message, should we await reply messaage or not? (I consider not.)


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
