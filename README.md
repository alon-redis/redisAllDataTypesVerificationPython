install the following:
```
apt install python3-pip -y
pip3 install redis
```
RUN:
```
root@host1:~# python3 ./int.py -h
usage: int.py [-h] [--host HOST] [--port PORT] [--ssl] [--protocol {2,3}] [--cluster] [--verify-only]

Redis Data Type Tester

optional arguments:
  -h, --help        show this help message and exit
  --host HOST       Redis host
  --port PORT       Redis port
  --ssl             Use SSL/TLS connection
  --protocol {2,3}  RESP protocol version (2 or 3)
  --cluster         Use cluster mode
  --verify-only     Run only verification using existing reference data
```
